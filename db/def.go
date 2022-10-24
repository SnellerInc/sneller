// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path"
	"strings"

	"github.com/SnellerInc/sneller/fsutil"
	"golang.org/x/exp/slices"
)

// Input is one input pattern
// belonging to a Definition.
type Input struct {
	// Pattern is the glob pattern that
	// specifies which files are fed into
	// the table. Patterns should be URIs
	// where the URI scheme (i.e. s3://, file://, etc.)
	// indicates where the data ought to come from.
	Pattern string `json:"pattern"`
	// Format is the format of the files in pattern.
	// If Format is the empty string, then the format
	// will be inferred from the file extension.
	Format string `json:"format,omitempty"`
	// Hints, if non-nil, is the hints associated
	// with the input data. The hints may perform
	// type-based coercion of certain paths, and may additionally
	// eliminate some of the data as it is parsed.
	// Hints data is format-specific.
	Hints json.RawMessage `json:"hints,omitempty"`
}

// Equal returns whether i and other are
// equivalent.
func (i Input) Equal(other Input) bool {
	return i.Pattern == other.Pattern &&
		i.Format == other.Format &&
		string(i.Hints) == string(other.Hints)
}

// TableDefinition describes the set of input files
// that belong to a table.
type TableDefinition struct {
	// Name is the name of the table
	// that will be produced from this Definition.
	// Name should match the location of the Definition
	// within the db filesystem hierarchy.
	Name string `json:"name"`
	// Inputs is the list of inputs that comprise the table.
	Inputs []Input `json:"input,omitempty"`
	// Features is a list of feature flags that
	// can be used to turn on features for beta-testing.
	Features []string `json:"beta_features,omitempty"`
}

// just pick an upper limit to prevent DoS
const maxDefSize = 1024 * 1024

func checkDef(f fs.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maxDefSize {
		return fmt.Errorf("definition of size %d beyond limit %d", info.Size(), maxDefSize)
	}
	return nil
}

// Equal returns whether d and other are
// equivalent. Equalivalent definitions marshal
// to equivalent JSON and have the same hash.
func (d *TableDefinition) Equal(other *TableDefinition) bool {
	if d == nil || other == nil {
		return d == nil && other == nil
	}
	return d.Name == other.Name &&
		slices.EqualFunc(d.Inputs, other.Inputs, (Input).Equal) &&
		slices.Equal(d.Features, other.Features)
}

// Hash returns a hash of the table definition
// that can be used to detect changes.
func (d *TableDefinition) Hash() []byte {
	hash := sha256.New()
	err := json.NewEncoder(hash).Encode(d)
	if err != nil {
		panic("db: failed to hash definition: " + err.Error())
	}
	return hash.Sum(nil)
}

// Definition describes a database and the
// tables therein.
type Definition struct {
	// Name is the name of the database.
	Name string `json:"name"`
	// Tables is the list of table definitions
	// stored in the root-level definition.
	Tables []*TableDefinition `json:"tables,omitempty"`
}

// DecodeDefinition decodes a root-level
// definition from src.
//
// See also: OpenDefinition
func DecodeDefinition(src io.Reader) (*Definition, error) {
	s := new(Definition)
	err := json.NewDecoder(src).Decode(s)
	return s, err
}

// OpenDefinition opens a root-level definition
// for the given database.
//
// OpenDefinition calls DecodeDefinition on
// definition.json at the appropriate path for
// the given db.
func OpenDefinition(s fs.FS, db string) (*Definition, error) {
	f, err := s.Open(DefinitionPath(db))
	if errors.Is(err, fs.ErrNotExist) {
		return synthDefinition(s, db)
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := checkDef(f); err != nil {
		return nil, err
	}
	d, err := DecodeDefinition(f)
	if err != nil {
		return nil, err
	}
	if d.Name != db {
		return nil, fmt.Errorf("definition name %q doesn't match %q", d.Name, db)
	}
	return d, nil
}

// synthDefinition attempts to synthesize a
// definition from per-table definition files in
// the given file system. This functionality is
// only a temporary stopgap to deal with
// databases that might not have been migrated
// and is expected to go away at some point.
func synthDefinition(s fs.FS, db string) (*Definition, error) {
	log.Printf("synthesizing definition for database %q", db)
	open := func(db, table string) (*TableDefinition, error) {
		f, err := s.Open(TableDefinitionPath(db, table))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		d := new(TableDefinition)
		err = json.NewDecoder(f).Decode(d)
		if err != nil {
			return nil, err
		}
		if d.Name != table {
			return nil, fmt.Errorf("table definition name %q doesn't match %q", d.Name, table)
		}
		// root definitions will eventually support
		// expanding table name templates so escape
		// '$' in table names
		d.Name = strings.ReplaceAll(d.Name, "$", "$$")
		return d, nil
	}
	tables, err := fs.ReadDir(s, path.Join("db", db))
	if err != nil {
		return nil, err
	}
	root := &Definition{Name: db}
	for i := range tables {
		def, err := open(db, tables[i].Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		root.Tables = append(root.Tables, def)
	}
	if len(root.Tables) == 0 {
		return nil, fs.ErrNotExist
	}
	slices.SortFunc(root.Tables, func(a, b *TableDefinition) bool {
		return a.Name < b.Name
	})
	// try and write out the root definition file
	if ofs, ok := s.(OutputFS); ok {
		if err := WriteDefinition(ofs, root); err != nil {
			log.Printf("failed to write definition for %q: %v", db, err)
		} else {
			log.Printf("successfully wrote definition for %q", db)
		}
	} else {
		log.Printf("not writing definition for %q: file system is read-only", db)
	}
	return root, nil
}

// WriteDefinition writes a root-level
// definition.
func WriteDefinition(dst OutputFS, s *Definition) error {
	if s.Name == "" {
		return fmt.Errorf("cannot write definition with no Name")
	}
	buf, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		return err
	}
	_, err = dst.WriteFile(DefinitionPath(s.Name), buf)
	return err
}

// Get looks for a table definition with the
// given name, or nil if not found.
func (d *Definition) Get(name string) *TableDefinition {
	for i := range d.Tables {
		if d.Tables[i].Name == name {
			return d.Tables[i]
		}
	}
	return nil
}

// Equal returns whether d and other are
// equivalent. Equalivalent root definitions
// marshal to equivalent JSON.
func (d *Definition) Equal(other *Definition) bool {
	if d == nil || other == nil {
		return d == nil && other == nil
	}
	return d.Name == other.Name &&
		slices.EqualFunc(d.Tables, other.Tables, (*TableDefinition).Equal)
}

// Expand produces expanded table definitions
// for each table in this definition.
//
// If tblpat != "", only tables matching the
// pattern will be included in the output.
//
// If any two table definitions produce a table
// with the same name, this returns an error.
func (d *Definition) Expand(who Tenant, tblpat string) ([]*TableDefinition, error) {
	if tblpat == "" {
		tblpat = "*"
	} else if _, err := path.Match(tblpat, ""); err != nil {
		// syntax check
		return nil, err
	}
	ignore := func(s string) bool {
		match, _ := path.Match(tblpat, s)
		return !match
	}
	// skipdir returns fs.SkipDir if the pattern
	// indicates that the directory can be skipped
	// after matching the first file (which is
	// only true if the last segment of the
	// pattern does not contain a capture group)
	// or nil otherwise.
	skipdir := func(pattern string) error {
		_, seg := path.Split(pattern)
		if seg == "" {
			return nil
		}
		has, ok := hascapture(seg)
		if !ok || has {
			return nil
		}
		return fs.SkipDir
	}
	var out []*TableDefinition
	type bookkeeping struct {
		out int // index into out (-1 => ignored)
		tbl int // index into d.Tables
		in  int // index into d.Tables[tbl].Input
	}
	seen := make(map[string]bookkeeping)
	var mr matcher
	for i := range d.Tables {
		// if the table name is not a template,
		// avoid walking its inputs entirely
		name, ok, err := detemplate(d.Tables[i].Name)
		if err != nil {
			return nil, err
		}
		if ok {
			// name is not a template
			if _, seen := seen[name]; seen {
				return nil, fmt.Errorf("duplicate table %q", name)
			}
			if ignore(name) {
				seen[name] = bookkeeping{out: -1, tbl: i}
				continue
			}
			def := d.Tables[i]
			if name != def.Name {
				// table name had "$$", make copy
				def = &TableDefinition{
					Name:     name,
					Inputs:   def.Inputs,
					Features: def.Features,
				}
			}
			seen[name] = bookkeeping{out: len(out), tbl: i}
			out = append(out, def)
			continue
		}
		// the table name is a template, so we have
		// to walk the inputs
		for j := range d.Tables[i].Inputs {
			in := &d.Tables[i].Inputs[j]
			ifs, pat, err := who.Split(in.Pattern)
			if err != nil {
				return nil, fmt.Errorf("%w: %q", err, in.Pattern)
			}
			prefix := ifs.Prefix()
			template := d.Tables[i].Name
			walk := func(name string, f fs.File, err error) error {
				if err != nil {
					return err
				}
				err = mr.match(pat, name, template)
				if err != nil || !mr.found {
					return err
				}
				// check if we have seen this table
				if bk, ok := seen[string(mr.result)]; ok {
					if bk.tbl != i {
						return fmt.Errorf("duplicate table %q", mr.result)
					}
					if bk.out == -1 || bk.in == j {
						// ignored or already added input
						return nil
					}
					def := out[bk.out]
					def.Inputs = append(def.Inputs, Input{
						Pattern: prefix + string(mr.glob),
						Format:  in.Format,
						Hints:   in.Hints,
					})
					bk.in = j
					seen[def.Name] = bk
					return skipdir(pat)
				}
				// first time seeing this table
				table := string(mr.result)
				if ignore(table) {
					seen[table] = bookkeeping{out: -1, tbl: i}
					return skipdir(pat)
				}
				seen[table] = bookkeeping{out: len(out), tbl: i, in: j}
				out = append(out, &TableDefinition{
					Name: table,
					Inputs: []Input{{
						Pattern: prefix + string(mr.glob),
						Format:  in.Format,
						Hints:   in.Hints,
					}},
					Features: d.Tables[i].Features,
				})
				return skipdir(pat)
			}
			glob, err := toglob(pat)
			if err != nil {
				return nil, fmt.Errorf("%w: %q", err, pat)
			}
			err = fsutil.WalkGlob(ifs, "", glob, walk)
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// A Resolver determines how input specifications
// are turned into input filesystems.
type Resolver interface {
	// Split should trim the prefix off of pattern
	// that specifies the source filesystem and return
	// the result as an InputFS and the trailing glob
	// pattern that can be applied to the input to yield
	// the results.
	Split(pattern string) (InputFS, string, error)
}

var (
	// ErrBadPattern should be returned by Resolver.Split
	// when it encounters an invalid pattern.
	ErrBadPattern = errors.New("bad pattern")
)

func badPattern(pat string) error {
	return fmt.Errorf("%q: %w", pat, ErrBadPattern)
}
