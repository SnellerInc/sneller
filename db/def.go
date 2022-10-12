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

// Definition describes the set of input files
// that belong to a table.
type Definition struct {
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
	// Generated is true if this table definition
	// was generated from a root-level database
	// definition. Do not set this manually.
	Generated bool `json:"generated,omitempty"`
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

// DecodeDefinition decodes a table definition
// from src.
//
// See also: OpenDefinition
func DecodeDefinition(src io.Reader) (*Definition, error) {
	s := new(Definition)
	err := json.NewDecoder(src).Decode(s)
	return s, err
}

// OpenDefinition opens a definition for
// the given database and table.
//
// OpenDefinition calls DecodeDefinition on
// definition.json in the appropriate path
// for the given db and table.
func OpenDefinition(s fs.FS, db, table string) (*Definition, error) {
	f, err := s.Open(DefinitionPath(db, table))
	if err != nil {
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
	if d.Name != table {
		return nil, fmt.Errorf("definition name %q doesn't match %q", d.Name, table)
	}
	return d, nil
}

// WriteDefinition writes a definition to the given database.
func WriteDefinition(dst OutputFS, db string, s *Definition) error {
	if s.Name == "" {
		return fmt.Errorf("cannot write definition with no Name")
	}
	buf, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = dst.WriteFile(DefinitionPath(db, s.Name), buf)
	return err
}

// Equal returns whether d and other are
// equivalent. Equalivalent definitions marshal
// to equivalent JSON and have the same hash.
func (d *Definition) Equal(other *Definition) bool {
	if d == nil || other == nil {
		return d == nil && other == nil
	}
	return d.Name == other.Name &&
		slices.EqualFunc(d.Inputs, other.Inputs, (Input).Equal) &&
		slices.Equal(d.Features, other.Features) &&
		d.Generated == other.Generated
}

// Hash returns a hash of the table definition
// that can be used to detect changes.
func (d *Definition) Hash() []byte {
	hash := sha256.New()
	err := json.NewEncoder(hash).Encode(d)
	if err != nil {
		panic("db: failed to hash definition: " + err.Error())
	}
	sum := hash.Sum(nil)
	return sum[:]
}

// RootDefinition describes a database and the
// tables therein.
type RootDefinition struct {
	// Name is the name of the database.
	Name string `json:"name"`
	// Tables is the list of table definitions
	// stored in the root-level definition.
	Tables []*Definition `json:"tables,omitempty"`
}

// DecodeRootDefinition decodes a root-level
// definition from src.
//
// See also: OpenRootDefinition
func DecodeRootDefinition(src io.Reader) (*RootDefinition, error) {
	s := new(RootDefinition)
	err := json.NewDecoder(src).Decode(s)
	return s, err
}

// OpenRootDefinition opens a root-level
// definition for the given database.
//
// OpenRootDefinition calls DecodeRootDefinition
// on definition.json at the appropriate path
// for the given db.
func OpenRootDefinition(s fs.FS, db string) (*RootDefinition, error) {
	f, err := s.Open(RootDefinitionPath(db))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := checkDef(f); err != nil {
		return nil, err
	}
	d, err := DecodeRootDefinition(f)
	if err != nil {
		return nil, err
	}
	if d.Name != db {
		return nil, fmt.Errorf("definition name %q doesn't match %q", d.Name, db)
	}
	return d, nil
}

// WriteRootDefinition writes a root-level
// definition.
func WriteRootDefinition(dst OutputFS, s *RootDefinition) error {
	if s.Name == "" {
		return fmt.Errorf("cannot write definition with no Name")
	}
	buf, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		return err
	}
	_, err = dst.WriteFile(RootDefinitionPath(s.Name), buf)
	return err
}

// Equal returns whether d and other are
// equivalent. Equalivalent root definitions
// marshal to equivalent JSON.
func (d *RootDefinition) Equal(other *RootDefinition) bool {
	if d == nil || other == nil {
		return d == nil && other == nil
	}
	return d.Name == other.Name &&
		slices.EqualFunc(d.Tables, other.Tables, (*Definition).Equal)
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
