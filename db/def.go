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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/SnellerInc/sneller/fsutil"
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

// Definition describes the set of input files
// that belong to a table.
type Definition struct {
	// Name is the name of the table
	// that will be produced from this Definition.
	// Name should match the location of the Definition
	// within the db filesystem hierarchy.
	Name string `json:"name"`
	// Inputs is the list of inputs that comprise the table.
	Inputs []Input `json:"input"`
}

func drop(lst []fsutil.NamedFile) {
	for i := range lst {
		lst[i].Close()
	}
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

// DecodeDefinition decodes a definition from src
// using suffix as the hint for the format
// of the data in src.
// (You may pass the result of {file}path.Ext
// directly as suffix if you are reading from
// an os.File or fs.File.)
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
// definition.json in the appropriate  path
// for the given db and table.
func OpenDefinition(s fs.FS, db, table string) (*Definition, error) {
	f, err := s.Open(DefinitionPattern(db, table))
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
	_, err = dst.WriteFile(DefinitionPattern(db, s.Name), buf)
	return err
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
