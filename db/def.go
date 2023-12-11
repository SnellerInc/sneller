// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package db

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"reflect"

	"github.com/SnellerInc/sneller/date"
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

// RetentionPolicy describes a policy for retaining data.
//
// For a given field and validity window, the retention
// policy only retains data that satisfies the relation
//
//	field >= (now - valid_for)
type RetentionPolicy struct {
	// Field is the path expression for the field
	// used to determine the age of a record for
	// the purpose of the data retention policy.
	//
	// Currently only timestamp fields are
	// supported.
	Field string `json:"field,omitempty"`
	// ValidFor is the validity window relative to
	// now.
	//
	// This is a string with a format like
	// "<n>y<n>m<n>d" where "<n>" is a number and
	// any component can be omitted.
	//
	// For example: "6m", "1000d", "1y6m15d"
	ValidFor date.Duration `json:"valid_for"`
}

// A Partition defines a synthetic field that is
// generated from parts of an input URI and used
// to partition table data.
type Partition struct {
	// Field is the name of the partition field. If
	// this field conflicts with a field in the
	// input data, the partition field will
	// override it.
	Field string `json:"field"`
	// Type is the type of the partition field.
	// If this is "", this defaults to "string".
	Type string `json:"type,omitempty"`
	// Value is a template string that is used to
	// produce the value for the partition field.
	// The template may reference parts of the
	// input URI specified in the input pattern.
	// If this is "", the field name is used to
	// determine the input URI part that will be
	// used to determine the value.
	Value string `json:"value,omitempty"`
}

// Definition describes the set of input files
// that belong to a table.
type Definition struct {
	// Inputs is the list of inputs that comprise the table.
	Inputs []Input `json:"input,omitempty"`
	// Partitions specifies synthetic fields that
	// are generated from components of the input
	// URI and used to partition table data.
	Partitions []Partition `json:"partitions,omitempty"`
	// Retention is the expiration policy for data.
	// Data older than the expiration window will
	// be periodically purged from the backing
	// store during table updates.
	Retention *RetentionPolicy `json:"retention_policy,omitempty"`
	// Features is a list of feature flags that
	// can be used to turn on features for beta-testing.
	Features []string `json:"beta_features,omitempty"`
	// SkipBackfill, if true, will cause this table
	// to skip scanning the source bucket(s) for matching
	// objects when the first objects are inserted into the table.
	SkipBackfill bool `json:"skip_backfill,omitempty"`
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

// Hash returns a hash of the table definition
// that can be used to detect changes.
func (d *Definition) Hash() []byte {
	hash := sha256.New()
	err := json.NewEncoder(hash).Encode(d)
	if err != nil {
		panic("db: failed to hash definition: " + err.Error())
	}
	return hash.Sum(nil)
}

// Equals returns whether or not the table
// definitions are equivalent.
func (d *Definition) Equals(x *Definition) bool {
	return reflect.DeepEqual(d, x)
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
	return d, nil
}

// WriteDefinition writes a definition to the given database.
func WriteDefinition(dst OutputFS, db, table string, s *Definition) error {
	buf, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		return err
	}
	_, err = dst.WriteFile(DefinitionPath(db, table), buf)
	return err
}

func badPattern(pat string) error {
	return fmt.Errorf("%q: %w", pat, fsutil.ErrBadPattern)
}
