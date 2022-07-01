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

// Package db implements
// the policy layout of databases,
// tables, and indices as a virtual
// filesystem tree.
//
package db

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// IndexPath returns the path
// at which the index for the given
// db and table would live relative
// to the root of the FS.
func IndexPath(db, table string) string {
	return path.Join("db", db, table, "index")
}

// DefinitionPattern returns the pattern
// for a path at which the definition for
// the given db and table would live relative
// to the root of the FS.
func DefinitionPattern(db, table string) string {
	return path.Join("db", db, table, "definition.json")
}

func strpart(p string, num int) (string, bool) {
	for num > 0 {
		s := strings.IndexByte(p, '/')
		if s < 0 || s == len(p)-1 {
			return "", false
		}
		p = p[s+1:]
		num--
	}
	s := strings.IndexByte(p, '/')
	if s <= 0 {
		return "", false
	}
	return p[:s], true
}

func ListComponent(s fs.FS, pattern string, part int) ([]string, error) {
	seen := make(map[string]struct{})
	walk := func(p string, f fs.File, err error) error {
		if err != nil {
			return err
		}
		f.Close()
		str, ok := strpart(p, part)
		if ok {
			seen[str] = struct{}{}
		}
		return nil
	}
	err := fsutil.WalkGlob(s, "", pattern, walk)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out, nil
}

// List returns the list of databases
// within a shared filesystem.
func List(s fs.FS) ([]string, error) {
	return ListComponent(s, IndexPath("*", "*"), 1)
}

// Tables returns the list of tables
// within a database within a shared filesystem.
func Tables(s fs.FS, db string) ([]string, error) {
	return ListComponent(s, IndexPath(db, "*"), 2)
}

// MaxIndexSize is the maximum size of an
// index object. (The purpose of an index size cap
// is to prevent us from reading arbitrarily-sized
// index objects before we have authenticated the
// objects.)
const MaxIndexSize = 15 * 1024 * 1024

// OpenIndex opens an index for the specific table and database.
// The key must correspond to the key used to sign the index
// when it was first inserted into the index.
func OpenIndex(s fs.FS, db, table string, key *blockfmt.Key) (*blockfmt.Index, error) {
	return openIndex(s, db, table, key, 0)
}

// OpenPartialIndex is equivalent to OpenIndex, but
// skips decoding Index.Inputs. The returned
// index is suitable for queries, but not for
// synchronizing tables.
func OpenPartialIndex(s fs.FS, db, table string, key *blockfmt.Key) (*blockfmt.Index, error) {
	return openIndex(s, db, table, key, blockfmt.FlagSkipInputs)
}

func openIndex(s fs.FS, db, table string, key *blockfmt.Key, opts blockfmt.Flag) (*blockfmt.Index, error) {
	// prevent DoS: make sure index
	// is reasonably sized
	fp := IndexPath(db, table)
	f, err := s.Open(fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() >= MaxIndexSize {
		return nil, fmt.Errorf("index %q is %d bytes; too big", fp, info.Size())
	}
	buf := make([]byte, info.Size())
	n, err := io.ReadFull(f, buf)
	if err != nil {
		return nil, err
	}
	return blockfmt.DecodeIndex(key, buf[:n], opts)
}

// ListTables list the names of all tables in the given
// database. The database name must not be empty.
//
// A table in the returned list does not guarantee that
// the table exists. For example, it may have been
// deleted between the call to ListTables and the call
// to OpenIndex.
func ListTables(s fs.FS, db string) ([]string, error) {
	if db == "" {
		return nil, errors.New("db.ListTables: no database specified")
	}
	base := path.Join("db", db)
	dirs, err := fs.ReadDir(s, base)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(dirs))
	for i := range dirs {
		if !dirs[i].IsDir() {
			continue
		}
		out = append(out, dirs[i].Name())
	}
	return out, nil
}
