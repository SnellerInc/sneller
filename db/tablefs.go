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

// Package db implements
// the policy layout of databases,
// tables, and indices as a virtual
// filesystem tree.
package db

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// TablePrefix returns the prefix
// at which the table files live
// relative to the root of the FS.
func TablePrefix(db, table string) string {
	return path.Join("db", db, table) + "/"
}

// IndexPath returns the path
// at which the index for the given
// db and table would live relative
// to the root of the FS.
func IndexPath(db, table string) string {
	return path.Join("db", db, table, "index")
}

// DefinitionPath returns the path
// at which the definition file for the given
// db and table would live relative
// to the root of the FS.
func DefinitionPath(db, table string) string {
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

// ListComponent performs a glob match on s
// for the given pattern and then yields a deduplicated
// list of path components corresponding to the given
// 0-indexed part number.
//
// For example, part 1 of "/foo/*/baz" would yield
// all of the components that matched "*".
func ListComponent(s fs.FS, pattern string, part int) ([]string, error) {
	var out []string
	all, err := fs.Glob(s, pattern)
	if err != nil {
		return nil, err
	}
	for i := range all {
		str, ok := strpart(all[i], part)
		if ok {
			out = append(out, str)
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
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
	i, _, err := openIndex(s, IndexPath(db, table), key, 0)
	return i, err
}

// OpenPartialIndex is equivalent to OpenIndex, but
// skips decoding Index.Inputs. The returned
// index is suitable for queries, but not for
// synchronizing tables.
func OpenPartialIndex(s fs.FS, db, table string, key *blockfmt.Key) (*blockfmt.Index, error) {
	i, _, err := openIndex(s, IndexPath(db, table), key, blockfmt.FlagSkipInputs)
	return i, err
}

func openIndex(s fs.FS, ipath string, key *blockfmt.Key, opts blockfmt.Flag) (*blockfmt.Index, fs.FileInfo, error) {
	// prevent DoS: make sure index
	// is reasonably sized
	f, err := s.Open(ipath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if info.Size() >= MaxIndexSize {
		return nil, info, fmt.Errorf("index %q is %d bytes; too big", ipath, info.Size())
	}
	buf := make([]byte, info.Size())
	n, err := io.ReadFull(f, buf)
	if err != nil {
		return nil, info, err
	}
	idx, err := blockfmt.DecodeIndex(key, buf[:n], opts)
	return idx, info, err
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
