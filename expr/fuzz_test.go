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

//go:build go1.18

package expr_test

import (
	"bufio"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
)

func addQueries(f *testing.F) {
	// add every file from vm/testdata/queries
	dir := filepath.Clean("../vm/testdata/queries")
	dirfs := os.DirFS(dir)
	walk := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || path.Ext(p) != ".test" {
			return nil
		}
		file, err := dirfs.Open(p)
		if err != nil {
			f.Fatal(err)
		}
		defer file.Close()
		s := bufio.NewScanner(file)
		var text []byte
		for s.Scan() {
			line := s.Text()
			if strings.HasPrefix(line, "---") {
				break
			}
			text = append(text, line...)
		}
		if s.Err() != nil {
			f.Fatal(err)
		}
		f.Add(text)
		return nil
	}
	err := fs.WalkDir(dirfs, ".", walk)
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte("SELECT \"*\"\"\x05\""))
	f.Add([]byte("SELECT 1E1000000 FROM foo"))
	f.Add([]byte("SELECT * FROM CHAR_LENGTH()%0"))
	f.Add([]byte("SELECT (0 ++ (0 ++ 0))"))
}

func FuzzCheck(f *testing.F) {
	addQueries(f)
	// confirm that expr.Check will not
	// panic when handed a query that parses correctly
	f.Fuzz(func(t *testing.T, text []byte) {
		q, err := partiql.Parse(text)
		if err != nil {
			return
		}
		if err := q.Check(); err != nil {
			return
		}
		q.Body = expr.Simplify(q.Body, expr.NoHint)

		// sometimes the next Check succeeds; sometimes it doesn't.
		// simplification may often expose type errors that we couldn't
		// discern before the simplification
		expr.Check(q.Body)

		// test encode doesn't panic,
		// and decode doesn't return an error
		var buf ion.Buffer
		var st ion.Symtab
		q.Body.Encode(&buf, &st)
		d, _, err := ion.ReadDatum(&st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		ret, err := expr.Decode(d)
		if err != nil {
			t.Fatalf("Decode of %s failed: %s", expr.ToString(q.Body), err)
		}
		// Encode -> Decode should yield an Equivalent expression
		if !expr.Equivalent(q.Body, ret) {
			t.Errorf("%s not equivalent to %s", expr.ToString(q.Body), expr.ToString(ret))
		}
	})
}
