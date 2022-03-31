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
		for i := range q.With {
			expr.Check(q.With[i].As)
		}
		if err := expr.Check(q.Body); err != nil {
			return
		}
		q.Body = expr.Simplify(q.Body, expr.HintFn(expr.NoHint))

		// test encode doesn't panic,
		// and decode doesn't return an error
		var buf ion.Buffer
		var st ion.Symtab
		q.Body.Encode(&buf, &st)
		ret, rest, err := expr.Decode(&st, buf.Bytes())
		if err != nil {
			t.Fatalf("Decode of %s failed: %s", expr.ToString(q.Body), err)
		}
		if len(rest) > 0 {
			t.Errorf("%d left-over bytes from Decode?", len(rest))
		}
		// Encode -> Decode should yield an Equivalent expression
		if !expr.Equivalent(q.Body, ret) {
			t.Errorf("%s not equivalent to %s", expr.ToString(q.Body), expr.ToString(ret))
		}
	})
}
