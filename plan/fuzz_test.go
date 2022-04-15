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

package plan_test

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
	"github.com/SnellerInc/sneller/plan"
)

type fuzzEnv struct{}

func (f fuzzEnv) Stat(t *expr.Table, filter expr.Node) (plan.TableHandle, error) {
	return nil, nil
}

func (f fuzzEnv) Schema(t *expr.Table) expr.Hint { return nil }

type fuzzSplitter struct{}

func (f fuzzSplitter) Split(t *expr.Table, h plan.TableHandle) ([]plan.Subtable, error) {
	return []plan.Subtable{
		plan.Subtable{
			Transport: &plan.LocalTransport{},
			Table: &expr.Table{
				Binding: expr.Bind(t.Expr, "local-copy-0"),
			},
		},
		plan.Subtable{
			Transport: &plan.LocalTransport{},
			Table: &expr.Table{
				Binding: expr.Bind(t.Expr, "local-copy-1"),
			},
		},
	}, nil
}

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
}

func FuzzNewPlan(f *testing.F) {
	addQueries(f)
	// confirm that expr.Check will not
	// panic when handed a query that parses correctly
	f.Fuzz(func(t *testing.T, text []byte) {
		q, err := partiql.Parse(text)
		if err != nil {
			return
		}
		tree, err := plan.New(q, fuzzEnv{})
		if err != nil {
			return
		}
		var buf ion.Buffer
		var st ion.Symtab
		err = tree.Encode(&buf, &st)
		if err != nil {
			t.Fatal(err)
		}
		_, err = plan.Decode(nil, &st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzNewSplit(f *testing.F) {
	addQueries(f)
	// confirm that expr.Check will not
	// panic when handed a query that parses correctly
	f.Fuzz(func(t *testing.T, text []byte) {
		q, err := partiql.Parse(text)
		if err != nil {
			return
		}
		tree, err := plan.NewSplit(q, fuzzEnv{}, fuzzSplitter{})
		if err != nil {
			return
		}
		var buf ion.Buffer
		var st ion.Symtab
		err = tree.Encode(&buf, &st)
		if err != nil {
			t.Fatal(err)
		}
		_, err = plan.Decode(nil, &st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	})
}
