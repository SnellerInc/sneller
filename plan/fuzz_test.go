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
	"context"
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
	"github.com/SnellerInc/sneller/vm"
)

type fuzzEnv struct{}

type fuzzHandle struct{}

func (f fuzzHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

func (f fuzzHandle) Size() int64 { return 0 }

func (f fuzzHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, nil
}

func (f fuzzEnv) Stat(_ expr.Node, _ *plan.Hints) (plan.TableHandle, error) {
	return fuzzHandle{}, nil
}

type fuzzDecoder struct{}

func (f fuzzDecoder) DecodeHandle(d ion.Datum) (plan.TableHandle, error) {
	return fuzzHandle{}, nil
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
	f.Add([]byte("SELECT 0 FROM A++A"))
	f.Add([]byte("SELECT x%x x, x FROM (SELECT x+x x FROM input)"))
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
		_, err = plan.Decode(fuzzDecoder{}, &st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzNewSplit(f *testing.F) {
	addQueries(f)
	f.Add([]byte("SELECT DATE_ADD(DAY,4444404440700000,UTCNOW())"))
	// confirm that expr.Check will not
	// panic when handed a query that parses correctly
	f.Fuzz(func(t *testing.T, text []byte) {
		q, err := partiql.Parse(text)
		if err != nil {
			return
		}
		tree, err := plan.NewSplit(q, fuzzEnv{})
		if err != nil {
			return
		}
		var buf ion.Buffer
		var st ion.Symtab
		err = tree.Encode(&buf, &st)
		if err != nil {
			t.Fatal(err)
		}
		_, err = plan.Decode(fuzzDecoder{}, &st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	})
}
