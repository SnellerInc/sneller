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

func (f fuzzEnv) Stat(_ expr.Node, _ *plan.Hints) (*plan.Input, error) {
	return &plan.Input{}, nil
}

func (f fuzzEnv) Geometry() *plan.Geometry {
	return &plan.Geometry{
		Peers: []plan.Transport{&plan.LocalTransport{}, &plan.LocalTransport{}},
	}
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
		_, err = plan.Decode(&st, buf.Bytes())
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
		_, err = plan.Decode(&st, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	})
}
