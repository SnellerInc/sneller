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

package vm

import (
	"io/ioutil"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func buftbl(buf []byte) *BufferedTable {
	return &BufferedTable{buf: buf, align: defaultAlign}
}

func TestCrossCount(t *testing.T) {
	buf := unhex(parkingCitations1KLines)
	rhsbuf, err := ioutil.ReadFile("../testdata/quintuple.ion")
	if err != nil {
		t.Fatal(err)
	}

	var out QueryBuffer
	cross := Cross(&out,
		buftbl(rhsbuf),
		"left", "right",
	)

	err = CopyRows(cross, buftbl(buf), 4)
	if err != nil {
		t.Fatal(err)
	}

	structs := structures(out.Bytes())
	if len(structs) != 1023*5 {
		t.Errorf("expected %d output structures, found %d", 1023*5, len(structs))
	}
	// every structure should have two fields
	for i := range structs {
		if len(structs[i]) != 2 {
			t.Fatalf("struct %d has %d fields?", i, len(structs[i]))
		}
	}
}

// Test NYC queries cross-joined so that we have
//   { left: { ... }, right: { n: ... }}
// for n=1 through n=5, so we can easily calculate
// what the new number of expected return values
// of each query should be
func TestNYCCrossJoined(t *testing.T) {
	buf, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}
	rhsbuf, err := ioutil.ReadFile("../testdata/quintuple.ion")
	if err != nil {
		t.Fatal(err)
	}

	lhsname, rhsname := "left", "right"
	frob := func(p *prog, expr func(p *prog) *value) *value {
		p.PushPath(lhsname)
		v := expr(p)
		p.PopPath()
		return v
	}

	var crossed QueryBuffer
	cross := Cross(&crossed,
		buftbl(rhsbuf),
		lhsname, rhsname,
	)
	err = CopyRows(cross, buftbl(buf), 1)
	if err != nil {
		t.Fatal(err)
	}
	var outst ion.Symtab
	outst.Unmarshal(crossed.Bytes())

	tcs := nycTestQueries
	for i := range tcs {
		name := tcs[i].name
		want := tcs[i].rows
		expr := tcs[i].expr
		t.Run(name, func(t *testing.T) {
			// test original(left) AND right.n = 1
			p := new(prog)
			p.Begin()
			lhs := frob(p, expr)
			rhs := p.Equals(p.Path(rhsname+".n"), p.Constant(1))
			p.Return(p.RowsMasked(p.ValidLanes(), p.And(lhs, rhs)))

			var out QueryBuffer
			var bc bytecode
			err := CopyRows(where(p, &out), crossed.Table(), 1)
			if err != nil {
				t.Fatal(err)
			}
			p.symbolize(&outst)
			err = p.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())
			got := 0
			if len(out.Bytes()) != 0 {
				var c Count
				err = CopyRows(&c, out.Table(), 4)
				if err != nil {
					t.Fatal(err)
				}
				got = int(c.Value())
			}
			if got != want {
				t.Errorf("matched %d rows, expected %d", got, want)
			}
		})
	}
}
