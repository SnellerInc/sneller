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
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func path(t testing.TB, s string) *expr.Path {
	p, err := expr.ParsePath(s)
	if err != nil {
		if t == nil {
			panic(err)
		}
		t.Helper()
		t.Fatal(err)
	}
	return p
}

func readVM(t testing.TB, name string) []byte {
	f, err := os.Open(name)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	size := info.Size()
	if size > PageSize {
		t.Helper()
		t.Fatalf("file %q larger than page size", name)
	}
	buf := Malloc()
	t.Cleanup(func() { Free(buf) })
	_, err = io.ReadFull(f, buf[:size])
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return buf
}

func TestSplat(t *testing.T) {
	var bc bytecode
	var p prog
	var st ion.Symtab

	buf := readVM(t, "../testdata/parking3.ion")
	rest, err := st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	delims := make([][2]uint32, 128)
	n, _ := scanvmm(rest, delims)
	if n != 60 {
		t.Fatalf("got %d rows at top level?", n)
	}

	p.Begin()
	field := p.Path("Entries")
	list := p.ssa2(stolist, field, field)
	p.Return(list)
	p.symbolize(&st)
	err = p.compile(&bc)
	if err != nil {
		t.Fatal(err)
	}

	// # of entries in the first
	// sixteen rows of the
	entrylengths := []int{
		15, 12, 3, 50,
		4, 8, 1, 70,
		14, 39, 5, 88,
		1, 2, 18, 1,
	}
	totalentries := 0
	for i := range entrylengths {
		totalentries += entrylengths[i]
	}

	outdelims := make([][2]uint32, 1024)
	outperm := make([]int32, len(outdelims))
	in, out := evalsplat(&bc, delims[:n], outdelims, outperm)
	if in != 16 {
		// we know we provided enough space
		// for all sixteen lanes to be
		// splatted
		t.Errorf("in: %d", in)
	}
	if out != totalentries {
		t.Errorf("out: %d, want %d", out, totalentries)
	}

	// test that the output permutation
	// is what we expect given the know
	// entry lengths
	j := 0
	for i := range entrylengths {
		span := entrylengths[i]
		for k := 0; k < span; k++ {
			if int(outperm[j+k]) != i {
				t.Errorf("outperm[%d]=%d, want %d", j+k, outperm[j+k], i)
			}
		}
		j += span
	}

	// test that delimiter object sizes
	// are sane
	for i := range outdelims[:out] {
		os, ds := objsize(vmref(outdelims[i]).mem())
		outsize := int(os) + int(ds)
		if outsize != int(outdelims[i][1]) {
			t.Errorf("delim %d: size %d, but computed %d", i, outdelims[i][1], outsize)
		}
	}

	// try consuming all the rows and
	// confirm that we get 1023 outputs
	total := 0
	delims = delims[:n]
	for len(delims) > 0 {
		in, out := evalsplat(&bc, delims, outdelims, outperm)
		if in > 16 {
			t.Errorf("consumed %d rows?", in)
		}
		if out > len(outdelims) {
			t.Errorf("output %d rows? (only space for %d)", out, len(outdelims))
		}
		total += out
		delims = delims[in:]
		outdelims = outdelims[out:]
		outperm = outperm[out:]
	}
	if total != 1023 {
		t.Errorf("splatted %d entries? (want 1023)", total)
	}
}

func TestUnnest(t *testing.T) {
	buf, err := ioutil.ReadFile("../testdata/parking3.ion")
	if err != nil {
		t.Fatal(err)
	}

	into := func(dst QuerySink) *UnnestProjection {
		return NewUnnest(dst,
			path(t, "Entries"),
			selection("Make as make"),
			selection("Ticket as ticket, Color as color, BodyStyle as bs"),
			nil,
		)
	}

	// select o.Name as name, i.Ticket as ticket, i.Color as color
	// from <tickets> as o, o.Entries as i
	var out QueryBuffer
	q := into(&out)

	err = CopyRows(q, buftbl(buf), 4)
	if err != nil {
		t.Fatal(err)
	}

	// check that the output is valid ion
	skipok(out.Bytes(), t)

	structs := structures(out.Bytes())
	if len(structs) != 1023 {
		t.Errorf("unnested %d structures?", len(structs))
	}

	// run a couple count(*) queries to confirm
	// we have projected the right fields
	for _, tc := range []struct {
		name  string
		rows  int
		where func(p *prog) *value
	}{
		{
			name: "bs = \"PA\"",
			rows: 882,
			where: func(p *prog) *value {
				return p.Equals(p.Path("bs"), p.Constant("PA"))
			},
		},
		{
			name: "color = \"BK\"",
			rows: 221,
			where: func(p *prog) *value {
				return p.Equals(p.Path("color"), p.Constant("BK"))
			},
		},
	} {
		name := tc.name
		rows := tc.rows
		proc := tc.where
		t.Run(name, func(t *testing.T) {
			var p prog
			var c Count
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), proc(&p)))

			err := CopyRows(into(where(&p, &c)), buftbl(buf), 4)
			if err != nil {
				t.Fatal(err)
			}
			got := int(c.Value())
			if got != rows {
				t.Errorf("got %d rows instead of %d", got, rows)
			}
		})
	}

}

func TestUnnestWhere(t *testing.T) {
	buf, err := ioutil.ReadFile("../testdata/parking3.ion")
	if err != nil {
		t.Fatal(err)
	}

	// select o.Make as make, i.Ticket as ticket
	// from <table> as o, o.Entries as i
	// where <expression>
	into := func(dst QuerySink, e expr.Node) *UnnestProjection {
		return NewUnnest(dst,
			path(t, "Entries"),
			selection("Make as make"),
			selection("Ticket as ticket"),
			e,
		)
	}

	// run a couple count(*) queries to confirm
	// we have projected the right fields
	for _, tc := range []struct {
		name  string
		rows  int
		where expr.Node
	}{
		{
			name:  "i.BodyStyle = \"PA\"",
			rows:  882,
			where: expr.Compare(expr.Equals, path(t, "BodyStyle"), expr.String("PA")),
		},
		{
			name:  "i.Color = \"BK\"",
			rows:  221,
			where: expr.Compare(expr.Equals, path(t, "Color"), expr.String("BK")),
		},
	} {
		name := tc.name
		rows := tc.rows
		where := tc.where
		t.Run(name, func(t *testing.T) {
			var c Count
			err := CopyRows(into(&c, where), buftbl(buf), 4)
			if err != nil {
				t.Fatal(err)
			}
			got := int(c.Value())
			if got != rows {
				t.Errorf("got %d rows instead of %d", got, rows)
			}
		})
	}
}

func BenchmarkUnnestParking3(b *testing.B) {
	var c Count
	buf, err := ioutil.ReadFile("../testdata/parking3.ion")
	if err != nil {
		b.Fatal(err)
	}

	q := NewUnnest(&c,
		path(b, "Entries"),
		selection("Make as make"),
		selection("Ticket as ticket, Color as color, BodyStyle as bs"),
		nil,
	)

	tbl := &looptable{count: int64(b.N), chunk: buf}
	parallel := runtime.GOMAXPROCS(0)
	b.SetParallelism(parallel)
	b.SetBytes(int64(0x57b8)) // size of output projection,
	err = CopyRows(q, tbl, parallel)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkUnnestWhereParking3(b *testing.B) {
	buf, err := ioutil.ReadFile("../testdata/parking3.ion")
	if err != nil {
		b.Fatal(err)
	}

	eval := func(dst *Count, e expr.Node) QuerySink {
		return NewUnnest(dst,
			path(b, "Entries"),
			selection("Make as make"),
			selection("Ticket as ticket"),
			e,
		)
	}

	exprs := []struct {
		name string
		expr expr.Node
	}{
		{
			name: "Entries.Color=\"BK\"",
			expr: expr.Compare(expr.Equals, path(b, "Color"), expr.String("BK")),
		},
		{
			name: "Entries.BodyStyle=\"TK\"",
			expr: expr.Compare(expr.Equals, path(b, "BodyStyle"), expr.String("TK")),
		},
	}

	for i := range exprs {
		expr := exprs[i].expr
		b.Run("WHERE "+exprs[i].name, func(b *testing.B) {
			var c Count
			query := eval(&c, expr)
			tbl := &looptable{count: int64(b.N), chunk: buf}
			parallel := runtime.GOMAXPROCS(0)
			b.SetParallelism(parallel)
			b.SetBytes(int64(0x57b8)) // size of output projection,
			err = CopyRows(query, tbl, parallel)
			if err != nil {
				b.Fatal(err)
			}
		})
	}

}

func BenchmarkUnpackParking3(b *testing.B) {
	var bc bytecode
	var p prog
	var st ion.Symtab

	buf := readVM(b, "../testdata/parking3.ion")
	rest, err := st.Unmarshal(buf)
	if err != nil {
		b.Fatal(err)
	}

	delims := make([][2]uint32, 128)
	n, _ := scanvmm(rest, delims)
	if n != 60 {
		b.Fatalf("got %d rows at top level?", n)
	}

	p.Begin()
	field := p.walk(path(b, "Entries"))
	list := p.ssa2(stolist, field, field)
	p.Return(list)
	p.symbolize(&st)
	err = p.compile(&bc)
	if err != nil {
		b.Fatal(err)
	}
	outdelims := make([][2]uint32, 1024)
	outperm := make([]int32, len(outdelims))

	b.SetBytes(0x42b8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		indelims := delims[:n]
		outd := outdelims
		outp := outperm
		total := 0
		for len(indelims) > 0 {
			in, out := evalsplat(&bc, indelims, outd, outp)
			indelims = indelims[in:]
			outd = outd[out:]
			outp = outp[out:]
			total += out
		}
		if total != 1023 {
			b.Fatalf("splatted %d rows?", total)
		}
	}
}
