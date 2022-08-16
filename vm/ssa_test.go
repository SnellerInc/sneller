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
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

// generate some additional test data
// using scripts to manipulate objects
// that are already checked-in:

// collect the set of symbol names
// referenced in 'dot' operations
// so we can generate a valid symbol table
func names(p *prog) []string {
	var out []string
	for i := range p.values {
		v := p.values[i]
		if v.op != sdot {
			continue
		}
		out = append(out, v.imm.(string))
	}
	return out
}

// CopyRows copies row from src to dst
// using the provided parallelism hint
// to indicate how many goroutines to use
// for processing rows.
//
// Deprecated: just call dst.WriteChunks directly
func CopyRows(dst QuerySink, src Table, parallel int) error {
	if parallel <= 0 {
		parallel = runtime.GOMAXPROCS(0)
	}
	return src.WriteChunks(dst, parallel)
}

func TestCompileSSA(t *testing.T) {
	progsyms := func(p *prog) *ion.Symtab {
		out := new(ion.Symtab)
		n := names(p)
		for i := range n {
			out.Intern(n[i])
		}
		return out
	}

	tcs := []struct {
		expr func(p *prog) *value
		text []string
	}{
		{
			expr: func(p *prog) *value {
				return p.Less(p.Dot("foo", p.ValidLanes()), p.Constant(3))
			},
			text: []string{
				"findsym u32(0xA)",
				"cmpv.imm.i64 i64(3)",
				"cmplt.imm.i i64(0)",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				return p.Equals(p.Dot("foo", p.ValidLanes()), p.Constant(1))
			},
			text: []string{
				"findsym u32(0xA)",
				"leneq u32(2)",
				"eqv4mask u32(289), u32(0xFFFF)",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				return p.Equals(p.Path("foo.bar"), p.Constant(1))
			},
			text: []string{
				"findsym u32(0xA)",
				"save.b [0]",
				"tuple", // clobbers base; see restore below
				"findsym u32(0xB)",
				"leneq u32(2)",
				"eqv4mask u32(289), u32(0xFFFF)",
				"load.b [0]", // restore lane pointers
				"ret",
			},
		},
		{
			// test that using a value pointer
			// from the stack works similarly
			// to using one that is populated in advance
			expr: func(p *prog) *value {
				base := p.Loadvalue(p.InitMem(), 0)
				return p.Equals(p.Dot("x", base), p.Constant(1))
			},
			text: []string{
				"loadzero.v [0]",
				"save.b [128]",
				"tuple",
				"findsym u32(0xA)",
				"leneq u32(2)",
				"eqv4mask u32(289), u32(0xFFFF)",
				"load.b [128]",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				return p.And(p.Equals(p.Path("foo.bar"), p.Constant(1)),
					p.Equals(p.Path("foo.baz"), p.Constant(1)))
			},
			text: []string{
				"findsym u32(0xA)",
				"save.b [0]",
				"tuple",
				"findsym u32(0xB)",
				"save.k [128]",
				"leneq u32(2)",
				"eqv4mask u32(289), u32(0xFFFF)",
				// FIXME: for subtle reasons,
				// this next instruction could be findsym3,
				// but we would have to identify the merge point
				// between these two sub-expressions
				"findsym2 [128], u32(0xC)",
				"leneq u32(2)",
				"eqv4mask u32(289), u32(0xFFFF)",
				"load.b [0]",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				return p.HasPrefix(p.Dot("name", p.ValidLanes()), "John", true)
			},
			text: []string{
				"findsym u32(0x4)",
				"unsymbolize",
				"unpack u8(0x8)",
				"contains_prefix_cs dict[0]",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				return p.HasPrefix(p.Dot("name", p.ValidLanes()), "John", false)
			},
			text: []string{
				"findsym u32(0x4)",
				"unsymbolize",
				"unpack u8(0x8)",
				"contains_prefix_ci dict[0]",
				"ret",
			},
		},
		{
			// test that AND-splicing works
			// (we don't reload the initial mask
			// for the second part of the conjunction)
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				lhs := p.Less(foo, p.Constant(1))
				rhs := p.Less(bar, p.Constant(2))
				return p.And(lhs, rhs)
			},
			text: []string{
				"findsym u32(0xA)",
				"save.k [0]",
				"cmpv.imm.i64 i64(1)",
				"cmplt.imm.i i64(0)",
				"findsym2 [0], u32(0xB)",
				"cmpv.imm.i64 i64(2)",
				"cmplt.imm.i i64(0)",
				"ret",
			},
		},
		{
			// same expression as above but
			// with the arguments to AND reversed;
			// should generate very similar code
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				lhs := p.Less(foo, p.Constant(1))
				rhs := p.Less(bar, p.Constant(2))
				return p.And(rhs, lhs)
			},
			text: []string{
				"findsym u32(0xA)",
				"save.k [0]",
				"cmpv.imm.i64 i64(1)",
				"cmplt.imm.i i64(0)",
				"findsym2 [0], u32(0xB)",
				"cmpv.imm.i64 i64(2)",
				"cmplt.imm.i i64(0)",
				"ret",
			},
		},
		{
			// test that mask save+restore works
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				lhs := p.Less(foo, p.Constant(1))
				rhs := p.Less(bar, p.Constant(2))
				return p.Or(lhs, rhs)
			},
			text: []string{
				"save.k [0]",
				"findsym u32(0xA)",
				"save.v [8]",
				"save.k [2]",
				"findsym2rev [0], u32(0xB)",
				"cmpv.imm.i64 i64(2)",
				"cmplt.imm.i i64(0)",
				"xchg.k [2]",
				"load.v [8]",
				"cmpv.imm.i64 i64(1)",
				"cmplt.imm.i i64(0)",
				"or.k [2]",
				"ret",
			},
		},
		{
			// test that conjunctions splice masks together
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				baz := p.Dot("baz", p.ValidLanes())
				return p.And(p.And(foo, bar), baz)
			},
			text: []string{
				"findsym u32(0xA)",
				"findsym3 u32(0xB)", // continue with prev. mask & offset
				"findsym3 u32(0xC)", // ... and again
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				return p.Equals(foo, bar)
			},
			text: []string{
				"findsym u32(0xA)",
				"save.v [0]",
				"save.k [128]",
				"findsym3 u32(0xB)", // continue with mask from above!
				"unsymbolize",       // produce first unsymbolized values
				"save.v [136]",      // save bar; load foo
				"load.v [0]",
				"xchg.k [128]",
				"unsymbolize", // unsymbolize foo
				"load.k [128]",
				"equalv [136]", // unsymbolize(foo) = unsymbolize(bar)
				"ret",
			},
		},
		{
			// test CSE of struct field lookup
			expr: func(p *prog) *value {
				// foo<3 AND foo>0
				return p.And(p.Less(p.Dot("foo", p.ValidLanes()), p.Constant(3)),
					p.Greater(p.Dot("foo", p.ValidLanes()), p.Constant(0)))
			},
			text: []string{
				// lots of stack shuffling, but notice
				// that there's only one 'findsym' op,
				// one 'tof64', and one 'toint'
				"findsym u32(0xA)",
				"save.k [0]",
				"cmpv.imm.i64 i64(0)",
				"cmpgt.imm.i i64(0)",
				"xchg.k [0]",
				"cmpv.imm.i64 i64(3)",
				"cmplt.imm.i i64(0)",
				"and.k [0]",
				"ret",
			},
		},
		{
			expr: func(p *prog) *value {
				foo := p.Dot("foo", p.ValidLanes())
				bar := p.Dot("bar", p.ValidLanes())
				return p.Less(foo, bar)
			},
			text: []string{
				"findsym u32(0xA)",
				"save.v [0]",
				"findsym3 u32(0xB)",
				"save.v [128]",
				"load.v [0]",
				"cmpv [128]",
				"cmplt.imm.i i64(0)",
				"ret",
			},
		},
	}

	for i := range tcs {
		expr := tcs[i].expr
		text := strings.Join(tcs[i].text, "\n") + "\n"

		name := fmt.Sprintf("case-%d", i+1)
		t.Run(name, func(t *testing.T) {
			var p, ps prog
			var bc bytecode
			var before strings.Builder
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(&p)))
			p.WriteTo(&before)

			// capture the SSA before compilation in case
			// we hit a panic
			defer func() {
				if err := recover(); err != nil {
					println("before compile:")
					println(before.String())
					panic(err)
				}
			}()

			st := progsyms(&p)
			p.Symbolize(st, &ps)
			testDomtree(&ps, t)

			// if GRAPHVIZ=1, dump test case SSA
			// to a graphical representation
			if os.Getenv("GRAPHVIZ") != "" {
				var buf bytes.Buffer
				p.Graphviz(&buf)
				os.WriteFile(name+".dot", buf.Bytes(), 0666)
			}

			err := ps.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			bcstr := bc.String()
			testDomtree(&ps, t)
			var afteropt strings.Builder
			ps.WriteTo(&afteropt)
			if bcstr != text {
				t.Logf("ssa before opt:\n%s", before.String())
				t.Logf("ssa after opt:\n%s", afteropt.String())
				t.Logf("bytecode:\n%s", bcstr)
				t.Logf("vstack size: %d", bc.vstacksize)
				t.Logf("hstack size: %d", bc.hstacksize)
				t.Errorf("wanted bytecode:\n%s", text)
			}
		})
	}
}

var ticketsTestQueries = []struct {
	name string
	rows int
	expr func(p *prog) *value
}{
	{
		name: "Make = \"HOND\" AND Color = \"BK\"",
		rows: 24,
		expr: func(p *prog) *value {
			return p.And(
				p.Equals(p.Dot("Make", p.ValidLanes()), p.Constant("HOND")),
				p.Equals(p.Dot("Color", p.ValidLanes()), p.Constant("BK")))
		},
	},
	{
		name: "TRIM(Make) = \"HOND\"",
		rows: 122,
		expr: func(p *prog) *value {
			return p.Equals(p.TrimWhitespace(p.Dot("Make", p.ValidLanes()), true, true), p.Constant("HOND"))
		},
	},
	{
		name: ".ViolationDescr LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.HasPrefix(p.Dot("ViolationDescr", p.ValidLanes()), "NO", true)
		},
	},
	{
		name: "LTRIM(.ViolationDescr) LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.HasPrefix(p.TrimWhitespace(p.Dot("ViolationDescr", p.ValidLanes()), true, false), "NO", true)
		},
	},
	{
		name: "RTRIM(.ViolationDescr) LIKE \"%NO\"",
		rows: 3,
		expr: func(p *prog) *value {
			return p.HasSuffix(p.TrimWhitespace(p.Dot("ViolationDescr", p.ValidLanes()), false, true), "NO", true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%REG\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.HasSuffix(p.Dot("ViolationDescr", p.ValidLanes()), "REG", true)
		},
	},
	{
		// like above, but more harder
		name: ".ViolationDescr LIKE \"%REG%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "REG", true)
		},
	},
	{
		// should match the same rows as above
		name: ".ViolationDescr LIKE \"%EVIDENCE%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "EVIDENCE", true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%GRID LOCK%\"",
		rows: 19,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "GRID LOCK", true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: ".ViolationDescr LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "NO", true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "UPPER(.ViolationDescr) LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "no", false)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "LOWER(.ViolationDescr) LIKE \"%no%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "NO", false)
		},
	},
	{
		// match 'EXPIRED TAGS' or 'METER EXPIRED'
		name: ".ViolationDescr LIKE \"%EXPIRED%\"",
		rows: 32,
		expr: func(p *prog) *value {
			return p.Contains(p.Dot("ViolationDescr", p.ValidLanes()), "EXPIRED", true)
		},
	},
	{
		// match NO EVIDENCE OF REG
		name: ".ViolationDescr LIKE \"_O%_DENCE%R_G\"",
		rows: 116,
		expr: func(p *prog) *value {
			vd := p.Dot("ViolationDescr", p.ValidLanes())
			return p.Like(vd, "_O%_DENCE%R_G", true)
		},
	},
	{
		// NO STOPPING/STANDING AM
		name: ".ViolationDescr LIKE \"NO_STO%STA%_AM\"",
		rows: 4,
		expr: func(p *prog) *value {
			vd := p.Dot("ViolationDescr", p.ValidLanes())
			return p.Like(vd, "NO_STO%STA%_AM", true)
		},
	},
	{
		// matches:
		// DP- RO NOT PRESENT
		// METER EXP.
		// METER EXPIRED
		// OUTSIDE LINES/METER
		// PREFERENTIAL PARKING
		name: ".ViolationDescr LIKE \"%E_E%\"",
		rows: 92,
		expr: func(p *prog) *value {
			vd := p.Dot("ViolationDescr", p.ValidLanes())
			return p.Like(vd, "%E_E%", true)
		},
	},
	{
		// matches:
		// EXCEED 72HRS-ST
		// EXCEED TIME LMT
		// METER EXP.
		// METER EXPIRED
		// NO EVIDENCE OF REG
		name: ".ViolationDescr LIKE \"%E__E%\"",
		rows: 200,
		expr: func(p *prog) *value {
			vd := p.Dot("ViolationDescr", p.ValidLanes())
			return p.Like(vd, "%E__E%", true)
		},
	},
}

var tickets2TestQueries = []struct {
	name string
	rows int
	expr func(p *prog) *value
}{
	{
		name: ".Issue.Time<1200",
		rows: 520,
		expr: func(p *prog) *value {
			return p.Less(p.Path("Issue.Time"), p.Constant(1200))
		},
	},
	{
		name: ".Coordinates.Lat < 100000 AND .Coordinates.Long < 100000",
		rows: 1023,
		expr: func(p *prog) *value {
			return p.And(
				p.Less(p.Path("Coordinates.Lat"), p.Constant(100000)),
				p.Less(p.Path("Coordinates.Long"), p.Constant(100000)))
		},
	},
	{
		name: ".Fields[0] IS TRUE",
		rows: 882,
		expr: func(p *prog) *value {
			return p.IsTrue(p.Index(p.Path("Fields"), 0))
		},
	},
	{
		name: ".Fields[1] == 1",
		rows: 112,
		expr: func(p *prog) *value {
			return p.Equals(p.Index(p.Path("Fields"), 1), p.Constant(1))
		},
	},
	{
		name: ".Fields[2] == \"01535\"",
		rows: 1,
		expr: func(p *prog) *value {
			return p.Equals(p.Index(p.Path("Fields"), 2), p.Constant("01535"))
		},
	},
}

var nestedTicketsQueries = []struct {
	name string
	rows int
	expr func(p *prog) *value
}{
	{
		name: ".Entries[0].BodyStyle == \"PA\"",
		rows: 45,
		expr: func(p *prog) *value {
			entries := p.Path("Entries")
			entry0 := p.Index(entries, 0)
			bodystyle := p.Dot("BodyStyle", entry0)
			return p.Equals(bodystyle, p.Constant("PA"))
		},
	},
	{
		name: ".Entries[3].BodyStyle == \"PA\"",
		rows: 31,
		expr: func(p *prog) *value {
			entries := p.Path("Entries")
			entry3 := p.Index(entries, 3)
			bodystyle := p.Dot("BodyStyle", entry3)
			return p.Equals(bodystyle, p.Constant("PA"))
		},
	},
}

var nycTestQueries = []struct {
	name string // name of test
	rows int    // expected output rows
	expr func(p *prog) *value
}{
	{
		name: ".passenger_count=2",
		rows: 1400,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Equals(p.Dot("passenger_count", all), p.Constant(2))
		},
	},
	{
		name: ".passenger_count<3",
		rows: 6605,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Less(p.Dot("passenger_count", all), p.Constant(3))
		},
	},
	{
		// same as above, but using a fp immediate
		name: ".passenger_count<2.5",
		rows: 6605,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Less(p.Dot("passenger_count", all), p.Constant(2.5))
		},
	},
	{
		name: ".trip_distance<3",
		rows: 6443,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Less(p.Dot("trip_distance", all), p.Constant(3))
		},
	},
	{
		name: ".trip_distance<2.5",
		rows: 5897,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Less(p.Dot("trip_distance", all), p.Constant(2.5))
		},
	},
	{
		name: ".trip_distance<=2.5",
		rows: 5917,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.LessEqual(p.Dot("trip_distance", all), p.Constant(2.5))
		},
	},
	{
		name: ".does_not_exist < 2 OR .trip_distance < 2.5",
		rows: 5897,
		expr: func(p *prog) *value {
			return p.Or(
				p.Less(p.Path("does_not_exist"), p.Constant(2)),
				p.Less(p.Path("trip_distance"), p.Constant(2.5)))
		},
	},
	{
		name: ".does_not_exist < 2 AND .trip_distance < 2.5",
		rows: 0,
		expr: func(p *prog) *value {
			return p.And(
				p.Less(p.Path("does_not_exist"), p.Constant(2)),
				p.Less(p.Path("trip_distance"), p.Constant(2.5)))
		},
	},
	{
		name: ".trip_distance>2.5",
		rows: 2643,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.Greater(p.Dot("trip_distance", all), p.Constant(2.5))
		},
	},
	{
		name: ".trip_distance>=2.5",
		rows: 2663,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			return p.GreaterEqual(p.Dot("trip_distance", all), p.Constant(2.5))
		},
	},
	{
		name: ".passenger_count<2 and .trip_distance>5",
		rows: 604,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			psngr := p.Dot("passenger_count", all)
			dist := p.Dot("trip_distance", all)
			return p.And(p.Less(psngr, p.Constant(2)), p.Greater(dist, p.Constant(5)))
		},
	},
	{
		name: ".passenger_count>1 or .trip_distance<1",
		rows: 4699,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			pass := p.Dot("passenger_count", all)
			dist := p.Dot("trip_distance", all)
			one := p.Constant(1)
			return p.Or(p.Greater(pass, one), p.Less(dist, one))
		},
	},
	{
		name: ".fare_amount==.total_amount",
		rows: 3820,
		expr: func(p *prog) *value {
			all := p.ValidLanes()
			lhs := p.Dot("fare_amount", all)
			rhs := p.Dot("total_amount", all)
			return p.Equals(lhs, rhs)
		},
	},
	{
		// two fp lanes, <
		name: ".tip_amount<.surcharge",
		rows: 2874,
		expr: func(p *prog) *value {
			tip := p.Dot("tip_amount", p.ValidLanes())
			surcharge := p.Dot("surcharge", p.ValidLanes())
			return p.Less(tip, surcharge)
		},
	},
	{
		// two fp lanes, >
		name: ".tip_amount>.surcharge",
		rows: 1709,
		expr: func(p *prog) *value {
			tip := p.Dot("tip_amount", p.ValidLanes())
			surcharge := p.Dot("surcharge", p.ValidLanes())
			return p.Greater(tip, surcharge)
		},
	},
	{
		// compare fp and int lane
		name: ".tip_amount>=.passenger_count",
		rows: 1297,
		expr: func(p *prog) *value {
			tip := p.Dot("tip_amount", p.ValidLanes())
			pass := p.Dot("passenger_count", p.ValidLanes())
			return p.GreaterEqual(tip, pass)
		},
	},
	{
		// compare fp and int lane;
		// reverse the types of the lanes from above
		name: ".passenger_count<=.tip_amount",
		rows: 1297,
		expr: func(p *prog) *value {
			tip := p.Dot("tip_amount", p.ValidLanes())
			pass := p.Dot("passenger_count", p.ValidLanes())
			return p.LessEqual(pass, tip)
		},
	},
	{
		// match 'VTS'
		name: ".VendorID LIKE VT%",
		rows: 7353,
		expr: func(p *prog) *value {
			return p.HasPrefix(p.Dot("VendorID", p.ValidLanes()), "VT", true)
		},
	},
	{
		// match 'VTS'
		name: ".VendorID LIKE VTS%",
		rows: 7353,
		expr: func(p *prog) *value {
			return p.HasPrefix(p.Dot("VendorID", p.ValidLanes()), "VTS", true)
		},
	},
	{
		name: ".VendorID LIKE %CMT",
		rows: 1055,
		expr: func(p *prog) *value {
			return p.HasSuffix(p.Dot("VendorID", p.ValidLanes()), "CMT", true)
		},
	},
	{
		// match 'CMT'
		name: ".VendorID LIKE %MT",
		rows: 1055,
		expr: func(p *prog) *value {
			return p.HasSuffix(p.Dot("VendorID", p.ValidLanes()), "MT", true)
		},
	},
	{
		// match 'VTS' or 'DDS'
		name: ".VendorID LIKE %S",
		rows: 7505,
		expr: func(p *prog) *value {
			return p.HasSuffix(p.Dot("VendorID", p.ValidLanes()), "S", true)
		},
	},
	{
		name: ".payment_type LIKE CASH%",
		rows: 5902,
		expr: func(p *prog) *value {
			pt := p.Dot("payment_type", p.ValidLanes())
			return p.HasPrefix(pt, "CASH", true)
		},
	},
	{
		// tickle CSE
		name: ".passenger_count>0 AND .passenger_count<3",
		rows: 6605,
		expr: func(p *prog) *value {
			return p.And(p.Greater(p.Dot("passenger_count", p.ValidLanes()), p.Constant(0)),
				p.Less(p.Dot("passenger_count", p.ValidLanes()), p.Constant(3)))
		},
	},
	{
		name: ".trip_distance>1 AND .trip_distance<3",
		rows: 4185,
		expr: func(p *prog) *value {
			return p.And(p.Greater(p.Dot("trip_distance", p.ValidLanes()), p.Constant(1)),
				p.Less(p.Dot("trip_distance", p.ValidLanes()), p.Constant(3)))
		},
	},
}

// test dominator tree invariants
func testDomtree(p *prog, t *testing.T) {
	var pi proginfo

	ord := p.order(&pi)
	idom := p.domtree(&pi)
	tr := p.spantree(&pi)
	for i := range ord {
		v := ord[i]
		node := &tr[v.id]
		if node.parent == nil {
			// the undef op is the only one that
			// is really allowed to be dead
			if v != p.ret && v.op != sundef {
				t.Errorf("%s not ret but node.parent==nil", v.Name())
			}
			continue
		}
		// node.parent should be the immediate postdominator
		if node.parent != idom[v.id] {
			t.Logf("idom %s is %s", v.Name(), idom[v.id].Name())
			t.Errorf("parent of %s is %s", v.Name(), node.parent.Name())
		}
		// every parent should postdominate v
		for p := node.parent; p != nil; p = tr[p.id].parent {
			if p == v {
				t.Errorf("parent = child? %s", p.Name())
				break
			}
			if !tr.postdom(p, v) {
				t.Errorf("parent range: [%d, %d]", tr[p.id].lo, tr[p.id].hi)
				t.Errorf("child range: [%d, %d]", tr[v.id].lo, tr[v.id].hi)
				t.Errorf("%s (parent) not postdom %s?", p.Name(), v.Name())
			}
		}
		// every child should be postdominated by v
		for c := node.child; c != nil; c = tr[c.id].sibling {
			if !tr.postdom(v, c) {
				t.Errorf("parent range: [%d, %d]", tr[v.id].lo, tr[v.id].hi)
				t.Errorf("child range: [%d, %d]", tr[c.id].lo, tr[c.id].hi)
				t.Errorf("%s not postdom (child-of-child) %s?", v.Name(), c.Name())
			}
		}
		// node should *not* be postdominated by siblings and vice-versa
		for sib := node.sibling; sib != nil; sib = tr[sib.id].sibling {
			if tr.postdom(v, sib) {
				t.Errorf("%s postdominates sibling %s?", v.Name(), sib.Name())
			}
			if tr.postdom(sib, v) {
				t.Errorf("%s postdominated by sibling %s?", v.Name(), sib.Name())
			}
		}
	}
	// dump tree
	if t.Failed() {
		t.Log("dominator tree:")
		dt := p.domtree(&pi)
		for i := range ord {
			v := ord[i]
			idom := dt[v.id]
			if idom == nil {
				t.Logf("(none) dom %s", v.Name())
			} else {
				t.Logf("%s idom %s", idom.Name(), v.Name())
			}
		}
	}
}

func TestSSATicketsQueries(t *testing.T) {
	var st ion.Symtab
	buf := unhex(parkingCitations1KLines)
	_, err := st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	tcs := ticketsTestQueries
	for i := range tcs {
		name := tcs[i].name
		want := tcs[i].rows
		t.Run(name, func(t *testing.T) {
			p := new(prog)
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), tcs[i].expr(p)))
			var sample prog
			var bc bytecode
			err = p.Symbolize(&st, &sample)
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())

			var out QueryBuffer
			err := CopyRows(where(p, &out), buftbl(buf), 4)
			if err != nil {
				t.Fatal(err)
			}
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
				t.Errorf("matched %d rows; expected %d", got, want)
			}
		})
	}
}

func TestSSATickets2Queries(t *testing.T) {
	var st ion.Symtab
	buf, err := os.ReadFile("../testdata/parking2.ion")
	if err != nil {
		t.Fatal(err)
	}
	skipok(buf, t)

	_, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	tcs := tickets2TestQueries
	for i := range tcs {
		name := tcs[i].name
		want := tcs[i].rows
		expr := tcs[i].expr
		t.Run(name, func(t *testing.T) {
			p := new(prog)
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(p)))
			var sample prog
			var bc bytecode
			err = p.Symbolize(&st, &sample)
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())

			var out QueryBuffer
			err := CopyRows(where(p, &out), buftbl(buf), 4)
			if err != nil {
				t.Fatal(err)
			}
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
				t.Errorf("matched %d rows; expected %d", got, want)
			}
		})
	}
}

func TestSSANYCQueries(t *testing.T) {
	var st ion.Symtab
	buf, err := os.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	tcs := nycTestQueries
	// produce a new copy of the table
	table := func() Table {
		return NewReaderAtTable(bytes.NewReader(buf), int64(len(buf)), defaultAlign)
	}
	// grab referenced symbol names from an SSA snippet
	progsel := func(p *prog) Selection {
		var names []expr.Binding
		for i := range p.values {
			v := p.values[i]
			if v.op != sdot {
				continue
			}
			names = append(names, expr.Bind(&expr.Path{First: v.imm.(string)}, ""))
		}
		return Selection(names)
	}
	for i := range tcs {
		name := tcs[i].name
		want := tcs[i].rows
		expr := tcs[i].expr
		t.Run(name, func(t *testing.T) {
			p := new(prog)
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(p)))
			var out QueryBuffer
			err := CopyRows(where(p, &out), table(), 4)
			if err != nil {
				t.Fatal(err)
			}
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
				t.Errorf("matched %d rows; expected %d", got, want)
			}

			var sample prog
			var bc bytecode
			err = p.Symbolize(&st, &sample)
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())
			var opt strings.Builder
			sample.WriteTo(&opt)
			t.Logf("ssa:\n%s", opt.String())

			// try it again with only the keys actually
			// required for the query as part of the
			// projection
			//
			// this should tickle the Select() code
			// "fast project" implementation by stiking
			// a RowConsumer (Where) as the output of
			// the Select
			*p = prog{}
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(p)))
			var c Count
			dst := NewProjection(progsel(p), where(p, &c))
			err = CopyRows(dst, table(), 4)
			if err != nil {
				t.Fatal(err)
			}
			if c.Value() != int64(want) {
				out.Reset()
				CopyRows(NewProjection(progsel(p), &out), table(), 1)

				st = ion.Symtab{}
				sample = prog{}
				rest, err := st.Unmarshal(out.Bytes())
				if err != nil {
					t.Error(err)
				}
				err = p.Symbolize(&st, &sample)
				if err != nil {
					t.Error(err)
				}
				err = sample.compile(&bc)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("bytecode:\n%s", bc.String())
				var opt strings.Builder
				sample.WriteTo(&opt)
				t.Logf("ssa:\n%s", opt.String())

				outbuf := rest
				t.Logf("beginning of projection: %x", outbuf[:16])

				t.Errorf("after projection: got %d, expected %d rows", c.Value(), want)
			}
		})

	}
}

func TestNestedTicketsQueries(t *testing.T) {
	buf, err := os.ReadFile("../testdata/parking3.ion")
	if err != nil {
		t.Fatal(err)
	}
	var st ion.Symtab
	_, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	tcs := nestedTicketsQueries
	for i := range tcs {
		name := tcs[i].name
		want := tcs[i].rows
		expr := tcs[i].expr
		t.Run(name, func(t *testing.T) {
			p := new(prog)
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(p)))
			var out QueryBuffer
			err := CopyRows(where(p, &out), buftbl(buf), 4)
			if err != nil {
				t.Fatal(err)
			}
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
				t.Errorf("matched %d rows; expected %d", got, want)
			}

			var sample prog
			var bc bytecode
			err = p.Symbolize(&st, &sample)
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())
			var opt strings.Builder
			sample.WriteTo(&opt)
			t.Logf("ssa:\n%s", opt.String())
		})
	}
}

func BenchmarkParkingTicketsQueries(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	tcs := ticketsTestQueries
	for i := range tcs {
		expr := tcs[i].expr
		b.Run(tcs[i].name, func(b *testing.B) {
			var c Count
			var p prog
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(&p)))
			w := where(&p, &c)
			b.SetBytes(int64(len(buf)))
			b.SetParallelism(1)

			start := time.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// TODO: parallelism!
				err := CopyRows(w, buftbl(buf), 1)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			rps := float64(b.N) * 1023 * float64(time.Second) / float64(time.Since(start))
			b.ReportMetric(rps, "rows/s")
		})
	}
}

func BenchmarkNYCQueries(b *testing.B) {
	buf, err := os.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		b.Fatal(err)
	}
	tcs := nycTestQueries
	for i := range tcs {
		expr := tcs[i].expr
		b.Run(tcs[i].name, func(b *testing.B) {
			var c Count
			var p prog
			p.Begin()
			p.Return(p.RowsMasked(p.ValidLanes(), expr(&p)))
			w := where(&p, &c)
			b.SetBytes(int64(len(buf)))
			b.SetParallelism(1)

			start := time.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// TODO: parallelism!
				err := CopyRows(w, buftbl(buf), 1)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			rps := float64(b.N) * 8560 * float64(time.Second) / float64(time.Since(start))
			b.ReportMetric(rps, "rows/s")
		})
	}
}
