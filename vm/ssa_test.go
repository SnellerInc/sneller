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
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/internal/stringext"

	"github.com/SnellerInc/sneller/expr"
)

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
			return p.Equals(p.TrimWhitespace(p.Dot("Make", p.ValidLanes()), trimBoth), p.Constant("HOND"))
		},
	},
	{
		name: ".ViolationDescr LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "NO", stringext.NoEscape, true)
		},
	},
	{
		name: "LTRIM(.ViolationDescr) LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.Like(p.TrimWhitespace(p.Dot("ViolationDescr", p.ValidLanes()), trimLeading), "NO%", stringext.NoEscape, true)
		},
	},
	{
		name: "RTRIM(.ViolationDescr) LIKE \"%NO\"",
		rows: 3,
		expr: func(p *prog) *value {
			return p.Like(p.TrimWhitespace(p.Dot("ViolationDescr", p.ValidLanes()), trimTrailing), "%NO", stringext.NoEscape, true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%REG\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%REG", stringext.NoEscape, true)
		},
	},
	{
		// like above, but more harder
		name: ".ViolationDescr LIKE \"%REG%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%REG%", stringext.NoEscape, true)
		},
	},
	{
		// should match the same rows as above
		name: ".ViolationDescr LIKE \"%EVIDENCE%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%EVIDENCE%", stringext.NoEscape, true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%GRID LOCK%\"",
		rows: 19,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%GRID LOCK%", stringext.NoEscape, true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: ".ViolationDescr LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%NO%", stringext.NoEscape, true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "UPPER(.ViolationDescr) LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%no%", stringext.NoEscape, false)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "LOWER(.ViolationDescr) LIKE \"%no%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%NO%", stringext.NoEscape, false)
		},
	},
	{
		// match 'EXPIRED TAGS' or 'METER EXPIRED'
		name: ".ViolationDescr LIKE \"%EXPIRED%\"",
		rows: 32,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%EXPIRED%", stringext.NoEscape, true)
		},
	},
	{
		// match NO EVIDENCE OF REG
		name: ".ViolationDescr LIKE \"_O%_DENCE%R_G\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "_O%_DENCE%R_G", stringext.NoEscape, true)
		},
	},
	{
		// NO STOPPING/STANDING AM
		name: ".ViolationDescr LIKE \"NO_STO%STA%_AM\"",
		rows: 4,
		expr: func(p *prog) *value {
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "NO_STO%STA%_AM", stringext.NoEscape, true)
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
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%E_E%", stringext.NoEscape, true)
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
			return p.Like(p.Dot("ViolationDescr", p.ValidLanes()), "%E__E%", stringext.NoEscape, true)
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
			return p.Less(bcpath(p, "Issue.Time"), p.Constant(1200))
		},
	},
	{
		name: ".Coordinates.Lat < 100000 AND .Coordinates.Long < 100000",
		rows: 1023,
		expr: func(p *prog) *value {
			return p.And(
				p.Less(bcpath(p, "Coordinates.Lat"), p.Constant(100000)),
				p.Less(bcpath(p, "Coordinates.Long"), p.Constant(100000)))
		},
	},
	{
		name: ".Fields[0] IS TRUE",
		rows: 882,
		expr: func(p *prog) *value {
			return p.IsTrue(p.Index(bcpath(p, "Fields"), 0))
		},
	},
	{
		name: ".Fields[1] == 1",
		rows: 112,
		expr: func(p *prog) *value {
			return p.Equals(p.Index(bcpath(p, "Fields"), 1), p.Constant(1))
		},
	},
	{
		name: ".Fields[2] == \"01535\"",
		rows: 1,
		expr: func(p *prog) *value {
			return p.Equals(p.Index(bcpath(p, "Fields"), 2), p.Constant("01535"))
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
			entries := bcpath(p, "Entries")
			entry0 := p.Index(entries, 0)
			bodystyle := p.Dot("BodyStyle", entry0)
			return p.Equals(bodystyle, p.Constant("PA"))
		},
	},
	{
		name: ".Entries[3].BodyStyle == \"PA\"",
		rows: 31,
		expr: func(p *prog) *value {
			entries := bcpath(p, "Entries")
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
				p.Less(bcpath(p, "does_not_exist"), p.Constant(2)),
				p.Less(bcpath(p, "trip_distance"), p.Constant(2.5)))
		},
	},
	{
		name: ".does_not_exist < 2 AND .trip_distance < 2.5",
		rows: 0,
		expr: func(p *prog) *value {
			return p.And(
				p.Less(bcpath(p, "does_not_exist"), p.Constant(2)),
				p.Less(bcpath(p, "trip_distance"), p.Constant(2.5)))
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

func TestSSATicketsQueries(t *testing.T) {
	var st symtab
	defer st.free()
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
			err = p.Symbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
			if err != nil {
				t.Fatal(err)
			}
			defer bc.reset()
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
	var st symtab
	defer st.free()
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
			err = p.Symbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
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
	var st symtab
	defer st.free()
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
			err = p.Symbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
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

				st.Reset()
				sample = prog{}
				rest, err := st.Unmarshal(out.Bytes())
				if err != nil {
					t.Error(err)
				}
				err = p.Symbolize(&st, &sample, &auxbindings{})
				if err != nil {
					t.Error(err)
				}
				err = sample.compile(&bc, &st)
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
	var st symtab
	defer st.free()
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
			err = p.Symbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
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

func bcpath(p *prog, str string) *value {
	fields := strings.Split(str, ".")
	base := p.Dot(fields[0], p.ValidLanes())

	fields = fields[1:]
	for i := range fields {
		base = p.Dot(fields[i], base)
	}

	return base
}
