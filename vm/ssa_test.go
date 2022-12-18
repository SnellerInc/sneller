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
			return p.and(
				p.equals(p.dot("Make", p.validLanes()), p.constant("HOND")),
				p.equals(p.dot("Color", p.validLanes()), p.constant("BK")))
		},
	},
	{
		name: "TRIM(Make) = \"HOND\"",
		rows: 122,
		expr: func(p *prog) *value {
			return p.equals(p.trimWhitespace(p.dot("Make", p.validLanes()), trimBoth), p.constant("HOND"))
		},
	},
	{
		name: ".ViolationDescr LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "NO%", stringext.NoEscape, true)
		},
	},
	{
		name: "LTRIM(.ViolationDescr) LIKE \"NO%\"",
		rows: 524,
		expr: func(p *prog) *value {
			return p.like(p.trimWhitespace(p.dot("ViolationDescr", p.validLanes()), trimLeading), "NO%", stringext.NoEscape, true)
		},
	},
	{
		name: "RTRIM(.ViolationDescr) LIKE \"%NO\"",
		rows: 3,
		expr: func(p *prog) *value {
			return p.like(p.trimWhitespace(p.dot("ViolationDescr", p.validLanes()), trimTrailing), "%NO", stringext.NoEscape, true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%REG\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%REG", stringext.NoEscape, true)
		},
	},
	{
		// like above, but more harder
		name: ".ViolationDescr LIKE \"%REG%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%REG%", stringext.NoEscape, true)
		},
	},
	{
		// should match the same rows as above
		name: ".ViolationDescr LIKE \"%EVIDENCE%\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%EVIDENCE%", stringext.NoEscape, true)
		},
	},
	{
		name: ".ViolationDescr LIKE \"%GRID LOCK%\"",
		rows: 19,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%GRID LOCK%", stringext.NoEscape, true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: ".ViolationDescr LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%NO%", stringext.NoEscape, true)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "UPPER(.ViolationDescr) LIKE \"%NO%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%no%", stringext.NoEscape, false)
		},
	},
	{
		// match 'NO EVIDENCE*', 'NO STOP*', 'NO PARK*', 'HANDICAP/NO*', and more
		name: "LOWER(.ViolationDescr) LIKE \"%no%\"",
		rows: 532,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%NO%", stringext.NoEscape, false)
		},
	},
	{
		// match 'EXPIRED TAGS' or 'METER EXPIRED'
		name: ".ViolationDescr LIKE \"%EXPIRED%\"",
		rows: 32,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%EXPIRED%", stringext.NoEscape, true)
		},
	},
	{
		// match NO EVIDENCE OF REG
		name: ".ViolationDescr LIKE \"_O%_DENCE%R_G\"",
		rows: 116,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "_O%_DENCE%R_G", stringext.NoEscape, true)
		},
	},
	{
		// NO STOPPING/STANDING AM
		name: ".ViolationDescr LIKE \"NO_STO%STA%_AM\"",
		rows: 4,
		expr: func(p *prog) *value {
			return p.like(p.dot("ViolationDescr", p.validLanes()), "NO_STO%STA%_AM", stringext.NoEscape, true)
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
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%E_E%", stringext.NoEscape, true)
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
			return p.like(p.dot("ViolationDescr", p.validLanes()), "%E__E%", stringext.NoEscape, true)
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
			return p.less(bcpath(p, "Issue.Time"), p.constant(1200))
		},
	},
	{
		name: ".Coordinates.Lat < 100000 AND .Coordinates.Long < 100000",
		rows: 1023,
		expr: func(p *prog) *value {
			return p.and(
				p.less(bcpath(p, "Coordinates.Lat"), p.constant(100000)),
				p.less(bcpath(p, "Coordinates.Long"), p.constant(100000)))
		},
	},
	{
		name: ".Fields[0] IS TRUE",
		rows: 882,
		expr: func(p *prog) *value {
			return p.isTrue(p.index(bcpath(p, "Fields"), 0))
		},
	},
	{
		name: ".Fields[1] == 1",
		rows: 112,
		expr: func(p *prog) *value {
			return p.equals(p.index(bcpath(p, "Fields"), 1), p.constant(1))
		},
	},
	{
		name: ".Fields[2] == \"01535\"",
		rows: 1,
		expr: func(p *prog) *value {
			return p.equals(p.index(bcpath(p, "Fields"), 2), p.constant("01535"))
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
			entry0 := p.index(entries, 0)
			bodystyle := p.dot("BodyStyle", entry0)
			return p.equals(bodystyle, p.constant("PA"))
		},
	},
	{
		name: ".Entries[3].BodyStyle == \"PA\"",
		rows: 31,
		expr: func(p *prog) *value {
			entries := bcpath(p, "Entries")
			entry3 := p.index(entries, 3)
			bodystyle := p.dot("BodyStyle", entry3)
			return p.equals(bodystyle, p.constant("PA"))
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
			all := p.validLanes()
			return p.equals(p.dot("passenger_count", all), p.constant(2))
		},
	},
	{
		name: ".passenger_count<3",
		rows: 6605,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.less(p.dot("passenger_count", all), p.constant(3))
		},
	},
	{
		// same as above, but using a fp immediate
		name: ".passenger_count<2.5",
		rows: 6605,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.less(p.dot("passenger_count", all), p.constant(2.5))
		},
	},
	{
		name: ".trip_distance<3",
		rows: 6443,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.less(p.dot("trip_distance", all), p.constant(3))
		},
	},
	{
		name: ".trip_distance<2.5",
		rows: 5897,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.less(p.dot("trip_distance", all), p.constant(2.5))
		},
	},
	{
		name: ".trip_distance<=2.5",
		rows: 5917,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.lessEqual(p.dot("trip_distance", all), p.constant(2.5))
		},
	},
	{
		name: ".does_not_exist < 2 OR .trip_distance < 2.5",
		rows: 5897,
		expr: func(p *prog) *value {
			return p.or(
				p.less(bcpath(p, "does_not_exist"), p.constant(2)),
				p.less(bcpath(p, "trip_distance"), p.constant(2.5)))
		},
	},
	{
		name: ".does_not_exist < 2 AND .trip_distance < 2.5",
		rows: 0,
		expr: func(p *prog) *value {
			return p.and(
				p.less(bcpath(p, "does_not_exist"), p.constant(2)),
				p.less(bcpath(p, "trip_distance"), p.constant(2.5)))
		},
	},
	{
		name: ".trip_distance>2.5",
		rows: 2643,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.greater(p.dot("trip_distance", all), p.constant(2.5))
		},
	},
	{
		name: ".trip_distance>=2.5",
		rows: 2663,
		expr: func(p *prog) *value {
			all := p.validLanes()
			return p.greaterEqual(p.dot("trip_distance", all), p.constant(2.5))
		},
	},
	{
		name: ".passenger_count<2 and .trip_distance>5",
		rows: 604,
		expr: func(p *prog) *value {
			all := p.validLanes()
			psngr := p.dot("passenger_count", all)
			dist := p.dot("trip_distance", all)
			return p.and(p.less(psngr, p.constant(2)), p.greater(dist, p.constant(5)))
		},
	},
	{
		name: ".passenger_count>1 or .trip_distance<1",
		rows: 4699,
		expr: func(p *prog) *value {
			all := p.validLanes()
			pass := p.dot("passenger_count", all)
			dist := p.dot("trip_distance", all)
			one := p.constant(1)
			return p.or(p.greater(pass, one), p.less(dist, one))
		},
	},
	{
		name: ".fare_amount==.total_amount",
		rows: 3820,
		expr: func(p *prog) *value {
			all := p.validLanes()
			lhs := p.dot("fare_amount", all)
			rhs := p.dot("total_amount", all)
			return p.equals(lhs, rhs)
		},
	},
	{
		// two fp lanes, <
		name: ".tip_amount<.surcharge",
		rows: 2874,
		expr: func(p *prog) *value {
			tip := p.dot("tip_amount", p.validLanes())
			surcharge := p.dot("surcharge", p.validLanes())
			return p.less(tip, surcharge)
		},
	},
	{
		// two fp lanes, >
		name: ".tip_amount>.surcharge",
		rows: 1709,
		expr: func(p *prog) *value {
			tip := p.dot("tip_amount", p.validLanes())
			surcharge := p.dot("surcharge", p.validLanes())
			return p.greater(tip, surcharge)
		},
	},
	{
		// compare fp and int lane
		name: ".tip_amount>=.passenger_count",
		rows: 1297,
		expr: func(p *prog) *value {
			tip := p.dot("tip_amount", p.validLanes())
			pass := p.dot("passenger_count", p.validLanes())
			return p.greaterEqual(tip, pass)
		},
	},
	{
		// compare fp and int lane;
		// reverse the types of the lanes from above
		name: ".passenger_count<=.tip_amount",
		rows: 1297,
		expr: func(p *prog) *value {
			tip := p.dot("tip_amount", p.validLanes())
			pass := p.dot("passenger_count", p.validLanes())
			return p.lessEqual(pass, tip)
		},
	},
	{
		// match 'VTS'
		name: ".VendorID LIKE VT%",
		rows: 7353,
		expr: func(p *prog) *value {
			return p.hasPrefix(p.dot("VendorID", p.validLanes()), "VT", true)
		},
	},
	{
		// match 'VTS'
		name: ".VendorID LIKE VTS%",
		rows: 7353,
		expr: func(p *prog) *value {
			return p.hasPrefix(p.dot("VendorID", p.validLanes()), "VTS", true)
		},
	},
	{
		name: ".VendorID LIKE %CMT",
		rows: 1055,
		expr: func(p *prog) *value {
			return p.hasSuffix(p.dot("VendorID", p.validLanes()), "CMT", true)
		},
	},
	{
		// match 'CMT'
		name: ".VendorID LIKE %MT",
		rows: 1055,
		expr: func(p *prog) *value {
			return p.hasSuffix(p.dot("VendorID", p.validLanes()), "MT", true)
		},
	},
	{
		// match 'VTS' or 'DDS'
		name: ".VendorID LIKE %S",
		rows: 7505,
		expr: func(p *prog) *value {
			return p.hasSuffix(p.dot("VendorID", p.validLanes()), "S", true)
		},
	},
	{
		name: ".payment_type LIKE CASH%",
		rows: 5902,
		expr: func(p *prog) *value {
			pt := p.dot("payment_type", p.validLanes())
			return p.hasPrefix(pt, "CASH", true)
		},
	},
	{
		// tickle CSE
		name: ".passenger_count>0 AND .passenger_count<3",
		rows: 6605,
		expr: func(p *prog) *value {
			return p.and(p.greater(p.dot("passenger_count", p.validLanes()), p.constant(0)),
				p.less(p.dot("passenger_count", p.validLanes()), p.constant(3)))
		},
	},
	{
		name: ".trip_distance>1 AND .trip_distance<3",
		rows: 4185,
		expr: func(p *prog) *value {
			return p.and(p.greater(p.dot("trip_distance", p.validLanes()), p.constant(1)),
				p.less(p.dot("trip_distance", p.validLanes()), p.constant(3)))
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), tcs[i].expr(p)))
			var sample prog
			var bc bytecode
			err = p.cloneSymbolize(&st, &sample, &auxbindings{})
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(p)))
			var sample prog
			var bc bytecode
			err = p.cloneSymbolize(&st, &sample, &auxbindings{})
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(p)))
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
			err = p.cloneSymbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())
			var opt strings.Builder
			sample.writeTo(&opt)
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(p)))
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
				err = p.cloneSymbolize(&st, &sample, &auxbindings{})
				if err != nil {
					t.Error(err)
				}
				err = sample.compile(&bc, &st)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("bytecode:\n%s", bc.String())
				var opt strings.Builder
				sample.writeTo(&opt)
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(p)))
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
			err = p.cloneSymbolize(&st, &sample, &auxbindings{})
			if err != nil {
				t.Error(err)
			}
			err = sample.compile(&bc, &st)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bytecode:\n%s", bc.String())
			var opt strings.Builder
			sample.writeTo(&opt)
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(&p)))
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
			p.begin()
			p.returnValue(p.rowsMasked(p.validLanes(), expr(&p)))
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
	base := p.dot(fields[0], p.validLanes())

	fields = fields[1:]
	for i := range fields {
		base = p.dot(fields[i], base)
	}

	return base
}
