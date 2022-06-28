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
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"runtime"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func mkagg(kind expr.AggregateOp, path, as string) AggBinding {
	p, err := expr.ParsePath(path)
	if err != nil {
		panic(err)
	}
	return AggBinding{Expr: &expr.Aggregate{Op: kind, Inner: p}, Result: as}
}

type testcol struct {
	name   string
	values []ion.Datum
}

// all of these test are run on
// the nyc-taxi data block repeated
// four times consecutively, so you
// can reproduce the expected output
// by concatenating that file four times
// and then dumping it as JSON into jq
var haggTests = []struct {
	agg      Aggregation
	group    expr.Node // field on which to group
	aggorder []int     // ordering of aggregation fields, if non-nil
	limit    int       // limit, if non-zero
	output   []testcol // output in column-major order
}{
	{
		// select VendorID, count(VendorID) group by VendorID
		agg:      Aggregation{mkagg(expr.OpCount, "VendorID", "count")},
		group:    path(nil, "VendorID"),
		aggorder: []int{0}, // order by count(VendorID)
		output: []testcol{
			{name: "VendorID", values: []ion.Datum{ion.String("DDS"), ion.String("CMT"), ion.String("VTS")}},
			// note: 608 + 4220 + 29412 = 8560 * 4
			{name: "count", values: []ion.Datum{ion.Uint(608), ion.Uint(4220), ion.Uint(29412)}},
		},
	},
	{
		// select VendorID, sum(passenger_count) as total group by VendorID
		agg:      Aggregation{mkagg(expr.OpSum, "passenger_count", "total")},
		group:    path(nil, "VendorID"),
		aggorder: []int{0}, // order by sum(passenger_count)
		output: []testcol{
			{name: "VendorID", values: []ion.Datum{ion.String("DDS"), ion.String("CMT"), ion.String("VTS")}},
			{name: "total", values: []ion.Datum{ion.Uint(900), ion.Uint(5504), ion.Uint(60904)}},
		},
	},
	{
		// select VendorID, min(passenger_count), max(passenger_count) group by VendorID order by min(passenger_count)
		agg:      Aggregation{mkagg(expr.OpMin, "passenger_count", "min"), mkagg(expr.OpMax, "passenger_count", "max")},
		group:    path(nil, "VendorID"),
		aggorder: []int{0},
		output: []testcol{
			{name: "VendorID", values: []ion.Datum{ion.String("VTS"), ion.String("DDS"), ion.String("CMT")}},
			{name: "min", values: []ion.Datum{ion.Uint(1), ion.Uint(1), ion.Uint(1)}},
			{name: "max", values: []ion.Datum{ion.Uint(6), ion.Uint(4), ion.Uint(5)}},
		},
	},
	{
		agg:      Aggregation{mkagg(expr.OpCount, "payment_type", "count")},
		group:    path(nil, "payment_type"),
		aggorder: []int{0}, // order by count(payment_type)
		output: []testcol{
			{name: "payment_type", values: []ion.Datum{ion.String("Dispute"), ion.String("No Charge"), ion.String("CREDIT"), ion.String("Cash"), ion.String("Credit"), ion.String("CASH")}},
			{name: "count", values: []ion.Datum{ion.Uint(4 * 1), ion.Uint(4 * 6), ion.Uint(4 * 33), ion.Uint(4 * 821), ion.Uint(4 * 1797), ion.Uint(4 * 5902)}},
		},
	},
}

func TestHashAggregate(t *testing.T) {
	buf, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}

	tcs := haggTests
	for i := range tcs {
		agg := tcs[i].agg
		group := tcs[i].group
		outcols := tcs[i].output
		name := agg.String() + " GROUP BY " + expr.ToString(group)
		ordering := tcs[i].aggorder
		t.Run(name, func(t *testing.T) {
			var qb QueryBuffer
			ha, err := NewHashAggregate(agg, Selection{{Expr: group}}, &qb)
			if err != nil {
				t.Fatal(err)
			}
			for i := range ordering {
				ha.OrderByAggregate(ordering[i], false)
			}
			// simulate the table being 4x repeated:
			intable := &looptable{chunk: buf, count: 4}
			err = intable.WriteChunks(ha, int(intable.count))
			if err != nil {
				t.Fatal(err)
			}
			err = ha.Close()
			if err != nil {
				t.Fatal(err)
			}
			outbuf := qb.Bytes()
			var st ion.Symtab
			var d ion.Datum
			rownum := 0
			for len(outbuf) > 0 {
				if ion.TypeOf(outbuf) == ion.NullType && ion.SizeOf(outbuf) > 1 {
					// nop pad
					outbuf = outbuf[ion.SizeOf(outbuf):]
					continue
				}
				d, outbuf, err = ion.ReadDatum(&st, outbuf)
				if err != nil {
					t.Fatalf("reading datum: %s", err)
				}
				s, ok := d.(*ion.Struct)
				if !ok {
					t.Fatalf("top-level datum isnt a struct: %#v", d)
				}
				if s.Len() != len(outcols) {
					t.Errorf("output row %d: has %d columns; want %d", rownum, s.Len(), len(outcols))
				}
				s.Each(func(f ion.Field) bool {
					name := f.Label
					var inner ion.Datum
					for j := range outcols {
						if outcols[j].name == name {
							inner = outcols[j].values[rownum]
							break
						}
					}
					if inner == nil {
						t.Fatalf("output row %d: unexpected field %q", rownum, name)
					}
					val := f.Value
					if !reflect.DeepEqual(val, inner) {
						t.Errorf("row %d field %q - got %#v want %#v", rownum, name, val, inner)
					}
					return true
				})
				rownum++
			}
		})
	}
}

type nopSink struct{}

func (n nopSink) Open() (io.WriteCloser, error) {
	return n, nil
}

func (n nopSink) Write(p []byte) (int, error) {
	return len(p), nil
}

func (n nopSink) Close() error { return nil }

func BenchmarkHashAggregate(b *testing.B) {
	nycbuf, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		b.Fatal(err)
	}
	const nycSize = 0xffef0 // actual ion bytes per chunk

	bcs := haggTests
	for i := range bcs {
		agg := bcs[i].agg
		group := bcs[i].group
		name := agg.String() + " GROUP BY " + expr.ToString(group)
		ordering := bcs[i].aggorder
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			b.Logf("name: %s", name)
			ha, err := NewHashAggregate(agg, Selection{{Expr: group}}, nopSink{})
			if err != nil {
				b.Fatal(err)
			}
			for i := range ordering {
				ha.OrderByAggregate(ordering[i], false)
			}
			b.ReportAllocs()
			b.SetBytes(nycSize)
			parallel := runtime.GOMAXPROCS(0)
			lp := &looptable{chunk: nycbuf, count: int64(b.N)}
			b.ResetTimer()
			err = CopyRows(ha, lp, parallel)
			if err != nil {
				b.Fatal(err)
			}
		})
	}
}
