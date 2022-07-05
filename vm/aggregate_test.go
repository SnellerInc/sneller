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
	"encoding/binary"
	"io/ioutil"
	"math"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func isFloat64Near(a, b float64) bool {
	diff := math.Abs(a - b)
	return diff < 0.0000001
}

// aggregate queries for testing+benchmarking
var nycAggregateTestQueries = []struct {
	name string  // name of test
	aggr float64 // expected aggregation output
	path string
	op   expr.AggregateOp
}{
	{
		name: "SUM(.fare_amount)",
		aggr: 81110.09927749634,
		path: "fare_amount",
		op:   expr.OpSum,
	},
	{
		name: "MIN(.fare_amount)",
		aggr: 2.5,
		path: "fare_amount",
		op:   expr.OpMin,
	},
	{
		name: "MAX(.fare_amount)",
		aggr: 116.9000015258789,
		path: "fare_amount",
		op:   expr.OpMax,
	},
}

func TestAggregateSSANYCQueries(t *testing.T) {
	buf, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}

	var st ion.Symtab
	_, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	// produce a new copy of the table
	table := func() Table {
		return NewReaderAtTable(bytes.NewReader(buf), int64(len(buf)), defaultAlign)
	}

	for _, testQuery := range nycAggregateTestQueries {
		name := testQuery.name
		want := testQuery.aggr
		field := testQuery.path
		op := testQuery.op
		t.Run(name, func(t *testing.T) {
			var out QueryBuffer
			agg := Aggregation{AggBinding{
				Expr:   &expr.Aggregate{Op: op, Inner: path(t, field)},
				Result: "x",
			}}
			aggregateQuery, err := NewAggregate(agg, &out)
			if err != nil {
				t.Fatal(err)
			}
			err = CopyRows(aggregateQuery, table(), 4)
			if err != nil {
				t.Fatal(err)
			}

			got := math.Float64frombits(binary.LittleEndian.Uint64(aggregateQuery.AggregatedData))
			if !isFloat64Near(got, want) {
				t.Errorf("Aggregation failed: Result=%.10f; Expected=%.10f", got, want)
			}
		})
	}
}

func TestAggregateSSACompileWithFilters(t *testing.T) {
	// given
	/* we test a query like:

		   SELECT COUNT(*) as total,
		          COUNT(*) FILTER (WHERE x > 0) as positive,
		          COUNT(*) FILTER (WHERE x < 0) as negative,
		          SUM(x) FILTER (WHERE x > 0) as sum_positive,
		          SUM(x) FILTER (WHERE x < 0) as sum_negative
		          AVG(x) FILTER (WHERE x > 0) as avg_positive,
		          AVG(x) FILTER (WHERE x < 0) as avg_negative
		   FROM tbl

	       And make sure that we have just two filters there (for x > 0 and x < 0)
	*/
	exprs := []AggBinding{
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpCount,
				Inner: expr.Star{},
			},
			Result: "total",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpCount,
				Inner: expr.Star{},
				Filter: &expr.Comparison{
					Op:    expr.Greater,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "positive",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpCount,
				Inner: expr.Star{},
				Filter: &expr.Comparison{
					Op:    expr.Less,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "negative",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpSum,
				Inner: &expr.Path{First: "x"},
				Filter: &expr.Comparison{
					Op:    expr.Greater,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "sum_positive",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpSum,
				Inner: &expr.Path{First: "x"},
				Filter: &expr.Comparison{
					Op:    expr.Less,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "sum_positive",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpAvg,
				Inner: &expr.Path{First: "x"},
				Filter: &expr.Comparison{
					Op:    expr.Greater,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "avg_positive",
		},
		{
			Expr: &expr.Aggregate{
				Op:    expr.OpAvg,
				Inner: &expr.Path{First: "x"},
				Filter: &expr.Comparison{
					Op:    expr.Less,
					Left:  &expr.Path{First: "x"},
					Right: expr.Integer(0),
				},
			},
			Result: "avg_positive",
		},
	}

	aggregate := Aggregate{
		bind: exprs,
	}

	/* The program for the input query is shown below. This
	   test checks just presence of a few of crucial opcodes,

			init
			undef
			initmem
			aggcount m2 k0 $0
			dot b0 k0 $x
			literal $0
			toint u1 v4 k4
			cmpgt.imm.i i6 k6 $0
			nand.k k6 k4
			tofloat u1 v4 k8
			cmpgt.imm.f f9 k9 $0
			or.k k10 k7
			or.k k6 k9
			aggcount m2 k11 $8
			literal $0
			cmplt.imm.i i6 k6 $0
			cmplt.imm.f f9 k9 $0
			or.k k16 k15
			aggcount m2 k17 $16
			literal $0
			tofloat u1 v4 k4
			nand.k k20 k4
			toint u20 v4 k21
			cvt.i@f i22 k22
			or.k k23 k20
			and.k k24 k11
			aggsum.f m2 f23 k25 $24
			literal $0
			and.k k24 k17
			aggsum.f m2 f23 k28 $40
			literal $0
			aggavg.f m2 f23 k25 $56
			literal $0
			aggavg.f m2 f23 k28 $72
			mergemem m3 m13 m18 m26 m29 m31 m33
	*/
	expected := map[string]int{
		// from filter x > 0
		"cmpgt.imm.i": 1,
		"cmpgt.imm.f": 1,

		// from filter x < 0
		"cmplt.imm.i": 1,
		"cmplt.imm.f": 1,

		// the aggregates
		"aggcount": 3,
		"aggsum.f": 2,
		"aggavg.f": 2,
	}

	// when
	err := aggregate.compileAggregate(aggregate.bind)
	if err != nil {
		t.Fatal(err)
	}

	// then
	hist := opcodesHistogram(aggregate.prog.values)

	ok := true
	for name, count := range expected {
		got := hist[name]
		if got != count {
			t.Logf("opcode %s - want: %d, got %d", name, count, got)
			ok = false
		}
	}

	if !ok {
		t.Error("wrong aggregation program")
	}
}

func opcodesHistogram(values []*value) map[string]int {
	h := make(map[string]int)
	for _, v := range values {
		h[v.op.String()] += 1
	}

	return h
}
