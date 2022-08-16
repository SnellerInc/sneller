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
	"math"
	"os"
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
		aggr: 81110.0993745000,
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
		aggr: 116.9,
		path: "fare_amount",
		op:   expr.OpMax,
	},
}

func TestAggregateSSANYCQueries(t *testing.T) {
	buf, err := os.ReadFile("../testdata/nyc-taxi.block")
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
