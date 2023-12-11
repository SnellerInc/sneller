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

package vm

import (
	"bytes"
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

			fn := aggregateQuery.aggregateOps[0].fn
			if finalize := aggregateOpInfoTable[fn].finalizeFunc; finalize != nil {
				finalize(aggregateQuery.AggregatedData)
			}

			got := getfloat64(aggregateQuery.AggregatedData, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !isFloat64Near(got, want) {
				t.Errorf("Aggregation failed: Result=%.10f; Expected=%.10f", got, want)
			}
		})
	}
}
