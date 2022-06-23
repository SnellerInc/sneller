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
	"io/ioutil"
	"testing"

	"github.com/SnellerInc/sneller/sorting"
)

/*
Input columns in nyc-taxi.block:

    VendorID tpep_pickup_datetime tpep_dropoff_datetime passenger_count
    trip_distance pickup_longitude pickup_latitude RatecodeID store_and_fwd_flag
    dropoff_longitude dropoff_latitude payment_type fare_amount surcharge mta_tax
    tip_amount tolls_amount total_amount
*/

func threads() int {
	return 4
}

func BenchmarkSortSingleStringColumn(b *testing.B) {
	benchmark(b, threads(), []string{"VendorID"})
}

func BenchmarkSortSingleFloatColumn(b *testing.B) {
	benchmark(b, threads(), []string{"total_amount"})
}

func BenchmarkSortSingleIntColumn(b *testing.B) {
	benchmark(b, threads(), []string{"passenger_count"})
}

func BenchmarkSortSingleDatetimeColumn(b *testing.B) {
	benchmark(b, threads(), []string{"tpep_pickup_datetime"})
}

func BenchmarkSortTwoColumnsStringInt(b *testing.B) {
	benchmark(b, threads(), []string{"payment_type", "passenger_count"})
}

func BenchmarkSortTwoColumnsStringString(b *testing.B) {
	benchmark(b, threads(), []string{"VendorID", "payment_type"})
}

func BenchmarkSortTwoColumnsFloatString(b *testing.B) {
	benchmark(b, threads(), []string{"total_amount", "payment_type"})
}

func BenchmarkSortTwoColumnsTimestampFloat(b *testing.B) {
	benchmark(b, threads(), []string{"tpep_pickup_datetime", "total_amount"})
}

func BenchmarkSortThreeColumnsStringFloatInt(b *testing.B) {
	benchmark(b, threads(), []string{"VendorID", "total_amount", "passenger_count"})
}

func benchmark(b *testing.B, parallelism int, columns []string) {
	// given
	input, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		b.Fatal(err)
	}

	orderBy := make([]SortColumn, len(columns))
	for i, col := range columns {
		orderBy[i] = SortColumn{Node: parsePath(col), Direction: sorting.Ascending, Nulls: sorting.NullsFirst}
	}

	b.SetBytes(int64(len(input)))

	output := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		output.Reset()
		sorter := NewOrder(output, orderBy, nil, parallelism)

		// then
		err = CopyRows(sorter, buftbl(input), parallelism)
		if err != nil {
			b.Fatal(err)
		}

		err = sorter.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
