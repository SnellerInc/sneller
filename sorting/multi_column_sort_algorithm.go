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

package sorting

import (
	"sort"
)

// Implementation of a multithread quicksort

// Note: the prefix "mcs" is derived from "multi-column sort"
type mcsArguments struct {
	data        *multipleColumnsData
	consumer    SortedDataConsumer
	mindistance int
}

func multiColumnSort(data *multipleColumnsData, pool ThreadPool, consumer SortedDataConsumer) {
	args := mcsArguments{
		data:        data,
		consumer:    consumer,
		mindistance: data.rp.QuicksortSplitThreshold,
	}

	pool.Enqueue(0, data.Len()-1, mcsThreadFunction, args)
}

func mcsThreadFunction(left int, right int, args interface{}, pool ThreadPool) {

	arguments := args.(mcsArguments)

	distance := right - left + 1
	if distance < arguments.mindistance {
		mcsSortSubrange(arguments.data, left, right)
		arguments.consumer.Notify(left, right)
		return
	}

	pivot := (left + right) / 2

	i, j := mscPartition(arguments.data, pivot, left, right)

	if left <= j {
		if arguments.data.limit.disjoint(indicesRange{left, j}) {
			arguments.consumer.Notify(left, j)
		} else {
			pool.Enqueue(left, j, mcsThreadFunction, args)
		}
	}

	if i <= right {
		if arguments.data.limit.disjoint(indicesRange{i, right}) {
			arguments.consumer.Notify(i, right)
		} else {
			pool.Enqueue(i, right, mcsThreadFunction, args)
		}
	}

	if j+1 <= i-1 {
		arguments.consumer.Notify(j+1, i-1)
	}
}

func mscPartition(data *multipleColumnsData, pivotIndex, left, right int) (int, int) {
	// Take a snapshot of pivot tuple, as the index of tuple may get overwritten
	// during parition -- that's a trait of this particular variant of partition.
	pivot := data.tuple(pivotIndex)

	lessThan := func(i int) bool {
		return data.lessIndexTupleUnsafe(i, pivot)
	}

	greaterThan := func(i int) bool {
		return data.lessTupleIndexUnsafe(pivot, i)
	}

	for left <= right {
		for lessThan(left) { // keys[left] < pivot
			left += 1
		}

		for greaterThan(right) { // keys[right] > pivot
			right -= 1
		}

		if left <= right {
			data.Swap(left, right)

			left += 1
			right -= 1
		}
	}

	return left, right
}

func mcsSortSubrange(data *multipleColumnsData, left, right int) {
	view := multipleColumnsData{
		records:    data.records[left : right+1],
		directions: data.directions,
		nullsOrder: data.nullsOrder,
		rp:         data.rp,
	}

	sort.Sort(&view)
}
