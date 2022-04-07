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

package sort

// Code generated by generator.go; DO NOT EDIT.

func scalarQuicksortAscUint64(left int, right int, args interface{}, pool ThreadPool) {

	arguments := args.(scalarSortArgumentsUint64)

	distance := right - left + 1
	if distance < arguments.mindistance {
		scalarQuicksortAscUint64SingleThread(arguments.keys, arguments.indices, left, right)
		arguments.consumer.Notify(left, right)
		return
	}

	pivot := arguments.keys[(left+right)/2]

	i, j := scalarPartitionAscUint64(arguments.keys, arguments.indices, pivot, left, right)

	if left <= j {
		pool.Enqueue(left, j, scalarQuicksortAscUint64, args)
	}

	if i <= right {
		pool.Enqueue(i, right, scalarQuicksortAscUint64, args)
	}

	if j+1 <= i-1 {
		arguments.consumer.Notify(j+1, i-1)
	}
}

func scalarQuicksortAscUint64SingleThread(keys []uint64, indices []uint64, left int, right int) {

	pivot := keys[(left+right)/2]

	i, j := scalarPartitionAscUint64(keys, indices, pivot, left, right)

	if left < j {
		scalarQuicksortAscUint64SingleThread(keys, indices, left, j)
	}

	if i < right {
		scalarQuicksortAscUint64SingleThread(keys, indices, i, right)
	}
}

func scalarPartitionAscUint64(keys []uint64, indices []uint64, pivot uint64, left int, right int) (int, int) {
	for left <= right {
		for keys[left] < pivot {
			left += 1
		}

		for keys[right] > pivot {
			right -= 1
		}

		if left <= right {
			keys[left], keys[right] = keys[right], keys[left]
			indices[left], indices[right] = indices[right], indices[left]

			left += 1
			right -= 1
		}
	}

	return left, right
}

func scalarQuicksortDescUint64(left int, right int, args interface{}, pool ThreadPool) {

	arguments := args.(scalarSortArgumentsUint64)

	distance := right - left + 1
	if distance < arguments.mindistance {
		scalarQuicksortDescUint64SingleThread(arguments.keys, arguments.indices, left, right)
		arguments.consumer.Notify(left, right)
		return
	}

	pivot := arguments.keys[(left+right)/2]

	i, j := scalarPartitionDescUint64(arguments.keys, arguments.indices, pivot, left, right)

	if left <= j {
		pool.Enqueue(left, j, scalarQuicksortDescUint64, args)
	}

	if i <= right {
		pool.Enqueue(i, right, scalarQuicksortDescUint64, args)
	}

	if j+1 <= i-1 {
		arguments.consumer.Notify(j+1, i-1)
	}
}

func scalarQuicksortDescUint64SingleThread(keys []uint64, indices []uint64, left int, right int) {

	pivot := keys[(left+right)/2]

	i, j := scalarPartitionDescUint64(keys, indices, pivot, left, right)

	if left < j {
		scalarQuicksortDescUint64SingleThread(keys, indices, left, j)
	}

	if i < right {
		scalarQuicksortDescUint64SingleThread(keys, indices, i, right)
	}
}

func scalarPartitionDescUint64(keys []uint64, indices []uint64, pivot uint64, left int, right int) (int, int) {
	for left <= right {
		for keys[left] < pivot {
			left += 1
		}

		for keys[right] > pivot {
			right -= 1
		}

		if left <= right {
			keys[left], keys[right] = keys[right], keys[left]
			indices[left], indices[right] = indices[right], indices[left]

			left += 1
			right -= 1
		}
	}

	return left, right
}
