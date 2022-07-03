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

import (
	"fmt"
	"math/rand"
	"testing"
)

// --------------------------------------------------

func TestCountingSortOfEmptySubarray(t *testing.T) {
	keys := make([]uint64, 32)
	indices := keys

	if !countingSortAscUint64(&keys[0], &indices[0], 0) {
		t.Errorf("Sorting routine should have handled an empty array")
	}

	if !countingSortDescUint64(&keys[0], &indices[0], 0) {
		t.Errorf("Sorting routine should have handled an empty array")
	}
}

func TestCountingSortAscDifferentKeys(t *testing.T) {
	start := 1
	for end := 1; end <= 32; end++ {
		keys, indices := testGenerateUniqKeys(start, end)
		testCountingSort(t, keys, indices, countingSortAscUint64, validateAscSortedIndices)
	}
}

func TestCountingSortAscRepeatedKeys(t *testing.T) {
	for size := 1; size <= 32; size++ {
		keys, indices := testGenerateRepeatedKeys(size)
		testCountingSort(t, keys, indices, countingSortAscUint64, validateAscSortedIndices)
	}
}

func TestCountingSortDescDifferentKeys(t *testing.T) {
	start := 1
	for end := 1; end <= 32; end++ {
		keys, indices := testGenerateUniqKeys(start, end)
		testCountingSort(t, keys, indices, countingSortDescUint64, validateDescSortedIndices)
	}
}

func TestCountingSortDescRepeatedKeys(t *testing.T) {
	for size := 1; size <= 32; size++ {
		keys, indices := testGenerateRepeatedKeys(size)
		testCountingSort(t, keys, indices, countingSortDescUint64, validateDescSortedIndices)
	}
}

// --------------------------------------------------

func testGenerateUniqKeys(start, end int) (keys []uint64, indices []uint64) {
	keys = makeKeys(start, end)
	rand.Seed(0)
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	indices = makeIndices(keys)

	return
}

func testGenerateRepeatedKeys(size int) (keys []uint64, indices []uint64) {
	keys = makeKeysWithDuplicates(size, 0.5)
	rand.Seed(0)
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	indices = makeIndices(keys)

	return
}

func testCountingSort(t *testing.T, keys, indices []uint64, sortfn func(*uint64, *uint64, int) bool, validatefn func([]uint64) error) {
	ret := sortfn(&keys[0], &indices[0], len(keys))
	if ret == false {
		t.Errorf("Sorting routine should have handled array of length %d", len(indices))
	}

	err := validatefn(indices)
	if err != nil {
		t.Error(err)
	}
}

func validateAscSortedIndices(indices []uint64) error {
	if !isSortedAscUint64(indices) {
		return fmt.Errorf("Indices are not sorted: %v", indices)
	}

	return nil
}

func validateDescSortedIndices(indices []uint64) error {
	if !isSortedDescUint64(indices) {
		return fmt.Errorf("Indices are not sorted: %v", indices)
	}

	return nil
}

// --------------------------------------------------

func makeIndices(keys []uint64) (indices []uint64) {
	indices = make([]uint64, len(keys))
	for i, k := range keys {
		indices[i] = 100 * k
	}

	return indices
}

func makeKeys(start, end int) (result []uint64) {
	result = make([]uint64, end-start+1)

	i := 0
	for v := start; v <= end; v++ {
		result[i] = uint64(v)
		i += 1
	}

	return result
}

func makeKeysWithDuplicates(size int, repProbability float32) (result []uint64) {
	result = make([]uint64, size)

	v := uint64(1)
	r := 0
	for i := 0; i < size; i++ {
		result[i] = uint64(v)
		if rand.Float32() < repProbability {
			v += 1
		} else {
			r += 1
		}
	}

	return result
}
