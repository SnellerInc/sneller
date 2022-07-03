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
	"math/rand"
	"testing"
)

type partitionfn func(*uint64, *uint64, uint64, int, int) (int, int)
type cmpfn func(uint64, uint64) bool

func lessEqUint64(a, b uint64) bool    { return a <= b }
func greaterEqUint64(a, b uint64) bool { return a >= b }

func TestAVX512PartitionAsc(t *testing.T) {
	for size := 128; size < 512; size++ {
		t.Logf("size=%d", size)
		rand.Seed(0) // make test repeatable
		keys, indices := makeRandomKeyIndices(size)
		testAVX512Partition(t, keys, indices, partitionAscUint64, lessEqUint64, greaterEqUint64)
	}
}

func TestAVX512PartitionDesc(t *testing.T) {
	for size := 128; size < 512; size++ {
		t.Logf("size=%d", size)
		rand.Seed(0) // make test repeatable
		keys, indices := makeRandomKeyIndices(size)
		testAVX512Partition(t, keys, indices, partitionDescUint64, greaterEqUint64, lessEqUint64)
	}
}

func TestAVX512PartitionKeepsAllDataAsc(t *testing.T) {
	for size := 128; size < 512; size++ {
		t.Logf("size=%d", size)
		keys, indices := makeUniqueKeyIndices(size)
		testAVX512PartitionKeepAllData(t, keys, indices, partitionAscUint64)
	}
}

func TestAVX512PartitionAscDegeneratedCase(t *testing.T) {
	for size := 128; size < 512; size++ {
		t.Logf("size=%d", size)
		keys, indices := makeConstKeyUniqueIndices(size)
		testAVX512PartitionDegeratedCase(t, keys, indices, partitionAscUint64)
	}
}

func TestAVX512PartitionDescDegeneratedCase(t *testing.T) {
	for size := 128; size < 512; size++ {
		t.Logf("size=%d", size)
		keys, indices := makeConstKeyUniqueIndices(size)
		testAVX512PartitionDegeratedCase(t, keys, indices, partitionDescUint64)
	}
}

func testAVX512Partition(t *testing.T, keys, indices []uint64, partition partitionfn, le, ge cmpfn) {
	// given
	pivot := keys[len(keys)/2]

	// when
	left, right := partition(&keys[0], &indices[0], pivot, 0, len(keys)-1)

	// then
	for i := 0; i < left; i++ {
		if !le(keys[i], pivot) {
			t.Errorf("[0..left-1] must not contain keys greater than the pivot")
			break
		}
	}

	for i := right + 1; i < len(keys); i++ {
		if !ge(keys[i], pivot) {
			t.Errorf("[right+1..len(keys)-1] must not contain keys less than the pivot")
			break
		}
	}

	for i := range keys {
		if keys[i]*10 != indices[i] {
			t.Errorf("indices and keys must be shuffled exactly the same way")
			break
		}
	}
}

func testAVX512PartitionKeepAllData(t *testing.T, keys, indices []uint64, partition partitionfn) {
	// given
	pivot := keys[len(keys)/2]

	// when
	partition(&keys[0], &indices[0], pivot, 0, len(keys)-1)

	// then
	if !allValuesAreUnique(keys) {
		t.Errorf("data corruption: keys got duplicated")
	}

	if !allValuesAreUnique(indices) {
		t.Errorf("data corruption: indices got duplicated")
	}
}

func testAVX512PartitionDegeratedCase(t *testing.T, keys, indices []uint64, partition partitionfn) {
	// given
	pivot := keys[len(keys)/2]

	// when
	left, right := partition(&keys[0], &indices[0], pivot, 0, len(keys)-1)

	if left == 0 {
		t.Errorf("vectorized partition should do some work")
	}

	if right == len(keys)-1 {
		t.Errorf("vectorized partition should do some work")
	}

	// then
	if !allValuesAreUnique(indices) {
		t.Errorf("data corruption: indices got duplicated")
	}
}

func makeRandomKeyIndices(size int) (keys []uint64, indices []uint64) {
	keys = make([]uint64, size)
	indices = make([]uint64, size)
	for i := range keys {
		keys[i] = rand.Uint64() / 10
		indices[i] = keys[i] * 10
	}

	return keys, indices
}

func makeUniqueKeyIndices(size int) (keys []uint64, indices []uint64) {
	keys = make([]uint64, size)
	indices = make([]uint64, size)
	for i := range keys {
		keys[i] = uint64(i) + 1
	}

	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := range keys {
		indices[i] = keys[i] * 10
	}

	return keys, indices
}

func allValuesAreUnique(arr []uint64) bool {
	seen := make(map[uint64]bool)
	for _, v := range arr {
		if seen[v] {
			return false
		}
	}

	return true
}

func makeConstKeyUniqueIndices(size int) (keys []uint64, indices []uint64) {
	keys = make([]uint64, size)
	indices = make([]uint64, size)
	for i := range keys {
		keys[i] = 42
		indices[i] = 100 + uint64(i)
	}

	return keys, indices
}
