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

package heap

import (
	"math/rand"
	"slices"
	"testing"
)

func TestHeap(t *testing.T) {
	x := make([]int, 0, 1000)
	less := func(x, y int) bool {
		return x < y
	}
	for len(x) < cap(x) {
		PushSlice(&x, rand.Int(), less)
	}
	sorted := make([]int, 0, len(x))
	for len(x) > 0 {
		sorted = append(sorted, PopSlice(&x, less))
	}
	if !slices.IsSorted(sorted) {
		t.Fatal("not sorted")
	}

	for len(x) < cap(x) {
		PushSlice(&x, rand.Int(), less)
	}
	// disturb ordering, then Fix
	x[len(x)/2] = 1
	FixSlice(x, len(x)/2, less)
	sorted = sorted[:0]
	for len(x) > 0 {
		sorted = append(sorted, PopSlice(&x, less))
	}
	if !slices.IsSorted(sorted) {
		t.Fatal("not sorted after FixSlice")
	}
}
