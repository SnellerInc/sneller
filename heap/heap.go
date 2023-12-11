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

// Package heap implements generic heap functions.
package heap

// FixSlice fixes the element x[index] in order
// to preserve the min-heap invariant determined
// by the provided comparison function.
func FixSlice[T any](x []T, index int, less func(x, y T) bool) {
	siftDown(x, index, less)
	siftUp(x, index, less)
}

// PopSlice removes the "smallest" element from x
// based on the provided comparison function
// and updates x appropriately to preserve the
// heap invariant.
func PopSlice[T any](x *[]T, less func(x, y T) bool) T {
	ret := (*x)[0]
	(*x)[0], *x = (*x)[len(*x)-1], (*x)[:len(*x)-1]
	if len(*x) > 0 {
		siftDown((*x), 0, less)
	}
	return ret
}

// PushSlice adds item to x while preserving
// the min-heap invariant determined by the
// provided comparison function.
func PushSlice[T any](x *[]T, item T, less func(x, y T) bool) {
	*x = append(*x, item)
	siftUp(*x, len(*x)-1, less)
}

// OrderSlice shuffles x into min-heap ordering
// according to the provided comparison function.
// If len(x) > 0, the "smallest" element in x will
// always be x[0].
func OrderSlice[T any](x []T, less func(x, y T) bool) {
	for i := len(x) - 1; i >= 0; i-- {
		siftDown(x, i, less)
		siftUp(x, i, less)
	}
}

func siftUp[T any](x []T, index int, less func(x, y T) bool) {
	for index > 0 {
		p := (index - 1) / 2
		if less(x[p], x[index]) {
			break
		}
		x[p], x[index] = x[index], x[p]
		index = p
	}
}

func siftDown[T any](x []T, index int, less func(x, y T) bool) {
	for {
		left := (index * 2) + 1
		right := left + 1
		if left >= len(x) {
			break
		}
		c := left
		if len(x) > right && less(x[right], x[left]) {
			c = right
		}
		if less(x[index], x[c]) {
			break
		}
		x[c], x[index] = x[index], x[c]
		index = c
	}
}
