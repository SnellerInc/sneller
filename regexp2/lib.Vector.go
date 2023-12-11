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

package regexp2

import "slices"

type vectorT[T comparable] []T

func newVector[T comparable]() vectorT[T] {
	var v vectorT[T]
	return v
}

// empty test whether vector is empty
func (v *vectorT[T]) empty() bool {
	return len(*v) == 0
}

// size gets size of vector
func (v *vectorT[T]) size() int {
	return len(*v)
}

// at access element at index
func (v *vectorT[T]) at(index int) T {
	return (*v)[index]
}

// pushBack element to back of vector
func (v *vectorT[T]) pushBack(e T) {
	*v = append(*v, e) // append to end
}

// contains test whether value is present
func (v *vectorT[T]) contains(e T) bool {
	return slices.Contains(*v, e)
}

// clear the vector but keep its capacity
func (v *vectorT[T]) clear() {
	*v = (*v)[:0]
}
