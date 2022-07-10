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

package regexp2

import "golang.org/x/exp/slices"

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
