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

import "golang.org/x/exp/maps"

type setT[T comparable] map[T]struct{}

func newSet[T comparable]() setT[T] {
	return map[T]struct{}{}
}

// Empty test whether set is empty
func (s *setT[T]) empty() bool {
	return len(*s) == 0
}

// contains test whether value is present
func (s *setT[T]) contains(e T) bool {
	_, present := (*s)[e]
	return present
}

// insert element to set
func (s *setT[T]) insert(e T) {
	(*s)[e] = struct{}{}
}

// Erase element from set
func (s *setT[T]) erase(e T) {
	delete(*s, e)
}

func (s *setT[T]) clear() {
	*s = map[T]struct{}{}
}

func (s *setT[T]) toVector() vectorT[T] {
	return maps.Keys(*s)
}

func (s *setT[T]) equal(other *setT[T]) bool {
	return maps.Equal(*s, *other)
}

func (s *setT[T]) len() int {
	return len(*s)
}
