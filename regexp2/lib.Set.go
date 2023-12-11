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
