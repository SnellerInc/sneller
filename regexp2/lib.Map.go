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

import "fmt"

type mapT[K comparable, V comparable] map[K]V

func newMap[K comparable, V comparable]() mapT[K, V] {
	return map[K]V{}
}

// at access value at key
func (m *mapT[K, V]) at(k K) V {
	if value, present := (*m)[k]; present {
		return value
	}
	panic(fmt.Sprintf("98f3cef3: Key %v not present in map %v", k, m))
}

// insert key value pair into map
func (m *mapT[K, V]) insert(k K, v V) {
	(*m)[k] = v
}

func (m *mapT[K, V]) clear() {
	*m = map[K]V{}
}

// containsKey test whether key is present
func (m *mapT[K, V]) containsKey(k K) bool {
	_, present := (*m)[k]
	return present
}
