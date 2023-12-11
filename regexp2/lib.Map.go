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
