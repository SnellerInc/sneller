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

type bitSetT []uint64

func newBitSet() bitSetT {
	return make([]uint64, 0)
}

// contains test whether value is present
func (s *bitSetT) contains(e int) bool {
	idx := e >> 6
	if idx >= len(*s) {
		return false
	}
	return ((*s)[idx] & (uint64(1) << uint(e&0b111111))) != 0
}

// insert element to set, return true when set changed; false otherwise
func (s *bitSetT) insert(e int) {
	idx := e >> 6
	length := len(*s)
	for idx >= length {
		*s = append(*s, 0)
		length++
	}
	(*s)[idx] |= uint64(1) << (e & 0b111111)
}
