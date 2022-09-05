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
