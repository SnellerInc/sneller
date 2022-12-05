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

// Package utf8 provides additional UTF-8 related functions.
package utf8

import (
	"encoding/binary"
	"math/bits"
)

// ValidStringLength returns the number of runes in a valid UTF-8 string
func ValidStringLength(str []byte) int {
	n := len(str)
	continuation := 0
	// We count how many continuation bytes (0b10xx_xxxxxx) are there.
	// Then the remaining bytes are leading bytes, and it's the number of runes.

	// process 8 bytes at once using a SWAR algorithm
	for len(str) >= 8 {
		qword := binary.LittleEndian.Uint64(str)
		str = str[8:]

		bit7 := qword & 0x8080808080808080
		if bit7 == 0 {
			// all 8 bytes are ASCII chars
			continue
		}

		bit6 := qword << 1
		comb := bit7 &^ bit6 // bit7 = 1 and bit6 = 0 => continuation byte
		continuation += bits.OnesCount64(comb)
	}

	// process the remaining 1..7 bytes
	for _, b := range str {
		if b&0b11_000000 == 0b10_000000 {
			continuation += 1
		}
	}

	return n - continuation
}
