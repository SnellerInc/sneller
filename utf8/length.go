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
