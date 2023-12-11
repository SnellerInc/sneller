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

package vm

// objsize parses an ion object TLV descriptor
// and returns the number of bytes in the descriptor
// and the size of the subsequent object (in that order)
func objsize(buf []byte) (int32, int32) {
	b0 := buf[0]
	if b0>>4 == 1 {
		return 1, 0 // boolean
	}
	nibble := b0 & 0xf
	switch nibble {
	case 15:
		return 1, 0 // null
	case 14:
		vi, used := uvint(buf[1:])
		return int32(used + 1), int32(vi)
	default:
		return 1, int32(nibble)
	}
}

// copyobj copies an 10n binary object from 'src'
// into 'dst' and returns the number of bytes copied
func copyobj(dst, src []byte) int {
	// TODO: write this in assembly? we'd
	// likely be able to avoid a couple
	// bounds checks...
	b0 := src[0]
	nibble := b0 & 0xf
	tag := b0 >> 4
	if tag == 1 || nibble == 0 || nibble == 15 {
		dst[0] = b0
		return 1
	}
	if nibble == 14 {
		// complex case: decode uvarint
		j := 1
		l := 0
		for {
			l <<= 7
			l |= int(src[j] & 0x7f)
			if src[j]&0x80 != 0 {
				break
			}
			j++
		}
		l += j + 1
		return copy(dst, src[:l])
	}
	return copy(dst, src[:1+nibble])
}

func uvint(buf []byte) (uint, int) {
	out := uint(0)
	i := 0
	for {
		out <<= 7
		out |= uint(buf[i] & 0x7f)
		if buf[i]&0x80 != 0 {
			break
		}
		i++
	}
	return out, i + 1
}
