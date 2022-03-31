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

package vm

import (
	"encoding/binary"

	"github.com/SnellerInc/sneller/ion"
)

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

func putenc(dst []byte, val uint32, size int8) int {
	if len(dst) >= 4 {
		binary.LittleEndian.PutUint32(dst, val)
	} else {
		for i := 0; i < int(size); i++ {
			dst[i] = byte(val)
			val >>= 8
		}
	}
	return int(size)
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

// encsize returns the number of bytes
// required to encode an object of size 'l'
// (including the 1-byte descriptor tag)
func encsize(l uint) int {
	if l < 14 {
		return 1
	}
	return 1 + ion.UVarintSize(l)
}
