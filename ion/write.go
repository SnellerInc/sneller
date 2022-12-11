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

package ion

// UnsafeWriteUVarint encodes uv as a uvarint number.
// Returns the number of stored bytes.
//
// It's required that dst has enough room for the encoded
// number (i.e., len(buf) >= Uvsize(uv)).
func UnsafeWriteUVarint(dst []byte, uv uint) int {
	ret := Uvsize(uv)
	off := ret - 1
	dst[off] = byte(uv&0x7f) | 0x80
	for off > 0 {
		off--
		uv >>= 7
		dst[off] = byte(uv & 0x7f)
	}
	return ret
}

// UnsafeWriteTag writes the header of an Ion object having size.
// Returns the number of stored bytes
//
// It's required that dst has enough room for the encoded header.
func UnsafeWriteTag(dst []byte, tag Type, size uint) int {
	if size < 14 {
		dst[0] = byte(tag<<4) | byte(size)
		return 1
	}
	dst[0] = byte(tag<<4) | 0xe
	return 1 + UnsafeWriteUVarint(dst[1:], size)
}

// NopPadding writes a header for Ion NOP operation for given padsize.
// It returns the size of header and the number of pad bytes that
// have to be written after the header.
//
// FIXME: it's not possible to produce exact padding for all possible
// padsize. The procedure never returns values that would execeed
// the required padding.
func NopPadding(dst []byte, padsize int) (int, int) {
	if padsize <= 0 {
		return 0, 0
	}

	padsize -= 1 // we have to emit TLV byte

	if padsize < 14 {
		dst[0] = byte(NullType<<4) | byte(padsize)
		return 1, padsize
	}

	dst[0] = byte(NullType<<4) | 0xe

	size1 := Uvsize(uint(padsize))
	size2 := Uvsize(uint(padsize - size1))
	if size1 == size2 {
		padsize -= size1
	} else {
		// size2+1 == size1
		padsize -= size1 + 1
	}

	written := UnsafeWriteUVarint(dst[1:], uint(padsize))
	return 1 + written, padsize
}
