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

// UnsafeAppendTag appends a type+length tag of the
// given type and size to dst and returns the extended buffer.
func UnsafeAppendTag(dst []byte, tag Type, size uint) []byte {
	if size < 14 {
		return append(dst, byte(tag<<4)|byte(size))
	}
	dst = append(dst, byte(tag<<4)|0xe)
	uv := Uvsize(size)
	for uv > 1 {
		uv--
		shift := uv * 7
		dst = append(dst, byte((size>>shift)&0x7f))
	}
	return append(dst, byte(size&0x7f)|0x80)
}

// NopPadding writes a header for Ion NOP operation for given padsize.
// It returns the size of header and the number of pad bytes that
// have to be written after the header.
//
// FIXME: it's not possible to produce exact padding for all possible
// padsize. The procedure never returns values that would exceed
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
