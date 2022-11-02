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

package ints

import (
	"unsafe"

	"golang.org/x/exp/constraints"
)

// TestBit check if the k-th bit is set in range "in"
func TestBit[T, K constraints.Integer](in []T, k K) bool {
	return (in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] & (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))) != 0
}

// SetBit sets the k-th bit in range "in"
func SetBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] |= (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

// ClearBit clears the k-th bit in range "in"
func ClearBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] &= ^(T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

// FlipBit inverts the k-th bit in range "in"
func FlipBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] ^= (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

// SetBits sets the bits [first, last) in range "in"
func SetBits[T, K constraints.Integer](in []T, first, last K) {
	bitsPerT := unsafe.Sizeof(in[0]) * 8
	mskT := bitsPerT - 1
	ones := (uint64(1) << bitsPerT) - 1
	firstIdx := uintptr(first) / bitsPerT
	firstMask := T(ones << (uintptr(first) & mskT))
	lastIdx := uintptr(last-1) / bitsPerT
	lastMask := T(ones >> ((uintptr(last-1) & mskT) ^ mskT))

	if firstIdx == lastIdx {
		in[firstIdx] |= (firstMask & lastMask)
	} else {
		in[firstIdx] |= firstMask
		for i := firstIdx + 1; i != lastIdx; i++ {
			in[i] = ^T(0)
		}
		in[lastIdx] |= lastMask
	}
}

// ClearBits clears the bits [first, last) in range "in"
func ClearBits[T, K constraints.Integer](in []T, first, last K) {
	bitsPerT := unsafe.Sizeof(in[0]) * 8
	mskT := bitsPerT - 1
	ones := (uint64(1) << bitsPerT) - 1
	firstIdx := uintptr(first) / bitsPerT
	firstMask := T(ones << (uintptr(first) & mskT))
	lastIdx := uintptr(last-1) / bitsPerT
	lastMask := T(ones >> ((uintptr(last-1) & mskT) ^ mskT))

	if firstIdx == lastIdx {
		in[firstIdx] &= ^(firstMask & lastMask)
	} else {
		in[firstIdx] &= ^firstMask
		for i := firstIdx + 1; i != lastIdx; i++ {
			in[i] = T(0)
		}
		in[lastIdx] &= ^lastMask
	}
}

// FlipBits inverts the bits [first, last) in range "in"
func FlipBits[T, K constraints.Integer](in []T, first, last K) {
	bitsPerT := unsafe.Sizeof(in[0]) * 8
	mskT := bitsPerT - 1
	ones := (uint64(1) << bitsPerT) - 1
	firstIdx := uintptr(first) / bitsPerT
	firstMask := T(ones << (uintptr(first) & mskT))
	lastIdx := uintptr(last-1) / bitsPerT
	lastMask := T(ones >> ((uintptr(last-1) & mskT) ^ mskT))

	if firstIdx == lastIdx {
		in[firstIdx] ^= (firstMask & lastMask)
	} else {
		in[firstIdx] ^= firstMask
		for i := firstIdx + 1; i != lastIdx; i++ {
			in[i] ^= ^T(0)
		}
		in[lastIdx] ^= lastMask
	}
}
