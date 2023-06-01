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
