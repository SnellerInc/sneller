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

func TestBit[T, K constraints.Integer](in []T, k K) bool {
	return (in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] & (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))) != 0
}

func SetBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] |= (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

func ClearBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] &= ^(T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

func FlipBit[T, K constraints.Integer](in []T, k K) {
	in[uintptr(k)/(unsafe.Sizeof(in[0])*8)] ^= (T(1) << (uintptr(k) % (unsafe.Sizeof(in[0]) * 8)))
}

func SetBits[T, K constraints.Integer](in []T, first, last K) {
	// TODO: optimize me
	for i := first; i < last; i++ {
		SetBit(in, i)
	}
}

func ClearBits[T, K constraints.Integer](in []T, first, last K) {
	// TODO: optimize me
	for i := first; i < last; i++ {
		ClearBit(in, i)
	}
}

func FlipBits[T, K constraints.Integer](in []T, first, last K) {
	// TODO: optimize me
	for i := first; i < last; i++ {
		FlipBit(in, i)
	}
}
