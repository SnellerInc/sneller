// Copyright (C) 2023 Sneller, Inc.
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

// Package memops implements accelerated memory block manipulation primitives
package memops

import (
	"unsafe"
)

type Pointerless interface {
	// TODO: should be constraints.Integer | constraints.Float | a recursive composition of Pointerless, but Go doesn't support this concept.
}

// ZeroMemory fills buf with zeros. CAUTION: must be used only for T not containing pointers!
func ZeroMemory[T Pointerless](buf []T) {
	zeroMemoryPointerless(unsafe.Pointer(unsafe.SliceData(buf)), uintptr(len(buf)*int(unsafe.Sizeof(buf[0]))))
}

//go:noescape
func zeroMemoryPointerless(ptr unsafe.Pointer, n uintptr)
