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
	"golang.org/x/exp/constraints"
)

// IsAligned returns true if and only if v is an integer multiple of alignment
func IsAligned(v, alignment uint) bool {
	return v%alignment == 0
}

// IsAligned8 returns true if and only if v is an integer multiple of alignment
func IsAligned8(v, alignment uint8) bool {
	return v%alignment == 0
}

// IsAligned16 returns true if and only if v is an integer multiple of alignment
func IsAligned16(v, alignment uint16) bool {
	return v%alignment == 0
}

// IsAligned32 returns true if and only if v is an integer multiple of alignment
func IsAligned32(v, alignment uint32) bool {
	return v%alignment == 0
}

// IsAligned64 returns true if and only if v is an integer multiple of alignment
func IsAligned64(v, alignment uint64) bool {
	return v%alignment == 0
}

// AlignDown returns v aligned down to a given alignment.
func AlignDown(v, alignment uint) uint {
	return (v / alignment) * alignment
}

// AlignDown8 returns v aligned down to a given alignment.
func AlignDown8(v, alignment uint8) uint8 {
	return (v / alignment) * alignment
}

// AlignDown16 returns v aligned down to a given alignment.
func AlignDown16(v, alignment uint16) uint16 {
	return (v / alignment) * alignment
}

// AlignDown32 returns v aligned down to a given alignment.
func AlignDown32(v, alignment uint32) uint32 {
	return (v / alignment) * alignment
}

// AlignDown64 returns v aligned down to a given alignment.
func AlignDown64(v, alignment uint64) uint64 {
	return (v / alignment) * alignment
}

// AlignUp returns v aligned up to a given alignment.
func AlignUp(v, alignment uint) uint {
	return ((v + alignment - 1) / alignment) * alignment
}

// AlignUp8 returns v aligned up to a given alignment.
func AlignUp8(v, alignment uint8) uint8 {
	return ((v + alignment - 1) / alignment) * alignment
}

// AlignUp16 returns v aligned up to a given alignment.
func AlignUp16(v, alignment uint16) uint16 {
	return ((v + alignment - 1) / alignment) * alignment
}

// AlignUp32 returns v aligned up to a given alignment.
func AlignUp32(v, alignment uint32) uint32 {
	return ((v + alignment - 1) / alignment) * alignment
}

// AlignUp64 returns v aligned up to a given alignment.
func AlignUp64(v, alignment uint64) uint64 {
	return ((v + alignment - 1) / alignment) * alignment
}

// ChunkCount returns the number of chunkSize-bit chunks needed to store n bits
func ChunkCount[T constraints.Unsigned](n, chunkSize T) T {
	return (n + chunkSize - 1) / chunkSize
}
