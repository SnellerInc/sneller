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
func IsAligned[T constraints.Integer](v, alignment T) bool {
	return v%alignment == 0
}

// AlignDown returns v aligned down to a given alignment.
// In other words, the return value is the largest integer multiple of alignment not greater than v.
// AlignDown(3, 4) == 0; AlignDown(11, 5) == 10; AlignDown(2, 2) == 2.
func AlignDown[T constraints.Integer](v, alignment T) T {
	return (v / alignment) * alignment
}

// AlignUp returns v aligned up to a given alignment.
// In other words, the return value is the smallest integer multiple of alignment not lesser than v.
// AlignUp(3, 4) == 4; AlignUp(11, 5) == 15; AlignUp(2, 2) == 2.
func AlignUp[T constraints.Integer](v, alignment T) T {
	return AlignDown(v+alignment-1, alignment)
}
