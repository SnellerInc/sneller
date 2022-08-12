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

// Package ints proviedes int-related common functions.
package ints

import (
	"golang.org/x/exp/constraints"
)

// Min returns the smaller value of x and y
func Min[T constraints.Integer](x, y T) T {
	if x <= y {
		return x
	}
	return y
}

// Max returns the greater value of x and y
func Max[T constraints.Integer](x, y T) T {
	if x >= y {
		return x
	}
	return y
}

// Clamp returns x if it is in [lo, hi]. Otherwise, the nearest bounding value is returned
func Clamp[T constraints.Integer](x, lo, hi T) T {
	return Max(lo, Min(x, hi))
}
