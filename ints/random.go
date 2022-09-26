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
	"crypto/rand"
	"unsafe"

	"golang.org/x/exp/constraints"
)

// RandomFillSlice fills a slice of []T with content produced by a cryptographically strong random number generator
func RandomFillSlice[T constraints.Integer](out []T) error {
	if n := len(out); n > 0 {
		_, err := rand.Read(unsafe.Slice((*byte)(unsafe.Pointer(&out[0])), n*int(unsafe.Sizeof(out[0]))))
		return err
	}
	return nil
}
