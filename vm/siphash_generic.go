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

//go:build !amd64
// +build !amd64

package vm

import (
	"unsafe"

	"github.com/dchest/siphash"
)

func siphashx8(k0, k1 uint64, base *byte, ends *[8]uint32) [2][8]uint64 {
	var r [2][8]uint64
	offs := uint32(0)

	for i := 0; i < 8; i++ {
		end := ends[i]
		buf := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(base), offs)), end-offs)
		r[0][i], r[1][i] = siphash.Hash128(k0, k1, buf)
		offs = end
	}
	return r
}
