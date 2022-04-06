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

package vm

import (
	"github.com/SnellerInc/sneller/ion"
)

// scan scans any buffer and produces
// relative displacments for all of
// the structures relative to &buf[0]
// beginning at offset start
//go:noescape
func scan(buf []byte, start int32, dst [][2]uint32) (int, int32)

// scanvmm scans a vmm-allocated buffer
// and produces absolute displacements
// relative to vmm for all of the structures
//go:noescape
func scanvmm(buf []byte, dst [][2]uint32) (int, int32)

// encoded returns a Symbol as its UVarInt
// encoded bytes (up to 4 bytes) and the mask
// necessary to examine just those bytes
//
// NOTE: only symbols up to 2^28 are supported
func encoded(sym ion.Symbol) (uint32, uint32, int8) {
	mask := uint32(0)
	out := uint32(0)
	size := int8(0)
	for sym != 0 {
		size++
		mask <<= 8
		out <<= 8
		if out == 0 {
			out |= 0x80
		}
		mask |= 0xff
		out |= uint32(sym) & 0x7f
		sym >>= 7
	}
	return out, mask, size
}
