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

package simd

import (
	"unsafe"
)

func VPXORQ(a, b, r *Vec64x8) {
	for i := range *r {
		r[i] = a[i] ^ b[i]
	}
}

func VSHUFI64X2(imm uint8, a, b, r *Vec64x8) {
	i0 := imm & 0x03
	i1 := (imm >> 2) & 0x03
	i2 := (imm >> 4) & 0x03
	i3 := (imm >> 6) & 0x03

	t := Vec64x8{
		b[i0*2], b[i0*2+1],
		b[i1*2], b[i1*2+1],
		a[i2*2], a[i2*2+1],
		a[i3*2], a[i3*2+1],
	}
	*r = t
}

func VPTERNLOGQ(imm uint8, a, b, r *Vec64x8) {
	var t Vec64x8
	for i := range *r {
		for j := 0; j < 64; j++ {
			idx := (((r[i] >> j) & 0x01) << 2) | (((b[i] >> j) & 0x01) << 1) | ((a[i] >> j) & 0x01)
			t[i] |= (uint64((imm>>idx)&0x01) << j)
		}
	}
	*r = t
}

func VMOVDQA64(a, r *Vec64x8) {
	*r = *a
}

func VMOVDQU8Z(p *uint8, offs int64, k uint64) Vec8x64 {
	var r Vec8x64
	s := unsafe.Slice((*uint8)(unsafe.Add(unsafe.Pointer(p), offs)), 64)
	for i := range r {
		if ((k >> i) & 0x01) != 0 {
			r[i] = s[i]
		}
	}
	return r
}

func VMOVDQU8(p *uint8, offs int64) Vec8x64 {
	return VMOVDQU8Z(p, offs, ^uint64(0))
}
