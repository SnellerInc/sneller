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
