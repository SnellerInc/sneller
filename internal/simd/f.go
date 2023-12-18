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
	"encoding/binary"
	"log"
	"unsafe"
)

type Mask [64]bool

var Mask16True = Mask{true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true}

type Gpr struct {
	Value uint64
	isPtr bool
	mem   []byte
}

func (g *Gpr) MakePtr(mem []byte) {
	g.isPtr = true
	g.mem = mem
}

func (g *Gpr) Ptr(offset int) []byte {
	if g.isPtr {
		idx := int(g.Value) + offset
		if idx < len(g.mem) {
			return g.mem[idx:]
		}
		log.Panicf("Gpr.Ptr: index %v out of range [%v, %v]", idx, 0, len(g.mem))
	}
	log.Panicf("Gpr.Ptr: not a pointer, cannot dereference a Value")
	return nil
}

type Flags struct {
	Zero bool
	//TODO all other flags when needed
}

func loadUint32(mem []byte, offset int) uint32 {
	buffer := make([]byte, 4)
	memEnd := len(mem)
	for i := 0; i < 4; i++ {
		if offset+i < memEnd {
			buffer[i] = mem[offset+i]
		} else {
			// we could give a warning that we are reading beyond the end of the ptr
			buffer[i] = 0
		}
	}
	return binary.LittleEndian.Uint32(buffer)
}

func Vpxorq(a, b, r *Vec64x8) {
	for i := range *r {
		r[i] = a[i] ^ b[i]
	}
}

func Vshufi64X2(imm uint8, a, b, r *Vec64x8) {
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

func Vpternlogq(imm uint8, a, b, r *Vec64x8) {
	var t Vec64x8
	for i := range *r {
		for j := 0; j < 64; j++ {
			idx := (((r[i] >> j) & 0x01) << 2) | (((b[i] >> j) & 0x01) << 1) | ((a[i] >> j) & 0x01)
			t[i] |= uint64((imm>>idx)&0x01) << j
		}
	}
	*r = t
}

func Vmovdqa64(a, r *Vec64x8) {
	*r = *a
}

func Vmovdqu8Z(p *uint8, offs int64, k uint64) Vec8x64 {
	var r Vec8x64
	s := unsafe.Slice((*uint8)(unsafe.Add(unsafe.Pointer(p), offs)), 64)
	for i := range r {
		if ((k >> i) & 0x01) != 0 {
			r[i] = s[i]
		}
	}
	return r
}

func Vmovdqu8(p *uint8, offs int64) Vec8x64 {
	return Vmovdqu8Z(p, offs, ^uint64(0))
}

func to16uint32Ptr(z *Vec8x64) *[16]uint32 {
	return (*[16]uint32)(unsafe.Pointer(z))
}

func Mask2Uint16(K *Mask) (result uint16) {
	for j := 0; j < 16; j++ {
		if K[j] {
			result |= 1 << j
		}
	}
	return
}

func Uint162Mask16(data uint16) (result Mask) {
	for j := 0; j < 16; j++ {
		result[j] = ((data >> j) & 1) == 1
	}
	return
}

func VpbroadcastdImm(imm uint32, r *Vec8x64) {
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = imm
	}
}

func VpbroadcastdMem(ptr []byte, r *Vec8x64) {
	value := binary.LittleEndian.Uint32(ptr)
	VpbroadcastdImm(value, r)
}

func VpbroadcastdMemK(ptr []byte, K *Mask, r *Vec8x64) {
	value := binary.LittleEndian.Uint32(ptr)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = value
		}
	}
}

func Vpermi2B(a, b, r *Vec8x64) {
	for j := 0; j < 64; j++ {
		d := r[j] & 0b1111111
		if d < 64 {
			r[j] = b[d]
		} else {
			r[j] = a[d&0b111111]
		}
	}
}

func Vpermb(a, b, r *Vec8x64) {
	for j := 0; j < 64; j++ {
		r[j] = a[b[j]&0b111111]
	}
}

func Vpermd(a, b, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = a2[b2[j]&0b1111]
	}
}

func Vpshufb(idx, b, r *Vec8x64) {
	b2 := *b
	offset := 0
	for k := 0; k < 4; k++ {
		for j := 0; j < 16; j++ {
			if (idx[offset] & 0b10000000) != 0 {
				r[offset] = 0
			} else {
				index := int(idx[offset]&0b1111) + (k * 16)
				r[offset] = b2[index]
			}
			offset++
		}
	}
}

func VpsrldImm(imm uint8, a, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = a2[j] >> imm
	}
}

func Vpsrlvd(a, b, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = b2[j] >> a2[j]
	}
}

func VpslldImm(imm uint8, a, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = a2[j] << imm
	}
}

func VpslldImmZ(imm uint8, a *Vec8x64, K *Mask, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = a2[j] << imm
		} else {
			r2[j] = 0
		}
	}
}

func Vpord(a, b, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = b2[j] | a2[j]
	}
}

func Vpandd(a, b, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = b2[j] & a2[j]
	}
}

func VpanddZ(a, b *Vec8x64, K *Mask, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = b2[j] & a2[j]
		} else {
			r2[j] = 0
		}
	}
}

func Vpxord(a, b, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = b2[j] ^ a2[j]
	}
}

func Vpaddd(a, b, r *Vec8x64) {
	VpadddK(a, b, &Mask16True, r)
}

func VpadddK(a, b *Vec8x64, K *Mask, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = b2[j] + a2[j]
		}
	}
}

func Vpsubd(a, b, r *Vec8x64) {
	VpsubdK(a, b, &Mask16True, r)
}

func VpsubdK(a, b *Vec8x64, K *Mask, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = b2[j] - a2[j]
		}
	}
}

func Vmovdqu8K(a *Vec8x64, K *Mask, r *Vec8x64) {
	for j := 0; j < 64; j++ {
		if K[j] {
			r[j] = a[j]
		}
	}
}

func Vmovdqa32(a, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		r2[j] = a2[j]
	}
}

func Vmovdqa32K(a *Vec8x64, K *Mask, r *Vec8x64) {
	a2 := to16uint32Ptr(a)
	r2 := to16uint32Ptr(r)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = a2[j]
		}
	}
}

func Vmovdqu32X(values [16]uint32, r *Vec8x64) {
	r2 := to16uint32Ptr(r)
	copy(r2[:], values[:])
}

func Vmovdqu32Mem(ptr []byte, r *Vec8x64) {
	copy(r[:], ptr[:64])
}

func cmpI(imm uint8, a, b int32, k bool) bool {
	switch imm {
	case 0: // _MM_CMPINT_EQ
		return k && (b == a)
	case 1: // _MM_CMPINT_LT
		return k && (b < a)
	case 2: // _MM_CMPINT_LE
		return k && (b <= a)
	case 4: // _MM_CMPINT_NE
		return k && (b != a)
	case 5: // _MM_CMPINT_NLT
		return k && (b >= a)
	case 6: // _MM_CMPINT_NLE
		return k && (b > a)
	default:
		log.Panicf("TODO: imm %v", imm)
		return false
	}
}

func cmpU(imm uint8, a, b uint32, k bool) bool {
	switch imm {
	case 0: // _MM_CMPINT_EQ
		return k && (b == a)
	case 1: // _MM_CMPINT_LT
		return k && (b < a)
	case 2: // _MM_CMPINT_LE
		return k && (b <= a)
	case 4: // _MM_CMPINT_NE
		return k && (b != a)
	case 5: // _MM_CMPINT_NLT
		return k && (b >= a)
	case 6: // _MM_CMPINT_NLE
		return k && (b > a)
	default:
		log.Panicf("TODO: imm %v", imm)
		return false
	}
}

func Vpcmpd(imm uint8, a, b *Vec8x64, r *Mask) {
	VpcmpdK(imm, a, b, &Mask16True, r)
}

func VpcmpdK(imm uint8, a, b *Vec8x64, K, r *Mask) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	for j := 0; j < 16; j++ {
		r[j] = cmpI(imm, int32(a2[j]), int32(b2[j]), K[j])
	}
}

func VpcmpudK(imm uint8, a, b *Vec8x64, K, r *Mask) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	for j := 0; j < 16; j++ {
		r[j] = cmpU(imm, a2[j], b2[j], K[j])
	}
}

func VpcmpudKBcst(imm uint8, ptr []byte, a *Vec8x64, K, r *Mask) {
	a2 := to16uint32Ptr(a)
	value := binary.LittleEndian.Uint32(ptr)
	for j := 0; j < 16; j++ {
		r[j] = cmpU(imm, value, a2[j], K[j])
	}
}

func Vpmovm2B(K *Mask, r *Vec8x64) {
	for j := 0; j < 64; j++ {
		if K[j] {
			r[j] = 0xFF
		} else {
			r[j] = 0
		}
	}
}

func Vpmovb2M(a *Vec8x64, r *Mask) {
	for j := 0; j < 64; j++ {
		r[j] = (a[j] & 0x80) != 0
	}
}

func Vpmovd2M(a *Vec8x64, r *Mask) {
	a2 := to16uint32Ptr(a)
	for j := 0; j < 16; j++ {
		r[j] = (a2[j] & 0x80000000) != 0
	}
}

func Vpgatherdd(base *Gpr, offsets *Vec8x64, K *Mask, r *Vec8x64) {
	r2 := to16uint32Ptr(r)
	offsets32 := to16uint32Ptr(offsets)
	for j := 0; j < 16; j++ {
		if K[j] {
			r2[j] = loadUint32(base.mem, int(offsets32[j]))
		}
	}
}

func VptestnmdK(a, b *Vec8x64, K, r *Mask) {
	a2 := to16uint32Ptr(a)
	b2 := to16uint32Ptr(b)
	for j := 0; j < 16; j++ {
		if K[j] {
			r[j] = (a2[j] & b2[j]) == 0
		} else {
			r[j] = false
		}
	}
}

func Ktestw(a, b *Mask, flags *Flags) {
	for j := 0; j < 16; j++ {
		if a[j] && b[j] {
			flags.Zero = false
			return
		}
	}
	flags.Zero = true
}

func Kmovw(a, r *Mask) {
	for j := 0; j < 16; j++ {
		r[j] = a[j]
	}
}

func KmovwMem(ptr []byte, r *Mask) {
	for i := 0; i < 2; i++ {
		b := ptr[i]
		for j := 0; j < 8; j++ {
			r[(i*8)+j] = ((b >> j) & 1) == 1
		}
	}
}

func KmovqMem(ptr []byte, r *Mask) {
	for i := 0; i < 8; i++ {
		b := ptr[i]
		for j := 0; j < 8; j++ {
			r[(i*8)+j] = ((b >> j) & 1) == 1
		}
	}
}

func Kxorw(a, b, r *Mask) {
	for j := 0; j < 16; j++ {
		r[j] = b[j] != a[j]
	}
}

func Korw(a, b, r *Mask) {
	for j := 0; j < 16; j++ {
		r[j] = b[j] || a[j]
	}
}

func Kandnw(a, b, r *Mask) {
	for j := 0; j < 16; j++ {
		r[j] = !(b[j]) && a[j]
	}
}

func Movq(a, r *Gpr) {
	r.Value = a.Value
	r.isPtr = a.isPtr
	if a.isPtr {
		r.mem = a.mem
	}
}

func Movl(a, r *Gpr) {
	Movq(a, r)
	r.Value &= 0xFFFFFFFF
}

func MovlMem(ptr []byte, r *Gpr) {
	r.Value = uint64(loadUint32(ptr, 0))
}

func Addq(a, r *Gpr, flags *Flags) {
	r.Value += a.Value
	flags.Zero = r.Value == 0
}

func AddqImm(imm uint64, r *Gpr, flags *Flags) {
	r.Value += imm
	flags.Zero = r.Value == 0
}

func Decl(r *Gpr, flags *Flags) {
	r.Value--
	r.Value &= 0xFFFFFFFF
	flags.Zero = r.Value == 0
}

func Incl(r *Gpr, flags *Flags) {
	r.Value++
	r.Value &= 0xFFFFFFFF
	//carry flag is not updated
	flags.Zero = r.Value == 0
}

func Testl(a, b *Gpr, flags *Flags) {
	tmp := (a.Value & 0xFFFFFFFF) & (b.Value & 0xFFFFFFFF)
	flags.Zero = tmp == 0
}
