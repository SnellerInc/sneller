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

package vm

import (
	"encoding/binary"
	"math"
	"math/bits"
	"unsafe"
)

func init() {
	opinfo[opaggminf].portable = bcaggminfgo
	opinfo[opaggmaxf].portable = bcaggmaxfgo
	opinfo[opaggmini].portable = bcaggminigo
	opinfo[opaggmaxi].portable = bcaggmaxigo
	opinfo[opaggandi].portable = bcaggandigo
	opinfo[opaggori].portable = bcaggorigo
	opinfo[opaggxori].portable = bcaggxorigo
	opinfo[opaggsumi].portable = bcaggsumigo
	opinfo[opaggsumf].portable = bcaggsumfgo
	opinfo[opaggork].portable = bcaggorkgo
	opinfo[opaggandk].portable = bcaggandkgo
	opinfo[opaggcount].portable = bcaggcountgo
	opinfo[opaggmergestate].portable = bcaggmergestatego

	opinfo[opaggslotandk].portable = bcaggslotandkgo
	opinfo[opaggslotork].portable = bcaggslotorkgo

	opinfo[opaggslotsumi].portable = bcaggslotsumigo
	opinfo[opaggslotsumf].portable = bcaggslotsumfgo
	opinfo[opaggslotavgf].portable = bcaggslotavgfgo
	opinfo[opaggslotavgi].portable = bcaggslotavgigo
	opinfo[opaggslotminf].portable = bcaggslotminfgo
	opinfo[opaggslotmini].portable = bcaggslotminigo
	opinfo[opaggslotmaxf].portable = bcaggslotmaxfgo
	opinfo[opaggslotmaxi].portable = bcaggslotmaxigo
	opinfo[opaggslotandi].portable = bcaggslotandigo
	opinfo[opaggslotori].portable = bcaggslotorigo
	opinfo[opaggslotxori].portable = bcaggslotxorigo
	opinfo[opaggslotcount].portable = bcaggslotcountgo
	opinfo[opaggslotcountv2].portable = bcaggslotcountv2go
	opinfo[opaggslotmergestate].portable = bcaggslotmergestatego

	opinfo[opaggapproxcount].portable = bcaggapproxcountgo
}

type f64AggState struct {
	value float64
	count int64
}

type i64AggState struct {
	value int64
	count int64
}

type kAggState struct {
	value1 uint64
	value2 uint64
}

type bAggState struct {
	offsets [bcLaneCount]uint32
	sizes   [bcLaneCount]uint32
}

type sumAggState struct {
	compensation [bcLaneCount]float64
	sum          [bcLaneCount]float64
	count        int64
}

func refAggState[T any](bc *bytecode, offs uint32) *T {
	return (*T)(unsafe.Add(bc.vmState.aggPtr, offs))
}

func hashAggValues(bc *bytecode) []byte {
	return (*radixTree64)(bc.vmState.aggPtr).values
}

func approxAggValues(bc *bytecode) []byte {
	const precisionBytes = 20 // PWTODO: guessed, replace with a correct value
	return unsafe.Slice((*byte)(bc.vmState.aggPtr), 1<<precisionBytes)
}

func bcaggapproxcountgo(bc *bytecode, pc int) int {
	imm0 := bcword32(bc, pc+0)
	h := *argptr[hRegData](bc, pc+4)
	r11 := bcword(bc, pc+6)
	srcmask := argptr[kRegData](bc, pc+8).mask
	values := approxAggValues(bc)
	r13 := 64 - r11 // R13 = 64 - R11 - hash bits

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			dx := h.lo[lane]                           // DX = higher 64-bit of the 128-bit hash
			cx := dx >> r11                            // CX - hash
			cx = (uint64)(bits.LeadingZeros64(cx) + 1) // CX = lzcnt(hash) + 1
			dx = dx >> r13                             // DX - bucket id
			// update HLL register
			mem := values[uint64(imm0)+dx:]
			bx := uint64(mem[0])
			bx = max(cx, bx)
			mem[0] = byte(bx)
		}
	}
	return pc + 10
}

func bcaggslotcountgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	srcmask := argptr[kRegData](bc, pc+6).mask
	values := hashAggValues(bc)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+buckets[lane]:]
			curCnt := binary.LittleEndian.Uint64(mem[8:])
			binary.LittleEndian.PutUint64(mem[8:], curCnt+1)
		}
	}
	return pc + 8
}

func bcaggslotcountv2go(bc *bytecode, pc int) int {
	return bcaggslotcountgo(bc, pc)
}

func bcaggminfgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[f64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Min(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxfgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[f64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Max(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggminigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = min(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = max(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggandigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m &= arg0.values[lane]
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggorigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m |= arg0.values[lane]
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggxorigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m ^= arg0.values[lane]
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggsumigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m += arg0.values[lane]
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggsumfgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	var x [bcLaneCount]float64
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			x[lane] = arg0.values[lane]
		}
	}

	state := refAggState[sumAggState](bc, imm)
	c := state.compensation
	sum := state.sum

	for lane := 0; lane < bcLaneCount; lane++ {
		t := sum[lane] + x[lane]
		absSum := math.Abs(sum[lane])
		absX := math.Abs(x[lane])

		var a, b float64
		if absSum >= absX {
			a = sum[lane]
			b = x[lane]
		} else {
			a = x[lane]
			b = sum[lane]
		}
		sum[lane] = t
		a -= t
		a += b
		c[lane] += a
	}

	state.sum = sum
	state.compensation = c
	state.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggorkgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	k1 := argptr[kRegData](bc, pc+4).mask
	k2 := argptr[kRegData](bc, pc+6).mask
	s := refAggState[kAggState](bc, imm)
	s.value2 |= uint64(k2)

	if k1&k2 != 0 {
		s.value1 = 1
	}

	return pc + 8
}

func bcaggandkgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	k1 := argptr[kRegData](bc, pc+4).mask
	k2 := argptr[kRegData](bc, pc+6).mask
	s := refAggState[kAggState](bc, imm)
	s.value2 |= uint64(k2)
	k1 &= k2

	if k1 != k2 {
		s.value1 = 0
	}

	return pc + 8
}

func bcaggcountgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	k1 := argptr[kRegData](bc, pc+4).mask
	s := refAggState[i64AggState](bc, imm)
	s.value += int64(bits.OnesCount16(k1))
	return pc + 6
}

func bcaggmergestatego(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	v := argptr[bRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	s := refAggState[bAggState](bc, imm)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			s.offsets[lane] = v.offsets[lane]
			s.sizes[lane] = v.sizes[lane]
		}
	}

	return pc + 8
}

func bcaggslotmergestatego(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	v := *argptr[bRegData](bc, pc+6)
	srcmask := argptr[kRegData](bc, pc+8).mask
	values := hashAggValues(bc)
	mem := values[imm+uint32(aggregateTagSize):]

	for lane := 0; lane < bcLaneCount; lane++ {
		r := ^uint32(0)
		if srcmask&(1<<lane) != 0 {
			r = buckets[lane]
		}
		binary.LittleEndian.PutUint32(mem[(0*64)+lane*4:], r)
		binary.LittleEndian.PutUint32(mem[(1*64)+lane*4:], v.offsets[lane])
		binary.LittleEndian.PutUint32(mem[(2*64)+lane*4:], v.sizes[lane])
	}
	return pc + 10
}

func bcaggslotandkgo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpK(bc, pc, func(a, b int64) int64 { return a & b })
}

func bcaggslotorkgo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpK(bc, pc, func(a, b int64) int64 { return a | b })
}

func bcaggslotsumigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return a + b })
}

func bcaggslotsumfgo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	src0 := argptr[f64RegData](bc, pc+6).values
	srcmask := argptr[kRegData](bc, pc+8).mask
	values := hashAggValues(bc)

	const compOffset = 0 * 64
	const sumOffset = 2 * 64
	const counterOffset = 4 * 64

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+uint32(aggregateTagSize)+buckets[lane]:]
			c := math.Float64frombits(binary.LittleEndian.Uint64(mem[compOffset:]))
			sum := math.Float64frombits(binary.LittleEndian.Uint64(mem[sumOffset:]))
			cnt := binary.LittleEndian.Uint64(mem[counterOffset:])
			x := src0[lane]

			t := sum + x
			var a, b float64

			if math.Abs(sum) >= math.Abs(x) {
				a = sum
				b = x
			} else {
				a = x
				b = sum
			}

			c += (a - t) + b
			sum = t

			binary.LittleEndian.PutUint64(mem[compOffset:], math.Float64bits(c))
			binary.LittleEndian.PutUint64(mem[sumOffset:], math.Float64bits(sum))
			binary.LittleEndian.PutUint64(mem[counterOffset:], cnt+1)
		}
	}
	return pc + 10
}

func bcaggslotavgfgo(bc *bytecode, pc int) int {
	return bcaggslotsumfgo(bc, pc)
}

func bcaggslotavgigo(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	srcmask := argptr[kRegData](bc, pc+8).mask
	src0 := argptr[i64RegData](bc, pc+6).values
	buckets := argptr[bRegData](bc, pc+4).offsets
	values := hashAggValues(bc)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+uint32(aggregateTagSize)+buckets[lane]:]
			binary.LittleEndian.PutUint64(mem, uint64(src0[lane])+binary.LittleEndian.Uint64(mem))
			binary.LittleEndian.PutUint64(mem[8:], binary.LittleEndian.Uint64(mem[8:])+1)
		}
	}
	return pc + 10
}

func bcaggslotminfgo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpF64(bc, pc, func(a, b float64) float64 { return min(a, b) })
}

func bcaggslotminigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return min(a, b) })
}

func bcaggslotmaxfgo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpF64(bc, pc, func(a, b float64) float64 { return max(a, b) })
}

func bcaggslotmaxigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return max(a, b) })
}

func bcaggslotandigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return a & b })
}

func bcaggslotorigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return a | b })
}

func bcaggslotxorigo(bc *bytecode, pc int) int {
	return aggregateSlotMarkOpI64(bc, pc, func(a, b int64) int64 { return a ^ b })
}

func aggregateSlotMarkOpI64(bc *bytecode, pc int, op func(a, b int64) int64) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	src0 := argptr[i64RegData](bc, pc+6).values
	srcmask := argptr[kRegData](bc, pc+8).mask
	values := hashAggValues(bc)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+uint32(aggregateTagSize)+buckets[lane]:]
			binary.LittleEndian.PutUint64(mem, uint64(op(src0[lane], (int64)(binary.LittleEndian.Uint64(mem)))))
			binary.LittleEndian.PutUint64(mem[8:], 1)
		}
	}
	return pc + 10
}

func aggregateSlotMarkOpF64(bc *bytecode, pc int, op func(a, b float64) float64) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	src0 := argptr[f64RegData](bc, pc+6).values
	srcmask := argptr[kRegData](bc, pc+8).mask
	values := hashAggValues(bc)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+uint32(aggregateTagSize)+buckets[lane]:]
			binary.LittleEndian.PutUint64(mem, math.Float64bits(op(src0[lane], math.Float64frombits(binary.LittleEndian.Uint64(mem)))))
			binary.LittleEndian.PutUint64(mem[8:], 1)
		}
	}
	return pc + 10
}

func aggregateSlotMarkOpK(bc *bytecode, pc int, op func(a, b int64) int64) int {
	imm := bcword32(bc, pc+0)
	buckets := argptr[bRegData](bc, pc+4).offsets
	k := argptr[kRegData](bc, pc+6).mask
	srcmask := argptr[kRegData](bc, pc+8).mask

	var srck [bcLaneCount]int64
	for lane := 0; lane < bcLaneCount; lane++ {
		if k&(1<<lane) != 0 {
			srck[lane] = -1
		}
	}

	values := hashAggValues(bc)
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			mem := values[imm+uint32(aggregateTagSize)+buckets[lane]:]
			binary.LittleEndian.PutUint64(mem, uint64(op(srck[lane], (int64)(binary.LittleEndian.Uint64(mem)))))
			binary.LittleEndian.PutUint64(mem[8:], 1)
		}
	}
	return pc + 10
}
