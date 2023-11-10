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

package vm

import (
	"encoding/binary"
	"os"
	"unsafe"

	"golang.org/x/sys/cpu"

	"github.com/SnellerInc/sneller/ion"
)

var portable = os.Getenv("SNELLER_PORTABLE") != "" || !cpu.X86.HasAVX512

type opfn func(bc *bytecode, pc int) int

// ops, arg slots, etc. are encoded as 16-bit integers:
func bcword(buf []byte, pc int) int {
	return int(binary.LittleEndian.Uint16(buf[pc:]))
}

func bcword64(buf []byte, pc int) uint64 {
	return binary.LittleEndian.Uint64(buf[pc:])
}

func slotcast[T any](b *bytecode, slot int) *T {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&b.vstack[0])), len(b.vstack)*8)
	ptr := unsafe.Pointer(&buf[slot])
	return (*T)(ptr)
}

func argptr[T any](b *bytecode, pc int) *T {
	return slotcast[T](b, bcword(b.compiled, pc))
}

func setvmref(dst *vRegData, src []vmref) {
	*dst = vRegData{}
	for i := 0; i < min(bcLaneCount, len(src)); i++ {
		dst.offsets[i] = src[i][0]
		dst.sizes[i] = src[i][1]
		if dst.sizes[i] == 0 {
			continue
		}
		mem := src[i].mem()
		dst.typeL[i] = byte(mem[0])
		dst.headerSize[i] = byte(ion.HeaderSizeOf(mem))
	}
}

func evalfiltergo(bc *bytecode, delims []vmref) int {
	i, j := 0, 0
	for i < len(delims) {
		next := delims[i:]
		next = next[:min(len(next), bcLaneCount)]
		mask := evalfiltergolanes(bc, next)
		if bc.err != 0 {
			return j
		}
		for k := range next {
			if mask == 0 {
				break
			}
			// compress delims + auxvals
			if (mask & 1) != 0 {
				for l := range bc.auxvals {
					bc.auxvals[l][j] = bc.auxvals[l][k]
				}
				delims[j] = next[k]
				j++
			}
			mask >>= 1
		}
		i += len(next)
	}
	return j
}

func bcauxvalgo(bc *bytecode, pc int) int {
	dstv := argptr[vRegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	auxv := bcword(bc.compiled, pc+4)
	lst := bc.auxvals[auxv][bc.auxpos:]
	lst = lst[:min(bcLaneCount, len(lst))]
	mask := uint16(0)
	for i := range lst {
		if lst[i][1] != 0 {
			mask |= (1 << i)
		}
	}
	setvmref(dstv, lst)
	dstk.mask = mask
	return pc + 6
}

func bcfindsymgo(bc *bytecode, pc int) int {
	dstv := argptr[vRegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcb := argptr[bRegData](bc, pc+4)
	symbol, _, _ := ion.ReadLabel(bc.compiled[pc+6:])
	srck := argptr[kRegData](bc, pc+10)

	src := *srcb // may alias output
	srcmask := srck.mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		start := src.offsets[i]
		width := src.sizes[i]
		mem := vmref{start, width}.mem()
		var sym ion.Symbol
		var err error
	symsearch:
		for len(mem) > 0 {
			sym, mem, err = ion.ReadLabel(mem)
			if err != nil {
				panic(err)
			}
			if sym == symbol {
				retmask |= (1 << i)
			}
			if sym >= symbol {
				dstv.offsets[i] = start + width - uint32(len(mem))
				dstv.sizes[i] = uint32(ion.SizeOf(mem))
				dstv.typeL[i] = byte(mem[0])
				dstv.headerSize[i] = byte(ion.HeaderSizeOf(mem))
				break symsearch
			}
			mem = mem[ion.SizeOf(mem):]
		}
	}
	dstk.mask = retmask
	return pc + 12
}

func bccmplti64immgo(bc *bytecode, pc int) int {
	dstk := argptr[kRegData](bc, pc)
	arg0 := argptr[i64RegData](bc, pc+2)
	arg1imm := int64(bcword64(bc.compiled, pc+4))
	srck := argptr[kRegData](bc, pc+12)

	mask := srck.mask
	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if arg0.values[i] < arg1imm {
			retmask |= (1 << i)
		}
	}
	dstk.mask = retmask & mask
	return pc + 14
}

func bccmpvi64immgo(bc *bytecode, pc int) int {
	retslot := argptr[i64RegData](bc, pc)
	retk := argptr[kRegData](bc, pc+2)
	argv := argptr[vRegData](bc, pc+4)
	imm := int64(bcword64(bc.compiled, pc+6))
	argk := argptr[kRegData](bc, pc+14)

	src := *argv // copied since argv may alias retslot
	mask := argk.mask
	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			retslot.values[i] = 0
			continue
		}
		start := src.offsets[i]
		width := src.sizes[i]
		if width == 0 {
			retslot.values[i] = 0
			continue
		}
		mem := vmref{start, width}.mem()
		var rv int64
		switch ion.Type(src.typeL[i] >> 4) {
		case ion.FloatType:
			f, _, _ := ion.ReadFloat64(mem)
			if f < float64(imm) {
				rv = -1
			} else if f > float64(imm) {
				rv = 1
			}
			retmask |= (1 << i)
		case ion.IntType:
			j, _, _ := ion.ReadInt(mem)
			if j < imm {
				rv = -1
			} else if j > imm {
				rv = 1
			}
			retmask |= (1 << i)
		case ion.UintType:
			u, _, _ := ion.ReadUint(mem)
			if imm < 0 || u < uint64(imm) {
				rv = -1
			} else if u > uint64(imm) {
				rv = 1
			}
			retmask |= (1 << i)
		}
		retslot.values[i] = rv
	}
	retk.mask = retmask
	return pc + 16
}

func bctuplego(bc *bytecode, pc int) int {
	dstb := argptr[bRegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srck := argptr[kRegData](bc, pc+6)

	src := *srcv
	mask := srck.mask
	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		dstb.offsets[i] = 0
		dstb.sizes[i] = 0
		if mask&(1<<i) == 0 ||
			ion.Type(src.typeL[i]>>4) != ion.StructType ||
			src.sizes[i] == 0 {
			continue
		}
		hdrsize := uint32(src.headerSize[i])
		dstb.offsets[i] = src.offsets[i] + hdrsize
		dstb.sizes[i] = src.sizes[i] - hdrsize
		retmask |= (1 << i)
	}
	dstk.mask = retmask
	return pc + 8
}

func init() {
	opinfo[opauxval].portable = bcauxvalgo
	opinfo[opfindsym].portable = bcfindsymgo
	opinfo[opcmpvi64imm].portable = bccmpvi64immgo
	opinfo[opcmplti64imm].portable = bccmplti64immgo
	opinfo[optuple].portable = bctuplego
}

func evalfiltergolanes(bc *bytecode, delims []vmref) uint16 {
	l := len(bc.compiled)
	pc := 0
	if len(delims) > bcLaneCount {
		panic("invalid len(delims) for evalfiltergolanes")
	}
	mask := uint16(0xffff)
	mask >>= bcLaneCount - len(delims)
	for pc < l && bc.err == 0 {
		op := bcop(bcword(bc.compiled, pc))
		switch op {
		case opinit:
			// supply args
			vsrc := argptr[vRegData](bc, pc+2)
			setvmref(vsrc, delims)
			ksrc := argptr[kRegData](bc, pc+4)
			ksrc.mask = mask
			pc += 6
		case opretbk:
			// ignoring the base part; it's just the delims
			retk := argptr[kRegData](bc, pc+4)
			mask = retk.mask
			bc.auxpos += len(delims)
			return mask
		default:
			pc += 2
			fn := opinfo[op].portable
			if fn == nil {
				bc.err = bcerrNotSupported
				return 0
			}
			pc = fn(bc, pc)
		}
	}
	// we should hit bcretk
	panic("invalid bytecode")
}
