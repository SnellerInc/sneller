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
	"unsafe"

	"golang.org/x/sys/cpu"

	"github.com/SnellerInc/sneller/ion"
)

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

func setvmrefB(dst *bRegData, src []vmref) {
	*dst = bRegData{}
	for i := 0; i < min(bcLaneCount, len(src)); i++ {
		dst.offsets[i] = src[i][0]
		dst.sizes[i] = src[i][1]
	}
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
		apos := bc.auxpos
		mask := evalfiltergolanes(bc, next)
		if bc.err != 0 {
			return j
		}
		// compress delims + auxvals
		for k := range next {
			if mask == 0 {
				break
			}
			if (mask & 1) != 0 {
				for l := range bc.auxvals {
					bc.auxvals[l][j] = bc.auxvals[l][apos+k]
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

func bcinitgo(bc *bytecode, pc int) int {
	delims := argptr[bRegData](bc, pc+0)
	delims.offsets = bc.vmState.delims.offsets
	delims.sizes = bc.vmState.delims.sizes

	mask := argptr[kRegData](bc, pc+2)
	mask.mask = bc.vmState.validLanes.mask
	return pc + 4
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

outer:
	for i := 0; i < bcLaneCount; i++ {
		start := src.offsets[i]
		width := src.sizes[i]
		dstv.offsets[i] = start
		dstv.sizes[i] = 0
		dstv.typeL[i] = 0
		dstv.headerSize[i] = 0
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{start, width}.mem()
		var sym ion.Symbol
		var err error
	symsearch:
		for len(mem) > 0 {
			sym, mem, err = ion.ReadLabel(mem)
			if err != nil {
				bc.err = bcerrCorrupt
				break outer
			}
			if sym > symbol {
				break symsearch
			}
			dstv.offsets[i] = start + width - uint32(len(mem))
			dstv.sizes[i] = uint32(ion.SizeOf(mem))
			dstv.typeL[i] = byte(mem[0])
			dstv.headerSize[i] = byte(ion.HeaderSizeOf(mem))
			if sym == symbol {
				retmask |= (1 << i)
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

func bcxnorkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = (k0.mask ^ (^k1.mask)) & bc.vmState.validLanes.mask
	return pc + 6
}

func bcandkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = k0.mask & k1.mask
	return pc + 6
}

func bcboxf64go(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[f64RegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask

	p := len(bc.scratch)
	want := 9 * bcLaneCount
	if cap(bc.scratch)-p < want {
		bc.err = bcerrMoreScratch
		return pc + 6
	}
	bc.scratch = bc.scratch[:p+want]
	mem := bc.scratch[p:]
	var buf ion.Buffer
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			dst.offsets[i] = 0
			dst.sizes[i] = 0
			dst.typeL[i] = 0
			dst.headerSize[i] = 0
			continue
		}
		buf.Set(mem[:0])
		buf.WriteCanonicalFloat(src.values[i])
		start, ok := vmdispl(mem)
		if !ok {
			panic("bad scratch buffer")
		}
		dst.offsets[i] = start
		dst.sizes[i] = 9
		dst.typeL[i] = mem[0]
		dst.headerSize[i] = 1 // ints and floats always have 1-byte headers
		mem = mem[9:]
	}
	return pc + 6
}

func bcretbkgo(bc *bytecode, pc int) int {
	b := argptr[bRegData](bc, pc+0)
	k := argptr[kRegData](bc, pc+2)

	bc.vmState.delims.offsets = b.offsets
	bc.vmState.delims.sizes = b.sizes
	bc.vmState.outputLanes.mask = k.mask

	return pc + 4
}

func bcretgo(bc *bytecode, pc int) int {
	bc.err = 0
	return pc
}

func init() {
	opinfo[opinit].portable = bcinitgo
	opinfo[opretbk].portable = bcretbkgo
	opinfo[opauxval].portable = bcauxvalgo
	opinfo[opfindsym].portable = bcfindsymgo
	opinfo[opcmpvi64imm].portable = bccmpvi64immgo
	opinfo[opcmplti64imm].portable = bccmplti64immgo
	opinfo[optuple].portable = bctuplego
	opinfo[opandk].portable = bcandkgo
	opinfo[opxnork].portable = bcxnorkgo
	opinfo[opret].portable = bcretgo
	opinfo[opboxf64].portable = bcboxf64go
}

func evalfindgo(bc *bytecode, delims []vmref, stride int) {
	stack := bc.vstack
	var alt bytecode
	l := len(bc.compiled)
	// convert stride to 64-bit words:
	stride = stride / int(unsafe.Sizeof(bc.vstack[0]))
	for len(delims) > 0 {
		mask := uint16(0xffff)
		lanes := bcLaneCount
		if len(delims) < lanes {
			mask >>= bcLaneCount - len(delims)
			lanes = len(delims)
		}
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = mask
		setvmrefB(&bc.vmState.delims, delims)
		pc := 0
		for pc < l && bc.err == 0 {
			op := bcop(bcword(bc.compiled, pc))
			pc += 2
			fn := opinfo[op].portable
			if fn != nil {
				pc = fn(bc, pc)
			} else if cpu.X86.HasAVX512 {
				pc = runSingle(bc, &alt, pc, bc.vmState.validLanes.mask)
			} else {
				bc.err = bcerrNotSupported
			}
		}
		if bc.err != 0 {
			return
		}
		delims = delims[lanes:]
		bc.auxpos += lanes
		bc.vstack = bc.vstack[stride:]
	}
	bc.vstack = stack
}

func evalfiltergolanes(bc *bytecode, delims []vmref) uint16 {
	l := len(bc.compiled)
	pc := 0
	if len(delims) > bcLaneCount {
		panic("invalid len(delims) for evalfiltergolanes")
	}
	mask := uint16(0xffff)
	mask >>= bcLaneCount - len(delims)
	var alt bytecode

	setvmrefB(&bc.vmState.delims, delims)
	bc.vmState.validLanes.mask = mask
	bc.vmState.outputLanes.mask = 0

	for pc < l && bc.err == 0 {
		op := bcop(bcword(bc.compiled, pc))
		pc += 2

		fn := opinfo[op].portable
		if fn != nil {
			pc = fn(bc, pc)
		} else if cpu.X86.HasAVX512 {
			pc = runSingle(bc, &alt, pc, bc.vmState.validLanes.mask)
		} else {
			bc.err = bcerrNotSupported
			return 0
		}
	}

	if bc.err != 0 {
		return 0
	}

	bc.auxpos += len(delims)
	return bc.vmState.outputLanes.mask
}

//go:noescape
func bcenter(bc *bytecode, k7 uint16)

// run a single bytecode instruction @ pc
func runSingle(bc, alt *bytecode, pc int, k7 uint16) int {
	// copy over everything except the compiled bytestream:
	compiled := alt.compiled
	*alt = *bc
	alt.compiled = compiled[:0]

	// create a new compiled bytestream with the single instr + return
	width := bcwidth(bc, pc-2)
	alt.compiled = append(alt.compiled, bc.compiled[pc-2:pc+width]...)
	alt.compiled = append(alt.compiled, byte(opret), byte(opret>>8))

	// evaluate the bytecode and copy back the error state
	bcenter(alt, k7)
	bc.err = alt.err
	bc.errpc = alt.errpc
	bc.errinfo = alt.errinfo
	bc.scratch = alt.scratch // copy back len(scratch)
	return pc + width
}
