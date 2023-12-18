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

	"golang.org/x/sys/cpu"

	"github.com/SnellerInc/sneller/ion"
)

type opfn func(bc *bytecode, pc int) int

// ops, arg slots, etc. are encoded as 16-bit integers:
func bcword(bc *bytecode, pc int) uint {
	return uint(binary.LittleEndian.Uint16(bc.compiled[pc:]))
}

func bcword8(bc *bytecode, pc int) byte {
	return bc.compiled[pc]
}

func bcword32(bc *bytecode, pc int) uint32 {
	return binary.LittleEndian.Uint32(bc.compiled[pc:])
}

func bcword64(bc *bytecode, pc int) uint64 {
	return binary.LittleEndian.Uint64(bc.compiled[pc:])
}

func bcfloat64(bc *bytecode, pc int) float64 {
	return math.Float64frombits(bcword64(bc, pc))
}

func slotcast[T any](b *bytecode, slot uint) *T {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&b.vstack[0])), len(b.vstack)*8)
	ptr := unsafe.Pointer(&buf[slot])
	return (*T)(ptr)
}

func argptr[T any](b *bytecode, pc int) *T {
	return slotcast[T](b, bcword(b, pc))
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

func getTLVSize(length uint) uint {
	if length < 14 {
		return 1
	}

	if length < (1 << 7) {
		return 2
	}

	if length < (1 << 14) {
		return 3
	}

	if length < (1 << 21) {
		return 4
	}

	if length < (1 << 28) {
		return 5
	}

	return 6
}

func encodeSymbol(dst []byte, offset int, symbol ion.Symbol) int {
	if symbol < (1 << 7) {
		dst[offset+0] = byte(symbol) | 0x80
		return 1
	}

	if symbol < (1 << 14) {
		dst[offset+0] = byte((symbol >> 7) & 0x7F)
		dst[offset+1] = byte(symbol&0xFF) | 0x80
		return 2
	}

	if symbol < (1 << 21) {
		dst[offset+0] = byte((symbol >> 14) & 0x7F)
		dst[offset+1] = byte((symbol >> 7) & 0x7F)
		dst[offset+2] = byte(symbol&0xFF) | 0x80
		return 3
	}

	if symbol < (1 << 28) {
		dst[offset+0] = byte((symbol >> 21) & 0x7F)
		dst[offset+1] = byte((symbol >> 14) & 0x7F)
		dst[offset+2] = byte((symbol >> 7) & 0x7F)
		dst[offset+3] = byte(symbol&0xFF) | 0x80
		return 4
	}

	panic("encodeSymbol: symbol ID out of range")
}

func encodeTLVUnsafe(dst []byte, offset int, valueType ion.Type, length uint) int {
	tag := byte(valueType) << 4
	if length < 14 {
		dst[offset] = tag | byte(length)
		return 1
	}

	dst[offset] = tag | 0xE

	if length < (1 << 7) {
		dst[offset+1] = byte(length) | 0x80
		return 2
	}

	if length < (1 << 14) {
		dst[offset+1] = byte((length >> 7) & 0x7F)
		dst[offset+2] = byte(length&0xFF) | 0x80
		return 3
	}

	if length < (1 << 21) {
		dst[offset+1] = byte((length >> 14) & 0x7F)
		dst[offset+2] = byte((length >> 7) & 0x7F)
		dst[offset+3] = byte(length&0xFF) | 0x80
		return 4
	}

	if length < (1 << 28) {
		dst[offset+1] = byte((length >> 21) & 0x7F)
		dst[offset+2] = byte((length >> 14) & 0x7F)
		dst[offset+3] = byte((length >> 7) & 0x7F)
		dst[offset+4] = byte(length&0xFF) | 0x80
		return 5
	}

	panic("encodeTLVUnsafe: length too large")
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
	auxv := bcword(bc, pc+4)
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

	bc.err = 0
	mask := argptr[kRegData](bc, pc+2)
	mask.mask = bc.vmState.validLanes.mask
	return pc + 4
}

func bcsplitgo(bc *bytecode, pc int) int {
	retv := argptr[vRegData](bc, pc)
	rets := argptr[sRegData](bc, pc+2)
	retk := argptr[kRegData](bc, pc+4)
	srcs := *argptr[sRegData](bc, pc+6)
	srcmask := argptr[kRegData](bc, pc+8).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcs.offsets[i], srcs.sizes[i]}.mem()
		if len(mem) == 0 {
			rets.offsets[i] = srcs.offsets[i]
			rets.sizes[i] = srcs.sizes[i]
			continue
		}
		size := ion.SizeOf(mem)
		rets.offsets[i] = srcs.offsets[i] + uint32(size)
		rets.sizes[i] = srcs.sizes[i] - uint32(size)
		retv.offsets[i] = srcs.offsets[i]
		retv.sizes[i] = uint32(size)
		retv.typeL[i] = byte(mem[0])
		retv.headerSize[i] = byte(ion.HeaderSizeOf(mem))
		retmask |= (1 << i)
	}
	retk.mask = retmask
	return pc + 10
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

func bcretgo(bc *bytecode, pc int) int {
	bc.err = 0
	bc.vmState.outputLanes.mask = bc.vmState.validLanes.mask
	bc.auxpos += bits.OnesCount16(bc.vmState.outputLanes.mask)
	return pc
}

func bcretkgo(bc *bytecode, pc int) int {
	k := argptr[kRegData](bc, pc+0)
	bc.vmState.outputLanes.mask = k.mask
	bc.auxpos += bits.OnesCount16(bc.vmState.validLanes.mask)
	return pc + 2
}

func bcretbkgo(bc *bytecode, pc int) int {
	b := argptr[bRegData](bc, pc+0)
	k := argptr[kRegData](bc, pc+2)

	bc.vmState.delims.offsets = b.offsets
	bc.vmState.delims.sizes = b.sizes
	bc.vmState.outputLanes.mask = k.mask
	bc.auxpos += bits.OnesCount16(bc.vmState.validLanes.mask)
	return pc + 4
}

func bcretbhkgo(bc *bytecode, pc int) int {
	b := argptr[bRegData](bc, pc+0)
	k := argptr[kRegData](bc, pc+4)

	bc.vmState.delims.offsets = b.offsets
	bc.vmState.delims.sizes = b.sizes
	bc.vmState.outputLanes.mask = k.mask
	bc.auxpos += bits.OnesCount16(bc.vmState.validLanes.mask)

	return pc + 6
}

func bcretskgo(bc *bytecode, pc int) int {
	s := argptr[sRegData](bc, pc+0)
	k := argptr[kRegData](bc, pc+2)
	bc.vmState.sreg = *s
	bc.vmState.outputLanes = *k
	bc.auxpos += bits.OnesCount16(bc.vmState.validLanes.mask)
	return pc + 4
}

func init() {
	opinfo[opinit].portable = bcinitgo
	opinfo[opret].portable = bcretgo
	opinfo[opretk].portable = bcretkgo
	opinfo[opretbk].portable = bcretbkgo
	opinfo[opretsk].portable = bcretskgo
	opinfo[opretbhk].portable = bcretbhkgo

	opinfo[opbroadcasti64].portable = bcbroadcasti64go
	opinfo[opabsi64].portable = bcabsi64go
	opinfo[opnegi64].portable = bcnegi64go
	opinfo[opsigni64].portable = bcsigni64go
	opinfo[opsquarei64].portable = bcsquarei64go
	opinfo[opbitnoti64].portable = bcbitnoti64go
	opinfo[opbitcounti64].portable = bcbitcounti64go
	opinfo[opbitcounti64v2].portable = bcbitcounti64go
	opinfo[opaddi64].portable = bcaddi64go
	opinfo[opaddi64imm].portable = bcaddi64immgo
	opinfo[opsubi64].portable = bcsubi64go
	opinfo[opsubi64imm].portable = bcsubi64immgo
	opinfo[oprsubi64imm].portable = bcrsubi64immgo
	opinfo[opmuli64].portable = bcmuli64go
	opinfo[opmuli64imm].portable = bcmuli64immgo
	opinfo[opdivi64].portable = bcdivi64go
	opinfo[opdivi64imm].portable = bcdivi64immgo
	opinfo[oprdivi64imm].portable = bcrdivi64immgo
	opinfo[opmodi64].portable = bcmodi64go
	opinfo[opmodi64imm].portable = bcmodi64immgo
	opinfo[oprmodi64imm].portable = bcrmodi64immgo
	opinfo[oppmodi64].portable = bcpmodi64go
	opinfo[oppmodi64imm].portable = bcpmodi64immgo
	opinfo[oprpmodi64imm].portable = bcrpmodi64immgo
	opinfo[opaddmuli64imm].portable = addmuli64immgo
	opinfo[opminvaluei64].portable = bcminvaluei64go
	opinfo[opminvaluei64imm].portable = bcminvaluei64immgo
	opinfo[opmaxvaluei64].portable = bcmaxvaluei64go
	opinfo[opmaxvaluei64imm].portable = bcmaxvaluei64immgo
	opinfo[opandi64].portable = bcandi64go
	opinfo[opandi64imm].portable = bcandi64immgo
	opinfo[opori64].portable = bcori64go
	opinfo[opori64imm].portable = bcori64immgo
	opinfo[opxori64].portable = bcxori64go
	opinfo[opxori64imm].portable = bcxori64immgo
	opinfo[opslli64].portable = bcslli64go
	opinfo[opslli64imm].portable = bcslli64immgo
	opinfo[opsrai64].portable = bcsrai64go
	opinfo[opsrai64imm].portable = bcsrai64immgo
	opinfo[opsrli64].portable = bcsrli64go
	opinfo[opsrli64imm].portable = bcsrli64immgo

	opinfo[opcmpv].portable = bccmpvgo
	opinfo[opsortcmpvnf].portable = bccmpvgo
	opinfo[opsortcmpvnl].portable = bccmpvgo
	opinfo[opcmpvk].portable = bccmpvkgo
	opinfo[opcmpvkimm].portable = bccmpvkimmgo
	opinfo[opcmpvf64].portable = bccmpvf64go
	opinfo[opcmpvf64imm].portable = bccmpvf64immgo
	opinfo[opcmpvi64].portable = bccmpvi64go
	opinfo[opcmpvi64imm].portable = bccmpvi64immgo
	opinfo[opcmpeqslice].portable = bccmpeqslicego
	opinfo[opcmpltstr].portable = bccmpltstrgo
	opinfo[opcmplestr].portable = bccmplestrgo
	opinfo[opcmpgtstr].portable = bccmpgtstrgo
	opinfo[opcmpgestr].portable = bccmpgestrgo
	opinfo[opcmpltk].portable = bccmpltkgo
	opinfo[opcmpltkimm].portable = bccmpltkimmgo
	opinfo[opcmplek].portable = bccmplekgo
	opinfo[opcmplekimm].portable = bccmplekimmgo
	opinfo[opcmpgtk].portable = bccmpgtkgo
	opinfo[opcmpgtkimm].portable = bccmpgtkimmgo
	opinfo[opcmpgek].portable = bccmpgekgo
	opinfo[opcmpgekimm].portable = bccmpgekimmgo
	opinfo[opcmpeqf64].portable = bccmpeqf64go
	opinfo[opcmpeqf64imm].portable = bccmpeqf64immgo
	opinfo[opcmpltf64].portable = bccmpltf64go
	opinfo[opcmpltf64imm].portable = bccmpltf64immgo
	opinfo[opcmplef64].portable = bccmplef64go
	opinfo[opcmplef64imm].portable = bccmplef64immgo
	opinfo[opcmpgtf64].portable = bccmpgtf64go
	opinfo[opcmpgtf64imm].portable = bccmpgtf64immgo
	opinfo[opcmpgef64].portable = bccmpgef64go
	opinfo[opcmpgef64imm].portable = bccmpgef64immgo
	opinfo[opcmpeqi64].portable = bccmpeqi64go
	opinfo[opcmpeqi64imm].portable = bccmpeqi64immgo
	opinfo[opcmplti64].portable = bccmplti64go
	opinfo[opcmplti64imm].portable = bccmplti64immgo
	opinfo[opcmplei64].portable = bccmplei64go
	opinfo[opcmplei64imm].portable = bccmplei64immgo
	opinfo[opcmpgti64].portable = bccmpgti64go
	opinfo[opcmpgti64imm].portable = bccmpgti64immgo
	opinfo[opcmpgei64].portable = bccmpgei64go
	opinfo[opcmpgei64imm].portable = bccmpgei64immgo
	opinfo[opcmpeqv].portable = bccmpeqvgo
	opinfo[opcmpeqvimm].portable = bccmpeqvimmgo

	opinfo[opauxval].portable = bcauxvalgo
	opinfo[opsplit].portable = bcsplitgo

	opinfo[oparraysize].portable = bcarraysizego
	opinfo[oparrayposition].portable = bcarraypositiongo
	opinfo[oparraysum].portable = bcarraysumgo
	opinfo[opvectorinnerproduct].portable = bcvectorinnerproductgo
	opinfo[opvectorinnerproductimm].portable = bcvectorinnerproductimmgo
	opinfo[opvectorl1distance].portable = bcvectorl1distancego
	opinfo[opvectorl1distanceimm].portable = bcvectorl1distanceimmgo
	opinfo[opvectorl2distance].portable = bcvectorl2distancego
	opinfo[opvectorl2distanceimm].portable = bcvectorl2distanceimmgo
	opinfo[opvectorcosinedistance].portable = bcvectorcosinedistancego
	opinfo[opvectorcosinedistanceimm].portable = bcvectorcosinedistanceimmgo

	opinfo[oplitref].portable = bclitrefgo
	opinfo[opisnullv].portable = bcisnullvgo
	opinfo[opisnotnullv].portable = bcisnotnullvgo
	opinfo[opistruev].portable = bcistruevgo
	opinfo[opisfalsev].portable = bcisfalsevgo
	opinfo[optypebits].portable = bctypebitsgo
	opinfo[opchecktag].portable = bcchecktaggo
	opinfo[opobjectsize].portable = bcobjectsizego
	opinfo[opfindsym].portable = bcfindsymgo
	opinfo[opfindsym2].portable = bcfindsym2go

	opinfo[opCmpStrEqCs].portable = func(bc *bytecode, pc int) int { return bcCmpStrGo(bc, pc, opCmpStrEqCs) }
	opinfo[opCmpStrEqCi].portable = func(bc *bytecode, pc int) int { return bcCmpStrGo(bc, pc, opCmpStrEqCi) }
	opinfo[opCmpStrEqUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcCmpStrGo(bc, pc, opCmpStrEqUTF8Ci) }

	opinfo[opCmpStrFuzzyA3].portable = func(bc *bytecode, pc int) int { return bcCmpStrFuzzyGo(bc, pc, opCmpStrFuzzyA3) }
	opinfo[opCmpStrFuzzyUnicodeA3].portable = func(bc *bytecode, pc int) int { return bcCmpStrFuzzyGo(bc, pc, opCmpStrFuzzyUnicodeA3) }
	opinfo[opHasSubstrFuzzyA3].portable = func(bc *bytecode, pc int) int { return bcCmpStrFuzzyGo(bc, pc, opHasSubstrFuzzyA3) }
	opinfo[opHasSubstrFuzzyUnicodeA3].portable = func(bc *bytecode, pc int) int { return bcCmpStrFuzzyGo(bc, pc, opHasSubstrFuzzyUnicodeA3) }

	opinfo[opSkip1charLeft].portable = func(bc *bytecode, pc int) int { return bcSkip1Go(bc, pc, opSkip1charLeft) }
	opinfo[opSkip1charRight].portable = func(bc *bytecode, pc int) int { return bcSkip1Go(bc, pc, opSkip1charRight) }
	opinfo[opSkipNcharLeft].portable = func(bc *bytecode, pc int) int { return bcSkipNGo(bc, pc, opSkipNcharLeft) }
	opinfo[opSkipNcharRight].portable = func(bc *bytecode, pc int) int { return bcSkipNGo(bc, pc, opSkipNcharRight) }

	opinfo[opTrimWsLeft].portable = func(bc *bytecode, pc int) int { return bcTrimWsGo(bc, pc, opTrimWsLeft) }
	opinfo[opTrimWsRight].portable = func(bc *bytecode, pc int) int { return bcTrimWsGo(bc, pc, opTrimWsRight) }
	opinfo[opTrim4charLeft].portable = func(bc *bytecode, pc int) int { return bcTrim4CharGo(bc, pc, opTrim4charLeft) }
	opinfo[opTrim4charRight].portable = func(bc *bytecode, pc int) int { return bcTrim4CharGo(bc, pc, opTrim4charRight) }

	opinfo[opoctetlength].portable = func(bc *bytecode, pc int) int { return bcLengthGo(bc, pc, opoctetlength) }
	opinfo[opcharlength].portable = func(bc *bytecode, pc int) int { return bcLengthGo(bc, pc, opcharlength) }
	opinfo[opSubstr].portable = bcSubstrGo
	opinfo[opSplitPart].portable = bcSplitPartGo

	opinfo[opContainsPrefixCs].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsPrefixCs) }
	opinfo[opContainsPrefixCi].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsPrefixCi) }
	opinfo[opContainsPrefixUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsPrefixUTF8Ci) }
	opinfo[opContainsSuffixCs].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSuffixCs) }
	opinfo[opContainsSuffixCi].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSuffixCi) }
	opinfo[opContainsSuffixUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSuffixUTF8Ci) }
	opinfo[opContainsSubstrCs].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSubstrCs) }
	opinfo[opContainsSubstrCi].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSubstrCi) }
	opinfo[opContainsSubstrUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcContainsPreSufSubGo(bc, pc, opContainsSubstrUTF8Ci) }

	opinfo[opEqPatternCs].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opEqPatternCs) }
	opinfo[opEqPatternCi].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opEqPatternCi) }
	opinfo[opEqPatternUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opEqPatternUTF8Ci) }
	opinfo[opContainsPatternCs].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opContainsPatternCs) }
	opinfo[opContainsPatternCi].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opContainsPatternCi) }
	opinfo[opContainsPatternUTF8Ci].portable = func(bc *bytecode, pc int) int { return bcContainsPatternGo(bc, pc, opContainsPatternUTF8Ci) }

	opinfo[opIsSubnetOfIP4].portable = bcIsSubnetOfIP4Go

	opinfo[opDfaT6].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT6) }
	opinfo[opDfaT7].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT7) }
	opinfo[opDfaT8].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT8) }
	opinfo[opDfaT6Z].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT6Z) }
	opinfo[opDfaT7Z].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT7Z) }
	opinfo[opDfaT8Z].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaT8Z) }
	opinfo[opDfaLZ].portable = func(bc *bytecode, pc int) int { return bcDFAGo(bc, pc, opDfaLZ) }

	opinfo[optuple].portable = bctuplego
	opinfo[opbroadcast0k].portable = bcbroadcast0kgo
	opinfo[opbroadcast1k].portable = bcbroadcast1kgo
	opinfo[opfalse].portable = bcfalsego
	opinfo[opnotk].portable = bcnotkgo
	opinfo[opandk].portable = bcandkgo
	opinfo[opandnk].portable = bcandnkgo
	opinfo[opork].portable = bcorkgo
	opinfo[opxork].portable = bcxorkgo
	opinfo[opxnork].portable = bcxnorkgo

	opinfo[opaggminf].portable = bcaggminf
	opinfo[opaggmaxf].portable = bcaggmaxf
	opinfo[opaggmini].portable = bcaggmini
	opinfo[opaggmaxi].portable = bcaggmaxi

	opinfo[opcvtktof64].portable = bccvtktof64
	opinfo[opcvtktoi64].portable = bccvtktoi64
	opinfo[opcvti64tok].portable = bccvti64tok
	opinfo[opcvtf64tok].portable = bccvtf64tok
	opinfo[opcvti64tof64].portable = bccvti64tof64
	opinfo[opcvtfloorf64toi64].portable = bccvtfloorf64toi64
	opinfo[opcvtceilf64toi64].portable = bccvtceilf64toi64
	opinfo[opcvttruncf64toi64].portable = bccvttruncf64toi64

	opinfo[ophashvalue].portable = bchashvaluego
	opinfo[ophashvalueplus].portable = bchashvalueplusgo
	opinfo[ophashmember].portable = bchashmembergo
	opinfo[ophashlookup].portable = bchashlookupgo

	opinfo[opzerov].portable = bczerovgo
	opinfo[opmovv].portable = bcmovvgo
	opinfo[opmovf64].portable = bcmovf64go
	opinfo[opmovi64].portable = bcmovi64go
	opinfo[opmovk].portable = bcmovkgo
	opinfo[opmovvk].portable = bcmovvkgo
	opinfo[opblendv].portable = bcblendvgo
	opinfo[opblendf64].portable = bcblendf64go
}

func evalfindgo(bc *bytecode, delims []vmref, stride int) {
	stack := bc.vstack
	var alt bytecode
	bc.scratch = bc.scratch[:len(bc.savedlit)] // reset scratch ONCE, here
	// convert stride to 64-bit words:
	stride = stride / int(unsafe.Sizeof(bc.vstack[0]))
	for len(delims) > 0 {
		mask := uint16(0xffff)
		lanes := bcLaneCount
		if len(delims) < lanes {
			mask >>= bcLaneCount - len(delims)
			lanes = len(delims)
		}
		bc.err = 0
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = mask
		setvmrefB(&bc.vmState.delims, delims)
		eval(bc, &alt, false)
		if bc.err != 0 {
			return
		}
		delims = delims[lanes:]
		bc.vstack = bc.vstack[stride:]
	}
	bc.vstack = stack
}

func evalsplatgo(bc *bytecode, indelims, outdelims []vmref, perm []int32) (int, int) {
	ipos, opos := 0, 0
	var alt bytecode
	for ipos < len(indelims) && opos < len(outdelims) {
		next := indelims[ipos:]
		mask := uint16(0xffff)
		if len(next) < bcLaneCount {
			mask >>= bcLaneCount - len(next)
		}
		setvmrefB(&bc.vmState.delims, indelims[ipos:])
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = 0
		eval(bc, &alt, true)
		if bc.err != 0 {
			return 0, 0
		}
		retmask := bc.vmState.outputLanes.mask
		output := opos
		lanes := min(bcLaneCount, len(next))
		for i := 0; i < lanes; i++ {
			if (retmask & (1 << i)) == 0 {
				continue
			}
			start := bc.vmState.sreg.offsets[i]
			width := bc.vmState.sreg.sizes[i]
			slice := vmref{start, width}.mem()
			for len(slice) > 0 {
				if output == len(outdelims) || output == len(perm) {
					// need to return early
					return ipos, opos
				}
				s := ion.SizeOf(slice)
				outdelims[output] = vmref{start, uint32(s)}
				perm[output] = int32(i + ipos)
				output++
				slice = slice[s:]
				start += uint32(s)
			}
		}
		// checkpoint splat
		opos = output
		ipos += lanes
	}
	return ipos, opos
}

func evalfiltergolanes(bc *bytecode, delims []vmref) uint16 {
	if len(delims) > bcLaneCount {
		panic("invalid len(delims) for evalfiltergolanes")
	}
	mask := uint16(0xffff)
	mask >>= bcLaneCount - len(delims)
	var alt bytecode
	setvmrefB(&bc.vmState.delims, delims)
	bc.vmState.validLanes.mask = mask
	bc.vmState.outputLanes.mask = 0
	eval(bc, &alt, true)
	if bc.err != 0 {
		return 0
	}
	return bc.vmState.outputLanes.mask
}

func evalprojectgo(bc *bytecode, delims []vmref, dst []byte, symbols []syminfo) (int, int) {
	offset := 0
	capacity := len(dst)
	rowsProcessed := 0

	dstDisp, ok := vmdispl(dst)
	if !ok {
		return 0, 0
	}

	var alt bytecode

	for rowsProcessed < len(delims) {
		initialDstLength := offset
		n := min(len(delims)-rowsProcessed, bcLaneCount)

		for lane := 0; lane < n; lane++ {
			bc.vmState.delims.offsets[lane] = uint32(delims[rowsProcessed+lane][0])
			bc.vmState.delims.sizes[lane] = uint32(delims[rowsProcessed+lane][1])
		}

		mask := uint16((1 << n) - 1)
		bc.err = 0
		bc.vmState.validLanes.mask = mask

		eval(bc, &alt, true)

		if bc.err != 0 {
			return initialDstLength, rowsProcessed
		}

		mask = bc.vmState.outputLanes.mask

		for lane := 0; lane < n; lane++ {
			if (mask & (uint16(1) << lane)) == 0 {
				continue
			}

			contentSize := uint(0)
			for i := 0; i < len(symbols); i++ {
				// Only account for the value if the value is not MISSING.
				vals := slotcast[vRegData](bc, uint(symbols[i].slot))
				if symbols[i].size != 0 && vals.sizes[lane] != 0 {
					contentSize += uint(symbols[i].size) + uint(vals.sizes[lane])
				}
			}

			headerSize := getTLVSize(contentSize)
			structSize := headerSize + contentSize

			if uint(capacity-offset) < structSize {
				return initialDstLength, rowsProcessed
			}

			offset += encodeTLVUnsafe(dst, offset, ion.StructType, contentSize)

			// Update delims with the projection output
			delims[rowsProcessed+lane][0] = uint32(dstDisp + uint32(offset))
			delims[rowsProcessed+lane][1] = uint32(contentSize)

			for i := 0; i < len(symbols); i++ {
				// Only serialize Key+Value if the value is not MISSING.
				vals := slotcast[vRegData](bc, uint(symbols[i].slot))
				if symbols[i].size != 0 && vals.sizes[lane] != 0 {
					valOffset := vals.offsets[lane]
					valLength := vals.sizes[lane]

					offset += encodeSymbol(dst, offset, symbols[i].value)
					offset += copy(dst[offset:], vmm[valOffset:valOffset+valLength])
				}
			}
		}

		rowsProcessed += n
	}

	return offset, rowsProcessed
}

func evaldedupgo(bc *bytecode, delims []vmref, hashes []uint64, tree *radixTree64, slot int) int {
	var alt bytecode
	indelims := delims
	dout := 0
	for len(indelims) > 0 {
		n := min(len(indelims), bcLaneCount)
		mask := uint16(0xffff)
		if n < bcLaneCount {
			mask >>= bcLaneCount - n
		}
		setvmrefB(&bc.vmState.delims, indelims[:n])
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = 0
		apos := bc.auxpos
		eval(bc, &alt, true)
		if bc.err != 0 {
			return 0
		}
		outhashes := slotcast[hRegData](bc, uint(slot))
		outmask := bc.vmState.outputLanes.mask
		for i := 0; i < n; i++ {
			if outmask&(1<<i) == 0 || tree.Offset(outhashes.lo[i]) >= 0 {
				continue // lane not active or tree contains hash already
			}
			delims[dout] = indelims[i]
			hashes[dout] = outhashes.lo[i]
			// compress auxvals
			for j, lst := range bc.auxvals {
				bc.auxvals[j][dout] = lst[apos+i]
			}
			dout++
		}
		indelims = indelims[n:]
	}
	return dout
}

func evalaggregatego(bc *bytecode, delims []vmref, aggregateDataBuffer []byte) int {
	var alt bytecode
	ret := 0
	bc.vmState.aggPtr = unsafe.Pointer(&aggregateDataBuffer[0])
	for len(delims) > 0 {
		n := min(len(delims), bcLaneCount)
		mask := uint16(0xffff) >> (bcLaneCount - n)
		setvmrefB(&bc.vmState.delims, delims)
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = 0
		eval(bc, &alt, true)
		if bc.err != 0 {
			return ret
		}
		delims = delims[n:]
		ret += n
	}
	return ret
}

func evalhashagggo(bc *bytecode, delims []vmref, tree *radixTree64) int {
	var alt bytecode
	ret := 0

	bc.err = 0
	bc.missingBucketMask = 0
	bc.vmState.aggPtr = unsafe.Pointer(tree)

	for len(delims) > 0 {
		n := min(len(delims), bcLaneCount)
		mask := uint16(0xffff) >> (bcLaneCount - n)

		setvmrefB(&bc.vmState.delims, delims)
		bc.vmState.validLanes.mask = mask
		bc.vmState.outputLanes.mask = 0

		eval(bc, &alt, true)
		if bc.err != 0 {
			return ret
		}

		delims = delims[n:]
		ret += n
	}
	return ret
}

//go:noescape
func bcenter(bc *bytecode, k7 uint16)

// eval evaluates bc and uses alt as scratch space
// for evaluating unimplemented opcodes via the assembly interpreter
func eval(bc, alt *bytecode, resetScratch bool) {
	l := len(bc.compiled)
	pc := 0
	if resetScratch {
		bc.scratch = bc.scratch[:len(bc.savedlit)]
	}
	for pc < l && bc.err == 0 {
		op := bcop(bcword(bc, pc))
		pc += 2
		fn := opinfo[op].portable
		if fn != nil {
			pc = fn(bc, pc)
		} else if cpu.X86.HasAVX512 {
			pc = runSingle(bc, alt, pc, bc.vmState.validLanes.mask)
		} else {
			bc.err = bcerrNotSupported
			break
		}
	}
}

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
	bc.scratch = alt.scratch // copy back len(scratch)
	bc.err = alt.err
	bc.errpc = alt.errpc
	bc.errinfo = alt.errinfo
	bc.missingBucketMask = alt.missingBucketMask
	return pc + width
}
