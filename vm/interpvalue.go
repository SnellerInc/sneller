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

import "github.com/SnellerInc/sneller/ion"

// A lookup table that is used to convert ION type to our own type.
var typeBitsLookupTable = [16]byte{
	1 << 0, // null -> null
	1 << 1, // bool -> bool
	1 << 2, // uint -> number
	1 << 2, // int -> number
	1 << 2, // float -> number
	1 << 2, // decimal -> number
	1 << 3, // timestamp -> timestamp
	1 << 4, // symbol -> string
	1 << 4, // string -> string
	0,      // clob is unused
	0,      // blob is unused
	1 << 5, // list -> list
	0,      // sexp is unused
	1 << 6, // struct -> struct
	0,      // annotation is unused
	0,      // reserved is unused
}

func countValuesInList(msg []byte) int {
	count := 0

	for len(msg) != 0 {
		valueSize := ion.SizeOf(msg)
		// sanity check: shouldn't happen if the data is valid
		if valueSize <= 0 {
			return -1
		}

		count++
		msg = msg[valueSize:]
	}

	return count
}

func countValuesInStruct(msg []byte) int {
	count := 0

	for len(msg) != 0 {
		_, val, err := ion.ReadLabel(msg)

		if err != nil {
			return -1
		}

		valueSize := ion.SizeOf(val)
		// sanity check: shouldn't happen if the data is valid
		if valueSize <= 0 {
			return -1
		}

		count++
		msg = val[valueSize:]
	}

	return count
}

func bclitrefgo(bc *bytecode, pc int) int {
	offset := bcword32(bc, pc+2) + bc.scratchoff
	size := bcword32(bc, pc+6)
	tlv := bcword8(bc, pc+10)
	headerSize := bcword8(bc, pc+11)

	dst := vRegData{}
	msk := bc.vmState.validLanes.mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.offsets[i] = offset
			dst.sizes[i] = size
			dst.typeL[i] = tlv
			dst.headerSize[i] = headerSize
		}
	}

	*argptr[vRegData](bc, pc) = dst
	return pc + 12
}

func bcisnullvgo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if src.typeL[i] != 0x0F {
			msk &^= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{msk}
	return pc + 6
}

func bcisnotnullvgo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if src.typeL[i] == 0x0F {
			msk &^= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{msk}
	return pc + 6
}

func bcistruevgo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if src.typeL[i] != 0x11 {
			msk &^= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{msk}
	return pc + 6
}

func bcisfalsevgo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if src.typeL[i] != 0x10 {
			msk &^= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{msk}
	return pc + 6
}

func bctypebitsgo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(uint64(typeBitsLookupTable[src.typeL[i]>>4]))
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcchecktaggo(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+4)
	tags := uint(bcword(bc, pc+6))

	dst := vRegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			if (tags & (uint(1) << (src.typeL[i] >> 4))) != 0 {
				dst.offsets[i] = src.offsets[i]
				dst.sizes[i] = src.sizes[i]
				dst.typeL[i] = src.typeL[i]
				dst.headerSize[i] = src.headerSize[i]
			} else {
				msk &^= 1 << i
			}
		}
	}

	*argptr[vRegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcobjectsizego(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+4)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+6).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		ionType := ion.Type(src.typeL[i] >> 4)
		content := vmref{src.offsets[i] + uint32(src.headerSize[i]), src.sizes[i] - uint32(src.headerSize[i])}.mem()

		if ionType == ion.ListType {
			count := countValuesInList(content)
			if count >= 0 {
				dst.values[i] = int64(count)
				continue
			}
		} else if ionType == ion.StructType {
			count := countValuesInStruct(content)
			if count >= 0 {
				dst.values[i] = int64(count)
				continue
			}
		}

		msk &^= 1 << i
	}

	*argptr[i64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
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

func bcfindsym2go(bc *bytecode, pc int) int {
	dstv := argptr[vRegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcb := *argptr[bRegData](bc, pc+4)
	srcv := *argptr[vRegData](bc, pc+6)
	srck := argptr[kRegData](bc, pc+8).mask
	symbol, _, _ := ion.ReadLabel(bc.compiled[pc+10:])
	srcmask := argptr[kRegData](bc, pc+14).mask

	// initial dst state is the previous src value state
	*dstv = srcv
	retmask := uint16(0)
outer:
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		searchpos := srcb.offsets[i]
		end := srcb.offsets[i] + srcb.sizes[i]
		if srck&(1<<i) != 0 {
			searchpos = srcv.offsets[i] + srcv.sizes[i]
		}
		if searchpos >= end {
			continue
		}
		mem := vmref{searchpos, end - searchpos}.mem()
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
			dstv.offsets[i] = end - uint32(len(mem))
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
	return pc + 16
}

func calcStringTlvAndHLen(valueSize uint32) (byte, byte) {
	tlv := byte(ion.StringType<<4) | byte(valueSize&0xFF)
	hLen := byte(1)
	if valueSize > 14 {
		tlv = byte(ion.StringType<<4) | 0xE
		hLen++
	}

	if valueSize > 129 {
		hLen++
	}

	if valueSize > 16386 {
		hLen++
	}

	if valueSize > 2097155 {
		hLen++
	}

	return tlv, hLen
}

func bcunsymbolizego(bc *bytecode, pc int) int {
	src := argptr[vRegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask

	dst := *src

	for i := 0; i < bcLaneCount; i++ {
		if (msk&(1<<i)) == 0 || src.sizes[i] == 0 {
			continue
		}

		tlv := src.typeL[i]
		if ion.Type(tlv>>4) != ion.SymbolType {
			continue
		}

		mem := vmref{src.offsets[i], src.sizes[i]}.mem()
		symbol, _, _ := ion.ReadSymbol(mem)

		symref := bc.symtab[symbol]
		dst.offsets[i] = symref[0]
		dst.sizes[i] = symref[1]

		// NOTE: We don't have to read the string data to properly
		// construct the tlv byte and its header size as we know it's
		// a string and we know how to calculate TLV and header size.
		dst.typeL[i], dst.headerSize[i] = calcStringTlvAndHLen(symref[1])
	}

	*argptr[vRegData](bc, pc) = dst
	return pc + 6
}
