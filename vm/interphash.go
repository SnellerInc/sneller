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

	"github.com/dchest/siphash"
)

func bchashvaluego(bc *bytecode, pc int) int {
	hdst := argptr[hRegData](bc, pc+0)
	bsrc := argptr[bRegData](bc, pc+2)
	argk := argptr[kRegData](bc, pc+4)

	// Take snapshots before updating hdst due to possible aliasing
	mask := argk.mask
	offsets := bsrc.offsets
	sizes := bsrc.sizes

	for lane := 0; lane < bcLaneCount; lane++ {
		hdst.lo[lane] = 0
		hdst.hi[lane] = 0
	}

	for lane := 0; lane < bcLaneCount; lane++ {
		if mask&(1<<lane) != 0 {
			mem := vmref{offsets[lane], sizes[lane]}.mem()
			lo, hi := siphash.Hash128(0, 0, mem)
			hdst.lo[lane] = lo
			hdst.hi[lane] = hi
		}
	}
	return pc + 6
}

func bchashvalueplusgo(bc *bytecode, pc int) int {
	hdst := argptr[hRegData](bc, pc+0)
	hhash := argptr[hRegData](bc, pc+2)
	bsrc := argptr[bRegData](bc, pc+4)
	argk := argptr[kRegData](bc, pc+6)

	// Take snapshots before updating hdst due to possible aliasing
	mask := argk.mask
	offsets := bsrc.offsets
	sizes := bsrc.sizes
	h1 := hhash

	for lane := 0; lane < bcLaneCount; lane++ {
		if mask&(1<<lane) != 0 {
			k0 := h1.lo[lane]
			k1 := h1.hi[lane]
			mem := vmref{offsets[lane], sizes[lane]}.mem()
			lo, hi := siphash.Hash128(k0, k1, mem)
			hdst.lo[lane] = lo
			hdst.hi[lane] = hi
		}
	}
	return pc + 8
}

func bchashmembergo(bc *bytecode, pc int) int {
	destk := argptr[kRegData](bc, pc+0)
	mask := argptr[kRegData](bc, pc+6).mask
	retmask := uint16(0)

	if mask != 0 {
		imm := bcword(bc, pc+4)
		t := bc.trees[imm]

		hptr := argptr[hRegData](bc, pc+2)
		for lane := 0; lane < bcLaneCount; lane++ {
			if mask&(1<<lane) != 0 {
				h := hptr.lo[lane]
				if r := t.Find(h); r != nil {
					retmask |= (1 << lane)
				}
			}
		}
	}
	destk.mask = retmask
	return pc + 8
}

func bchashlookupgo(bc *bytecode, pc int) int {
	// Take snapshots before updating hdst due to possible aliasing
	destv := argptr[vRegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+8).mask
	retmask := uint16(0)

	if srcmask != 0 {
		hashes := argptr[hRegData](bc, pc+4).lo
		imm := bcword(bc, pc+6)
		t := bc.trees[imm]
		for lane := 0; lane < bcLaneCount; lane++ {
			if srcmask&(1<<lane) != 0 {
				h := hashes[lane]
				if r := t.Find(h); r != nil {
					retmask |= (1 << lane)
					offs := binary.LittleEndian.Uint32(r[0:4])
					size := binary.LittleEndian.Uint32(r[4:8])
					destv.offsets[lane] = offs
					destv.sizes[lane] = size
					mem := vmref{offs, size}.mem()
					destv.typeL[lane] = mem[0]
					destv.headerSize[lane] = byte(getTLVSize(uint(size)))
				}
			}
		}
	}

	for lane := 0; lane < bcLaneCount; lane++ {
		if retmask&(1<<lane) == 0 {
			destv.offsets[lane] = 0
			destv.sizes[lane] = 0
			destv.typeL[lane] = 0
			destv.headerSize[lane] = 0
		}
	}
	destk.mask = retmask
	return pc + 10
}
