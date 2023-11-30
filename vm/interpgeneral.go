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

func bczerovgo(bc *bytecode, pc int) int {
	*argptr[vRegData](bc, pc+0) = vRegData{}
	return pc + 2
}

func bcmovvgo(bc *bytecode, pc int) int {
	destv := argptr[vRegData](bc, pc+0)
	srcv := argptr[vRegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+4).mask
	r := vRegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.offsets[lane] = srcv.offsets[lane]
			r.sizes[lane] = srcv.sizes[lane]
			r.typeL[lane] = srcv.typeL[lane]
			r.headerSize[lane] = srcv.headerSize[lane]
		}
	}

	*destv = r
	return pc + 6
}

func bcmovf64go(bc *bytecode, pc int) int {
	destf64 := argptr[f64RegData](bc, pc+0)
	srcf64 := argptr[f64RegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = srcf64.values[lane]
		}
	}

	*destf64 = r
	return pc + 6
}

func bcmovi64go(bc *bytecode, pc int) int {
	desti64 := argptr[i64RegData](bc, pc+0)
	srci64 := argptr[i64RegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+4).mask
	r := i64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = srci64.values[lane]
		}
	}

	*desti64 = r
	return pc + 6
}

func bcmovkgo(bc *bytecode, pc int) int {
	destk := argptr[kRegData](bc, pc+0)
	srcmask := argptr[kRegData](bc, pc+2).mask
	destk.mask = srcmask
	return pc + 4
}

func bcmovvkgo(bc *bytecode, pc int) int {
	destv := argptr[vRegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	r := vRegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.offsets[lane] = srcv.offsets[lane]
			r.sizes[lane] = srcv.sizes[lane]
			r.typeL[lane] = srcv.typeL[lane]
			r.headerSize[lane] = srcv.headerSize[lane]
		}
	}

	*destv = r
	destk.mask = srcmask
	return pc + 8
}

func bcblendvgo(bc *bytecode, pc int) int {
	destv := argptr[vRegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	srcmask1 := argptr[kRegData](bc, pc+6).mask
	srcv1 := argptr[vRegData](bc, pc+4)
	srcmask2 := argptr[kRegData](bc, pc+10).mask
	srcv2 := argptr[vRegData](bc, pc+8)
	r := vRegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask1&(1<<lane) != 0 {
			r.offsets[lane] = srcv1.offsets[lane]
			r.sizes[lane] = srcv1.sizes[lane]
			r.typeL[lane] = srcv1.typeL[lane]
			r.headerSize[lane] = srcv1.headerSize[lane]
		}
		if srcmask2&(1<<lane) != 0 {
			r.offsets[lane] = srcv2.offsets[lane]
			r.sizes[lane] = srcv2.sizes[lane]
			r.typeL[lane] = srcv2.typeL[lane]
			r.headerSize[lane] = srcv2.headerSize[lane]
		}
	}

	*destv = r
	destk.mask = srcmask1 | srcmask2
	return pc + 12
}

func bcblendf64go(bc *bytecode, pc int) int {
	destf64 := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	srcmaska := argptr[kRegData](bc, pc+6).mask
	srcf64a := argptr[f64RegData](bc, pc+4)
	srcmaskb := argptr[kRegData](bc, pc+10).mask
	srcf64b := argptr[f64RegData](bc, pc+8)
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmaska&(1<<lane) != 0 {
			r.values[lane] = srcf64a.values[lane]
		}
		if srcmaskb&(1<<lane) != 0 {
			r.values[lane] = srcf64b.values[lane]
		}
	}

	*destf64 = r
	destk.mask = srcmaska | srcmaskb
	return pc + 12
}
