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

import "github.com/SnellerInc/sneller/ion"

func init() {
	opinfo[opunboxcoercef64].portable = bcunboxcoercef64go
	opinfo[opunboxcoercei64].portable = bcunboxcoercei64go
	opinfo[opunboxcvtf64].portable = bcunboxcvtf64go
	opinfo[opunboxcvti64].portable = bcunboxcvti64go
	opinfo[opunboxts].portable = bcunboxtsgo
	opinfo[opunboxktoi64].portable = bcunboxktoi64go
	opinfo[opunpack].portable = bcunpackgo
}

func bcunboxktoi64go(bc *bytecode, pc int) int {
	dst := argptr[i64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out i64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.BoolType:
			k, _, _ := ion.ReadBool(mem)
			if k {
				out.values[i] = 1
			}
			retmask |= 1 << i
		default:
			// do nothing
		}
	}
	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunboxtsgo(bc *bytecode, pc int) int {
	dst := argptr[i64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out i64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.TimestampType:
			dt, _, _ := ion.ReadTime(mem)
			out.values[i] = dt.UnixMicro()
			retmask |= 1 << i
		default:
			// do nothing
		}
	}
	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunboxcvti64go(bc *bytecode, pc int) int {
	dst := argptr[i64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out i64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.FloatType:
			f, _, _ := ion.ReadFloat64(mem)
			out.values[i] = int64(f)
			retmask |= (1 << i)
		case ion.IntType:
			v, _, _ := ion.ReadInt(mem)
			out.values[i] = v
			retmask |= 1 << i
		case ion.UintType:
			u, _, _ := ion.ReadUint(mem)
			out.values[i] = int64(u)
			retmask |= 1 << i
		case ion.BoolType:
			k, _, _ := ion.ReadBool(mem)
			if k {
				out.values[i] = 1
			} else {
				out.values[i] = 0
			}
			retmask |= 1 << i
		default:
			// leave mask unset
		}
	}

	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunboxcvtf64go(bc *bytecode, pc int) int {
	dst := argptr[f64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out f64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.FloatType:
			f, _, _ := ion.ReadFloat64(mem)
			out.values[i] = f
			retmask |= (1 << i)
		case ion.IntType:
			v, _, _ := ion.ReadInt(mem)
			out.values[i] = float64(v)
			retmask |= 1 << i
		case ion.UintType:
			u, _, _ := ion.ReadUint(mem)
			out.values[i] = float64(u)
			retmask |= 1 << i
		case ion.BoolType:
			k, _, _ := ion.ReadBool(mem)
			if k {
				out.values[i] = 1.0
			} else {
				out.values[i] = 0.0
			}
			retmask |= 1 << i
		default:
			// leave mask unset
		}
	}

	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunboxcoercei64go(bc *bytecode, pc int) int {
	dst := argptr[i64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out i64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.FloatType:
			f, _, _ := ion.ReadFloat64(mem)
			out.values[i] = int64(f)
			retmask |= (1 << i)
		case ion.IntType:
			v, _, _ := ion.ReadInt(mem)
			out.values[i] = v
			retmask |= 1 << i
		case ion.UintType:
			u, _, _ := ion.ReadUint(mem)
			out.values[i] = int64(u)
			retmask |= 1 << i
		default:
			// leave mask unset
		}
	}

	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunboxcoercef64go(bc *bytecode, pc int) int {
	dst := argptr[f64RegData](bc, pc)
	dstk := argptr[kRegData](bc, pc+2)
	srcv := argptr[vRegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	var out f64RegData
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		mem := vmref{srcv.offsets[i], srcv.sizes[i]}.mem()
		switch ion.Type(srcv.typeL[i] >> 4) {
		case ion.FloatType:
			f, _, _ := ion.ReadFloat64(mem)
			out.values[i] = f
			retmask |= (1 << i)
		case ion.IntType:
			v, _, _ := ion.ReadInt(mem)
			out.values[i] = float64(v)
			retmask |= 1 << i
		case ion.UintType:
			u, _, _ := ion.ReadUint(mem)
			out.values[i] = float64(u)
			retmask |= 1 << i
		default:
			// leave mask unset
		}
	}

	*dst = out
	dstk.mask = retmask
	return pc + 8
}

func bcunpackgo(bc *bytecode, pc int) int {
	rets := argptr[sRegData](bc, pc)
	retk := argptr[kRegData](bc, pc+2)
	argv := argptr[vRegData](bc, pc+4)
	imm := bcword(bc, pc+6)
	argk := argptr[kRegData](bc, pc+8)

	srcmask := argk.mask
	retmask := uint16(0)
	var out sRegData
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 || uint(argv.typeL[i]>>4) != imm || argv.sizes[i] == 0 {
			continue
		}
		out.offsets[i] = argv.offsets[i] + uint32(argv.headerSize[i])
		out.sizes[i] = argv.sizes[i] - uint32(argv.headerSize[i])
		retmask |= (1 << i)
	}
	retk.mask = retmask
	*rets = out
	return pc + 10
}
