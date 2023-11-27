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
	"math"
)

func bccvtktof64(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	srcmask := argptr[kRegData](bc, pc+2).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = 1.0
		}
	}

	*dest = r
	return pc + 4
}

func bccvtktoi64(bc *bytecode, pc int) int {
	dest := argptr[i64RegData](bc, pc+0)
	srcmask := argptr[kRegData](bc, pc+2).mask
	r := i64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = 1
		}
	}

	*dest = r
	return pc + 4
}

func bccvti64tok(bc *bytecode, pc int) int {
	destk := argptr[kRegData](bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+4).mask
	retmask := uint16(0)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			if arg0.values[lane] != 0 {
				retmask |= (1 << lane)
			}
		}
	}

	destk.mask = retmask
	return pc + 6
}

func bccvtf64tok(bc *bytecode, pc int) int {
	destk := argptr[kRegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	srcmask := argptr[kRegData](bc, pc+4).mask
	retmask := uint16(0)

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			if arg0.values[lane] != 0.0 {
				retmask |= (1 << lane)
			}
		}
	}

	destk.mask = retmask
	return pc + 6
}

func bccvti64tof64(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = float64(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = srcmask
	return pc + 8
}

func bccvtfloorf64toi64(bc *bytecode, pc int) int {
	dest := argptr[i64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	r := i64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = int64(math.Floor(arg0.values[lane]))
		}
	}

	*dest = r
	destk.mask = srcmask
	return pc + 8
}

func bccvtceilf64toi64(bc *bytecode, pc int) int {
	dest := argptr[i64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	r := i64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = int64(math.Ceil(arg0.values[lane]))
		}
	}

	*dest = r
	destk.mask = srcmask
	return pc + 8
}

func bccvttruncf64toi64(bc *bytecode, pc int) int {
	dest := argptr[i64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask
	r := i64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			r.values[lane] = int64(math.Trunc(arg0.values[lane]))
		}
	}

	*dest = r
	destk.mask = srcmask
	return pc + 8
}
