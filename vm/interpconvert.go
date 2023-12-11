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
