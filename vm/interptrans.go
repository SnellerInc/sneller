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

func init() {
	opinfo[opisnanf].portable = bcisnanfgo
	opinfo[opcbrtf64].portable = bccbrtf64go
	opinfo[oplnf64].portable = bclnf64go
	opinfo[opln1pf64].portable = bcln1pf64go
	opinfo[oplog2f64].portable = bclog2f64go
	opinfo[oplog10f64].portable = bclog10f64go
	opinfo[oppowf64].portable = bcpowf64go
	opinfo[opacosf64].portable = bcacosf64go
	opinfo[ophypotf64].portable = bchypotf64go
	opinfo[opexpf64].portable = bcexpf64go
	opinfo[opexpm1f64].portable = bcexpm1f64go
	opinfo[opexp2f64].portable = bcexp2f64go
	opinfo[opexp10f64].portable = bcexp10f64go
	opinfo[opasinf64].portable = bcasinf64go
	opinfo[opatan2f64].portable = bcatan2f64go
	opinfo[opsinf64].portable = bcsinf64go
	opinfo[opcosf64].portable = bccosf64go
	opinfo[optanf64].portable = bctanf64go
	opinfo[opatanf64].portable = bcatanf64go
	opinfo[oppowuintf64].portable = bcpowuintf64go
}

func bcpowuintf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	imm := bcword64(bc, pc+4)
	argmask := argptr[kRegData](bc, pc+12).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = powint(arg0.values[lane], imm)
		}
	}

	*dest = r
	return pc + 14
}

func powint(x float64, exponent uint64) float64 {
	res := 1.0
	pow := x

	for exponent != 0 {
		if exponent&1 != 0 {
			res = res * pow
		}
		exponent = exponent >> 1
		pow = pow * pow
	}
	return res
}

func bcsinf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefSin(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bccosf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefCos(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bctanf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefTan(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcisnanfgo(bc *bytecode, pc int) int {
	destk := argptr[kRegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	var dstmask uint16

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			if math.IsNaN(arg0.values[lane]) {
				dstmask |= (1 << lane)
			}
		}
	}

	destk.mask = dstmask
	return pc + 6
}

func bcexpf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefExp(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcexp2f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefExp2(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcexp10f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefExp10(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcexpm1f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefExpm1(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bclnf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefLn(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcln1pf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefLn1p(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bclog2f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefLog2(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bclog10f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefLog10(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bccbrtf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefCbrt(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcacosf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefACos(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcasinf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefASin(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcatanf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefATan(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcpowf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefPow(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bchypotf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefHypot(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcatan2f64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = sleefAtan2(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}
