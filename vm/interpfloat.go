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

func init() {
	opinfo[opabsf64].portable = bcabsf64go
	opinfo[opnegf64].portable = bcnegf64go
	opinfo[optruncf64].portable = bctruncf64go
	opinfo[opfloorf64].portable = bcfloorf64go
	opinfo[opceilf64].portable = bcceilf64go
	opinfo[oproundf64].portable = bcroundf64go
	opinfo[oproundevenf64].portable = bcroundevenf64go
	opinfo[opsquaref64].portable = bcsquaref64go
	opinfo[opsqrtf64].portable = bcsqrtf64go
	opinfo[opaddf64].portable = bcaddf64go
	opinfo[opaddf64imm].portable = bcaddf64immgo
	opinfo[opsubf64].portable = bcsubf64go
	opinfo[opsubf64imm].portable = bcsubf64immgo
	opinfo[oprsubf64imm].portable = bcrsubf64immgo
	opinfo[opmulf64].portable = bcmulf64go
	opinfo[opmulf64imm].portable = bcmulf64immgo
	opinfo[opdivf64].portable = bcdivf64go
	opinfo[opdivf64imm].portable = bcdivf64immgo
	opinfo[oprdivf64imm].portable = bcrdivf64immgo
	opinfo[opmodf64].portable = bcmodf64go
	opinfo[opmodf64imm].portable = bcmodf64immgo
	opinfo[oprmodf64imm].portable = bcrmodf64immgo
	opinfo[opminvaluef64].portable = bcminvaluef64go
	opinfo[opminvaluef64imm].portable = bcminvaluef64immgo
	opinfo[opmaxvaluef64].portable = bcmaxvaluef64go
	opinfo[opmaxvaluef64imm].portable = bcmaxvaluef64immgo
	opinfo[oppmodf64].portable = bcpmodf64go
	opinfo[oppmodf64imm].portable = bcpmodf64immgo
	opinfo[oprpmodf64imm].portable = bcrpmodf64immgo
	opinfo[opsignf64].portable = bcsignf64go
	opinfo[opbroadcastf64].portable = bcbroadcastf64go
}

func bcabsf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Abs(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcnegf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = -arg0.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bctruncf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Trunc(arg0.values[lane])
		}
	}

	*dest = r
	return pc + 6
}

func bcfloorf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Floor(arg0.values[lane])
		}
	}

	*dest = r
	return pc + 6
}

func bcceilf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Ceil(arg0.values[lane])
		}
	}

	*dest = r
	return pc + 6
}

func bcroundf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Round(arg0.values[lane])
		}
	}

	*dest = r
	return pc + 6
}

func bcroundevenf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.RoundToEven(arg0.values[lane])
		}
	}

	*dest = r
	return pc + 6
}

func bcbroadcastf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	imm := bcfloat64(bc, pc+2)
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		r.values[lane] = imm
	}

	*dest = r
	return pc + 10
}

func bcsquaref64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	argmask := argptr[kRegData](bc, pc+4).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			x := arg0.values[lane]
			r.values[lane] = x * x
		}
	}

	*dest = r
	return pc + 6
}

func bcsqrtf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Sqrt(arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}

func bcaddf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] + arg1.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcaddf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] + imm
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcsubf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] - arg1.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcsubf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] - imm
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcrsubf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = imm - arg0.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcmulf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] * arg1.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcmulf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] * imm
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcdivf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] / arg1.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcdivf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = arg0.values[lane] / imm
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcrdivf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = imm / arg0.values[lane]
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func modtruncf64(a, b float64) float64 {
	return math.FMA(math.Trunc(a/b), -b, a)
}

func bcmodf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = modtruncf64(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcmodf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = modtruncf64(arg0.values[lane], imm)
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcrmodf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = modtruncf64(imm, arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcminvaluef64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	arg1 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Min(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	return pc + 8
}

func bcminvaluef64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	imm := bcfloat64(bc, pc+4)
	argmask := argptr[kRegData](bc, pc+12).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Min(arg0.values[lane], imm)
		}
	}

	*dest = r
	return pc + 14
}

func bcmaxvaluef64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	arg1 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Max(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	return pc + 8
}

func bcmaxvaluef64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+2)
	imm := bcfloat64(bc, pc+4)
	argmask := argptr[kRegData](bc, pc+12).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = math.Max(arg0.values[lane], imm)
		}
	}

	*dest = r
	return pc + 14
}

func pmod(a, b float64) float64 {
	b = math.Abs(b)
	return math.FMA(math.Floor(a/b), -b, a)
}

func bcpmodf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	arg1 := argptr[f64RegData](bc, pc+6)
	argmask := argptr[kRegData](bc, pc+8).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = pmod(arg0.values[lane], arg1.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 10
}

func bcpmodf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = pmod(arg0.values[lane], imm)
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcrpmodf64immgo(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	imm := bcfloat64(bc, pc+6)
	argmask := argptr[kRegData](bc, pc+14).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = pmod(imm, arg0.values[lane])
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 16
}

func bcsignf64go(bc *bytecode, pc int) int {
	dest := argptr[f64RegData](bc, pc+0)
	destk := argptr[kRegData](bc, pc+2)
	arg0 := argptr[f64RegData](bc, pc+4)
	argmask := argptr[kRegData](bc, pc+6).mask
	r := f64RegData{}

	for lane := 0; lane < bcLaneCount; lane++ {
		if argmask&(1<<lane) != 0 {
			r.values[lane] = max(min(arg0.values[lane], 1.0), -1.0)
		}
	}

	*dest = r
	destk.mask = argmask
	return pc + 8
}
