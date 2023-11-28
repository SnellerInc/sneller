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
	"math/bits"
)

func bcbroadcasti64go(bc *bytecode, pc int) int {
	dst := argptr[i64RegData](bc, pc)
	imm := int64(bcword64(bc, pc+2))

	for i := 0; i < bcLaneCount; i++ {
		dst.values[i] = imm
	}

	return pc + 10
}

func bcabsi64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		val := src.values[i]

		if val == math.MinInt64 {
			msk ^= uint16(1 << i)
			continue
		}

		if val < 0 {
			val = -val
		}
		dst.values[i] = val
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
}

func bcnegi64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		val := src.values[i]

		if val == math.MinInt64 {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = -val
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
}

func bcsigni64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}
		dst.values[i] = max(min(src.values[i], 1), -1)
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
}

func bcsquarei64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		val := src.values[i]

		if val < 0 {
			val = -val
		}

		if val > 3037000499 {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = val * val
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
}

func bcbitnoti64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		val := src.values[i]
		dst.values[i] = ^val
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcbitcounti64go(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	msk := argptr[kRegData](bc, pc+4).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		dst.values[i] = int64(bits.OnesCount64(uint64(src.values[i])))
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcaddi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a + b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcaddi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a + imm
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcsubi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a - b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcsubi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a - imm
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcrsubi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = imm - a
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcmuli64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a * b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcmuli64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a * imm
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcdivi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]

		if b == 0 || (a == math.MinInt64 && b == -1) {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = a / b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcdivi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	if imm != 0 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]
			if a == math.MinInt64 && imm == -1 {
				msk ^= uint16(1 << i)
				continue
			}
			dst.values[i] = a / imm
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcrdivi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		b := src.values[i]
		if b == 0 || (imm == math.MinInt64 && b == -1) {
			msk ^= uint16(1 << i)
			continue
		}
		dst.values[i] = imm / b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcmodi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]

		if b == 0 {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = a % b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcmodi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	if imm != 0 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]
			dst.values[i] = a % imm
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcrmodi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		b := src.values[i]
		if b == 0 {
			msk ^= uint16(1 << i)
			continue
		}
		dst.values[i] = imm % b
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func pmodi64(a, b int64) int64 {
	res := a % b
	if res < 0 {
		if b < 0 {
			res = res - b
		} else {
			res = res + b
		}
	}
	return res
}

func bcpmodi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	msk := argptr[kRegData](bc, pc+8).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]

		if b == 0 || (a == math.MinInt64 && b == math.MinInt64) {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = pmodi64(a, b)
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcpmodi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	if imm != 0 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]

			if a == math.MinInt64 && imm == math.MinInt64 {
				msk ^= uint16(1 << i)
				continue
			}

			dst.values[i] = pmodi64(a, imm)
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func bcrpmodi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+4)
	imm := int64(bcword64(bc, pc+6))
	msk := argptr[kRegData](bc, pc+14).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		b := src.values[i]

		if b == 0 || (imm == math.MinInt64 && b == math.MinInt64) {
			msk ^= uint16(1 << i)
			continue
		}

		dst.values[i] = pmodi64(imm, b)
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 16
}

func addmuli64immgo(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+4)
	bval := argptr[i64RegData](bc, pc+6)
	imm := int64(bcword64(bc, pc+8))
	msk := argptr[kRegData](bc, pc+16).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a + b*imm
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 18
}

func bcmaxvaluei64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = max(a, b)
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcmaxvaluei64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := int64(bcword64(bc, pc+4))
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = max(a, imm)
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcminvaluei64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = min(a, b)
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcminvaluei64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := int64(bcword64(bc, pc+4))
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = min(a, imm)
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcandi64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a & b
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcandi64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := int64(bcword64(bc, pc+4))
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a & imm
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcori64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a | b
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcori64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := int64(bcword64(bc, pc+4))
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a | imm
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcxori64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := bval.values[i]
		dst.values[i] = a ^ b
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcxori64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := int64(bcword64(bc, pc+4))
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := src.values[i]
		dst.values[i] = a ^ imm
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcslli64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := uint64(bval.values[i])

		if b > 63 {
			dst.values[i] = 0
		} else {
			dst.values[i] = a << b
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcslli64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := bcword64(bc, pc+4)
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	if imm < 64 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]
			dst.values[i] = a << imm
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcsrai64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := uint64(bval.values[i])

		if b > 63 {
			dst.values[i] = 0
		} else {
			dst.values[i] = a >> b
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcsrai64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := bcword64(bc, pc+4)
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	if imm < 64 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]
			dst.values[i] = a >> imm
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}

func bcsrli64go(bc *bytecode, pc int) int {
	aval := argptr[i64RegData](bc, pc+2)
	bval := argptr[i64RegData](bc, pc+4)
	msk := argptr[kRegData](bc, pc+6).mask
	dst := i64RegData{}

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		a := aval.values[i]
		b := uint64(bval.values[i])

		if b > 63 {
			dst.values[i] = 0
		} else {
			dst.values[i] = int64(uint64(a) >> b)
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 8
}

func bcsrli64immgo(bc *bytecode, pc int) int {
	src := argptr[i64RegData](bc, pc+2)
	imm := bcword64(bc, pc+4)
	msk := argptr[kRegData](bc, pc+12).mask
	dst := i64RegData{}

	if imm < 64 {
		for i := 0; i < bcLaneCount; i++ {
			if (msk & (1 << i)) == 0 {
				continue
			}

			a := src.values[i]
			dst.values[i] = int64(uint64(a) >> imm)
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 14
}
