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
	//	"fmt"
	"math"
	"math/bits"
	"unsafe"
)

type f64AggState struct {
	value float64
	count int64
}

type i64AggState struct {
	value int64
	count int64
}

func refAggState[T any](bc *bytecode, offs uint32) *T {
	return (*T)(unsafe.Add(bc.vmState.aggPtr, offs))
}

func bcaggminf(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	m := math.Inf(0)
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Min(m, arg0.values[lane])
		}
	}

	s := refAggState[f64AggState](bc, imm)
	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxf(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	m := math.Inf(-1)
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Max(m, arg0.values[lane])
		}
	}

	s := refAggState[f64AggState](bc, imm)
	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmini(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	m := int64(math.MaxInt64)
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = min(m, arg0.values[lane])
		}
	}
	s := refAggState[i64AggState](bc, imm)
	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxi(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	m := int64(math.MinInt64)
	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = max(m, arg0.values[lane])
		}
	}
	s := refAggState[i64AggState](bc, imm)
	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}
