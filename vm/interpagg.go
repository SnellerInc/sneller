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

	s := refAggState[f64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Min(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxf(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[f64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[f64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = math.Max(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmini(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = min(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}

func bcaggmaxi(bc *bytecode, pc int) int {
	imm := bcword32(bc, pc+0)
	arg0 := argptr[i64RegData](bc, pc+4)
	srcmask := argptr[kRegData](bc, pc+6).mask

	s := refAggState[i64AggState](bc, imm)
	m := s.value

	for lane := 0; lane < bcLaneCount; lane++ {
		if srcmask&(1<<lane) != 0 {
			m = max(m, arg0.values[lane])
		}
	}

	s.value = m
	s.count += int64(bits.OnesCount16(srcmask))
	return pc + 8
}
