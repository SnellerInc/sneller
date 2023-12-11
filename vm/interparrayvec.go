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
	"bytes"
	"math"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

func readSymbolID(mem []byte, length uint) int {
	if len(mem) < int(length) {
		return -1
	}

	_ = mem[:length]
	sym := uint(0)

	for i := uint(0); i < length; i++ {
		sym = (sym << 8) | uint(mem[i])
	}

	return int(sym)
}

func valueEquals(bc *bytecode, aVal, bVal []byte) bool {
	if len(aVal) == 0 || len(bVal) == 0 {
		return false
	}

	aTLV := aVal[0]
	bTLV := bVal[0]

	aType := ion.Type(aTLV >> 4)
	bType := ion.Type(bTLV >> 4)

	// This would also fast-compare two symbols.
	if aType == bType {
		return bytes.Equal(aVal, bVal)
	}

	// Handle the case that A is a symbol and B is a string.
	if aType == ion.SymbolType {
		if bType != ion.StringType {
			return false
		}

		symbolID := readSymbolID(aVal[1:], uint(aTLV&0xF))
		if uint(symbolID) >= uint(len(bc.symtab)) {
			return false
		}

		return bytes.Equal(bc.symtab[symbolID].mem(), bVal)
	}

	// Handle the case that A is a string and B is a symbol.
	if bType == ion.SymbolType {
		if aType != ion.StringType {
			return false
		}

		symbolID := readSymbolID(bVal[1:], uint(bTLV&0xF))
		if uint(symbolID) >= uint(len(bc.symtab)) {
			return false
		}

		return bytes.Equal(aVal, bc.symtab[symbolID].mem())
	}

	return false
}

func bcarraysizego(bc *bytecode, pc int) int {
	src := argptr[sRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			count := countValuesInList(vmref{src.offsets[i], src.sizes[i]}.mem())
			if count >= 0 {
				dst.values[i] = int64(count)
			}
		}
	}

	*argptr[i64RegData](bc, pc+0) = dst
	return pc + 6
}

func bcarraypositiongo(bc *bytecode, pc int) int {
	list := argptr[sRegData](bc, pc+4)
	item := argptr[vRegData](bc, pc+6)

	dst := i64RegData{}
	dstMask := uint16(0)
	srcMask := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (srcMask & (1 << i)) != 0 {
			list := vmref{list.offsets[i], list.sizes[i]}.mem()
			position := int64(1)
			for len(list) != 0 {
				aSize := ion.SizeOf(list)
				if valueEquals(bc, list[0:aSize], vmref{item.offsets[i], item.sizes[i]}.mem()) {
					dst.values[i] = position
					dstMask |= 1 << i
					break
				}
				list = list[aSize:]
				position++
			}
		}
	}

	*argptr[i64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{dstMask}

	return pc + 10
}

func bcarraysumgo(bc *bytecode, pc int) int {
	src := argptr[sRegData](bc, pc+4)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+6).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			list := vmref{src.offsets[i], src.sizes[i]}.mem()
			acc := float64(0)

			for len(list) != 0 {
				val, next, err := ion.ReadCoerceFloat64(list)
				if err != nil {
					acc = 0
					msk ^= 1 << i
					break
				}
				acc += val
				list = next
			}
			dst.values[i] = acc
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 8
}

func bcvectorinnerproductgo(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	src2 := argptr[sRegData](bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()
			list2 := vmref{src2.offsets[i], src2.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && len(list2) != 0 {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)
				val2, next2, err2 := ion.ReadCoerceFloat64(list2)

				if err1 == nil && err2 == nil {
					acc = math.FMA(val1, val2, acc)
				}

				list1 = next1
				list2 = next2
			}

			dst.values[i] = acc
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorinnerproductimmgo(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	dictSlot := bcword(bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	dictString := bc.dict[dictSlot]
	dictSize := len(dictString) / 8
	dictData := unsafe.Pointer(unsafe.StringData(dictString))

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			index := 0
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && index < dictSize {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)

				if err1 == nil {
					val2 := *((*float64)(unsafe.Add(dictData, index*8)))
					acc = math.FMA(val1, val2, acc)
				}

				list1 = next1
				index++
			}

			dst.values[i] = acc
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorl1distancego(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	src2 := argptr[sRegData](bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()
			list2 := vmref{src2.offsets[i], src2.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && len(list2) != 0 {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)
				val2, next2, err2 := ion.ReadCoerceFloat64(list2)

				if err1 == nil && err2 == nil {
					acc += math.Abs(val1 - val2)
				}

				list1 = next1
				list2 = next2
			}

			dst.values[i] = acc
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorl1distanceimmgo(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	dictSlot := bcword(bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	dictString := bc.dict[dictSlot]
	dictSize := len(dictString) / 8
	dictData := unsafe.Pointer(unsafe.StringData(dictString))

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			index := 0
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && index < dictSize {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)

				if err1 == nil {
					val2 := *((*float64)(unsafe.Add(dictData, index*8)))
					acc += math.Abs(val1 - val2)
				}

				list1 = next1
				index++
			}

			dst.values[i] = acc
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorl2distancego(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	src2 := argptr[sRegData](bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()
			list2 := vmref{src2.offsets[i], src2.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && len(list2) != 0 {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)
				val2, next2, err2 := ion.ReadCoerceFloat64(list2)

				if err1 == nil && err2 == nil {
					d := math.Abs(val1 - val2)
					acc = math.FMA(d, d, acc)
				}

				list1 = next1
				list2 = next2
			}

			dst.values[i] = math.Sqrt(acc)
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorl2distanceimmgo(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	dictSlot := bcword(bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	dictString := bc.dict[dictSlot]
	dictSize := len(dictString) / 8
	dictData := unsafe.Pointer(unsafe.StringData(dictString))

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			index := 0
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()

			acc := float64(0)
			for len(list1) != 0 && index < dictSize {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)

				if err1 == nil {
					val2 := *((*float64)(unsafe.Add(dictData, index*8)))
					d := math.Abs(val1 - val2)
					acc = math.FMA(d, d, acc)
				}

				list1 = next1
				index++
			}

			dst.values[i] = math.Sqrt(acc)
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorcosinedistancego(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	src2 := argptr[sRegData](bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()
			list2 := vmref{src2.offsets[i], src2.sizes[i]}.mem()

			acc11 := float64(0)
			acc12 := float64(0)
			acc22 := float64(0)
			for len(list1) != 0 && len(list2) != 0 {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)
				val2, next2, err2 := ion.ReadCoerceFloat64(list2)

				if err1 == nil && err2 == nil {
					acc11 = math.FMA(val1, val1, acc11)
					acc12 = math.FMA(val1, val2, acc12)
					acc22 = math.FMA(val2, val2, acc22)
				}

				list1 = next1
				list2 = next2
			}

			dist := math.Sqrt(acc11 * acc22)
			result := float64(0)

			if dist > 0 {
				result = 1 - (acc12 / dist)
			}

			dst.values[i] = result
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}

func bcvectorcosinedistanceimmgo(bc *bytecode, pc int) int {
	src1 := argptr[sRegData](bc, pc+4)
	dictSlot := bcword(bc, pc+6)
	dst := f64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask

	dictString := bc.dict[dictSlot]
	dictSize := len(dictString) / 8
	dictData := unsafe.Pointer(unsafe.StringData(dictString))

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			index := 0
			list1 := vmref{src1.offsets[i], src1.sizes[i]}.mem()

			acc11 := float64(0)
			acc12 := float64(0)
			acc22 := float64(0)
			for len(list1) != 0 && index < dictSize {
				val1, next1, err1 := ion.ReadCoerceFloat64(list1)

				if err1 == nil {
					val2 := *((*float64)(unsafe.Add(dictData, index*8)))
					acc11 = math.FMA(val1, val1, acc11)
					acc12 = math.FMA(val1, val2, acc12)
					acc22 = math.FMA(val2, val2, acc22)
				}

				list1 = next1
				index++
			}

			dist := math.Sqrt(acc11 * acc22)
			result := float64(0)

			if dist > 0 {
				result = 1 - (acc12 / dist)
			}

			dst.values[i] = result
		}
	}

	*argptr[f64RegData](bc, pc+0) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{msk}

	return pc + 10
}
