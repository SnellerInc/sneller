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
	"encoding/binary"

	"github.com/SnellerInc/sneller/internal/stringext"
)

func bcCmpStrGo(bc *bytecode, pc int, op bcop) int {
	dstK := argptr[kRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	dickSlotID := bcword(bc, pc+4)
	needle := deEncodeNeedleOp(bc.dict[dickSlotID], op)
	inputK := argptr[kRegData](bc, pc+6).mask
	outputK := uint16(0)

	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, Needle) bool)
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			if ref(data, needle) {
				outputK |= 1 << i
			}
		}
	}
	dstK.mask = outputK
	return pc + 8
}

func bcCmpStrFuzzyGo(bc *bytecode, pc int, op bcop) int {
	dstK := argptr[kRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	threshold := argptr[i64RegData](bc, pc+4).values
	dickSlotID := bcword(bc, pc+6)
	needle := deEncodeNeedleOp(bc.dict[dickSlotID], op)
	inputK := argptr[kRegData](bc, pc+8).mask
	outputK := uint16(0)

	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, Needle, int) bool)
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			if ref(data, needle, int(threshold[i])) {
				outputK |= 1 << i
			}
		}
	}
	dstK.mask = outputK
	return pc + 10
}

func bcSkip1Go(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	dstK := argptr[kRegData](bc, pc+2)
	srcS := *argptr[sRegData](bc, pc+4) // copied since argv may alias dstS
	inputK := argptr[kRegData](bc, pc+6).mask
	outputK := uint16(0)

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, int) (bool, OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane, expOffset, expLength := ref(data, 1)
			if expLane {
				outputK |= 1 << i
				tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
				tmpS.sizes[i] = uint32(expLength)
			}
		}
	}
	*dstS = tmpS
	dstK.mask = outputK
	return pc + 8
}

func bcSkipNGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	dstK := argptr[kRegData](bc, pc+2)
	srcS := *argptr[sRegData](bc, pc+4) // copied since srcS may alias dstS
	skipCount := argptr[i64RegData](bc, pc+6).values
	inputK := argptr[kRegData](bc, pc+8).mask
	outputK := uint16(0)

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, int) (bool, OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane, expOffset, expLength := ref(data, int(skipCount[i]))
			if expLane {
				outputK |= 1 << i
				tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
				tmpS.sizes[i] = uint32(expLength)
			}
		}
	}
	*dstS = tmpS
	dstK.mask = outputK
	return pc + 10
}

func bcTrimWsGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	inputK := argptr[kRegData](bc, pc+4).mask

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data) (OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expOffset, expLength := ref(data)
			tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
			tmpS.sizes[i] = uint32(expLength)
		}
	}
	*dstS = tmpS
	return pc + 6
}

func bcTrim4CharGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	dickSlotID := bcword(bc, pc+4)
	cutset := Needle(bc.dict[dickSlotID])
	inputK := argptr[kRegData](bc, pc+6).mask

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, Needle) (OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expOffset, expLength := ref(data, cutset)
			tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
			tmpS.sizes[i] = uint32(expLength)
		}
	}
	*dstS = tmpS
	return pc + 8
}

func bcLengthGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[i64RegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	inputK := argptr[kRegData](bc, pc+4).mask

	tmpS := i64RegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data) int)
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			tmpS.values[i] = int64(ref(data))
		}
	}
	*dstS = tmpS
	return pc + 6
}

func bcSubstrGo(bc *bytecode, pc int) int {
	dstS := argptr[sRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	begin := argptr[i64RegData](bc, pc+4).values
	length := argptr[i64RegData](bc, pc+6).values
	inputK := argptr[kRegData](bc, pc+8).mask

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(opSubstr).(func(Data, int, int) (OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expOffset, expLength := ref(data, int(begin[i]), int(length[i]))
			tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
			tmpS.sizes[i] = uint32(expLength)
		}
	}
	*dstS = tmpS
	return pc + 10
}

func bcSplitPartGo(bc *bytecode, pc int) int {
	dstS := argptr[sRegData](bc, pc)
	dstK := argptr[kRegData](bc, pc+2)
	srcS := argptr[sRegData](bc, pc+4)
	dickSlotID := bcword(bc, pc+6)
	delimiter := rune(bc.dict[dickSlotID][0])
	idx := argptr[i64RegData](bc, pc+8).values
	inputK := argptr[kRegData](bc, pc+10).mask
	outputK := uint16(0)

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(opSplitPart).(func(Data, int, rune) (bool, OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane, expOffset, expLength := ref(data, int(idx[i]), delimiter)
			if expLane {
				outputK |= 1 << i
				tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
				tmpS.sizes[i] = uint32(expLength)
			}
		}
	}
	*dstS = tmpS
	dstK.mask = outputK
	return pc + 12
}

func bcContainsPreSufSubGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	dstK := argptr[kRegData](bc, pc+2)
	srcS := argptr[sRegData](bc, pc+4)
	dickSlotID := bcword(bc, pc+6)
	needle := deEncodeNeedleOp(bc.dict[dickSlotID], op)
	inputK := argptr[kRegData](bc, pc+8).mask
	outputK := uint16(0)

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane, expOffset, expLength := ref(data, needle)
			if expLane {
				outputK |= 1 << i
				tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
				tmpS.sizes[i] = uint32(expLength)
			}
		}
	}
	*dstS = tmpS
	dstK.mask = outputK
	return pc + 10
}

func bcContainsPatternGo(bc *bytecode, pc int, op bcop) int {
	dstS := argptr[sRegData](bc, pc)
	dstK := argptr[kRegData](bc, pc+2)
	srcS := argptr[sRegData](bc, pc+4)
	dickSlotID := bcword(bc, pc+6)
	pattern := deEncodePatternOp(bc.dict[dickSlotID], op)
	inputK := argptr[kRegData](bc, pc+8).mask
	outputK := uint16(0)

	tmpS := sRegData{}
	// compute the expected results according to the reference implementation
	ref := refFunc(op).(func(Data, *stringext.Pattern) (bool, OffsetZ2, LengthZ3))
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane, expOffset, expLength := ref(data, &pattern)
			if expLane {
				outputK |= 1 << i
				tmpS.offsets[i] = uint32(expOffset) + srcS.offsets[i]
				tmpS.sizes[i] = uint32(expLength)
			}
		}
	}
	*dstS = tmpS
	dstK.mask = outputK
	return pc + 10
}

func bcIsSubnetOfIP4Go(bc *bytecode, pc int) int {
	dstK := argptr[kRegData](bc, pc)
	srcS := argptr[sRegData](bc, pc+2)
	dickSlotID := bcword(bc, pc+4)
	minX, maxX := stringext.DeEncodeBCD(bc.dict[dickSlotID])
	inputK := argptr[kRegData](bc, pc+6).mask
	outputK := uint16(0)

	minY := binary.BigEndian.Uint32(minX[:])
	maxY := binary.BigEndian.Uint32(maxX[:])

	// compute the expected results according to the reference implementation
	ref := refFunc(opIsSubnetOfIP4).(func(Data, uint32, uint32) bool)
	for i := 0; i < bcLaneCount; i++ {
		if ((inputK >> i) & 1) == 1 {
			data := Data(vmref{srcS.offsets[i], srcS.sizes[i]}.mem())
			expLane := ref(data, minY, maxY)
			if expLane {
				outputK |= 1 << i
			}
		}
	}
	dstK.mask = outputK
	return pc + 8
}

func bcDFAGo(bc *bytecode, pc int, op bcop) int {
	srcS := argptr[sRegData](bc, pc+2)
	inputK := argptr[kRegData](bc, pc+6).mask
	dsByte := []byte(bc.dict[bcword(bc, pc+4)])
	*argptr[kRegData](bc, pc) = DfaGoImpl(op, vmm[:], inputK, srcS.offsets, srcS.sizes, dsByte)
	return pc + 8
}
