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
	"fmt"
	"math"
)

type assembler struct {
	code       []byte
	scratchuse int
}

func (a *assembler) grabCode() []byte {
	r := a.code
	a.code = nil
	return r
}

func (a *assembler) emitImmU8(imm uint8) {
	a.code = append(a.code, byte(imm))
}

func (a *assembler) emitImmU16(imm uint16) {
	a.code = append(a.code, byte(imm), byte(imm>>8))
}

func (a *assembler) emitImmU32(imm uint32) {
	a.code = append(a.code, byte(imm), byte(imm>>8), byte(imm>>16), byte(imm>>24))
}

func (a *assembler) emitImmU64(imm uint64) {
	a.code = append(a.code, byte(imm), byte(imm>>8), byte(imm>>16), byte(imm>>24), byte(imm>>32), byte(imm>>40), byte(imm>>48), byte(imm>>56))
}

// emitOpcodeValue emits a 16-bit opcode value to the assembler buffer
// without any other arguments. In addition, it tracks the use of scratch
// buffer and automatically increments `a.scratchuse` when the opcode
// actually uses scratch (for example it creates a string slice or value).
func (a *assembler) emitOpcodeValue(op bcop) {
	a.scratchuse += op.scratch()
	if a.scratchuse > PageSize {
		a.scratchuse = PageSize
	}
	a.code = append(a.code, byte(op), byte(op>>8))
}

// emitOpcodeArg emits a single argument to the assembler buffer.
func (a *assembler) emitOpcodeArg(arg any, argType bcArgType) {
	switch argType {
	case bcK, bcS, bcV, bcB, bcH, bcL:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcDictSlot:
		a.emitImmU16(uint16(toi64(arg)))
	case bcAuxSlot:
		a.emitImmU16(uint16(toi64(arg)))
	case bcAggSlot:
		a.emitImmU32(uint32(arg.(aggregateslot)))
	case bcSymbolID:
		a.emitImmU32(arg.(uint32))
	case bcLitRef:
		if lr, ok := arg.(litref); ok {
			a.emitImmU32(lr.offset)
			a.emitImmU32(lr.length)
			a.emitImmU8(lr.tlv)
			a.emitImmU8(lr.hLen)
		} else {
			panic(fmt.Sprintf("expected litref, found: %v", arg))
		}
	case bcImmI8:
		a.emitImmU8(uint8(toi64(arg)))
	case bcImmI16:
		a.emitImmU16(uint16(toi64(arg)))
	case bcImmI32:
		a.emitImmU32(uint32(toi64(arg)))
	case bcImmI64:
		a.emitImmU64(uint64(toi64(arg)))
	case bcImmU8:
		a.emitImmU8(uint8(toi64(arg)))
	case bcImmU16:
		a.emitImmU16(uint16(toi64(arg)))
	case bcImmU32:
		a.emitImmU32(uint32(toi64(arg)))
	case bcImmU64:
		a.emitImmU64(uint64(toi64(arg)))
	case bcImmF64:
		a.emitImmU64(math.Float64bits(tof64(arg)))
	default:
		panic(fmt.Sprintf("unhandled opcode argument %v", argType))
	}
}
func (a *assembler) emitOpcode(op bcop, args ...any) {
	info := &opinfo[op]

	// verify the number of arguments matches its signature
	argCount := len(info.out) + len(info.in)
	if len(info.va) != 0 {
		panic(fmt.Sprintf("error when emitting opcode '%v': emitOpcode() cannot emit opcode that uses variable arguments", op))
	}
	if len(args) != argCount {
		panic(fmt.Sprintf("invalid argument count while emitting opcode '%v' (count=%d, required=%d)", op, len(args), argCount))
	}

	// emit opcode and required arguments
	a.emitOpcodeValue(op)
	for i := range info.out {
		a.emitOpcodeArg(args[i], info.out[i])
	}
	n := len(info.out)
	for i := range info.in {
		a.emitOpcodeArg(args[i+n], info.in[i])
	}
}

func (a *assembler) emitOpcodeVA(op bcop, args []any) {
	info := &opinfo[op]

	// Verify the number of arguments matches the signature. vaImms contains a signature of each
	// argument tuple that is considered a single va argument. For example if the bc instruction
	// uses [stString, stBool] tuple, it's a group of 2 values for each va argument.
	baseArgCount := len(info.in) + len(info.out)
	vaTupleSize := len(info.va)

	if vaTupleSize == 0 {
		panic(fmt.Sprintf("error when emitting opcode '%v': emitOpcodeVA() can only emit opcode that uses variable arguments", op))
	}

	if len(args) < baseArgCount {
		panic(fmt.Sprintf("invalid immediate count while emitting opcode '%v' (count=%d, mandatory=%d, tupleSize=%d)",
			op, len(args), baseArgCount, vaTupleSize))
	}

	vaLength := (len(args) - baseArgCount) / vaTupleSize
	if baseArgCount+vaLength*vaTupleSize != len(args) {
		panic(fmt.Sprintf("invalid immediate count while emitting opcode '%v' (count=%d, mandatory=%d, tupleSize=%d)",
			op, len(args), baseArgCount, vaTupleSize))
	}

	// emit opcode and required arguments
	a.emitOpcodeValue(op)
	for i := range info.out {
		a.emitOpcodeArg(args[i], info.out[i])
	}
	n := len(info.out)
	for i := range info.in {
		a.emitOpcodeArg(args[i+n], info.in[i])
	}

	// emit va (length followed by arguments)
	a.emitImmU32(uint32(vaLength))
	j := 0
	for i := baseArgCount; i < len(args); i++ {
		a.emitOpcodeArg(args[i], info.va[j])
		j++
		if j >= len(info.va) {
			j = 0
		}
	}
}
