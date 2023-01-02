// Copyright (C) 2022 Sneller, Inc.
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
	"fmt"
	"math"

	"github.com/SnellerInc/sneller/ion"
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

func (a *assembler) emitImmUPtr(imm uintptr) {
	a.emitImmU64(uint64(imm))
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
	a.emitImmUPtr(op.address())
}

// emitOpcodeArg emits a single argument to the assembler buffer.
func (a *assembler) emitOpcodeArg(arg any, argType bcArgType) {
	switch argType {
	case bcReadK, bcWriteK:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcReadS, bcWriteS:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcReadV, bcWriteV, bcReadWriteV:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcReadB, bcWriteB:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcReadH, bcWriteH:
		a.emitImmU16(uint16(arg.(stackslot)))
	case bcDictSlot:
		a.emitImmU16(uint16(toi64(arg)))
	case bcAuxSlot:
		a.emitImmU16(uint16(toi64(arg)))
	case bcAggSlot:
		a.emitImmU32(uint32(arg.(aggregateslot)))
	case bcHashSlot:
		a.emitImmU32(uint32(arg.(aggregateslot)))
	case bcSymbolID:
		a.emitImmU32(uint32(arg.(ion.Symbol)))
	case bcLitRef:
		a.emitImmU64(uint64(toi64(arg)))
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
	argCount := len(info.args)
	if len(info.va) != 0 {
		panic(fmt.Sprintf("error when emitting opcode '%v': emitOpcode() cannot emit opcode that uses variable arguments", op))
	}

	if len(args) != argCount {
		panic(fmt.Sprintf("invalid argument count while emitting opcode '%v' (count=%d, required=%d)", op, len(args), argCount))
	}

	// emit opcode and required arguments
	a.emitOpcodeValue(op)
	for i := 0; i < argCount; i++ {
		a.emitOpcodeArg(args[i], info.args[i])
	}
}

func (a *assembler) emitOpcodeVA(op bcop, args []any) {
	info := &opinfo[op]

	// Verify the number of arguments matches the signature. vaImms contains a signature of each
	// argument tuple that is considered a single va argument. For example if the bc instruction
	// uses [stString, stBool] tuple, it's a group of 2 values for each va argument.
	baseArgCount := len(info.args)
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
	for i := 0; i < baseArgCount; i++ {
		a.emitOpcodeArg(args[i], info.args[i])
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
