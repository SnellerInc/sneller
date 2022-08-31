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
)

type assembler struct {
	code []byte
}

func (a *assembler) grabCode() []byte {
	r := a.code
	a.code = nil
	return r
}

func (a *assembler) emitImm(imm uint64, size int) {
	switch size {
	case 1:
		a.emitImmU8(uint8(imm))
	case 2:
		a.emitImmU16(uint16(imm))
	case 4:
		a.emitImmU32(uint32(imm))
	case 8:
		a.emitImmU64(imm)
	default:
		panic(fmt.Sprintf("invalid immediate size: %d", size))
	}
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

func (a *assembler) emitOpcode(op bcop) {
	a.emitImmUPtr(op.address())
}

func opcodeToBytes(op bcop) []byte {
	asm := assembler{}
	asm.emitOpcode(op)
	return asm.grabCode()
}
