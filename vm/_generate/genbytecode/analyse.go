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

package main

import (
	"fmt"
	"strings"
)

var debugPrint = func(format string, args ...any) {}

const (
	// output arguments for unpack
	SLOT = 1 + iota
	DICT
	ZI8
	ZI16
	ZI32
	ZI64
	ZF64
	RU16
	RU32
	RU64
	MASK

	// generic in/our arguments for load/store
	gpregIn   = 0x010
	xmmregIn  = 0x020
	zmmregIn  = 0x040
	kregIn    = 0x080
	gpregOut  = 0x100
	xmmregOut = 0x200
	zmmregOut = 0x400
	kregOut   = 0x800
)

var ignoredMacros map[string]struct{}

func analyseOpcode(opcode Opcode) error {
	if ignoredMacros == nil {
		ignoredMacros = make(map[string]struct{})
		for _, s := range ignoredMacrosList {
			ignoredMacros[s] = struct{}{}
		}
	}

	var a opcodeAnalyser
	a.init(opcode)

	for _, instruction := range opcode.instructions {
		debugPrint("%s: %s\n", opcode.name, instruction)
		e, err := parseExpression(instruction)
		check(err)

		if inv, ok := e.(Invocation); ok {
			args := inv.args
			if s, ok := strings.CutPrefix(inv.name, "BC_UNPACK_"); ok {
				a.unpack(s, args)
			} else {
				switch inv.name {
				case "BC_LOAD_RU16_FROM_SLOT":
					a.load(args, gpregOut, SLOT)
				case "BC_LOAD_K1_FROM_SLOT":
					a.load(args, kregOut, SLOT)
				case "BC_LOAD_K1_K2_FROM_SLOT":
					a.load(args, kregOut, kregOut, SLOT)
				case "BC_LOAD_I64_FROM_SLOT", "BC_LOAD_F64_FROM_SLOT",
					"BC_LOAD_SLICE_FROM_SLOT", "BC_LOAD_VALUE_SLICE_FROM_SLOT":
					a.load(args, zmmregOut, zmmregOut, SLOT)
				case "BC_LOAD_ZMM_FROM_SLOT", "BC_LOAD_VALUE_HLEN_FROM_SLOT", "BC_LOAD_VALUE_TYPEL_FROM_SLOT":
					a.load(args, zmmregOut, SLOT)

				case "BC_LOAD_BUCKET_FROM_SLOT":
					a.load(args, zmmregOut, SLOT, MASK)
				case "BC_LOAD_SLICE_FROM_SLOT_MASKED", "BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED":
					a.load(args, zmmregOut, zmmregOut, SLOT, MASK)
				case "BC_LOAD_F64_FROM_SLOT_MASKED", "BC_LOAD_I64_FROM_SLOT_MASKED":
					a.load(args, zmmregOut, zmmregOut, SLOT, MASK, MASK)
				case "BC_LOAD_VALUE_HLEN_FROM_SLOT_MASKED":
					a.load(args, zmmregOut, SLOT, MASK)

				case "BC_STORE_I64_TO_SLOT", "BC_STORE_F64_TO_SLOT",
					"BC_STORE_SLICE_TO_SLOT":
					a.store(args, zmmregIn, zmmregIn, SLOT)
				case "BC_STORE_K_TO_SLOT":
					a.store(args, kregIn, SLOT)
				case "BC_STORE_VALUE_TO_SLOT":
					a.store(args, zmmregIn, zmmregIn, zmmregIn, zmmregIn, SLOT)
				case "BC_STORE_VALUE_TO_SLOT_X":
					a.store(args, zmmregIn, zmmregIn, xmmregIn, xmmregIn, SLOT)
				case "BC_STORE_RU16_TO_SLOT":
					a.store(args, gpregIn, SLOT)
				case "BC_STORE_RU32_TO_SLOT":
					a.store(args, gpregIn, SLOT)

				case "NEXT_ADVANCE":
					if len(opcode.spec.va) == 0 {
						// varargs make this calculation of the next opcode
						// far boyond the scope of this tool
						advance, err := a.evalNextAdvance(inv)
						if err != nil {
							return err
						}
						if advance != a.stackSize {
							return fmt.Errorf("stack size is %d, next advance is %d", a.stackSize, advance)
						}
					}

				case "BC_CHECK_SCRATCH_CAPACITY":
					if opcode.scratch == "" {
						return fmt.Errorf("scratch is not set, but %q is present", inv.name)
					} else {
						specsize, err := evalString(opcode.scratch)
						if err == nil {
							checksize, ok := a.evalCheckScratch(inv)
							if ok && specsize != checksize {
								return fmt.Errorf("scratch in spec is %d, checked size is %d", specsize, checksize)
							}
						}
					}

				default:
					_, ok := ignoredMacros[inv.name]
					if !ok {
						return fmt.Errorf("unsupported macro %q", inv.name)
					}
				}
			}
		}

		if a.err != nil {
			break
		}
	}

	return a.err
}

const (
	bcSlotSize    = 2
	bcDictSize    = 2
	bcAggSlotSize = 4
	bcLitRefSize  = 10
	bcSymbolSize  = 4
	bcImm16Size   = 2
	bcImm64Size   = 8
)

type opcodeAnalyser struct {
	stack     map[int]*slotState // offset => slot state
	stackSize int                // total size of stack (varargs opcodes it's the minimum size)
	registers map[RegisterGPR]*registerContent
	err       error
}

func (a *opcodeAnalyser) errf(format string, args ...any) {
	a.err = fmt.Errorf(format, args...)
}

func (a *opcodeAnalyser) init(op Opcode) {
	a.stack = make(map[int]*slotState)
	a.registers = make(map[RegisterGPR]*registerContent)

	// out registers
	offset := 0
	out := slots2string(op.spec.out)
	for _, code := range out {
		size := slotcode2size(uint8(code))
		a.stack[offset] = &slotState{
			readonly: false,
			offset:   offset,
			size:     size,
		}

		debugPrint("stack slot at %d: size %d, type: output\n", offset, size)
		offset += size
	}

	// in registers
	in := slots2string(op.spec.in)
	for _, code := range in {
		size := slotcode2size(uint8(code))
		a.stack[offset] = &slotState{
			readonly: true,
			offset:   offset,
			size:     size,
		}
		debugPrint("stack slot at %d: size %d, type: input\n", offset, size)
		offset += size
	}

	// slot for VM count
	if len(op.spec.va) > 0 {
		size := 4
		a.stack[offset] = &slotState{
			readonly: true,
			offset:   offset,
			size:     size,
		}
		debugPrint("stack slot at %d: size %d, type: varargs count\n", offset, size)
		offset += size
	}

	a.stackSize = offset
}

type unpackTarget uint8

const (
	unpackToGPR unpackTarget = iota
	unpackToZMM
)

type unpackType struct {
	target unpackTarget
	size   int
}

func decodeUnpackSuffix(s string) (result []unpackType, err error) {
	tmp := strings.Split(s, "_")
	add := func(target unpackTarget, size int) {
		result = append(result, unpackType{target: target, size: size})
	}
	addslot := func(n int) {
		for i := 0; i < n; i++ {
			add(unpackToGPR, bcSlotSize)
		}
	}

	for i, t := range tmp {
		switch t {
		case "ZI8":
			add(unpackToZMM, 1)
		case "ZI16":
			add(unpackToZMM, 2)
		case "ZI32":
			add(unpackToZMM, 4)
		case "ZI64", "ZF64":
			add(unpackToZMM, 8)
		case "SLOT":
			addslot(1)
		case "2xSLOT":
			addslot(2)
		case "3xSLOT":
			addslot(3)
		case "4xSLOT":
			addslot(4)
		case "5xSLOT":
			addslot(5)
		case "DICT":
			add(unpackToGPR, bcDictSize)
		case "RU16":
			add(unpackToGPR, 2)
		case "RU32":
			add(unpackToGPR, 4)
		case "RU64":
			add(unpackToGPR, 8)
		default:
			err = fmt.Errorf("unsupported item %d of name: %q", i, t)
			return
		}
	}

	return
}

func (a *opcodeAnalyser) unpack(nameSuffix string, args []any) {
	types, err := decodeUnpackSuffix(nameSuffix)
	if err != nil {
		a.err = err
		return
	}

	n := len(args)
	k := len(types) + 1
	if n != k {
		a.errf("expected %d args, got %d", k, n)
	}

	offset, err := evalExpression(args[0])
	if err != nil {
		a.err = err
		return
	}

	args = args[1:]
	for i, t := range types {
		switch t.target {
		case unpackToGPR:
			reg := a.allocateGPR(args[i])
			if a.err != nil {
				return
			}

			slot := a.accessStack(offset, t.size)
			if a.err != nil {
				return
			}

			reg.slot = slot

		case unpackToZMM:
			a.resolveZMM(args[i], "OUT")
			err := a.err
			if a.err != nil {
				a.err = nil
				a.resolveYMM(args[i], "OUT")
				if a.err == nil {
					err = nil
				}
			}
			if err != nil {
				return
			}

			a.accessStack(offset, t.size)
			if a.err != nil {
				return
			}

		default:
			panicf("unhandled unpack type %d", t)
		}

		offset += t.size
	}
}

func (a *opcodeAnalyser) checkArgs(args []any, types ...int) {
	for i, t := range types {
		switch t {
		case gpregOut:
			a.resolveGPR(args[i], "OUT")
		case xmmregOut:
			a.resolveXMM(args[i], "OUT")
		case zmmregOut:
			a.resolveZMM(args[i], "OUT")
		case kregOut:
			a.resolveKreg(args[i], "OUT")

		case gpregIn:
			a.resolveGPR(args[i], "IN")
		case xmmregIn:
			a.resolveXMM(args[i], "IN")
		case zmmregIn:
			a.resolveZMM(args[i], "IN")
		case kregIn:
			a.resolveKreg(args[i], "IN")

			// input args
		case SLOT:
			slotreg := a.resolveGPR(args[i], "IN")
			if a.err != nil {
				return
			}

			rc := a.findGPR(slotreg)
			if a.err != nil {
				return
			}
			debugPrint("register %s holds slot %d\n", rc.reg, rc.slot.offset)
		case MASK:
			a.resolveKreg(args[i], "IN")
		default:
			panicf("unhandled case %d: %d", i, t)
		}

		if a.err != nil {
			return
		}
	}
}

func (a *opcodeAnalyser) load(args []any, types ...int) {
	a.checkArgs(args, types...)
}

func (a *opcodeAnalyser) store(args []any, types ...int) {
	a.checkArgs(args, types...)
}

func (a *opcodeAnalyser) findGPR(reg RegisterGPR) *registerContent {
	rc, ok := a.registers[reg]
	if !ok {
		a.errf("register %s was not assigned earlier", reg)
	}

	return rc
}

func (a *opcodeAnalyser) allocateGPR(reg any) *registerContent {
	r := a.resolveGPR(reg, "OUT")
	if a.err != nil {
		return nil
	}

	rc, ok := a.registers[r]
	if !ok {
		rc = &registerContent{reg: r}
		a.registers[r] = rc
	}

	return rc
}

func (a *opcodeAnalyser) resolveGPR(token any, inout string) (reg RegisterGPR) {
	switch r := token.(type) {
	case RegisterGPR:
		return r

	case Invocation:
		if r.name == inout {
			if len(r.args) == 1 {
				return a.resolveGPR(r.args[0], inout)
			}

			a.errf("%q requires just one argument", r.name)
		} else {
			a.errf("unexpected call to %q", r.name)
		}
		return 0

	// ugly fixup until we add full define handling
	case string:
		switch r {
		case "AGG_BUFFER_PTR_ORIG":
			reg, _ := string2gpr("CX")
			return reg
		case "HASHMEM_PTR":
			reg, _ := string2gpr("R8")
			return reg
		case "BITS_PER_HLL_BUCKET":
			reg, _ := string2gpr("R14")
			return reg
		}
	}

	a.errf("%q is not a GPR register", token)
	return 0
}

func (a *opcodeAnalyser) resolveXMM(token any, inout string) (reg RegisterXMM) {
	switch r := token.(type) {
	case RegisterXMM:
		return r

	case Invocation:
		if r.name == inout {
			if len(r.args) == 1 {
				return a.resolveXMM(r.args[0], inout)
			}

			a.errf("%q requires just one argument", r.name)
		} else {
			a.errf("unexpected call to %q", r.name)
		}
		return 0
	}

	a.errf("%q is not a XMM register", token)
	return 0
}

func (a *opcodeAnalyser) resolveYMM(token any, inout string) (reg RegisterYMM) {
	switch r := token.(type) {
	case RegisterYMM:
		return r

	case Invocation:
		if r.name == inout {
			if len(r.args) == 1 {
				return a.resolveYMM(r.args[0], inout)
			}
			a.errf("%q requires just one argument", r.name)
		} else {
			a.errf("unexpected call to %q", r.name)
		}
		return 0
	}

	a.errf("%q is not a YMM register", token)
	return 0
}

func (a *opcodeAnalyser) resolveZMM(token any, inout string) (reg RegisterZMM) {
	switch r := token.(type) {
	case RegisterZMM:
		return r

	case Invocation:
		if r.name == inout {
			if len(r.args) == 1 {
				return a.resolveZMM(r.args[0], inout)
			}

			a.errf("%q requires just one argument", r.name)
		} else {
			a.errf("unexpected call to %q", r.name)
		}
		return 0
	}

	a.errf("%q is not a ZMM register", token)
	return 0
}

func (a *opcodeAnalyser) resolveKreg(token any, inout string) (reg RegisterK) {
	switch r := token.(type) {
	case RegisterK:
		return r

	case Invocation:
		if r.name == inout {
			if len(r.args) == 1 {
				return a.resolveKreg(r.args[0], inout)
			}

			a.errf("%q requires just one argument", r.name)
		} else {
			a.errf("unexpected call to %q", r.name)
		}
		return 0
	}

	a.errf("%q is not a mask register", token)
	return 0
}

func (a *opcodeAnalyser) accessStack(offset, size int) *slotState {
	debugPrint("reading %d bytes from %d\n", size, offset)

	slot, ok := a.stack[offset]
	if !ok {
		a.errf("stack slot at offset %d: not exists", offset)
		return nil
	}

	if slot.size != size {
		a.errf("stack slot at offset %d: has size %d, requested %d", offset, slot.size, size)
		return nil
	}

	return slot
}

func (a *opcodeAnalyser) evalCheckScratch(inv Invocation) (int, bool) {
	if len(inv.args) == 0 {
		return 0, false
	}

	fn, ok := inv.args[0].(Invocation)
	if !ok {
		return 0, false
	}

	if fn.name != "$" || len(fn.args) != 1 {
		return 0, false
	}

	num, err := evalExpression(fn.args[0])
	if err != nil {
		return 0, false
	}

	return num, true
}

func (a *opcodeAnalyser) evalNextAdvance(inv Invocation) (int, error) {
	const macro = "NEXT_ADVANCE"
	if len(inv.args) != 1 {
		return 0, fmt.Errorf("%s: wrong number of arguments", macro)
	}

	return evalExpression(inv.args[0])
}

type slotState struct {
	readonly bool
	offset   int
	size     int
}

func slotcode2size(c uint8) int {
	switch c {
	case 'k':
		return bcSlotSize
	case 's':
		return bcSlotSize
	case 'v':
		return bcSlotSize
	case 'b':
		return bcSlotSize
	case 'h':
		return bcSlotSize
	case 'l':
		return bcSlotSize
	case 'x':
		return bcDictSize
	case 'p':
		return bcSlotSize
	case 'a':
		return bcAggSlotSize
	case 'y':
		return bcSymbolSize
	case 'd':
		return bcLitRefSize
	case 'C':
		return 1
	case 'W':
		return 2
	case 'I':
		return 4
	case 'i':
		return 8
	case '1':
		return 1
	case '2':
		return 2
	case '4':
		return 4
	case '8':
		return 8
	case 'f':
		return 8
	}

	panicf("wrong slot code %c", c)
	return -1
}

type registerContent struct {
	reg  RegisterGPR
	slot *slotState
}

// BC macros known to be implementation helpers, not VM related stuff
var ignoredMacrosList = []string{
	"BC_AGGREGATE_SLOT_COUNT_OP",
	"BC_AGGREGATE_SLOT_MARK_OP",
	"BC_ALLOC_SLICE",
	"BC_ARITH_OP_F64_IMM_IMPL",
	"BC_ARITH_OP_F64_IMM_IMPL_K",
	"BC_ARITH_OP_F64_IMPL",
	"BC_ARITH_OP_F64_IMPL_K",
	"BC_ARITH_OP_I64_IMM_IMPL",
	"BC_ARITH_OP_I64_IMM_IMPL_K",
	"BC_ARITH_OP_I64_IMPL",
	"BC_ARITH_OP_I64_IMPL_K",
	"BC_ARITH_REVERSE_OP_F64_IMM_IMPL",
	"BC_ARITH_REVERSE_OP_I64_IMM_IMPL",
	"BC_CALC_ADVANCE",
	"BC_CALC_STRING_TLV_AND_HLEN",
	"BC_CALC_VALUE_HLEN",
	"BC_CMP_OP_F64_IMM",
	"BC_CMP_OP_I64",
	"BC_CMP_OP_I64_IMM",
	"BC_COMPOSE_YEAR_TO_DAYS",
	"BC_DECOMPOSE_TIMESTAMP_PARTS",
	"BC_DIV_FLOOR_I64VEC_BY_U64IMM",
	"BC_DIV_TRUNC_I64VEC_BY_I64VEC",
	"BC_DIV_TRUNC_I64VEC_BY_I64IMM",
	"BC_MOD_FLOOR_I64VEC_BY_U64IMM",
	"BC_MOD_FLOOR_I64VEC_BY_U64VEC",
	"BC_DIV_U32_RCP_2X",
	"BC_DIV_U32_RCP_2X_MASKED",
	"BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST",
	"_BC_ERROR_HANDLER_MORE_SCRATCH",
	"BC_EXTRACT_HMS_FROM_TIMESTAMP",
	"BC_FAST_ASIN_4ULP",
	"BC_FAST_COS_4ULP",
	"BC_FAST_LN_4ULP",
	"BC_FAST_SIN_4ULP",
	"BC_FILL_ONES",
	"BC_GET_SCRATCH_BASE_GP",
	"BC_GET_SCRATCH_BASE_ZMM",
	"BC_HORIZONTAL_LENGTH_SUM",
	"BC_MERGE_VMREFS_TO_VALUE",
	"BC_MOD_FLOOR_F64",
	"BC_MOD_TRUNC_F64",
	"BC_MODI64_IMPL",
	"BC_MOD_U32_RCP_2X_MASKED",
	"BC_NEUMAIER_SUM",
	"BC_NEUMAIER_SUM_LANE",
	"BC_POWINT",
	"BC_ROUND_OP_F64_IMPL",
	"BC_STR_CHANGE_CASE",
}
