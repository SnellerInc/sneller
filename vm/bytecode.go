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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

//go:generate go run _generate/genops.go
//go:generate gofmt -w ops_gen.go
//go:generate go run _generate/genconst.go -i evalbc_amd64.s -o bc_constant_gen.h
//go:generate go run ./_generate/genbytecode/ -i evalbc_amd64.s -o bytecode_gen.go
//go:generate gofmt -w bytecode_gen.go

// --- How to Add an Instruction ---
//  - define a new TEXT label in evalbc_{arch}.s
//    that begins with 'bc'
//  - run 'go generate'
//  - add opcode information below

const bcSlotSize = 2                    // stack slot size in bytes
const bcLaneCount = 16                  // number of lanes processed per iteration
const bcLaneCountMask = bcLaneCount - 1 // number of lanes as mask

// actual bytecode constants are generated automatically
// by reading the assembly source and generating a named
// constant for each bytecode function
type bcop uint16

// bcArgType is a type of a single BC instruction argument that follows its opcode
// identifier.
type bcArgType uint8

const (
	bcK        bcArgType = iota // bool stack-slot, shown as k[imm]
	bcS                         // scalar stack-slot, shown as s[imm]
	bcV                         // value stack-slot, shown as v[imm]
	bcB                         // struct pointer, shown as b[imm]
	bcH                         // hash-slot, shown as h[imm]
	bcL                         // bucket-slot, shown as l[imm]
	bcDictSlot                  // 16-bit dictionary reference, shown as dict[imm]
	bcAuxSlot                   // 32-bit aux value slot
	bcAggSlot                   // 32-bit aggregation slot
	bcSymbolID                  // 32-bit symbol identifier
	bcLitRef                    // 64-bit value reference
	bcImmI8                     // signed 8-bit int immediate argument
	bcImmI16                    // signed 16-bit int immediate argument
	bcImmI32                    // signed 32-bit int immediate argument
	bcImmI64                    // signed 64-bit int immediate argument
	bcImmU8                     // unsigned 8-bit int immediate argument
	bcImmU16                    // unsigned 16-bit int immediate argument
	bcImmU32                    // unsigned 32-bit int immediate argument
	bcImmU64                    // unsigned 64-bit int immediate argument
	bcImmF64                    // 64-bit float immediate argument
)

func (a bcArgType) String() string {
	switch a {
	case bcK:
		return "K"
	case bcS:
		return "S"
	case bcV:
		return "V"
	case bcB:
		return "B"
	case bcH:
		return "H"
	case bcL:
		return "L"
	case bcDictSlot:
		return "DictSlot"
	case bcAuxSlot:
		return "AuxSlot"
	case bcAggSlot:
		return "AggSlot"
	case bcSymbolID:
		return "SymbolID"
	case bcLitRef:
		return "LitRef"
	case bcImmI8:
		return "ImmI8"
	case bcImmI16:
		return "ImmI16"
	case bcImmI32:
		return "ImmI32"
	case bcImmI64:
		return "ImmI64"
	case bcImmU8:
		return "ImmU8"
	case bcImmU16:
		return "ImmU16"
	case bcImmU32:
		return "ImmU32"
	case bcImmU64:
		return "ImmU64"
	case bcImmF64:
		return "ImmF64"
	default:
		return "<Unknown>"
	}
}

// Maps each bcArgType into a width of the immediate in bytes.
var bcImmWidth = [...]uint8{
	bcK:        bcSlotSize,
	bcS:        bcSlotSize,
	bcV:        bcSlotSize,
	bcH:        bcSlotSize,
	bcL:        bcSlotSize,
	bcB:        bcSlotSize,
	bcDictSlot: 2,
	bcAuxSlot:  2,
	bcAggSlot:  4,
	bcSymbolID: 4,
	bcLitRef:   10,
	bcImmI8:    1,
	bcImmI16:   2,
	bcImmI32:   4,
	bcImmI64:   8,
	bcImmU8:    1,
	bcImmU16:   2,
	bcImmU32:   4,
	bcImmU64:   8,
	bcImmF64:   8,
}

func (a bcArgType) immWidth() int {
	return int(bcImmWidth[a])
}

type bcopinfo struct {
	text string // opcode name
	// out and in are the output and input argument specs, respectively;
	// the serialized argument layout for non-variadic ops is
	//
	//   out ... in ...
	//
	// and for variadic ops it is
	//
	//   out ... in ... uint32(chunks) chunks...
	//
	// where each chunk is len(va) arguments consecutively
	out, in []bcArgType
	va      []bcArgType
	scratch int // desired scratch space (up to PageSize)
}

func (op bcop) scratch() int { return opinfo[op].scratch }

// bcerr is an error code returned
// from the bytecode execution engine
type bcerr int32

const (
	// MoreScratch means that there was
	// not enough space in the scratch buffer
	// to re-box an unboxed value
	bcerrMoreScratch bcerr = iota + 1
	// NeedRadix means that there was a failed
	// radix tree lookup, which may be solved
	// by performing radix tree updates and
	// re-trying the operation
	//
	// the errinfo field will be set to the
	// hash slot containing the computed hashes
	bcerrNeedRadix
	// Corrupt is returned when the bytecode
	// fails a bounds check or some other
	// piece of sanity-checking
	bcerrCorrupt
	// TreeCorrupt is returned when the
	// bytecode fails a bounds check
	// in a radix tree lookup
	bcerrTreeCorrupt
	// NullSymbolTable is returned when unsymbolize
	// found symbols, but couldn't process them as
	// there was no symbol table
	bcerrNullSymbolTable
)

func (b bcerr) Error() string {
	switch b {
	case bcerrMoreScratch:
		return "insufficient scratch space"
	case bcerrNeedRadix:
		return "missing radix tree entry"
	case bcerrCorrupt:
		return "internal assertion failed"
	case bcerrTreeCorrupt:
		return "radix tree bounds-check failed"
	case bcerrNullSymbolTable:
		return "null symbol table"
	default:
		return "unknown bytecode error"
	}
}

func (b *bytecode) prepare(rp *rowParams) {
	b.auxvals = rp.auxbound
	b.auxpos = 0
}

type bytecode struct {
	// XXX struct offsets known to assembly!
	compiled []byte   // actual opcodes
	vstack   []uint64 // value scratch space
	dict     []string // string dictionary
	symtab   []vmref  // symtab[id] -> boxed string
	auxvals  [][]vmref
	auxpos   int

	trees []*radixTree64 // trees used for hashmember, etc.

	// scratch buffer used for projection
	scratch []byte
	// number of bytes to reserve for scratch, total
	scratchtotal int
	// allocation epoch; see symtab.epoch
	epoch int
	// relative displacment of scratch relative to vmm
	scratchoff uint32

	// savedlit is the saved literal contents
	// that are copied into scratch[:] before execution
	savedlit []byte

	//lint:ignore U1000 not unused; used in assembly
	// Area that is used by bytecode instructions to temporarily spill registers.
	// 512 bytes can be used to spill up to 8 ZMM registers (or more registers of
	// any choice). Note that spill area is designed to be used only by a single
	// bytecode instruction at a time, it should not be used to persist any data
	// during the execution of bytecode.
	spillArea [512]byte

	vstacksize int

	// set from abort handlers
	err   bcerr
	errpc int32
	// additional error information;
	// error-specific
	errinfo int
}

type bcFormatFlags uint

const (
	// Redacted mode (the default).
	//
	// This turns off all formatting features.
	bcFormatRedacted = 0

	// Formatter will try to output the string representation
	// of each symbol found in the compiled BC program.
	//
	// NOTE: That this flag cannot be used in production!
	bcFormatSymbols bcFormatFlags = (1 << iota)

	//lint:ignore U1000 can be used during debugging
	//
	// Enable all format features, which can be useful during debugging
	bcFormatAll = bcFormatSymbols
)

func readUIntFromBC(buf []byte) uint64 {
	value := uint64(0)
	for i := 0; i < len(buf); i++ {
		value |= uint64(buf[i]) << (i * 8)
	}
	return value
}

func formatArgs(bc *bytecode, dst *strings.Builder, compiled []byte, args []bcArgType, flags bcFormatFlags) int {
	offset := 0
	size := len(compiled)

	for i, argType := range args {
		width := argType.immWidth()
		if size-offset < width {
			return -1
		}

		if i != 0 {
			dst.WriteString(", ")
		}

		switch argType {
		case bcK:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "k[%d]", value)
		case bcS:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "s[%d]", value)
		case bcV:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "v[%d]", value)
		case bcB:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "b[%d]", value)
		case bcH:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "h[%d]", value)
		case bcL:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "l[%d]", value)
		case bcAuxSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "aux[%d]", value)
		case bcAggSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "agg[%d]", value)
		case bcDictSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "dict[%d]", value)
		case bcSymbolID:
			value := readUIntFromBC(compiled[offset : offset+width])
			if (flags & bcFormatSymbols) != 0 {
				decoded := decodeSymbolID(uint32(value))
				if uint64(decoded) < uint64(len(bc.symtab)) {
					encodedSymbolValue := bc.symtab[decoded].mem()
					str, _, err := ion.ReadString(encodedSymbolValue)
					if err == nil {
						fmt.Fprintf(dst, "sym(%d, %q)", decoded, str)
					} else {
						fmt.Fprintf(dst, "sym(%d, <%v>)", decoded, err)
					}
					continue
				}
			}
			fmt.Fprintf(dst, "sym(%d)", value)

		case bcImmI8:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i8(%d)", int8(value))
		case bcImmI16:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i16(%d)", int16(value))
		case bcImmI32:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i32(%d)", int32(value))
		case bcImmI64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i64(%d)", int64(value))
		case bcImmU8:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u8(%d)", value)
		case bcImmU16:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u16(%d)", value)
		case bcImmU32:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u32(%d)", value)
		case bcImmU64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u64(%d)", value)
		case bcImmF64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "f64(%g)", math.Float64frombits(value))

		case bcLitRef:
			litOff := readUIntFromBC(compiled[offset : offset+4])
			litLen := readUIntFromBC(compiled[offset+4 : offset+8])
			litTLV := compiled[offset+8]
			litHLen := compiled[offset+9]
			fmt.Fprintf(dst, "litref(%d, %d, tlv=0x%02X, hLen=%d)", litOff, litLen, litTLV, litHLen)

		default:
			panic(fmt.Sprintf("Unhandled immediate type: %v", argType))
		}
		offset += width
	}

	return offset
}

func visitBytecode(bc *bytecode, fn func(offset int, op bcop, info *bcopinfo) error) error {
	compiled := bc.compiled
	size := len(compiled)
	offset := int(0)

	for offset < size {
		if size-offset < 8 {
			return fmt.Errorf("cannot decode opcode of size %d while there is only %d bytes left", 8, size-offset)
		}

		opaddr := uintptr(binary.LittleEndian.Uint64(compiled[offset:]))
		offset += 8

		op, ok := opcodeID(opaddr)
		if !ok {
			fmt.Fprintf(os.Stderr, "bytedode: %x\n", bc.compiled)
			return fmt.Errorf("failed to translate opcode address 0x%x", opaddr)
		}

		info := &opinfo[op]
		startoff := offset

		for _, argtype := range info.out {
			offset += argtype.immWidth()
		}
		for _, argtype := range info.in {
			offset += argtype.immWidth()
		}

		if len(info.va) != 0 {
			if size-offset < 4 {
				return fmt.Errorf("cannot decode va-length consisting of %d bytes while there is only %d bytes left", 4, size-offset)
			}

			vaLength := int(binary.LittleEndian.Uint32(compiled[offset:]))
			offset += 4

			width := 0
			for _, argtype := range info.va {
				width += argtype.immWidth()
			}

			offset += width * vaLength
		}

		// at this point the current opcode and its arguments are valid
		err := fn(startoff, op, info)
		if err != nil {
			return err
		}
	}

	return nil
}

func formatBytecode(bc *bytecode, flags bcFormatFlags) string {
	var b strings.Builder
	compiled := bc.compiled

	err := visitBytecode(bc, func(offset int, op bcop, info *bcopinfo) error {
		if len(info.out) != 0 {
			immSize := formatArgs(bc, &b, compiled[offset:], info.out, flags)
			if immSize == -1 {
				return nil
			}
			offset += immSize
			b.WriteString(" = ")
		}
		b.WriteString(info.text)

		if len(info.in) != 0 {
			b.WriteString(" ")
			immSize := formatArgs(bc, &b, compiled[offset:], info.in, flags)
			if immSize == -1 {
				return nil
			}
			offset += immSize
		}

		if len(info.va) != 0 {
			vaLength := uint(binary.LittleEndian.Uint32(compiled[offset:]))
			offset += 4

			if len(info.in) != 0 {
				b.WriteString(", ")
			} else {
				b.WriteString(" ")
			}

			fmt.Fprintf(&b, "va(%d)", vaLength)
			for vaIndex := 0; vaIndex < int(vaLength); vaIndex++ {
				b.WriteString(", {")
				immSize := formatArgs(bc, &b, compiled[offset:], info.va, flags)
				if immSize == -1 {
					return nil
				}
				offset += immSize
				b.WriteString("}")
			}
		}

		b.WriteString("\n")
		return nil
	})

	if err != nil {
		fmt.Fprintf(&b, "<bytecode error: %s>", err)
	}

	return b.String()
}

func (b *bytecode) String() string {
	return formatBytecode(b, bcFormatRedacted)
}

// finalize append the final 'return' instruction
// to the bytecode buffer and checks that the stack
// depth is sane
func (b *bytecode) finalize() error {
	return nil
}

// Makes sure that the virtual stack size is at least `size` (in bytes).
func (b *bytecode) ensureVStackSize(size int) {
	if b.vstacksize < size {
		b.vstacksize = size
	}
}

func alignVStackBuffer(buf []uint64) []uint64 {
	alignmentInU64Units := uintptr(bcStackAlignment >> 3)

	addr := uintptr(unsafe.Pointer(&buf[0]))
	alignDiff := (alignmentInU64Units - (addr >> 3)) & (alignmentInU64Units - 1)
	return buf[int(alignDiff):cap(buf)]
}

// Allocates all stacks that are needed to execute the bytecode program.
func (b *bytecode) allocStacks() {
	vSize := (b.vstacksize + 7) >> 3
	if cap(b.vstack) < vSize {
		b.vstack = alignVStackBuffer(make([]uint64, vSize+((bcStackAlignment-1)>>3)))
	}
}

func (b *bytecode) dropScratch() {
	b.epoch = -1
	b.scratch = nil
	// this will trigger a fault if it is used:
	b.scratchoff = 0x80000000
}

// restoreScratch updates the scratch state in b
// so that it has the correct number of bytes allocated
// from the symbol table's spare pages
func (b *bytecode) restoreScratch(st *symtab) {
	b.symtab = st.symrefs
	if b.scratchtotal == 0 {
		// this will trigger a fault if it is used:
		b.scratchoff = 0x80000000
		return
	}
	if b.epoch != st.epoch || cap(b.scratch) < b.scratchtotal {
		b.scratch = st.slab.malloc((b.scratchtotal + 15) &^ 15)
	}
	b.scratch = b.scratch[:copy(b.scratch, b.savedlit)]
	b.scratchoff, _ = vmdispl(b.scratch[:1])
}

func (b *bytecode) reset() {
	*b = bytecode{}
}
