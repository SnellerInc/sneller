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

// Helper functions for unit testing individual opcode functions.
// For sample usage please see evalbc_test.go.

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

func buftbl(buf []byte) *BufferedTable {
	return &BufferedTable{buf: buf, align: defaultAlign}
}

func vRegAsUInt64Slice(ptr *vRegData) []uint64 {
	return (*(*[vRegSize / 8]uint64)(unsafe.Pointer(ptr)))[:]
}

func sRegAsUInt64Slice(ptr *sRegData) []uint64 {
	return (*(*[sRegSize / 8]uint64)(unsafe.Pointer(ptr)))[:]
}

func i64RegAsUInt64Slice(ptr *i64RegData) []uint64 {
	return (*(*[sRegSize / 8]uint64)(unsafe.Pointer(ptr)))[:]
}

func f64RegAsUInt64Slice(ptr *f64RegData) []uint64 {
	return (*(*[sRegSize / 8]uint64)(unsafe.Pointer(ptr)))[:]
}

func appendZerosToUInt64Slice(slice []uint64, count int) []uint64 {
	n := (count + 7) / 8
	for i := 0; i < n; i++ {
		slice = append(slice, 0)
	}
	return slice
}

// bctestContext defines input/output parameters
// for an opcode.
//
// This matches specification from bc_amd64.h
type bctestContext struct {
	data []byte   // SI = VIRT_BASE; the input buffer
	dict []string // dictionary for bytecode
}

//go:noescape
func bctest_run_aux(bc *bytecode, ctx *bctestContext, activeLanes uint64)

func (c *bctestContext) Free() {
	if c.data != nil {
		Free(c.data)
		c.data = nil
	}
}

func (c *bctestContext) clear() {
	c.data = c.data[:0]
	c.dict = c.dict[:0]
}

func (c *bctestContext) ensureData() []byte {
	if c.data == nil {
		c.data = Malloc()
		c.data = c.data[:0]
	}
	return c.data
}

func (c *bctestContext) appendData(value string) {
	c.data = append(c.ensureData(), value...)
}

func (c *bctestContext) vRegFromValues(values []any, st *ion.Symtab) vRegData {
	out := vRegData{}

	if len(values) > bcLaneCount {
		panic(fmt.Sprintf("Can set up to %d input values for VM opcode, not %d", bcLaneCount, len(values)))
	}

	if st == nil {
		st = &ion.Symtab{}
	}

	c.ensureData()

	var buf ion.Buffer
	var chunk []byte
	for i := range values {
		base, ok := vmdispl(c.data[len(c.data):cap(c.data)])
		if !ok {
			panic("c.data more than 1MB?")
		}

		switch v := values[i].(type) {
		case []byte:
			chunk = v

		case string:
			chunk = []byte(v)

		case ion.Datum:
			buf.Reset()
			v.Encode(&buf, st)
			chunk = buf.Bytes()

		default:
			typ := reflect.TypeOf(v).String()
			panic("only bytes, string and ion.Datum are supported, got " + typ)
		}

		out.offsets[i] = uint32(base)
		out.sizes[i] = uint32(len(chunk))
		c.data = append(c.data, chunk...)
	}

	return out
}

func (c *bctestContext) sRegFromStrings(values []string) sRegData {
	out := sRegData{}

	if len(values) > bcLaneCount {
		panic(fmt.Sprintf("Can set up to %d input values for VM opcode, not %d", bcLaneCount, len(values)))
	}

	c.ensureData()

	for i, str := range values {
		base, ok := vmdispl(c.data[len(c.data):cap(c.data)])
		if !ok {
			panic("c.data more than 1MB?")
		}

		if len(str) != 0 {
			out.offsets[i] = base
			out.sizes[i] = uint32(len(str))
			c.data = append(c.data, str...)
		}
	}
	return out
}

func padNBytes(s string, nBytes int) string {
	buf := []byte(s + strings.Repeat(string([]byte{0}), nBytes))
	return string(buf)[:len(s)]
}

// setDict sets the first dictionary value
func (c *bctestContext) setDict(value string) {
	c.dict = append(c.dict[:0], padNBytes(value, 4))
}

// Execute runs a single opcode. It serializes all inputs to virtual stack,
// allocates stack slots passed to the instruction, and after the execution
// it deserializes content from virtual stack back to output arguments passed
// in testArgs.
func (c *bctestContext) ExecuteOpcode(op bcop, testArgs []any, activeLanes uint16) error {
	info := &opinfo[op]

	if len(info.args) != len(testArgs) {
		panic(fmt.Sprintf("argument count mismatch: opcode %s requires %d arguments, %d given", info.text, len(info.args), len(testArgs)))
	}

	args := make([]any, len(info.args))
	vStack := []uint64{}

	// serialize arguments to vStack
	for i := range testArgs {
		testArg := testArgs[i]

		// set the argument to a stack slot by default (saves
		// us some typing in each bcReadX|bcWriteX handler)
		args[i] = stackslot(len(vStack) * 8)

		switch info.args[i] {
		case bcWriteK:
			vStack = appendZerosToUInt64Slice(vStack, kRegSize)

		case bcWriteV:
			vStack = appendZerosToUInt64Slice(vStack, vRegSize)

		case bcWriteS:
			vStack = appendZerosToUInt64Slice(vStack, sRegSize)

		case bcReadK:
			k := uint64(0)
			switch v := testArg.(type) {
			case uint:
				k = uint64(v)
			case uint16:
				k = uint64(v)
			case *kRegData:
				k = uint64(v.mask)
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcReadK requires *kRegData|uint|uint16 data types", i))
			}
			vStack = append(vStack, k)

		case bcReadV:
			switch v := testArg.(type) {
			case *vRegData:
				vStack = append(vStack, vRegAsUInt64Slice(v)...)
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcReadV requires *vRegData data type", i))
			}

		case bcReadS:
			switch v := testArg.(type) {
			case *sRegData:
				vStack = append(vStack, sRegAsUInt64Slice(v)...)
			case *i64RegData:
				vStack = append(vStack, i64RegAsUInt64Slice(v)...)
			case *f64RegData:
				vStack = append(vStack, f64RegAsUInt64Slice(v)...)
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcReadV requires *sRegData|*i64RegData|*f64RegData data types", i))
			}

		case bcDictSlot:
			slot := uint16(0)

			switch v := testArg.(type) {
			case int:
				slot = uint16(v)
			case uint:
				slot = uint16(v)
			case uint16:
				slot = uint16(v)
			case uint32:
				slot = uint16(v)
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcReadV requires uint16|uint32 data types", i))
			}

			args[i] = slot

		case bcImmI8, bcImmI16, bcImmI32, bcImmI64, bcImmU8, bcImmU16, bcImmU32, bcImmU64, bcImmF64:
			// no need to do anything special regarding immediates; they are passed as is
			args[i] = testArg

		default:
			// if you hit this panic it means you are trying
			// to test something not supported at the moment
			panic(fmt.Sprintf("unsupported argument type: %s", info.args[i].String()))
		}
	}

	a := assembler{}
	a.emitOpcode(op, args...)
	a.emitOpcode(opret)

	bc := bytecode{
		compiled: a.code,
		dict:     c.dict,
		vstack:   vStack,
	}

	bctest_run_aux(&bc, c, uint64(activeLanes))

	if bc.err != 0 {
		return fmt.Errorf("bytecode error: %s (%d)", bc.err.Error(), bc.err)
	}

	// deserialize arguments from vStack
	for i := range testArgs {
		testArg := testArgs[i]

		switch info.args[i] {
		case bcWriteK:
			offset := int(args[i].(stackslot)) / 8

			switch v := testArg.(type) {
			case *kRegData:
				v.mask = uint16(vStack[offset] & 0xFFFF)
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcWriteK requires *kRegData data type", i))
			}

		case bcWriteV:
			start := int(args[i].(stackslot)) / 8
			end := start + start + vRegSize/8

			switch v := testArg.(type) {
			case *vRegData:
				copy(vRegAsUInt64Slice(v), vStack[start:end])
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcWriteV requires *vRegData data type", i))
			}

		case bcWriteS:
			start := int(args[i].(stackslot)) / 8
			end := start + start + sRegSize/8

			switch v := testArg.(type) {
			case *sRegData:
				copy(sRegAsUInt64Slice(v), vStack[start:end])
			case *i64RegData:
				copy(i64RegAsUInt64Slice(v), vStack[start:end])
			case *f64RegData:
				copy(f64RegAsUInt64Slice(v), vStack[start:end])
			default:
				panic(fmt.Sprintf("failed to extract argument #%d: bcWriteS requires *sRegData|*i64RegData*f64RegData data types", i))
			}
		}
	}

	return nil
}

// NOTE: I tried to use generics for these, but since unsafe.Sizeof() cannot be used
// with a parameter type it won't work, so there are multiple functions that do the
// same.

func verifyKRegOutput(t *testing.T, output, expected *kRegData) {
	if *output != *expected {
		t.Errorf("K register doesn't match: output 0b%b (0x%X) doesn't match 0b%b (0x%X)",
			output.mask, output.mask, expected.mask, expected.mask)
	}
}

func verifySRegOutput(t *testing.T, output, expected *sRegData) {
	if *output != *expected {
		t.Errorf("S register doesn't match:")
		for i := 0; i < bcLaneCount; i++ {
			if output.offsets[i] != expected.offsets[i] || output.sizes[i] != expected.sizes[i] {
				t.Logf("lane {%d}: output [%d:%d] doesn't match [%d:%d]",
					i, output.offsets[i], output.sizes[i], expected.offsets[i], expected.sizes[i])
			}
		}
	}
}

func verifySRegOutputP(t *testing.T, output, expected *sRegData, predicate *kRegData) {
	outputMaskedS := *output
	expectedMaskedS := *expected

	for i := 0; i < bcLaneCount; i++ {
		if (predicate.mask & (1 << i)) == 0 {
			outputMaskedS.offsets[i] = 0
			outputMaskedS.sizes[i] = 0
			expectedMaskedS.offsets[i] = 0
			expectedMaskedS.sizes[i] = 0
		}
	}

	verifySRegOutput(t, &outputMaskedS, &expectedMaskedS)
}

func verifyVRegOutput(t *testing.T, output, expected *vRegData) {
	if *output != *expected {
		t.Errorf("V register doesn't match:")
		for i := 0; i < bcLaneCount; i++ {
			if output.offsets[i] != expected.offsets[i] || output.sizes[i] != expected.sizes[i] {
				t.Logf("lane {%d}: output [%d:%d] doesn't match [%d:%d]",
					i, output.offsets[i], output.sizes[i], expected.offsets[i], expected.sizes[i])
			}
		}
	}
}

//lint:ignore U1000 available for use
func verifyVRegOutputP(t *testing.T, output, expected *vRegData, predicate *kRegData) {
	outputMaskedV := *output
	expectedMaskedV := *expected

	for i := 0; i < bcLaneCount; i++ {
		if (predicate.mask & (1 << i)) == 0 {
			outputMaskedV.offsets[i] = 0
			outputMaskedV.sizes[i] = 0
			expectedMaskedV.offsets[i] = 0
			expectedMaskedV.sizes[i] = 0
		}
	}

	verifyVRegOutput(t, &outputMaskedV, &expectedMaskedV)
}

func verifyI64RegOutput(t *testing.T, output, expected *i64RegData) {
	if *output != *expected {
		t.Errorf("S register doesn't match:")
		for i := 0; i < bcLaneCount; i++ {
			if output.values[i] != expected.values[i] {
				t.Logf("lane {%d}: output (%d) doesn't match (%d)",
					i, output.values[i], expected.values[i])
			}
		}
	}
}

//lint:ignore U1000 available for use
func verifyI64RegOutputP(t *testing.T, output, expected *i64RegData, predicate *kRegData) {
	outputMaskedS := *output
	expectedMaskedS := *expected

	for i := 0; i < bcLaneCount; i++ {
		if (predicate.mask & (1 << i)) == 0 {
			outputMaskedS.values[i] = 0
			outputMaskedS.values[i] = 0
		}
	}

	verifyI64RegOutput(t, &outputMaskedS, &expectedMaskedS)
}

func verifyF64RegOutput(t *testing.T, output, expected *f64RegData) {
	if *output != *expected {
		t.Errorf("S register doesn't match:")
		for i := 0; i < bcLaneCount; i++ {
			if output.values[i] != expected.values[i] {
				t.Logf("lane {%d}: output (%f) doesn't match (%f)",
					i, output.values[i], expected.values[i])
			}
		}
	}
}

//lint:ignore U1000 available for use
func verifyF64RegOutputP(t *testing.T, output, expected *f64RegData, predicate *kRegData) {
	outputMasked := *output
	expectedMasked := *expected

	for i := 0; i < bcLaneCount; i++ {
		if (predicate.mask & (1 << i)) == 0 {
			outputMasked.values[i] = 0
			outputMasked.values[i] = 0
		}
	}

	verifyF64RegOutput(t, &outputMasked, &expectedMasked)
}
