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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"

	"github.com/SnellerInc/sneller/ion"
)

func buftbl(buf []byte) *BufferedTable {
	return &BufferedTable{buf: buf, align: defaultAlign}
}

// bctestContext defines input/output parameters
// for an opcode.
//
// This matches specification from bc_amd64.h
type bctestContext struct {
	data    []byte // SI = VIRT_BASE; the input buffer
	current uint16 // K1 = current mask bits
	valid   uint16 // K7 = valid mask bits

	// 'current row'
	structBase [16]uint32 // Z0 = struct base
	structLen  [16]uint32 // Z1 = struct len

	// 'current scalar'
	scalar [2][8]uint64 // Z2 + Z3 = current scalar

	// 'current value'
	valueBase [16]uint32 // Z30 = this field base
	valueLen  [16]uint32 // Z31 = this field len

	stack []byte // R12 = VIRT_VALUES

	// dictionary for bytecode
	dict []string
}

//go:noescape
func bctest_run_aux(bc *bytecode, ctx *bctestContext)

func (c *bctestContext) Free() {
	if c.data != nil {
		Free(c.data)
		c.data = nil
	}
}

// Execute runs a single opcode. It setups all the needed
// CPU registers and, after the opcode finished, read
// them back.
func (c *bctestContext) Execute(op bcop) error {
	asm := assembler{}
	asm.emitOpcode(op)
	asm.emitOpcode(opret)
	p := asm.grabCode()
	return c.execute(p)
}

// ExecuteImm2 runs a single opcode with a 2-byte immediate.
func (c *bctestContext) ExecuteImm2(op bcop, imm2 uint16) error {
	asm := assembler{}
	asm.emitOpcode(op)
	asm.emitImmU16(imm2)
	asm.emitOpcode(opret)
	p := asm.grabCode()
	return c.execute(p)
}

// Execute2Imm2 runs a single opcode with two 2-byte immediate.
func (c *bctestContext) Execute2Imm2(op bcop, imm2a, imm2b uint16) error {
	asm := assembler{}
	asm.emitOpcode(op)
	asm.emitImmU16(imm2a)
	asm.emitImmU16(imm2b)
	asm.emitOpcode(opret)
	p := asm.grabCode()
	return c.execute(p)
}

func (c *bctestContext) execute(prog []byte) error {
	bc := bytecode{
		compiled: prog,
		dict:     c.dict,
	}

	bctest_run_aux(&bc, c)
	if bc.err != 0 {
		return fmt.Errorf("bytecode error: %s (%d)", bc.err.Error(), bc.err)
	}

	return nil
}

func (c *bctestContext) getScalarUint64() (result [16]uint64) {
	for i := 0; i < 16; i++ {
		result[i] = c.scalar[i/8][i%8]
	}

	return
}

func (c *bctestContext) setScalarInt64(values []int64) {
	if len(values) > 16 {
		panic("Can set up to 16 scalar values for VM opcode")
	}

	for i, v := range values {
		c.scalar[i/8][i%8] = uint64(v)
	}
}

func (c *bctestContext) getScalarInt64() (result [16]int64) {
	for i := 0; i < 16; i++ {
		result[i] = int64(c.scalar[i/8][i%8])
	}

	return
}

func (c *bctestContext) getScalarUint32() (result [2][16]uint32) {
	for i := 0; i < 16; i++ {
		if i%2 == 0 {
			result[0][i] = uint32(c.scalar[0][i/2]) // offset
			result[1][i] = uint32(c.scalar[1][i/2]) // length
		} else {
			result[0][i] = uint32(c.scalar[0][i/2] >> 32) // offset
			result[1][i] = uint32(c.scalar[1][i/2] >> 32) // length
		}
	}
	return
}

func (c *bctestContext) setScalarFloat64(values []float64) {
	if len(values) > 16 {
		panic("Can set up to 16 scalar values for VM opcode")
	}

	for i, v := range values {
		c.scalar[i/8][i%8] = math.Float64bits(v)
	}
}

func (c *bctestContext) getScalarFloat64() (result [16]float64) {
	for i := 0; i < 16; i++ {
		result[i] = math.Float64frombits(c.scalar[i/8][i%8])
	}

	return
}

func (c *bctestContext) setData(value string) {
	if c.data == nil {
		c.data = Malloc()
	}
	c.data = c.data[:0]
	c.data = append(c.data, value...)
}

func (c *bctestContext) addScalarStrings(values []string, padding []byte) {
	if len(values) > 16 {
		panic("Can set up to 16 input values for VM opcode")
	}
	if c.data == nil {
		c.data = Malloc()
		c.data = c.data[:0]
	}
	for i, str := range values {
		base, ok := vmdispl(c.data[len(c.data):cap(c.data)])
		if !ok {
			panic("c.data more than 1MB?")
		}
		if i%2 == 0 {
			c.scalar[0][i/2] = uint64(base)
			c.scalar[1][i/2] = uint64(len(str))
		} else {
			c.scalar[0][i/2] |= uint64(base) << 32
			c.scalar[1][i/2] |= uint64(len(str)) << 32
		}
		c.data = append(c.data, str...)
		c.data = append(c.data, padding...)
	}
}

func (c *bctestContext) setScalarStrings(values []string, padding []byte) {
	if c.data == nil {
		c.data = Malloc()
	}
	c.data = c.data[:0] // clear data and then add new values
	c.addScalarStrings(values, padding)
}

func (c *bctestContext) setInputIonFields(values []interface{}, st *ion.Symtab) {
	if len(values) > 16 {
		panic("Can set up to 16 input values for VM opcode")
	}
	if c.data == nil {
		c.data = Malloc()
	}
	c.data = c.data[:0]
	var buf ion.Buffer
	var symtab *ion.Symtab

	if st != nil {
		symtab = st
	} else {
		symtab = &ion.Symtab{}
	}

	var chunk []byte
	for i := range values {
		switch v := values[i].(type) {
		case []byte:
			chunk = v

		case string:
			chunk = []byte(v)

		case ion.Datum:
			buf.Reset()
			v.Encode(&buf, symtab)
			chunk = buf.Bytes()

		default:
			typ := reflect.TypeOf(v).String()
			panic("only bytes, string and ion.Datum are supported, got " + typ)
		}
		base, ok := vmdispl(c.data[len(c.data):cap(c.data)])
		if !ok {
			panic("c.data more than 1MB?")
		}
		c.valueBase[i] = uint32(base)
		c.valueLen[i] = uint32(len(chunk))
		c.data = append(c.data, chunk...)
	}
}

func (c *bctestContext) addStack(value []byte) {
	c.stack = append(c.stack, value...)
}

func (c *bctestContext) setStackUint64(values []uint64) {
	c.stack = c.stack[:0]
	c.addStackUint64(values)
}

func (c *bctestContext) addStackUint64(values []uint64) {
	buf := make([]byte, 8)
	for i := range values {
		binary.LittleEndian.PutUint64(buf, values[i])
		c.addStack(buf)
	}
}

// Taint initializes all input values with some random bits.
//
// It is meant to be used before calling any setScalar or
// setInput procedure to make sure the opcode sets all
// expected output registers.
func (c *bctestContext) Taint() {
	hash32 := func(s string) uint32 {
		a := fnv.New32()
		a.Write([]byte(s))
		return a.Sum32()
	}

	hash64 := func(s string) uint64 {
		a := fnv.New64()
		a.Write([]byte(s))
		return a.Sum64()
	}

	c.current = uint16(hash32("K1"))
	c.valid = uint16(hash32("K7"))

	z0 := hash32("Z0")
	z1 := hash32("Z1")
	z30 := hash32("Z30")
	z31 := hash32("Z31")
	for i := 0; i < 16; i++ {
		c.structBase[i] = z0
		c.structLen[i] = z1
		c.valueBase[i] = z30
		c.valueLen[i] = z31
	}

	z2 := hash64("Z2")
	z3 := hash64("Z3")
	for i := 0; i < 8; i++ {
		c.scalar[0][i] = z2
		c.scalar[1][i] = z3
	}
}
