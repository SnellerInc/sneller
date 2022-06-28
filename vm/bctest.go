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
	"hash/fnv"
	"math"

	"github.com/SnellerInc/sneller/ion"
)

// bctestContext defines input/output parameters
// for an opcode.
//
// This matches specification from bc_amd64.h
type bctestContext struct {
	data    []byte // SI = the input buffer
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

	// dictionary for bytecode
	dict []string
}

// go:noescape
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
	bc := bytecode{
		compiled: []byte{
			byte(op), byte(op >> 8),
			byte(opret), byte(opret >> 8),
		},
		dict: c.dict,
	}

	bctest_run_aux(&bc, c)
	if bc.err != 0 {
		return fmt.Errorf("bytecode error: %s (%d)", bc.err.Error(), bc.err)
	}

	return nil
}

//lint:ignore U1000 kept for symmetry
func (c *bctestContext) setScalarUint64(values []uint64) {
	if len(values) > 16 {
		panic("Can set up to 16 scalar values for VM opcode")
	}

	for i, v := range values {
		c.scalar[i/8][i%8] = v
	}
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

func (c *bctestContext) setScalarIonFields(values []interface{}) {
	if len(values) > 16 {
		panic("Can set up to 16 input values for VM opcode")
	}

	if c.data == nil {
		c.data = Malloc()
	}
	c.data = c.data[:0]

	var chunk []byte
	for i := range values {
		switch v := values[i].(type) {
		case []byte:
			chunk = v

		case string:
			chunk = []byte(v)

		default:
			panic("only bytes and string are supported")
		}
		base, ok := vmdispl(c.data[len(c.data):cap(c.data)])
		if !ok {
			panic("c.data more than 1MB?")
		}
		if i%2 == 0 {
			c.scalar[0][i/2] = uint64(base)
			c.scalar[1][i/2] = uint64(len(chunk))
		} else {
			c.scalar[0][i/2] |= uint64(base) << 32
			c.scalar[1][i/2] |= uint64(len(chunk)) << 32
		}

		c.data = append(c.data, chunk...)
	}
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
			panic("only bytes, string and ion.Datum are supported")
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
	c.valid = uint16(hash32("K2"))

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
