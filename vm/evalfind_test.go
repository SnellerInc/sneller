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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func TestBoxFloatWritesAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st ion.Symtab
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Mul(expr.Float(1), &expr.Path{First: "num"})

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String() // FIXME: it's a silly way of checking if we have opcode in program
	if !strings.Contains(img, "boxfloat") {
		t.Fatal("Expected 'boxfloat' to be present in bytecode")
	}

	// {"num": 3.123456789} -- not an integer to force fp boxing
	ionRecord := []byte{0x8a, 0x48, 0x40, 0x08, 0xfc, 0xd6, 0xe9, 0xb9, 0xcb, 0x1b}

	// finally prepare data for evalfindbc
	buf := Malloc()
	defer Free(buf)
	buf = buf[:16*len(ionRecord)]
	base, _ := vmdispl(buf)
	delims := make([]vmref, 16)
	for i := 0; i < 16; i++ {
		copy(buf[i*len(ionRecord):], ionRecord)
		delims[i][0] = uint32(i*len(ionRecord)) + base
		delims[i][1] = uint32(len(ionRecord))
	}

	// force non-empty scratch
	reserve(t, &findbc, 64)

	// when
	evalfindbc(&findbc, delims, vRegSize)

	// then
	// When converting float values, the procedure 'boxfloat' is allowed to
	// modify 9*16 bytes starting from the current length of the scratch buffer.
	expected := ionRecord[1:]
	checkScratch(t, findbc.scratch, expected, findbc.scratchreserve)
}

func reserve(t *testing.T, b *bytecode, n int) {
	if b.scratch != nil {
		Free(b.scratch)
	}
	b.scratch = Malloc()
	// we test that parts of this memory
	// remain untouched, so we need to
	// explicitly clear it
	for i := range b.scratch {
		b.scratch[i] = 0
	}
	t.Cleanup(func() {
		Free(b.scratch)
	})
	b.scratchoff, _ = vmdispl(b.scratch)
	b.scratchreserve = n
}

func TestBoxIntegerWritesLargeIntegersAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st ion.Symtab
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Add(expr.Integer(0), &expr.Path{First: "num"})

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String()
	if !strings.Contains(img, "boxfloat") {
		t.Fatal("Expected 'boxfloat' to be present in bytecode")
	}

	// {"num": 8-byte-integer}
	ionRecord := []byte{0x8a, 0x28, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}

	// finally prepare data for evalfindbc
	buf := Malloc()
	defer Free(buf)
	buf = buf[:16*len(ionRecord)]
	base, _ := vmdispl(buf)
	delims := make([]vmref, 16)
	for i := 0; i < 16; i++ {
		copy(buf[i*len(ionRecord):], ionRecord)
		delims[i][0] = uint32(i*len(ionRecord)) + base
		delims[i][1] = uint32(len(ionRecord) - 1)
	}

	// force non-empty scratch
	reserve(t, &findbc, 64)

	// when
	evalfindbc(&findbc, delims, vRegSize)

	// then
	// Note: Math operations are done on floats and then converted back to ints,
	//       this is why the two least bytes are not equal the original ones.
	expected := []byte{0x28, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x78, 0x00}
	checkScratch(t, findbc.scratch, expected, findbc.scratchreserve)
}

func TestBoxIntegerWritesIntegersAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st ion.Symtab
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Add(expr.Integer(0), &expr.Path{First: "num"})

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String()
	if !strings.Contains(img, "boxfloat") {
		t.Fatal("Expected 'boxfloat' to be present in bytecode")
	}

	// {"num": less-then-8-bytes-integer}
	ionRecord := []byte{0x8a, 0x25, 0x11, 0x22, 0x33, 0x44, 0x55}

	// finally prepare data for evalfindbc
	buf := Malloc()
	defer Free(buf)
	buf = buf[:16*len(ionRecord)]
	delims := make([]vmref, 16)
	base, _ := vmdispl(buf)
	for i := 0; i < 16; i++ {
		copy(buf[i*len(ionRecord):], ionRecord)
		delims[i][0] = uint32(i*len(ionRecord)) + base
		delims[i][1] = uint32(len(ionRecord) - 1)
	}

	// force non-empty scratch
	reserve(t, &findbc, 64)

	// when
	evalfindbc(&findbc, delims, vRegSize)

	// Note: Math operations are done on floats and then converted back to ints,
	//       this is why the two least bytes are not equal the original ones.
	expected := []byte{0x25, 0x11, 0x22, 0x33, 0x44, 0x55, 0x00, 0x00}
	checkScratch(t, findbc.scratch, expected, findbc.scratchreserve)
}

func checkScratch(t *testing.T, scratch []byte, expected []byte, initialOffset int) {
	for i := 0; i < 16; i++ {
		buf := scratch[initialOffset+i*len(expected):]
		buf = buf[:len(expected)]
		if !bytes.Equal(buf, expected) {
			t.Logf("got: % 02x", buf)
			t.Logf("expected: % 02x", expected)
			t.Errorf("wrong results at %d", i)
		}
	}

	if !allBytesZero(scratch[:initialOffset]) {
		t.Errorf("modified bytes outside allowed range")
	}
	if !allBytesZero(scratch[len(scratch):cap(scratch)]) {
		t.Errorf("modified bytes outside allowed range")
	}
}

func symbolizeTest(findbc *bytecode, st *ion.Symtab, node expr.Node) error {
	var program prog

	program.Begin()
	mem0 := program.InitMem()
	var mem []*value
	val, err := program.compileStore(mem0, node, 0, true)
	if err != nil {
		return err
	}
	mem = append(mem, val)
	program.Return(program.MergeMem(mem...))
	program.symbolize(st, &auxbindings{})
	err = program.compile(findbc)
	if err != nil {
		return fmt.Errorf("symbolizeTest: %w", err)
	}

	return nil
}

func allBytesZero(b []byte) bool {
	return bytes.Count(b, []byte{0x00}) == len(b)
}
