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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
)

func clearScratch(bc *bytecode) {
	mem := bc.scratch[len(bc.scratch):cap(bc.scratch)]
	for i := range mem {
		mem[i] = 0
	}
}

func TestBoxFloatWritesAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st symtab
	defer st.free()
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Mul(expr.Float(1), expr.Ident("num"))

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String() // FIXME: it's a silly way of checking if we have opcode in program
	if !strings.Contains(img, "box.f64") {
		t.Fatal("Expected 'box.f64' to be present in bytecode")
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

	clearScratch(&findbc)
	// when
	evalfindbc(&findbc, delims, vRegSize)

	// then
	// When converting float values, the procedure 'boxfloat' is allowed to
	// modify 9*16 bytes starting from the current length of the scratch buffer.
	expected := ionRecord[1:]
	checkScratch(t, findbc.scratch, expected)
}

func TestBoxIntegerWritesLargeIntegersAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st symtab
	defer st.free()
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Add(expr.Integer(0), expr.Ident("num"))

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String()
	if !strings.Contains(img, "box.f64") {
		t.Fatal("Expected 'box.f64' to be present in bytecode")
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

	clearScratch(&findbc)
	// when
	evalfindbc(&findbc, delims, vRegSize)

	// then
	// Note: Math operations are done on floats and then converted back to ints,
	//       this is why the two least bytes are not equal the original ones.
	expected := []byte{0x28, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x78, 0x00}
	checkScratch(t, findbc.scratch, expected)
}

func TestBoxIntegerWritesIntegersAtValidOffsetsInScratch(t *testing.T) {
	// given
	var st symtab
	defer st.free()
	sym := st.Intern("num")
	if int(sym) != 10 {
		t.Fatal("Wrong symbol id")
	}

	node := expr.Add(expr.Integer(0), expr.Ident("num"))

	var findbc bytecode
	err := symbolizeTest(&findbc, &st, node)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	img := findbc.String()
	if !strings.Contains(img, "box.f64") {
		t.Fatal("Expected 'box.f64' to be present in bytecode")
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

	clearScratch(&findbc)
	// when
	evalfindbc(&findbc, delims, vRegSize)

	// Note: Math operations are done on floats and then converted back to ints,
	//       this is why the two least bytes are not equal the original ones.
	expected := []byte{0x25, 0x11, 0x22, 0x33, 0x44, 0x55, 0x00, 0x00}
	checkScratch(t, findbc.scratch, expected)
}

func checkScratch(t *testing.T, scratch []byte, expected []byte) {
	for i := 0; i < 16; i++ {
		buf := scratch[i*len(expected):]
		buf = buf[:len(expected)]
		if !bytes.Equal(buf, expected) {
			t.Logf("got: % 02x", buf)
			t.Logf("expected: % 02x", expected)
			t.Errorf("wrong results at %d", i)
		}
	}

	// these should remain zero after clearScratch()
	if !allBytesZero(scratch[len(scratch):cap(scratch)]) {
		t.Errorf("modified bytes outside allowed range")
	}
}

func symbolizeTest(findbc *bytecode, st *symtab, node expr.Node) error {
	var program prog

	program.begin()
	mem0 := program.initMem()
	var mem []*value
	val, err := program.compileStore(mem0, node, 0, true)
	if err != nil {
		return err
	}
	mem = append(mem, val)
	program.returnValue(program.mergeMem(mem...))
	program.symbolize(st, &auxbindings{})
	err = program.compile(findbc, st, "symbolizeTest")
	if err != nil {
		return fmt.Errorf("symbolizeTest: %w", err)
	}

	return nil
}

func allBytesZero(b []byte) bool {
	return bytes.Count(b, []byte{0x00}) == len(b)
}
