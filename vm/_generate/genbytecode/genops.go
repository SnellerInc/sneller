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

package main

import (
	"fmt"
	"io"
)

func writeOpcodeConstants(f io.Writer, opcodes []Opcode, byopcode map[string]int) error {
	write := func(s string, args ...any) {
		fmt.Fprintf(f, s, args...)
		fmt.Fprintf(f, "\n")
	}

	write("")
	write("const (")

	for i := range opcodes {
		write("\t%s bcop = %d", opcodes[i].goconst(), i)
	}

	write("\t%s = %d", "_maxbcop", len(opcodes))
	write(")")

	write("")
	write("type opreplace struct {from, to bcop}")
	write("var patchAVX512Level2 []opreplace = []opreplace{")
	for i := range opcodes {
		proc := &opcodes[i]
		if !proc.isVariant() {
			continue
		}
		id, ok := byopcode[proc.baseOpcode()]
		if !ok {
			return fmt.Errorf("can't find base name")
		}
		base := &opcodes[id]
		write("\t{from: %s, to: %s},", proc.goconst(), base.goconst())
	}
	write("}")

	return nil
}

func writeAsmTable(f io.Writer, opcodes []Opcode) {
	write := func(s string, args ...any) {
		fmt.Fprintf(f, s, args...)
		fmt.Fprintf(f, "\n")
	}

	write(`#include "textflag.h"`)
	write("")
	write(autogenerated)
	write("")

	const data = "opaddrs"
	const trap = "trap"

	offset := 0
	for i := range opcodes {
		write("DATA %s+0x%03x(SB)/8, $bc%s(SB)", data, offset, opcodes[i].name)
		offset += 8
	}

	k := len(opcodes)
	n := nextPower(len(opcodes))
	for i := k; i < n; i++ {
		write("DATA %s+0x%03x(SB)/8, $bc%s(SB)", data, offset, trap)
		offset += 8
	}

	write("GLOBL %s(SB), RODATA|NOPTR, $0x%04x", data, offset)
}

func nextPower(x int) int {
	n := 1
	for n < x {
		n *= 2
	}

	return n
}
