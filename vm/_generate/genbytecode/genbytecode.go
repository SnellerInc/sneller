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
	"strings"
)

// postprocessOpcodes parses the spec strings and prepare
// data for be written out.
func postprocessOpcodes(opcodes []Opcode) (map[string]int, error) {
	byopcode := make(map[string]int)
	for i := range opcodes {
		proc := &opcodes[i]
		byopcode[proc.name] = i

		if proc.isVariant() {
			continue
		}
		err := proc.processComments()
		if err != nil {
			return nil, fmt.Errorf("%s: opcode %s: %s", proc.location, proc.name, err)
		} else {
			if verbose {
				fmt.Printf("%-21s: %s", proc.name, proc.spec.name)
				if proc.scratch != "" {
					fmt.Printf(" (scratch: %s)", proc.scratch)
				}
				fmt.Printf("\n")
			}
		}
	}

	err := checkForDuplicates(opcodes)
	if err != nil {
		return nil, err
	}

	err = assignBaseSpec(opcodes, byopcode)
	if err != nil {
		return nil, err
	}

	return byopcode, nil
}

// checkForDuplicates checks if names of opcodes do not repeat
func checkForDuplicates(opcodes []Opcode) error {
	seen := make(map[string]int)
	for i := range opcodes {
		proc := &opcodes[i]
		if proc.isVariant() {
			continue
		}

		id, ok := seen[proc.spec.name]
		if !ok {
			seen[proc.spec.name] = i
		} else {
			p2 := &opcodes[id]
			return fmt.Errorf("%s: procedure %q already defined for %q (%s)",
				proc.location,
				proc.spec.name,
				p2.name, p2.location)
		}
	}

	return nil
}

// assignBaseSpec completes higher version of opcodes with spec from their base opcodes.
func assignBaseSpec(opcodes []Opcode, byopcode map[string]int) error {
	for i := range opcodes {
		proc := &opcodes[i]
		if !proc.isVariant() {
			continue
		}
		id, ok := byopcode[proc.baseOpcode()]
		if !ok {
			return fmt.Errorf("can't find base name")
		}

		p2 := opcodes[id]
		proc.spec = p2.spec
	}

	return nil
}

// generateArgTypeSeqs produces a single sequence of bcArgType that
// is referenced by slices of in/out/vararg in bytecode
func generateArgTypeSeqs(opcodes []Opcode) (string, error) {
	set := make(map[string]struct{})
	for i := range opcodes {
		spec := opcodes[i].spec
		in := slots2string(spec.in)
		out := slots2string(spec.out)
		va := slots2string(spec.va)

		if in != "" {
			set[in] = struct{}{}
		}
		if out != "" {
			set[out] = struct{}{}
		}
		if va != "" {
			set[va] = struct{}{}
		}
	}

	return packstrings(set), nil
}

const autogenerated = "// Code generated automatically; DO NOT EDIT"

func writeDefinitions(f io.Writer, opcodes []Opcode, argtype string) error {
	write := func(s string, args ...any) {
		fmt.Fprintf(f, s, args...)
	}

	writeln := func(s string, args ...any) {
		fmt.Fprintf(f, s, args...)
		fmt.Fprintf(f, "\n")
	}

	writeln("package vm")
	writeln("")
	writeln(autogenerated)
	writeln("")
	writeln("var opinfo = [_maxbcop]bcopinfo{")

	fmtargs := func(args []StackSlot) {
		s := slots2string(args)
		pos := strings.Index(argtype, s)
		if pos < 0 {
			panicf("can't locate %q in argtype", s)
		}

		write("bcargs[%d:%d]", pos, pos+len(args))
		write(" /* {")
		for i, c := range s {
			if i > 0 {
				write(", ")
			}
			write(slotcode2goconst(uint8(c)))
		}
		write("} */")
	}

	for i := range opcodes {
		p := &opcodes[i]
		spec := p.spec
		write("%s: {text: %q", p.goconst(), spec.name)
		if len(spec.out) > 0 {
			write(", out: ")
			fmtargs(spec.out)
		}
		if len(spec.in) > 0 {
			write(", in: ")
			fmtargs(spec.in)
		}
		if len(spec.va) > 0 {
			write(", va: ")
			fmtargs(spec.va)
		}
		if p.scratch != "" {
			write(", scratch: %s", p.scratch)
		}
		writeln("},")
	}
	writeln("}")
	writeln("")
	bcargs := WrappedBuilder{Width: 76, Indent: strings.Repeat(" ", 8)}
	bcargs.Append(fmt.Sprintf("var bcargs = [%d]bcArgType{", len(argtype)))
	for i := range argtype {
		bcargs.Append(slotcode2goconst(argtype[i]) + ", ")
	}
	bcargs.Append("}\n")
	bcargs.WriteTo(f)

	return nil
}
