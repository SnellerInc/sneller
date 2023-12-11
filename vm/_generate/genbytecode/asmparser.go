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
	"slices"
	"strings"
)

// Assembler parser extract from the source individual opcodes, including
// their body and doc comment.
func extractOpcodes(path string) ([]Opcode, error) {
	e := extractOpcodeAux{}
	reader := AssemblerLineReader{}

	e.linefn = e.scan
	err := reader.process(path, func(loc *Location, line string) error {
		e.location = loc
		return e.linefn(line)
	})

	return e.opcodes, err
}

// extractOpcodeAux is an executor class for extractOpcodes fuction
type extractOpcodeAux struct {
	opcodes  []Opcode
	comments []string
	current  *Opcode
	location *Location
	linefn   func(string) error
}

func (e *extractOpcodeAux) procedure(line string) error {
	if strings.HasPrefix(line, "TEXT ") {
		return fmt.Errorf("unexpected start of procedure")
	}

	s := strings.TrimSpace(line)

	keep, endOfProc := e.classify(s)
	if keep {
		e.current.instructions = append(e.current.instructions, s)
	}

	if endOfProc {
		e.flush()
		e.linefn = e.scan
	}

	return nil
}

func (e *extractOpcodeAux) scan(line string) error {
	if comment, ok := strings.CutPrefix(line, "//"); ok {
		e.comments = append(e.comments, comment)
		return nil
	}

	if tmp, ok := strings.CutPrefix(line, "TEXT bc"); ok {
		opcode, _, ok := strings.Cut(tmp, "(SB)")
		if ok && e.valid(opcode) {
			e.flush()
			e.current = &Opcode{
				location: *e.location,
				comments: slices.Clone(e.comments),
				name:     opcode,
			}
			e.linefn = e.procedure
		}
		return nil
	}

	// any other line resets the comments collection
	e.comments = e.comments[:0]
	return nil
}

func (e *extractOpcodeAux) flush() {
	if e.current == nil {
		return
	}

	e.opcodes = append(e.opcodes, *e.current)
	e.current = nil
}

func (e *extractOpcodeAux) valid(opcode string) bool {
	// procedures ending with `_tail` are common code
	// used by multiple opcodes.
	return !strings.HasSuffix(opcode, "_tail")
}

func (e *extractOpcodeAux) classify(s string) (keep bool, eop bool) {
	switch {
	case s == "RET":
		eop = true

	case s == "BC_RETURN_SUCCESS()":
		eop = true

	case s == "NEXT()":
		eop = true

	case s == "_BC_ERROR_HANDLER_MORE_SCRATCH()":
		keep = true
		eop = true

	case strings.HasPrefix(s, "NEXT_ADVANCE("):
		keep = true
		eop = true

	case strings.HasPrefix(s, "JMP") && strings.Contains(s, "_tail"):
		keep = true
		eop = true

	case strings.HasPrefix(s, "BC_"):
		keep = true
	}
	return
}
