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

	"golang.org/x/exp/slices"
)

// Opcode represents all parameters for the given opcode
type Opcode struct {
	location Location // where the opcode is defined

	// data extracted during file scanning
	name         string   // name of opcode (without the leading 'bc')
	comments     []string // doc comments
	instructions []string // list of instructions we like to analyse later on

	// data extracted from comments
	spec    *BytecodeSpec
	scratch string // verbatim copy of scratch size (it's not checked, just pasted directly)
}

// isVariant returns true if the opcode is another variant of other opcode
func (o *Opcode) isVariant() bool {
	return strings.HasSuffix(o.name, "_v2")
}

func (o *Opcode) baseOpcode() string {
	b, _ := strings.CutSuffix(o.name, "_v2")
	return b
}

func (o *Opcode) goconst() string {
	if base, ok := strings.CutSuffix(o.name, "_v2"); ok {
		return "op" + base + "v2"
	}

	return "op" + o.name
}

func (o *Opcode) processComments() error {
	for _, line := range o.comments {
		if containsAll(line, "=()") {
			bs, err := parseBytecodeSpec(line)
			if err == nil {
				if o.spec != nil {
					return fmt.Errorf("bytecode spec given more than once")
				}

				o.spec = bs
			}
		} else {
			line = strings.TrimSpace(line)
			if scratchStr, ok := strings.CutPrefix(line, "scratch:"); ok {
				scratchStr = strings.TrimSpace(scratchStr)
				if scratchStr == "" {
					return fmt.Errorf("scratch expression is missing")
				}
				if o.scratch != "" {
					return fmt.Errorf("scratch expression is specified more than once")
				}

				o.scratch = scratchStr
			}
		}
	}

	if o.spec == nil {
		return fmt.Errorf("no bytecode spec found in doc comment")
	}

	return nil
}

type BytecodeSpec struct {
	name string      // textual representation
	out  []StackSlot // output arguments
	in   []StackSlot // input arguments
	va   []StackSlot // variadic arguments
}

type StackSlot struct {
	code  uint8  // single-letter code of stack slot
	text  string // user input
	index int    // index in the physical buffer
}

func (s *StackSlot) String() string {
	return fmt.Sprintf("%c[%d]", s.code, s.index)
}

func slotcode2goconst(c uint8) string {
	switch c {
	case 'k':
		return "bcK"
	case 's':
		return "bcS"
	case 'v':
		return "bcV"
	case 'b':
		return "bcB"
	case 'h':
		return "bcH"
	case 'l':
		return "bcL"
	case 'x':
		return "bcDictSlot"
	case 'p':
		return "bcAuxSlot"
	case 'a':
		return "bcAggSlot"
	case 'y':
		return "bcSymbolID"
	case 'd':
		return "bcLitRef"
	case 'C':
		return "bcImmI8"
	case 'W':
		return "bcImmI16"
	case 'I':
		return "bcImmI32"
	case 'i':
		return "bcImmI64"
	case '1':
		return "bcImmU8"
	case '2':
		return "bcImmU16"
	case '4':
		return "bcImmU32"
	case '8':
		return "bcImmU64"
	case 'f':
		return "bcImmF64"
	}

	panicf("wrong slot code %c", c)
	return ""
}

func containsAll(s, set string) bool {
	for _, c := range set {
		if !strings.ContainsRune(s, c) {
			return false
		}
	}

	return true
}

func underline(tokens []any, pos ...int) (string, string) {
	b := &strings.Builder{}
	u := &strings.Builder{}

	write := func(i int, s string) {
		b.WriteString(s)
		n := len(s)
		if slices.Contains(pos, i) {
			u.WriteString(strings.Repeat("^", n))
		} else {
			u.WriteString(strings.Repeat(" ", n))
		}
	}

	for i := range tokens {
		switch v := tokens[i].(type) {
		case string:
			write(i, v)
		case uint8:
			write(i, fmt.Sprintf("%c", v))
		case int:
			write(i, fmt.Sprintf("%d", v))
		case nil:
			write(i, "<nil>")
		default:
			panic(fmt.Sprintf("wrong type %T", v))
		}
	}

	return b.String(), u.String()
}

func (b *BytecodeSpec) String() string {
	f := &strings.Builder{}

	format := func(slots []StackSlot) {
		for i := range slots {
			if i > 0 {
				f.WriteString(", ")
			}

			f.WriteString(slots[i].String())
		}
	}

	format(b.out)
	f.WriteString(" = ")

	f.WriteString(b.name)

	f.WriteRune('(')
	format(b.in)
	f.WriteRune(')')

	return f.String()
}

func slots2string(slots []StackSlot) (res string) {
	tmp := slices.Clone(slots)
	slices.SortFunc(tmp, func(a, b StackSlot) int {
		return a.index - b.index
	})

	for i := range tmp {
		res += string(rune(tmp[i].code))
	}

	return
}
