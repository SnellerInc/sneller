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
)

// parseBytecodeSpec parses the specification of bytecode
func parseBytecodeSpec(s string) (*BytecodeSpec, error) {
	tokens, err := lexOpcodeSpec(s)
	if err != nil {
		return nil, err
	}

	p := &BytecodeSpecParser{tokens: tokens}
	bs, err := p.bytecodeSpec()
	if err != nil {
		underline(p.tokens, p.idx)
	}
	return bs, err
}

// BytecodeSpecParser parses specification of bytecode procedure.
type BytecodeSpecParser struct {
	tokens []any
	idx    int
	err    error
}

func (p *BytecodeSpecParser) bytecodeSpec() (*BytecodeSpec, error) {
	bs := &BytecodeSpec{}
	p.sequence(
		// v[0].k[1] = func.i64(v[2], s[3]).k[4]
		func(p *BytecodeSpecParser) {
			bs.out = p.parseOutArguments() // v[0].k[1]
		},
		p.expectCharacter('='),
		func(p *BytecodeSpecParser) {
			bs.name = p.literal()
		},
		p.expectCharacter('('),
		func(p *BytecodeSpecParser) {
			bs.in, bs.va = p.parseInArguments()
		},
		p.expectCharacter(')'),
		func(p *BytecodeSpecParser) {
			mask := p.optionalStackSlot()
			if p.err == nil && mask != nil {
				bs.in = append(bs.in, *mask)
			}
		},
	)

	return bs, p.err
}

func (p *BytecodeSpecParser) parseOutArguments() (res []StackSlot) {
	var allowedChar uint8
outer:
	for {
		switch v := p.current().(type) {
		case uint8:
			if v != allowedChar {
				break outer
			}
			p.consume()
			allowedChar = 0
		case string:
			switch v {
			case "_":
				p.consume()
				allowedChar = 0

			default: // slot and its optional mask
				ss1, ss2 := p.stackSlotPair()
				if p.err != nil {
					return
				}
				res = append(res, ss1)
				if ss2 != nil {
					res = append(res, *ss2)
				}
			}

			allowedChar = ','

		default:
			break outer
		}
	}
	return
}

func (p *BytecodeSpecParser) parseInArguments() (res []StackSlot, va []StackSlot) {
	var allowedChar uint8
outer:
	for {
		switch v := p.current().(type) {
		case uint8:
			if v != allowedChar {
				break outer
			}
			p.consume()
			allowedChar = 0
		case string:
			switch v {
			case "_":
				res = append(res, StackSlot{
					code: '_',
				})
				p.consume()

			case "varargs", "va": // variable args
				p.consume()
				p.sequence(
					p.expectCharacter('('),
					func(p *BytecodeSpecParser) {
						va = p.parseVaArguments()
					},
					p.expectCharacter(')'),
				)
				if p.err != nil {
					return
				}

			default: // slot and its optional mask
				ss := p.stackSlot()
				if p.err != nil {
					return
				}
				res = append(res, ss)

				if p.isCharacter('.') {
					p.consume()
					ss := p.stackSlot()
					if p.err != nil {
						return
					}
					res = append(res, ss)
				}
			}

			allowedChar = ','

		default:
			break outer
		}
	}
	return
}

func (p *BytecodeSpecParser) current() any {
	if p.idx < len(p.tokens) {
		return p.tokens[p.idx]
	}
	return nil
}

func (p *BytecodeSpecParser) consume() {
	if p.idx < len(p.tokens) {
		p.idx++
	}
}

func (p *BytecodeSpecParser) sequence(matchers ...func(*BytecodeSpecParser)) {
	for i := range matchers {
		matchers[i](p)
		if p.err != nil {
			return
		}
	}
}

func (p *BytecodeSpecParser) literal() string {
	switch v := p.current().(type) {
	case string:
		p.consume()
		return v

	default:
		p.err = fmt.Errorf("expected literal, got %T", v)
		return ""
	}
}

func (p *BytecodeSpecParser) number() int {
	switch v := p.current().(type) {
	case int:
		p.consume()
		return v

	default:
		p.err = fmt.Errorf("expected number, got %T", v)
		return 0
	}
}

func (p *BytecodeSpecParser) isCharacter(expected uint8) bool {
	if v, ok := p.current().(uint8); ok {
		return v == expected
	}

	return false
}

func (p *BytecodeSpecParser) expectCharacter(expected uint8) func(*BytecodeSpecParser) {
	return func(p *BytecodeSpecParser) {
		switch v := p.current().(type) {
		case uint8:
			if v != expected {
				p.err = fmt.Errorf("expected character %c, got %c", expected, v)
			} else {
				p.consume()
			}

		default:
			p.err = fmt.Errorf("expected character, got %T", v)
		}
	}
}

// parseStackSlot parses `literal[number]`
func (p *BytecodeSpecParser) stackSlot() (ss StackSlot) {
	p.sequence(
		func(p *BytecodeSpecParser) {
			ss.text = p.literal()
			ss.code = string2slotcode(ss.text)
			if ss.code == 0 {
				p.err = fmt.Errorf("%q is not a valid slot name", ss.text)
			}
		},
		p.expectCharacter('['),
		func(p *BytecodeSpecParser) {
			ss.index = p.number()
		},
		p.expectCharacter(']'),
	)
	return
}

func string2slotcode(s string) uint8 {
	switch s {
	case "k", "v", "s", "b", "h", "a", "l", "d", "p", "x":
		return s[0]

	case "f64", "i64", "slice", "ts", "str":
		return 's'

	case "k@imm", "v@imm", "imm16", "i16@imm":
		return '2'

	case "dict":
		return 'x'

	case "u16@imm":
		return '2'

	case "f64@imm":
		return 'f'

	case "i64@imm":
		return 'i'

	case "u64@imm":
		return '8'

	case "i32@imm":
		return 'D'

	case "value":
		return 'v'

	case "symbol":
		return 'y'

	case "litref":
		return 'd'
	}

	return 0
}

// parseStackSlotPair parses `literal[number] | literal[number].mask[number]`
func (p *BytecodeSpecParser) stackSlotPair() (ss1 StackSlot, ss2 *StackSlot) {
	ss1 = p.stackSlot()
	if p.err != nil {
		return
	}

	if p.isCharacter('.') {
		p.consume()
		tmp := p.stackSlot()
		ss2 = &tmp
	}

	return
}

func (p *BytecodeSpecParser) optionalStackSlot() (ss *StackSlot) {
	if !p.isCharacter('.') {
		return nil
	}
	p.consume()
	tmp := p.stackSlot()
	return &tmp
}

func (p *BytecodeSpecParser) parseVaArguments() (res []StackSlot) {
	var allowedChar uint8
outer:
	for {
		switch v := p.current().(type) {
		case uint8:
			if v != allowedChar {
				break outer
			}
			p.consume()
			allowedChar = 0
		case string:
			ss1, ss2 := p.stackSlotPair()
			if p.err != nil {
				return
			}
			res = append(res, ss1)
			if ss2 != nil {
				res = append(res, *ss2)
			}

			allowedChar = ','

		default:
			break outer
		}
	}
	return
}

// ------------------------------------------------------------

// lexOpcodeSpec does lexical analysis for opcode specification.
//
// Output is an array of strings, ints and a few punctuations
// characters (stored as uint8).
func lexOpcodeSpec(s string) ([]any, error) {
	var tokens []any

	number := func(s string) (int, string) {
		x := 0
		for i, c := range s {
			if c >= '0' && c <= '9' {
				x = 10*x + int(c-'0')
			} else {
				return x, s[i:]
			}
		}

		return x, ""
	}

	ident := func(s string) (string, string) {
		for i, c := range s {
			switch {
			case c == '_':
			case c == '.':
			case c == '@':
			case c == '+':
			case c >= 'a' && c <= 'z':
			case c >= 'A' && c <= 'Z':
			case c >= '0' && c <= '9':
			default:
				return s[:i], s[i:]
			}
		}

		return s, ""
	}

	for len(s) > 0 {
		c := s[0]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			name, tail := ident(s)
			s = tail
			tokens = append(tokens, name)
			continue
		}

		switch c {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			s = s[1:]

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			number, tail := number(s)
			s = tail
			tokens = append(tokens, number)

		case '(', ')', '[', ']', ',', '=', '.':
			tokens = append(tokens, c)
			s = s[1:]

		case '_':
			tokens = append(tokens, string(c))
			s = s[1:]

		default:
			return nil, fmt.Errorf("unsupported char '%c'", c)
		}
	}

	return tokens, nil
}
