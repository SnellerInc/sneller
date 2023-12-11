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

type Invocation struct {
	name string
	args []any
}

type RegisterGPR uint8

type RegisterXMM uint8

type RegisterYMM uint8

type RegisterZMM uint8

type RegisterK uint8

func parseExpression(s string) (any, error) {
	tokens, err := lexExpression(s)
	if err != nil {
		return nil, err
	}

	p := expressionParser{tokens: tokens, pos: 0}
	e := p.expression(nil)

	return e, p.err
}

type expressionParser struct {
	tokens []any
	pos    int
	err    error
}

func (p *expressionParser) expression(breakon func(uint8) bool) any {
	var expressions []any
	for !p.eos() && p.err == nil {
		c := p.current()
		if breakon != nil {
			if chr, ok := c.(uint8); ok && breakon(chr) {
				break
			}
		}

		if s, ok := c.(string); ok {
			next := p.peek(1)
			if chr, ok := next.(uint8); ok && chr == '(' {
				p.consume()
				p.consume()
				args := p.invocationArgs()
				expressions = append(expressions, Invocation{
					name: s,
					args: args,
				})
			} else {
				if gpr, ok := string2gpr(s); ok {
					expressions = append(expressions, gpr)
				} else if xmm, ok := string2xmm(s); ok {
					expressions = append(expressions, xmm)
				} else if ymm, ok := string2ymm(s); ok {
					expressions = append(expressions, ymm)
				} else if zmm, ok := string2zmm(s); ok {
					expressions = append(expressions, zmm)
				} else if kreg, ok := string2kreg(s); ok {
					expressions = append(expressions, kreg)
				} else {
					expressions = append(expressions, c)
				}
				p.consume()
			}
		} else {
			if chr, ok := c.(uint8); ok {
				switch chr {
				case ',', '(', ')':
					p.err = fmt.Errorf("unexpected char '%c'", chr)
				}
			}
			expressions = append(expressions, c)
			p.consume()
		}
	}

	if len(expressions) == 1 {
		return expressions[0]
	}

	return expressions
}

func (p *expressionParser) invocationArgs() []any {
	var args []any
	for {
		arg := p.expression(func(chr uint8) bool {
			return chr == ',' || chr == ')'
		})

		if p.err != nil {
			break
		}

		args = append(args, arg)

		c := p.current()
		if chr, ok := c.(uint8); ok {
			if chr == ')' {
				p.consume()
				return args
			} else if chr == ',' {
				p.consume()
			} else {
				p.err = fmt.Errorf("unexpected char '%c'", chr)
				break
			}
		} else {
			p.err = fmt.Errorf("unexpected token %q", c)
			break
		}
	}

	return args
}

func (p *expressionParser) eos() bool {
	return p.pos >= len(p.tokens)
}

func (p *expressionParser) peek(n int) any {
	pos := p.pos + n
	if pos >= len(p.tokens) {
		return nil
	}
	return p.tokens[pos]
}

func (p *expressionParser) current() any {
	return p.peek(0)
}

func (p *expressionParser) consume() {
	if p.pos < len(p.tokens) {
		p.pos++
	}
}

func lexExpression(s string) ([]any, error) {
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
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
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

		case '(', ')', '[', ']', ',', '+', '*', ';':
			tokens = append(tokens, c)
			s = s[1:]

		case '/':
			if len(s) > 1 && s[1] == '/' { // a comment
				s = s[:0]
				break
			}

			return nil, fmt.Errorf("unsupported char '%c'", c)

		case '-':
			s = s[1:]
			number, tail := number(s)
			if s == tail {
				return nil, fmt.Errorf("expected a number")
			}

			s = tail
			tokens = append(tokens, -number)

		case '$':
			tokens = append(tokens, string(c))
			s = s[1:]

		default:
			return nil, fmt.Errorf("unsupported char '%c'", c)
		}
	}

	return tokens, nil
}

var gprnames = []string{
	"AX", "BX", "CX", "DX",
	"R8", "R9", "R10", "R11", "R12", "R13", "R14", "R15"}

var gprlookup map[string]RegisterGPR

func string2gpr(s string) (RegisterGPR, bool) {
	if gprlookup == nil {
		gprlookup = make(map[string]RegisterGPR)
		for i, s := range gprnames {
			gprlookup[s] = RegisterGPR(i)
		}
	}

	r, ok := gprlookup[s]
	return r, ok
}

func (r RegisterGPR) String() string {
	if int(r) < len(gprnames) {
		return gprnames[r]
	}
	return fmt.Sprintf("<GPR=%d>", r)
}

var kregnames = []string{"K0", "K1", "K2", "K3", "K4", "K5", "K6", "K7"}

var kreglookup map[string]RegisterK

func string2kreg(s string) (RegisterK, bool) {
	if kreglookup == nil {
		kreglookup = make(map[string]RegisterK)
		for i, s := range kregnames {
			kreglookup[s] = RegisterK(i)
		}
	}

	r, ok := kreglookup[s]
	return r, ok
}

func (r RegisterK) String() string {
	if int(r) < len(kregnames) {
		return kregnames[r]
	}
	return fmt.Sprintf("<Kreg=%d>", r)
}

var xmmnames = []string{
	"X0", "X1", "X3", "X4", "X5", "X6", "X7",
	"X8", "X9", "X10", "X11", "X12", "X13", "X14", "X15"}

var xmmlookup map[string]RegisterXMM

func string2xmm(s string) (RegisterXMM, bool) {
	if xmmlookup == nil {
		xmmlookup = make(map[string]RegisterXMM)
		for i, s := range xmmnames {
			xmmlookup[s] = RegisterXMM(i)
		}
	}

	r, ok := xmmlookup[s]
	return r, ok
}

func (r RegisterXMM) String() string {
	if int(r) < len(xmmnames) {
		return xmmnames[r]
	}
	return fmt.Sprintf("<XMM=%d>", r)
}

var ymmnames = []string{
	"Y0", "Y1", "Y3", "Y4", "Y5", "Y6", "Y7",
	"Y8", "Y9", "Y10", "Y11", "Y12", "Y13", "Y14", "Y15"}

var ymmlookup map[string]RegisterYMM

func string2ymm(s string) (RegisterYMM, bool) {
	if ymmlookup == nil {
		ymmlookup = make(map[string]RegisterYMM)
		for i, s := range ymmnames {
			ymmlookup[s] = RegisterYMM(i)
		}
	}

	r, ok := ymmlookup[s]
	return r, ok
}

func (r RegisterYMM) String() string {
	if int(r) < len(ymmnames) {
		return ymmnames[r]
	}
	return fmt.Sprintf("<YMM=%d>", r)
}

var zmmnames = []string{
	"Z0", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "Z8", "Z9",
	"Z10", "Z11", "Z12", "Z13", "Z14", "Z15", "Z16", "Z17", "Z18", "Z19",
	"Z20", "Z21", "Z22", "Z23", "Z24", "Z25", "Z26", "Z27", "Z28", "Z29",
	"Z30", "Z31"}

var zmmlookup map[string]RegisterZMM

func string2zmm(s string) (RegisterZMM, bool) {
	if zmmlookup == nil {
		zmmlookup = make(map[string]RegisterZMM)
		for i, s := range zmmnames {
			zmmlookup[s] = RegisterZMM(i)
		}
	}

	r, ok := zmmlookup[s]
	return r, ok
}

func (r RegisterZMM) String() string {
	if int(r) < len(zmmnames) {
		return zmmnames[r]
	}
	return fmt.Sprintf("<ZMM=%d>", r)
}
