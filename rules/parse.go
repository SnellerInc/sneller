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

package rules

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"text/scanner"
)

// Parse parses a list of rules from a file.
func Parse(r io.Reader) ([]Rule, error) {
	var err error
	error := func(s *scanner.Scanner, msg string) {
		s.ErrorCount++
		if err == nil {
			err = fmt.Errorf("%s:%d:%d: %s", s.Filename, s.Line, s.Column, msg)
		}
	}
	s := new(scanner.Scanner)
	s = s.Init(r)
	if f, ok := r.(*os.File); ok {
		s.Position.Filename = f.Name()
	}
	s.Error = error
	var rules []Rule
	p := &parser{src: s}
	for !p.atEOF() && s.ErrorCount == 0 {
		loc := s.Pos()
		conj := p.conj()
		if !p.arrow() {
			break
		}
		rules = append(rules, Rule{Location: loc, From: conj, To: p.term()})
	}
	if s.ErrorCount > 0 {
		return nil, fmt.Errorf("%s (and %d other errors)", err, s.ErrorCount-1)
	}
	return rules, nil
}

// parser is an LL(1) parser
type parser struct {
	src     *scanner.Scanner
	la      rune // lookahead character
	lavalid bool // lookahead is valid
}

// peek gets the lookahead character
// without updating the parser state
// (unless no lookahead char is present)
func (p *parser) peek() rune {
	if !p.lavalid {
		p.la = p.src.Scan()
		p.lavalid = true
	}
	return p.la
}

// next updates the lookahead token and returns it
func (p *parser) next() rune {
	r := p.peek()
	p.lavalid = false
	return r
}

func (p *parser) atEOF() bool {
	return p.peek() == scanner.EOF
}

func (p *parser) ok() bool {
	return p.src.ErrorCount == 0
}

func (p *parser) consume(r rune) bool {
	if p.peek() == r {
		p.lavalid = false
		return true
	}
	return false
}

func (p *parser) conj() []Value {
	if p.atEOF() {
		return nil
	}
	first := p.value()
	if !p.ok() {
		return nil
	}
	out := []Value{first}
	for p.ok() && p.consume(',') {
		v := p.value()
		if v == nil {
			break // error
		}
		out = append(out, v)
	}
	return out
}

func (p *parser) arrow() bool {
	return p.consume('-') && p.consume('>')
}

func unquote(x string) String {
	// the scanner should have already
	// validated the syntax here:
	out, err := strconv.Unquote(x)
	if err != nil {
		panic(err)
	}
	return String(out)
}

func unbacktick(x string) String {
	return String(x[1 : len(x)-1])
}

func (p *parser) value() Value {
	r := p.next()
	switch r {
	case scanner.RawString:
		return unbacktick(p.src.TokenText())
	case scanner.String:
		return unquote(p.src.TokenText())
	case '(':
		return p.list()
	default:
		p.src.Error(p.src, "unexpected token "+scanner.TokenString(r)+" "+p.src.TokenText())
		return nil
	}
}

func (p *parser) list() Value {
	var out []Term
	for r := p.peek(); r != ')' && p.ok(); r = p.peek() {
		out = append(out, p.term())
	}
	p.next() // skip ')'
	return List(out)
}

func (p *parser) term() Term {
	switch r := p.next(); r {
	case scanner.RawString:
		return Term{
			Value:    unbacktick(p.src.TokenText()),
			Location: p.src.Pos(),
		}
	case scanner.String:
		return Term{
			Value:    unquote(p.src.TokenText()),
			Location: p.src.Pos(),
		}
	case '(':
		pos := p.src.Pos()
		return Term{
			Value:    p.list(),
			Location: pos,
		}
	case scanner.Ident:
		name := p.src.TokenText()
		pos := p.src.Pos()
		var v Value
		if p.consume(':') {
			v = p.value()
		}
		return Term{Name: name, Value: v, Location: pos}
	default:
		p.src.Error(p.src, "unexpected token "+scanner.TokenString(r))
	}
	return Term{}
}
