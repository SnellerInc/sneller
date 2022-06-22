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

package jsonrl

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

// this is the initial read buffer size;
// we need to be able to buffer each
// terminal datum (strings, numbers, null, etc.),
// including the field name if we are looking at
// a structure
var startObjectSize = 2 * 1024

// MaxDatumSize is the maximum size of a terminal
// datum in the JSON input. Fields that exceed this
// size are rejected. (In practice this is the upper bound
// on the size of strings in the source data.)
const MaxDatumSize = 512 * 1024

// MaxObjectDepth is the maximum level of
// recursion allowed in a JSON object.
const MaxObjectDepth = 64

// MaxIndexingDepth is the maximum depth
// at which sparse indexing metadata will be
// collected.
const MaxIndexingDepth = 3

var (
	// ErrNoMatch is returned from Convert
	// when the size of one of the fields
	// of the input object exceeds MaxObjectSize
	ErrNoMatch = errors.New("jsonrl: bad JSON object")
	// ErrTooLarge is returned from Convert
	// when the input would require more than
	// MaxObjectSize bytes of buffering in order
	// for a complete object to be parsed.
	ErrTooLarge = errors.New("jsonrl: object too large")
)

type token int

const (
	tokDatum  token = iota // terminal datum
	tokComma               // ,
	tokLBrace              // {
	tokRBrace              // }
	tokLBrack              // [
	tokRBrack              // ]
	tokEOF
)

//go:generate ragel -L -Z -G2 lex2.rl

// parser tracks the JSON parse state
type parser struct {
	tok    token
	auxtok token
	depth  int
	output *state
}

// reader performs buffering for parsing
type reader struct {
	buf     []byte //
	rpos    int    // buf[rpos:len(buf)] is valid for reading
	flushed int
	input   io.Reader
	err     error
	atEOF   bool
}

// buffered is the number of
// available (buffered) bytes
func (b *reader) buffered() int { return len(b.buf) - b.rpos }

// fill the buffer, realloc'ing as necessary
func (b *reader) fill() {
	if b.input == nil {
		b.atEOF = true
		return
	}
	b.shift()
	if b.atEOF {
		return
	}
	if len(b.buf) == cap(b.buf) {
		next := make([]byte, 2*len(b.buf))
		b.buf = next[:copy(next, b.buf)]
	}
	tail := b.buf[len(b.buf):cap(b.buf)]
	n, err := b.input.Read(tail)
	b.buf = b.buf[:len(b.buf)+n]
	if err != nil {
		if errors.Is(err, io.EOF) {
			b.atEOF = true
		} else {
			b.err = err
		}
	}
}

// consumed is the number of bytes consumed
// from the input reader so far
func (b *reader) consumed() int { return b.flushed + b.rpos }

// copy data to the front of the buffer
func (b *reader) shift() {
	// kill spaces before shifting
	b.chomp()
	b.flushed += b.rpos
	if b.rpos == len(b.buf) {
		b.buf = b.buf[:0]
	} else if b.rpos > 0 {
		b.buf = b.buf[:copy(b.buf, b.avail())]
	}
	b.rpos = 0
}

func (b *reader) chomp() {
	for b.rpos < len(b.buf) && isSpace(b.buf[b.rpos]) {
		b.rpos++
	}
}

// search for (and discard) the given string constant,
// eating whitespace as we go
func (b *reader) lexOne(token string) error {
	for b.err == nil {
		if len(b.avail()) < len(token) {
			if !b.assertFill() {
				if b.err != nil {
					return b.err
				}
				return fmt.Errorf("unexpected EOF while seeking %q %w", token, ErrNoMatch)
			}
			continue
		}
		b.chomp()
		cur := b.avail()
		if len(cur) >= len(token) && bytes.Equal(cur[:len(token)], []byte(token)) {
			b.rpos += len(token)
			return nil
		}
	}
	return b.err
}

// avail is currently-buffered data
func (b *reader) avail() []byte {
	return b.buf[b.rpos:]
}

// assert there are some bytes to process
func (b *reader) assertFill() bool {
	if b.buffered() == 0 && !b.atEOF && b.err == nil {
		b.fill()
	}
	return b.buffered() > 0
}

func isSpace(c byte) bool {
	switch c {
	case '\n', '\r', ' ', '\f', '\v', '\t':
		return true
	default:
		return false
	}
}

func (t *parser) parseRecord(b *reader) error {
	t.depth++
	if t.depth >= MaxObjectDepth {
		return fmt.Errorf("%w (max object depth exceeded)", ErrTooLarge)
	}
	t.output.beginRecord()
	first := true
outer:
	for {
		err := t.lexField(b)
		if err != nil {
			return err
		}
		switch t.tok {
		default:
			panic("unexpected token from parseRecord lexField")
		case tokRBrace:
			if !first {
				return fmt.Errorf("%w: rejecting ',' before '}'", ErrNoMatch)
			}
			break outer
		case tokLBrace:
			err = t.parseRecord(b)
		case tokLBrack:
			err = t.parseList(b)
		case tokDatum:
			if t.auxtok != tokRBrace {
				panic("bad parser.auxtok after lexField")
			}
			break outer
		}
		// we only hit this path
		// if we had to parse a sub-{list,struct}
		first = false
		if err != nil {
			return err
		}
		err = t.lexMoreStruct(b)
		if err != nil {
			return err
		}
		switch t.tok {
		default:
			panic("unexpected token from lexMoreStruct")
		case tokRBrace:
			break outer
		case tokComma:
			// continue loop
		}
	}
	t.depth--
	t.output.endRecord()
	return nil
}

func (t *parser) parseList(b *reader) error {
	t.depth++
	if t.depth >= MaxObjectDepth {
		return fmt.Errorf("%w (max object depth exceeded)", ErrTooLarge)
	}
	t.output.beginList()
	first := true
outer:
	for {
		err := t.lexListField(b, true)
		if err != nil {
			return err
		}
		switch t.tok {
		default:
			panic("invalid token from lexListField")
		case tokRBrack:
			// we should only see this
			// on the first loop iteration
			if !first {
				return fmt.Errorf("%w: rejecting ',' before ']'", ErrNoMatch)
			}
			break outer // terminating ']'
		case tokLBrack:
			err = t.parseList(b)
		case tokLBrace:
			err = t.parseRecord(b)
		case tokDatum:
			// we terminated the loop inside the lexer
			if t.auxtok != tokRBrack {
				panic("bad parser.auxtok after lexListField")
			}
			break outer
		}
		// we only hit this path if
		// we had to lex a sub-{list,struct}
		if err != nil {
			return err
		}
		first = false
		err = t.lexMoreList(b)
		if err != nil {
			return err
		}
		switch t.tok {
		case tokRBrack:
			break outer
		case tokComma:
			// continue
		default:
			panic("unexpected token from lexMoreList")
		}
	}
	t.depth--
	t.output.endList()
	return nil
}

// parse a top-level object
//
// we allow records or arrays-of-records here
func (t *parser) parseTopLevel(b *reader) error {
	err := t.lexToplevel(b)
	if err != nil {
		return err
	}
	switch t.tok {
	case tokLBrace:
		err = t.parseRecord(b)
		if err != nil {
			return err
		}
		return t.output.Commit()
	case tokLBrack:
		// parse top-level list of records
		return t.parseFlattenList(b)
	case tokEOF:
		return nil
	default:
		panic("invalid token returned from lexToplevel")
	}
}

// parse a list, but emit its contents
// as separate objects instead of as a list
func (t *parser) parseFlattenList(b *reader) error {
	first := true
outer:
	for {
		err := t.lexListField(b, false)
		if err != nil {
			return err
		}
		switch t.tok {
		case tokRBrack:
			if !first {
				return fmt.Errorf("%w: rejecting ',' before ']'", ErrNoMatch)
			}
			break outer // terminating ']'
		case tokLBrace:
			// inner structure
			err = t.parseRecord(b)
			if err != nil {
				return err
			}
			err = t.output.Commit()
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w (top-level list should only contain structures)", ErrNoMatch)
		}
		first = false
		err = t.lexMoreList(b)
		if err != nil {
			return err
		}
		switch t.tok {
		case tokRBrack:
			break outer
		case tokComma:
			// continue
		default:
			panic("invalid token returned from lexMoreList")
		}
	}
	return nil
}

// Deprecated: use Convert
func parseObject(st *state, in []byte) (int, error) {
	b := &reader{
		buf:   in,
		atEOF: true,
	}
	tb := &parser{output: st}
	err := tb.parseTopLevel(b)
	return b.consumed(), err
}

// Convert reads JSON records from src and writes
// them to dst. If hints is non-nil, it uses hints
// to determine how certain fields are interpreted.
//
// The JSON in src should be zero or more records,
// optionally wrapped in a JSON array. Convert
// will automatically flatten top-level arrays-of-records.
//
// Convert will return an error if the input JSON is malformed,
// if it violates some internal limit (see MaxObjectSize, MaxObjectDepth),
// or if the object does not fit in dst.Align after being
// serialized as ion data.
func Convert(src io.Reader, dst *ion.Chunker, hints *Hint) error {
	st := newState(dst)
	st.UseHints(hints)
	tb := &parser{output: st}
	in := &reader{
		buf:   make([]byte, 0, startObjectSize),
		input: src,
	}
	rec := 0
	for {
		err := tb.parseTopLevel(in)
		if err != nil {
			return fmt.Errorf("object %d: %w", rec, err)
		}
		if tb.tok == tokEOF {
			break
		}
		rec++
	}
	return dst.Flush()
}
