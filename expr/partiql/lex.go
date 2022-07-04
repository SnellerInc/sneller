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

package partiql

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strconv"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
)

//go:generate goyacc partiql.y
//go:generate goimports -w y.go

const eof = -1

// used in testing
var faketime *expr.Timestamp

type scanner struct {
	from []byte
	pos  int

	err    error
	result expr.Node
	with   []expr.CTE
	into   expr.Node
	// notkw is set when
	// we are not in keyword context
	notkw bool

	// value of UTCNOW(); populated lazily
	// (we need every instance of UTCNOW()
	// to produce the same time exactly,
	// so we can't call time.Now() more than once)
	now *expr.Timestamp
}

func (s *scanner) utcnow() *expr.Timestamp {
	if faketime != nil {
		return faketime
	}
	if s.now == nil {
		s.now = &expr.Timestamp{Value: date.Now()}
	}
	return s.now
}

func (s *scanner) Err() error {
	return s.err
}

// chomp whitespace from input
func (s *scanner) chompws() {
	for s.pos < len(s.from) {
		if isspace(s.from[s.pos]) {
			s.notkw = false
			s.pos++
		} else if s.from[s.pos] == '#' {
			s.pos++
			for s.pos < len(s.from) && s.from[s.pos] != '\n' {
				s.pos++
			}
		} else {
			break
		}
	}
}

func (s *scanner) peek() byte {
	if s.pos == len(s.from) {
		s.err = io.EOF
		return 0
	}
	return s.from[s.pos]
}

func (s *scanner) peekat(i int) byte {
	if s.pos+i < len(s.from) {
		return s.from[s.pos+i]
	}
	return 0
}

func isdigit(x byte) bool {
	return x >= '0' && x <= '9'
}

func isalpha(x byte) bool {
	return (x >= 'a' && x <= 'z') || (x >= 'A' && x <= 'Z')
}

func isident(x byte) bool {
	return isalpha(x) || isdigit(x) || x == '_' || x == '@'
}

func isspace(x byte) bool {
	return x == ' ' || x == '\n' || x == '\t' || x == '\r' || x == '\f' || x == '\v'
}

func (s *scanner) Lex(l *yySymType) int {
	if s.err != nil || s.pos >= len(s.from) {
		return eof
	}
	s.chompws()
	if s.pos >= len(s.from) {
		return eof
	}
	b := s.peek()
	if isdigit(b) || ((b == '-' || b == '.') && isdigit(s.peekat(1))) {
		return s.lexNumber(l)
	}
	switch b {
	case '\'':
		return s.lexString(l)
	case '"':
		return s.lexQuotedIdent(l)
	case '`':
		return s.lexIon(l)
	}
	// NOTE: isident() accepts isdigit(),
	// but due to the check above, we always
	// parse words starting with a digit as a number
	if isident(b) {
		return s.lexIdent(l)
	}
	switch b {
	case '=':
		s.pos++
		return EQ
	case '!':
		if s.peekat(1) == '=' {
			s.pos += 2
			return NE
		}
		s.pos++
		return NOT
	case '<':
		if s.peekat(1) == '<' {
			s.pos += 2
			return SHIFT_LEFT_LOGICAL
		}
		if s.peekat(1) == '=' {
			s.pos += 2
			return LE
		}
		if s.peekat(1) == '>' {
			s.pos += 2
			return NE
		}
		s.pos++
		return LT
	case '>':
		if s.peekat(1) == '>' {
			if s.peekat(2) == '>' {
				s.pos += 3
				return SHIFT_RIGHT_LOGICAL
			}
			s.pos += 2
			return SHIFT_RIGHT_ARITHMETIC
		}
		if s.peekat(1) == '=' {
			s.pos += 2
			return GE
		}
		s.pos++
		return GT
	case '.':
		// if we encounter a dot,
		// the text *immediately* following this
		// cannot be a keyword
		s.pos++
		s.notkw = true
		return int(b)
	case '|':
		if s.peekat(1) == '|' {
			s.pos += 2
			return CONCAT
		}
		s.notkw = false
		s.pos++
		return int(b)
	case '+':
		if s.peekat(1) == '+' {
			s.pos += 2
			return APPEND
		}
		s.notkw = false
		s.pos++
		return int(b)
	case ',', '*', '-', '/', '%', ':', '&', '^', '~', '[', ']', '(', ')', '{', '}':
		// literal operators
		s.notkw = false
		s.pos++
		return int(b)
	default:
		s.err = &LexerError{
			Position: s.pos,
			Length:   1,
			Message:  fmt.Sprintf("unexpected character %q", b)}

		return ERROR
	}
}

// issep returns whether x is a word separator
//
// right now this is whitespace, parentheses, or ','
func issep(x byte) bool {
	return isspace(x) || x == '(' || x == ')' || x == ','
}

// lex an identifier and either return it
// as an identifier or a keyword (if it matches one)
//
// as a bit of a hack, we use some state in the lexer
// to determine if a keyword could be present in the
// current lexical context (otherwise you can't have
// columns named 'outer' or 'join' or 'select', etc.)
func (s *scanner) lexIdent(l *yySymType) int {
	startpos := s.pos
	s.pos++
	for s.pos < len(s.from) && isident(s.from[s.pos]) {
		s.pos++
	}
	wordend := s.pos == len(s.from) || issep(s.from[s.pos])
	if !s.notkw && wordend {
		// don't perform string allocation if we have a keyword
		term := kwterms.get(s.from[startpos:s.pos])
		if term != -1 {
			// following AS or BY, interpret the
			// next word as a case-sensitive identifier
			if term == AS {
				s.chompws()
				s.notkw = true
			}
			return term
		}
		aggop := aggterms.get(s.from[startpos:s.pos])
		if aggop != -1 {
			l.integer = aggop
			return AGGREGATE
		}
	}
	s.notkw = s.notkw || !wordend
	l.str = string(s.from[startpos:s.pos])
	return ID
}

// lexNumber lexes a number-like thing
// (NOTE: this is too permissive; we do the actual
// checking for valid numbers at parse time)
func (s *scanner) lexNumber(l *yySymType) int {
	startpos := s.pos
	seendot := s.from[s.pos] == '.'
	s.pos++
	seenE := false
	seenX := false
	ok := func(x byte) bool {
		// accept just one '.' character
		if x == '.' && !seendot {
			seendot = true
			return true
		}
		// accept just one 'e' character
		if !seenE && x == 'e' || x == 'E' {
			seenE = true
			return true
		}
		// accept just one 'x' character
		if !seenX && x == 'x' || x == 'X' {
			seenX = true
			return true
		}
		// if we have processed an 'x'
		// then hex characters are acceptable
		if seenX && ((x >= 'a' && x <= 'f') || (x >= 'A' && x <= 'F')) {
			return true
		}
		return x >= '0' && x <= '9'
	}
	for s.pos < len(s.from) && ok(s.from[s.pos]) {
		s.pos++
	}
	// FIXME: don't allocate a string here
	str := string(s.from[startpos:s.pos])
	i, err := strconv.ParseInt(str, 0, 64)
	if err == nil {
		l.expr = expr.Integer(i)
		return NUMBER
	}
	f, err := strconv.ParseFloat(str, 64)
	if err == nil {
		l.expr = expr.Float(f)
		return NUMBER
	}
	r, okk := new(big.Rat).SetString(str)
	if !okk {
		s.err = err
		return ERROR
	}
	// limit the amount of space this big.Rat can occupy
	// FIXME: determine this before parsing it!
	if !r.Num().IsInt64() || !r.Denom().IsInt64() {
		s.err = fmt.Errorf("text string %q produces a number out-of-range", str)
		return ERROR
	}
	l.expr = (*expr.Rational)(r)
	return NUMBER
}

func isprint(x byte) bool {
	return x >= 32 && x < 127
}

func (s *scanner) lexQuotedIdent(l *yySymType) int {
	startpos := s.pos
	s.pos++ // skip leading '"'
	ok := false
	needquote := false
	for ; s.pos < len(s.from); s.pos++ {
		if s.from[s.pos] == '\\' {
			needquote = true
			s.pos++
			continue
		}
		if !isprint(s.from[s.pos]) {
			needquote = true
		}
		if s.from[s.pos] == '"' {
			ok = true
			break
		}
	}
	if !ok {
		s.err = io.ErrUnexpectedEOF
		return ERROR
	}
	s.pos++
	if !needquote {
		l.str = string(s.from[startpos+1 : s.pos-1])
		return ID
	}
	out, err := strconv.Unquote(string(s.from[startpos:s.pos]))
	if err != nil {
		s.err = err
		return ERROR
	}
	l.str = out
	return ID
}

func (s *scanner) lexString(l *yySymType) int {
	s.pos++ // ignore starting character
	startpos := s.pos
	ok := false
	needquote := false
	for ; s.pos < len(s.from); s.pos++ {
		if s.from[s.pos] == '\'' {
			ok = true
			break
		}
		if s.from[s.pos] == '\\' {
			needquote = true
			s.pos++
			continue
		}
		if !isprint(s.from[s.pos]) {
			needquote = true
		}
	}
	if !ok {
		s.err = io.ErrUnexpectedEOF
		return ERROR
	}
	s.pos++ // ignore ending character
	// fast-path for strings that don't need quoting:
	// just produce the bytes directly
	if !needquote {
		l.str = string(s.from[startpos : s.pos-1])
		return STRING
	}

	// otherwise, do the slow thing
	out, err := unescape(s.from[startpos : s.pos-1])
	if err != nil {
		s.err = err
		return ERROR
	}
	l.str = out
	return STRING
}

func (s *scanner) lexIon(l *yySymType) int {
	// TODO: support lexing an arbitrary
	// textual ion datum; right now we only
	// lex timestamps!
	body := s.from[s.pos+1:]
	end := bytes.IndexByte(body, '`')
	if end == -1 {
		s.err = fmt.Errorf("unterminated ion datum literal")
		return ERROR
	}
	t, ok := date.Parse(body[:end])
	if !ok {
		s.err = fmt.Errorf("couldn't parse ion literal %q", s.from[s.pos:s.pos+end])
		return ERROR
	}
	s.pos = s.pos + end + 2
	l.expr = &expr.Timestamp{Value: t}
	return ION
}

func toint(e expr.Node) (int, error) {
	if i, ok := e.(expr.Integer); ok {
		return int(i), nil
	}
	if f, ok := e.(expr.Float); ok {
		if float64(int(f)) != float64(f) {
			return 0, fmt.Errorf("cannot use %g as an index", float64(f))
		}
		return int(f), nil
	}
	// FIXME
	r := (*big.Rat)(e.(*expr.Rational))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("integer out-of-range for indexing")
	}
	return int(r.Num().Int64()), nil
}

func (s *scanner) Error(msg string) {
	err := &LexerError{Position: s.pos}
	if s.err != nil {
		err.Message = fmt.Sprintf("%s (%s)", msg, s.err)
	} else {
		err.Message = msg
	}

	s.err = err
}

// LexerError describes a lexing error
type LexerError struct {
	Position int    // offset in the input string
	Length   int    // length of wrong substring (0 if unknown)
	Message  string // textual descritption of an error
}

func (e *LexerError) Error() string {
	return fmt.Sprintf("at position %d: %s", e.Position, e.Message)
}

func toAggregate(op expr.AggregateOp, body expr.Node, distinct bool, filter expr.Node, over *expr.Window) *expr.Aggregate {
	if distinct && op == expr.OpCount {
		op = expr.OpCountDistinct
	}
	return &expr.Aggregate{Op: op, Inner: body, Over: over, Filter: filter}
}
