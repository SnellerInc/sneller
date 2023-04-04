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

//go:generate goyacc partiql.y
//go:generate goimports -w y.go
//go:generate go run _generate/main.go -i keywords.txt -o lookup_gen.go
//go:generate go fmt lookup_gen.go

package partiql

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
)

const eof = -1

func init() {
	expr.IsKeyword = func(x string) bool {
		term, _ := lookupKeyword([]byte(x))
		return term != -1
	}
}

// used in testing
var faketime *expr.Timestamp

type scanner struct {
	from []byte
	pos  int

	err    error
	result *expr.Query
	// notkw is set when
	// we are not in keyword context
	notkw bool
	// the last symbol returned by `Lex`
	lastsym int

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
		s.now = &expr.Timestamp{Value: date.Now().Truncate(time.Microsecond)}
	}
	return s.now
}

func (s *scanner) Err() error {
	return s.err
}

// position determines a human-readable line and column coordinates
func (s *scanner) position(p int) (line int, column int, ok bool) {
	if p > len(s.from) {
		return 0, 0, false
	}

	buf := s.from
	line = 1
	column = 1
	for len(buf) > 0 {
		end := bytes.IndexByte(buf, '\n')
		if end == -1 {
			end = len(buf)
			buf = buf[:0]
		} else {
			buf = buf[end+1:]
		}

		if p <= end {
			// position in the current line or at the '\n'
			column = p + 1
			ok = true
			return
		}

		column = end + 1
		p -= end + 1
		if p > 0 {
			line++
		}
	}

	return
}

// chomp whitespace from input
func (s *scanner) chompws() {
	for s.pos < len(s.from) {
		if isspace(s.from[s.pos]) {
			s.notkw = false
			s.pos++
		} else {
			const (
				singleline = 1
				multiline  = 2
			)
			comment := 0
			switch c := s.from[s.pos]; c {
			case '#':
				comment = singleline
				s.pos += 1
			case '-':
				if s.pos+1 < len(s.from) && s.from[s.pos+1] == '-' {
					comment = singleline
					s.pos += 2
				}
			case '/':
				if s.pos+1 < len(s.from) && s.from[s.pos+1] == '*' {
					comment = multiline // don't alter s.pos here
				}
			case '*':
				if s.pos+1 < len(s.from) && s.from[s.pos+1] == '/' {
					s.err = s.mkerror(len("*/"), `unexpected "/" or end of multi-line comment`)
					return
				}
			}

			switch comment {
			case 0:
				return
			case singleline:
				p := bytes.IndexByte(s.from[s.pos:], '\n')
				if p >= 0 {
					s.pos += p + 1
				} else {
					s.pos = len(s.from)
				}
			case multiline:
				s.multlinecomment()
				if s.err != nil {
					return
				}
			}
		}
	}
}

func (s *scanner) multlinecomment() {
	stack := []int{s.pos} // a stack of open comments positions
	s.pos += 2
	for len(stack) > 0 {
		if s.pos >= len(s.from) {
			if len(stack) > 0 {
				s.pos = stack[len(stack)-1] // move cursor to the last comment start
				s.err = s.mkerror(len("/*"), "unterminated comment")
			}
			return
		}
		rest := s.from[s.pos:]
		p := bytes.IndexAny(rest, "/*")
		if p == -1 {
			s.pos = len(s.from)
			continue
		}
		s.pos += p

		switch rest[p] {
		case '*': // try to match '*/'
			if p+1 < len(rest) && rest[p+1] == '/' {
				stack = stack[:len(stack)-1]
				s.pos += 2
			} else {
				s.pos += 1
			}

		case '/': // try to match '/*'
			if p+1 < len(rest) && rest[p+1] == '*' {
				stack = append(stack, s.pos)
				s.pos += 2
			} else {
				s.pos += 1
			}
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
	s.lastsym = s.lex(l)
	return s.lastsym
}

func (s *scanner) lex(l *yySymType) int {
	if s.err != nil || s.pos >= len(s.from) {
		return eof
	}
	s.chompws()
	if s.err != nil || s.pos >= len(s.from) {
		return eof
	}
	b := s.peek()
	if isdigit(b) {
		return s.lexNumber(l)
	}
	if b == '-' && isdigit(s.peekat(1)) {
		if s.lastsym == NUMBER {
			// the case: NUMBER-{digits} --- return the '-' operator
			//                 ^^
			//                 we're here
			s.notkw = false
			s.pos++
			return '-'
		}
		return s.lexNumber(l)
	}
	if b == '.' && isdigit(s.peekat(1)) {
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
	case ',', '*', '-', '/', '%', ':', '&', '^', '[', ']', '(', ')', '{', '}':
		// literal operators
		s.notkw = false
		s.pos++
		return int(b)
	case '~':
		switch s.peekat(1) {
		case '*':
			s.pos += 2
			return REGEXP_MATCH_CI
		case '~':
			if s.peekat(2) == '*' {
				s.pos += 3
				return ILIKE
			}
			s.pos += 2
			return LIKE
		}
		s.pos++
		return '~'
	default:
		s.err = s.mkerror(1, "unexpected character %q", b)
		return ERROR
	}
}

// issep returns whether x is a word separator
func issep(x byte) bool {
	if isspace(x) {
		return true
	}

	switch x {
	case '(', ')', ',', '=', '<', '>', '!', '~':
		return true
	}

	return false
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
		term, enum := lookupKeyword(s.from[startpos:s.pos])
		if term == AGGREGATE {
			l.integer = enum
			return AGGREGATE
		} else if term != -1 {
			// SQL keyword following AS or BY, interpret the
			// next word as a case-sensitive identifier
			if term == AS {
				s.chompws()
				s.notkw = true
			}
			return term
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
	floatnum := s.from[s.pos] == '.'
	s.pos++
	var prev byte

	ok := func(x byte) bool {
		switch x {
		// white-space chars
		case ' ', '\n', '\t', '\r', '\f', '\v':
			return false

		// operators
		case '(', ')', '[', ']', '{', '}', '*', '/', '%', '&', '!', '^', '~', '|', ',':
			return false

		case '-', '+':
			// it's might be a sign inside the engineering notation
			esign := prev == 'e' || prev == 'E'
			floatnum = floatnum || esign
			return esign

		case '.':
			floatnum = true
		}

		return true
	}

	for s.pos < len(s.from) && ok(s.from[s.pos]) {
		prev = s.from[s.pos]
		s.pos++
	}

	// FIXME: don't allocate a string here
	str := string(s.from[startpos:s.pos])
	if !floatnum {
		i, err := strconv.ParseInt(str, 0, 64)
		if err == nil {
			l.expr = expr.Integer(i)
			return NUMBER
		}
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
		s.err = s.mkerror(len(str), "text string %q produces a number out-of-range", str)
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
	out, err := expr.Unescape(s.from[startpos : s.pos-1])
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
		s.err = s.mkerror(len(body), "unterminated ion datum literal, missing '`'")
		return ERROR
	}
	t, ok := date.Parse(body[:end])
	if !ok {
		s.err = s.mkerror(end+2, "couldn't parse ion literal %s", s.from[s.pos:s.pos+end+2])
		return ERROR
	}
	t = t.Truncate(time.Microsecond)
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

func (s *scanner) mkerror(length int, msg string, args ...any) *LexerError {
	err := &LexerError{}
	err.Message = fmt.Sprintf(msg, args...)
	err.Position = s.pos
	err.Length = length
	err.Line, err.Column, _ = s.position(err.Position)

	return err
}

func (s *scanner) Error(msg string) {
	if s.err != nil {
		return
	}

	s.err = s.mkerror(0, msg)
}

// LexerError describes a lexing error
type LexerError struct {
	Position int    // offset in the input string
	Line     int    // line
	Column   int    // column
	Length   int    // length of wrong substring (0 if unknown)
	Message  string // textual description of an error
}

func (e *LexerError) Error() string {
	if e.Line > 0 && e.Column > 0 {
		return fmt.Sprintf("at %d:%d: %s", e.Line, e.Column, e.Message)
	}

	return fmt.Sprintf("at position %d: %s", e.Position, e.Message)
}

var exprstar = expr.Star{}

func toAggregate(op expr.AggregateOp, distinct bool, args []expr.Node, filter expr.Node, over *expr.Window) (*expr.Aggregate, error) {
	agg, err := toAggregateAux(op, distinct, args, filter, over)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", op, err)
	}

	return agg, nil
}

func toAggregateAux(op expr.AggregateOp, distinct bool, args []expr.Node, filter expr.Node, over *expr.Window) (*expr.Aggregate, error) {
	var body expr.Node
	if len(args) > 0 {
		body = args[0]
		args = args[1:]
	}

	if distinct {
		if op == expr.OpCount {
			op = expr.OpCountDistinct
		}
		if !op.AcceptDistinct() {
			return nil, fmt.Errorf("does not accept DISTINCT")
		}
	}

	if expr.Equal(body, exprstar) {
		if !op.AcceptStar() {
			return nil, fmt.Errorf("does not accept '*'")
		}
	} else {
		if !op.AcceptExpression() {
			return nil, fmt.Errorf("accepts only *")
		}
	}

	switch op {
	case expr.OpApproxCountDistinct:
		return createApproxCountDistinct(body, args, filter, over)
	case expr.OpApproxPercentile:
		return createApproxPercentile(body, args, filter, over)
	default:
		if len(args) > 0 {
			return nil, fmt.Errorf("does not accept arguments")
		}

		return &expr.Aggregate{Op: op, Inner: body, Over: over, Filter: filter}, nil
	}
}

func createApproxCountDistinct(body expr.Node, args []expr.Node, filter expr.Node, over *expr.Window) (*expr.Aggregate, error) {
	if len(args) > 1 {
		return nil, fmt.Errorf("accepts at most 1 argument")
	}

	precision := expr.ApproxCountDistinctDefaultPrecision
	if len(args) == 1 {
		precisionExpr, ok := args[0].(expr.Integer)
		if !ok {
			return nil, fmt.Errorf("precision has to be a constant integer")
		}

		precision = int(precisionExpr)
		if precision < expr.ApproxCountDistinctMinPrecision || precision > expr.ApproxCountDistinctMaxPrecision {
			return nil, fmt.Errorf("precision has to be in range [%d, %d]",
				expr.ApproxCountDistinctMinPrecision, expr.ApproxCountDistinctMaxPrecision)
		}
	}

	return &expr.Aggregate{
		Op:        expr.OpApproxCountDistinct,
		Precision: uint8(precision),
		Inner:     body,
		Over:      over,
		Filter:    filter}, nil
}

func createApproxPercentile(body expr.Node, args []expr.Node, filter expr.Node, over *expr.Window) (*expr.Aggregate, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("accepts 1 argument")
	}
	p, ok := args[0].(expr.Float)
	if !ok {
		return nil, fmt.Errorf("percentile p=%v has to be floating point", args[0])
	}
	if p < 0.0 || p > 1.0 {
		return nil, fmt.Errorf("percentile p=%v has to be in range [0.0, 1.0]", p)
	}
	return &expr.Aggregate{
		Op:     expr.OpApproxPercentile,
		Misc:   float32(p),
		Inner:  body,
		Over:   over,
		Filter: filter}, nil
}

func createCase(optionalExpr expr.Node, limbs []expr.CaseLimb, elseExpr expr.Node) expr.Node {
	if optionalExpr != nil {
		// "simplified" CASE
		for i := range limbs {
			limbs[i].When = expr.Compare(expr.Equals, optionalExpr, limbs[i].When)
		}
	}

	return &expr.Case{
		Limbs: limbs,
		Else:  elseExpr,
	}
}

func parseExplain(s string) (expr.ExplainFormat, error) {
	switch s {
	case "":
		return expr.ExplainNone, nil
	case "default":
		return expr.ExplainDefault, nil
	case "text":
		return expr.ExplainText, nil
	case "list":
		return expr.ExplainList, nil
	case "gv", "graphviz":
		return expr.ExplainGraphviz, nil
	}

	return expr.ExplainNone, fmt.Errorf("%q is a wrong explain type", s)
}
