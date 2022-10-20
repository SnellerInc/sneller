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
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/expr"
)

func init() {
	yyErrorVerbose = true
}

var parserPool = sync.Pool{
	New: func() interface{} {
		return &yyParserImpl{}
	},
}

func newParser() *yyParserImpl {
	return parserPool.Get().(*yyParserImpl)
}

func dropParser(p *yyParserImpl) {
	parserPool.Put(p)
}

// Parse parses a PartiQL Select-From-Where query
// and returns the result, or an error if one
// is encountered.
func Parse(in []byte) (*expr.Query, error) {
	s := &scanner{from: in}
	p := newParser()
	ret := p.Parse(s)
	dropParser(p)
	if s.err != nil && s.err != io.EOF {
		return nil, s.err
	}
	if ret != 0 {
		return nil, fmt.Errorf("parse error %d", ret)
	}
	return &expr.Query{
		With: s.with,
		Into: s.into,
		Body: s.result,
	}, nil
}

// we parse CAST() using identifiers
// rather than keywords so that we can
// preserve the invariant that the token
// immediately following an 'AS' token
// is always parsed as an identifier
// (the type name following AS inside a CAST
// has no grammatical significance anyway)
func buildCast(inner expr.Node, id string) (expr.Node, bool) {
	var ts expr.TypeSet
	switch strings.ToUpper(id) {
	case "INTEGER":
		ts = expr.IntegerType
	case "FLOAT":
		ts = expr.FloatType
	case "BOOLEAN":
		ts = expr.BoolType
	case "NULL":
		ts = expr.NullType
	case "MISSING":
		ts = expr.MissingType
	case "TIMESTAMP":
		ts = expr.TimeType
	case "STRING":
		ts = expr.StringType
	case "DECIMAL":
		ts = expr.DecimalType
	case "STRUCT":
		ts = expr.StructType
	case "LIST":
		ts = expr.ListType
	case "SYMBOL":
		ts = expr.SymbolType
	default:
		return nil, false
	}
	return &expr.Cast{From: inner, To: ts}, true
}

// weekday parses a weekday from string
func weekday(id string) (expr.Weekday, bool) {
	switch strings.ToUpper(id) {
	case "SUNDAY":
		return expr.Sunday, true
	case "MONDAY":
		return expr.Monday, true
	case "TUESDAY":
		return expr.Tuesday, true
	case "WEDNESDAY":
		return expr.Wednesday, true
	case "THURSDAY":
		return expr.Thursday, true
	case "FRIDAY":
		return expr.Friday, true
	case "SATURDAY":
		return expr.Saturday, true
	default:
		return 0, false
	}
}

func timePart(id string) (expr.Timepart, bool) {
	var part expr.Timepart
	switch strings.ToUpper(id) {
	case "MICROSECOND", "MICROSECONDS":
		part = expr.Microsecond
	case "MILLISECOND", "MILLISECONDS":
		part = expr.Millisecond
	case "SECOND":
		part = expr.Second
	case "MINUTE":
		part = expr.Minute
	case "HOUR":
		part = expr.Hour
	case "DAY":
		part = expr.Day
	case "DOW":
		part = expr.DOW
	case "DOY":
		part = expr.DOY
	case "WEEK":
		part = expr.Week
	case "MONTH":
		part = expr.Month
	case "QUARTER":
		part = expr.Quarter
	case "YEAR":
		part = expr.Year
	default:
		return 0, false
	}
	return part, true
}

// timePartFor parses an expr.Timepart for a particular function `fn`.
//
// The reason for having this function is asymmetricity of time parts
// that can be used with date manipulation and extraction functions.
// For example EXTRACT() supports more time parts than DATE_TRUNC().
func timePartFor(id, fn string) (expr.Timepart, bool) {
	part, ok := timePart(id)
	if !ok {
		return 0, false
	}

	// reject parts that are not supported by some timestamp related functions
	switch fn {
	case "DATE_ADD":
		if part == expr.DOW || part == expr.DOY {
			return 0, false
		}
	case "DATE_DIFF":
		if part == expr.DOW || part == expr.DOY {
			return 0, false
		}
	case "DATE_TRUNC":
		if part == expr.DOW || part == expr.DOY {
			return 0, false
		}
	case "EXTRACT":
		if part == expr.Week {
			return 0, false
		}
	}

	return part, ok
}

func exists(s *expr.Select) expr.Node {
	if s.Limit != nil && int(*s.Limit) == 0 {
		return expr.Bool(false)
	}
	// if this is a correlated sub-query, we would
	// reject it if it didn't specify column(s) explicitly;
	// just insert a dummy column instead
	if len(s.Columns) == 1 && s.Columns[0].Expr == (expr.Star{}) {
		s.Columns[0].Expr = expr.Bool(true)
	}
	lim := expr.Integer(1)
	s.Limit = &lim
	return expr.Is(s, expr.IsNotMissing)
}

// decodeDistinct inteprets the node list collected by `maybe_toplevel_distinct`
// as inputs for `expr.Select`. The matching is as follows:
// if nodes == nil   then SELECT ...
// if nodes == []    then SELECT DISTINCT ...
// if nodes == [...] then SELECT DISTINCT ON (...) ...
func decodeDistinct(nodes []expr.Node) (distinct bool, distinctExpr []expr.Node) {
	if nodes == nil {
		return false, nil
	}

	if len(nodes) == 0 {
		return true, nil
	}

	return false, nodes
}

const (
	trimLeading = iota
	trimTrailing
	trimBoth
)

// createTrimInvocation creates trim/ltrim/rtrim invocation from an SQL query.
func createTrimInvocation(trimType int, str, charset expr.Node) (expr.Node, error) {
	op := expr.Unspecified
	switch trimType {
	case trimLeading:
		op = expr.Ltrim
	case trimTrailing:
		op = expr.Rtrim
	case trimBoth:
		op = expr.Trim
	}

	if op == expr.Unspecified {
		return nil, fmt.Errorf("value %d is not a valid trim type", trimType)
	}

	if charset != nil {
		return expr.Call(op, str, charset), nil
	}

	return expr.Call(op, str), nil
}
