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

package partiql

import (
	"fmt"
	"io"
	"strconv"
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
	return s.result, nil
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

func parseIntervalQuantity(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func parseIntervalPart(s string) (expr.Timepart, bool) {
	switch strings.ToUpper(s) {
	case "DAY", "DAYS":
		return expr.Day, true
	case "HOUR", "HOURS":
		return expr.Hour, true
	case "MINUTE", "MINUTES":
		return expr.Minute, true
	case "SECOND", "SECONDS":
		return expr.Second, true
	case "MILLISECOND", "MILLISECONDS":
		return expr.Millisecond, true
	case "MICROSECOND", "MICROSECONDS":
		return expr.Microsecond, true
	}
	return 0, false
}

func parseInterval(s string) (int64, error) {
	fields := strings.Fields(s)
	i := 0

	if len(fields) == 0 {
		return 0, fmt.Errorf("invalid interval %q: interval cannot be empty", s)
	}

	interval := int64(0)

	// Parse <int> <string> pairs
	for {
		if i == len(fields) {
			break
		}

		if i+2 > len(fields) {
			return 0, fmt.Errorf("invalid interval %q", s)
		}

		quantity, quantityErr := parseIntervalQuantity(fields[i])
		if quantityErr != nil {
			return 0, fmt.Errorf("invalid interval %q: %q is not a valid quantity", s, fields[i])
		}

		part, partOk := parseIntervalPart(fields[i+1])
		if !partOk {
			return 0, fmt.Errorf("invalid interval %q: %q is not a valid interval part", s, fields[i+1])
		}

		interval += quantity * int64(expr.TimePartMultiplier[part])
		i += 2
	}

	return interval, nil
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

type selectWithInto struct {
	sel  *expr.Select
	into expr.Node
}

type unionItem struct {
	typ expr.UnionType
	sel expr.Node
}

func buildUnion(n expr.Node, unions []unionItem) expr.Node {
	switch len(unions) {
	case 0:
		return n
	case 1:
		return &expr.Union{
			Type:  unions[0].typ,
			Left:  n,
			Right: unions[0].sel,
		}
	default:
		return &expr.Union{
			Type:  unions[0].typ,
			Left:  n,
			Right: buildUnion(unions[0].sel, unions[1:]),
		}
	}
}

func buildQuery(explain string, with []expr.CTE, selinto selectWithInto, unions []unionItem) (*expr.Query, error) {
	exp, err := parseExplain(explain)
	if err != nil {
		return nil, err
	}

	return &expr.Query{
		Explain: exp,
		With:    with,
		Into:    selinto.into,
		Body:    buildUnion(selinto.sel, unions),
	}, nil
}
