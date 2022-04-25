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

func timePart(id string) (expr.Timepart, bool) {
	var part expr.Timepart
	switch strings.ToUpper(id) {
	case "MICROSECOND":
		part = expr.Microsecond
	case "MILLISECOND":
		part = expr.Millisecond
	case "SECOND":
		part = expr.Second
	case "MINUTE":
		part = expr.Minute
	case "HOUR":
		part = expr.Hour
	case "DAY":
		part = expr.Day
	case "MONTH":
		part = expr.Month
	case "YEAR":
		part = expr.Year
	default:
		return 0, false
	}
	return part, true
}
