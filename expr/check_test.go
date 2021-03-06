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

package expr

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	testcases := []struct {
		expr Node
		kind error
		msg  string
	}{
		{
			// !(3)
			expr: &Not{Integer(3)},
			kind: &TypeError{},
		},
		{
			// TRUE AND "xyz"
			expr: And(Bool(true), String("xyz")),
			kind: &TypeError{},
		},
		{
			// x.y + FALSE
			expr: Add(path("x", "y"), Bool(false)),
			kind: &TypeError{},
		},
		{
			// x.y / 0
			expr: Div(path("x", "y"), Integer(0)),
			kind: &TypeError{},
		},
		{
			// x LIKE TRUE
			expr: Compare(Like, path("x"), Bool(true)),
			kind: &SyntaxError{},
		},
		{
			// case with non-boolean arms
			expr: casen(Integer(3), path("x"), path("y")),
			kind: &TypeError{},
		},
		{
			expr: &Cast{From: path("x"), To: DecimalType},
			kind: &SyntaxError{},
		},
		{
			expr: &Cast{From: path("y"), To: SymbolType},
			kind: &SyntaxError{},
		},
		{
			expr: CallOp(Contains, path("x")),
			kind: &SyntaxError{},
		},
		{
			expr: CallOp(Contains, Integer(3), String("xyz")),
			kind: &TypeError{},
		},
		{
			expr: Compare(Equals, CallOp(DateExtractYear, path("x")), String("y")),
			kind: &TypeError{},
		},
		{
			CallOp(ObjectSize, String("foo"), Integer(5)),
			&SyntaxError{},
			"SIZE expects one argument, but found 2",
		},
		{
			CallOp(ObjectSize),
			&SyntaxError{},
			"SIZE expects one argument, but found 0",
		},
		{
			CallOp(ObjectSize, Null{}),
			&TypeError{},
			"SIZE is undefined for values of type null",
		},
		{
			CallOp(ObjectSize, String("foo")),
			&TypeError{},
			"SIZE is undefined for values of type string",
		},
		{
			CallOp(ObjectSize, Integer(1)),
			&TypeError{},
			"SIZE is undefined for values of type integer",
		},
		{
			CallOp(ObjectSize, Float(1.5)),
			&TypeError{},
			"SIZE is undefined for values of type float",
		},
		{
			CallOp(ObjectSize, &Rational{}),
			&TypeError{},
			"SIZE is undefined for values of type rational",
		},
		{
			CallOp(ObjectSize, Bool(true)),
			&TypeError{},
			"SIZE is undefined for values of type bool",
		},
		{
			CallOp(ObjectSize, &Timestamp{}),
			&TypeError{},
			"SIZE is undefined for values of type timestamp",
		},
		{
			CallOp(ObjectSize, Star{}),
			&TypeError{},
			"SIZE is undefined for values of type expr.Star",
		},
		{
			CallOp(ObjectSize, CallOp(SubString, String("test"), Integer(2))),
			&TypeError{},
			"SIZE is undefined for values of type *expr.Builtin",
		},
		{
			CallOp(ObjectSize, And(path("enabled"), path("active"))),
			&TypeError{},
			"SIZE is undefined for values of type *expr.Logical",
		},
		{
			&Path{First: "z", Rest: &LiteralIndex{Field: -1}},
			&TypeError{},
			"illegal index",
		},
	}
	for i := range testcases {
		err := Check(testcases[i].expr)
		if err == nil {
			t.Errorf("testcase %d (%s): no error", i, ToString(testcases[i].expr))
			continue
		}
		into := testcases[i].kind
		if !errors.As(err, &into) {
			t.Errorf("testcase %d (%s): error %T not a type error", i, testcases[i].expr, err)
			continue
		}
		err1 := innermostError(err)
		err2 := innermostError(testcases[i].kind)
		if reflect.TypeOf(err1) != reflect.TypeOf(err2) {
			t.Errorf("testcase %d (%s): error %T is not %T", i, testcases[i].expr, err1, err2)
			continue
		}
		if len(testcases[i].msg) > 0 {
			msg := fmt.Sprintf("%s", err)
			if !strings.Contains(msg, testcases[i].msg) {
				t.Errorf("testcase %d (%s): '%s' is not present in error message '%s'", i, testcases[i].expr, testcases[i].msg, msg)
				continue
			}
		}
		testEquivalence(testcases[i].expr, t)
	}
}

func innermostError(err error) error {
	var result error
	for {
		result = err
		err = errors.Unwrap(err)
		if err == nil {
			return result
		}
	}
}
