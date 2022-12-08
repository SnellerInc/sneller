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

func TestCheckExpressions(t *testing.T) {
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
			expr: Call(Contains, path("x")),
			kind: &SyntaxError{},
		},
		{
			expr: Call(Contains, Integer(3), String("xyz")),
			kind: &TypeError{},
		},
		{
			expr: Compare(Equals, Call(DateExtractYear, path("x")), String("y")),
			kind: &TypeError{},
		},
		{
			Call(ObjectSize, String("foo"), Integer(5)),
			&SyntaxError{},
			"SIZE expects one argument, but found 2",
		},
		{
			Call(ObjectSize),
			&SyntaxError{},
			"SIZE expects one argument, but found 0",
		},
		{
			Call(ObjectSize, Null{}),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, String("foo")),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, Integer(1)),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, Float(1.5)),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, &Rational{}),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, Bool(true)),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, &Timestamp{}),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, Star{}),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, Call(Substring, String("test"), Integer(2))),
			&TypeError{},
			"SIZE expects",
		},
		{
			Call(ObjectSize, And(path("enabled"), path("active"))),
			&TypeError{},
			"SIZE expects",
		},
		{
			&Index{Inner: Ident("z"), Offset: -1},
			&TypeError{},
			"negative",
		},
		{
			&Index{Inner: &List{Values: []Constant{Null{}, Null{}}}, Offset: 3},
			&TypeError{},
			"index",
		},
		{
			&Index{Inner: Call(MakeList, Null{}, Null{}), Offset: 3},
			&TypeError{},
			"index",
		},
		{
			// SELECT ASSERT_ION_TYPE()
			Call(AssertIonType),
			&SyntaxError{},
			"requires at least 2 arguments",
		},
		{
			// SELECT ASSERT_ION_TYPE(x, 'test')
			Call(AssertIonType, path("x"), String("test")),
			&SyntaxError{},
			"argument 1 has to be an integer",
		},
		{
			// SELECT ASSERT_ION_TYPE(x, 0x0a, 512),
			Call(AssertIonType, path("x"), Integer(0x0a), Integer(512)),
			nil,
			"value 512 is not a supported Ion type",
		},
	}
	for i := range testcases {
		err := Check(testcases[i].expr)
		if err == nil {
			t.Errorf("testcase %d (%s): no error", i, ToString(testcases[i].expr))
			continue
		}
		into := (any)(testcases[i].kind)
		if !errors.As(err, &into) {
			t.Errorf("testcase %d (%s): error %T not a type error", i, testcases[i].expr, err)
			continue
		}
		if testcases[i].kind != nil {
			err1 := innermostError(err)
			err2 := innermostError(testcases[i].kind)
			if reflect.TypeOf(err1) != reflect.TypeOf(err2) {
				t.Errorf("testcase %d (%s): error %T is not %T", i, testcases[i].expr, err1, err2)
				continue
			}
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
