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

func TestCheckValidExpressions(t *testing.T) {
	testcases := []struct {
		expr Node
	}{
		{
			// regression test: nullptr dereference on NaN
			expr: Div(path("x"), NaN),
		},
	}
	for i := range testcases {
		tc := &testcases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			err := Check(tc.expr)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
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
