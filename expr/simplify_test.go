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
	"bufio"
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func casen(args ...Node) *Case {
	c := &Case{}
	for i := 0; i < len(args); i += 2 {
		if len(args)-i == 1 {
			c.Else = args[i]
			return c
		}
		c.Limbs = append(c.Limbs, CaseLimb{
			When: args[i],
			Then: args[i+1],
		})
	}
	return c
}

func ts(str string) Node {
	tm, ok := date.Parse([]byte(str))
	if !ok {
		panic("bad time: " + str)
	}
	return &Timestamp{Value: tm}
}

func coalesce(args ...Node) Node {
	return Coalesce(args)
}

func TestSimplify(t *testing.T) {
	testcases := []struct {
		before, after Node
	}{
		{
			// Or(x, x) -> x
			Or(Is(path("t", "x"), IsNull), Is(path("t", "x"), IsNull)),
			Is(path("t", "x"), IsNull),
		},
		{
			// And(x, x) -> x
			And(Is(path("t", "x"), IsNull), Is(path("t", "x"), IsNull)),
			Is(path("t", "x"), IsNull),
		},
		{
			// And(true, x) -> x
			And(Bool(true), Is(path("t", "x"), IsNull)),
			Is(path("t", "x"), IsNull),
		},
		{
			// x = x -> CASE x IS NOT NULL THEN TRUE ELSE MISSING
			Compare(Equals, path("x"), path("x")),
			casen(Is(path("x"), IsNotNull), Bool(true), Missing{}),
		},
		{
			// x <> x -> CASE x IS NOT NULL THEN FALSE ELSE MISSING
			Compare(NotEquals, path("x"), path("x")),
			casen(Is(path("x"), IsNotNull), Bool(false), Missing{}),
		},
		{
			// 3 = 4 -> false
			Compare(Equals, Integer(3), Integer(4)),
			Bool(false),
		},
		{
			// "foo" = 3 -> false
			Compare(Equals, String("foo"), Integer(3)),
			Bool(false),
		},
		{
			// FALSE AND t.x IS NULL -> FALSE
			And(Bool(false), Is(path("t", "x"), IsNull)),
			Bool(false),
		},
		{
			// TRUE OR t.x IS NULL -> TRUE
			Or(Bool(true), Is(path("t", "x"), IsNull)),
			Bool(true),
		},
		{
			// FALSE OR t.x IS NULL -> t.x IS NULL
			Or(Bool(false), Is(path("t", "x"), IsNull)),
			Is(path("t", "x"), IsNull),
		},
		{
			// 3 < 5 -> TRUE
			Compare(Less, Integer(3), Float(3.5)),
			Bool(true),
		},
		{
			// expr <> TRUE -> NOT expr
			Xor(Compare(Less, path("x"), path("y")), Bool(true)),
			Compare(GreaterEquals, path("x"), path("y")),
		},
		{
			Xnor(Bool(false), Compare(Greater, path("y"), path("x"))),
			Compare(LessEquals, path("y"), path("x")),
		},
		{
			// 1000/350 < 3.0
			Compare(Less, Div(Integer(1000), Integer(350)), Float(3.0)),
			Bool(true),
		},
		{
			Mul(Float(3.0), Float(3.0)),
			Float(9.0),
		},
		{
			Div(Float(3.0), Float(3.0)),
			Float(1.0),
		},
		{
			Call("ROUND", Float(3.1)),
			Float(3.0),
		},
		{
			Call("ROUND", Float(3.5)),
			Float(4.0),
		},
		{
			Call("ROUND", Float(3.9)),
			Float(4.0),
		},
		{
			Call("ROUND", Float(-3.1)),
			Float(-3.0),
		},
		{
			Call("ROUND", Float(-3.5)),
			Float(-3.0),
		},
		{
			Call("ROUND", Float(-3.9)),
			Float(-4.0),
		},
		{
			Call("ROUND_EVEN", Float(3.1)),
			Float(3.0),
		},
		{
			Call("ROUND_EVEN", Float(3.5)),
			Float(4.0),
		},
		{
			Call("ROUND_EVEN", Float(3.9)),
			Float(4.0),
		},
		{
			Call("ROUND_EVEN", Float(-3.1)),
			Float(-3.0),
		},
		{
			Call("ROUND_EVEN", Float(-3.5)),
			Float(-4.0),
		},
		{
			Call("ROUND_EVEN", Float(-3.9)),
			Float(-4.0),
		},
		{
			Call("TRUNC", Float(3.1)),
			Float(3.0),
		},
		{
			Call("TRUNC", Float(3.9)),
			Float(3.0),
		},
		{
			Call("TRUNC", Float(-3.1)),
			Float(-3.0),
		},
		{
			Call("TRUNC", Float(-3.9)),
			Float(-3.0),
		},
		{
			Call("FLOOR", Float(3.1)),
			Float(3.0),
		},
		{
			Call("FLOOR", Float(3.9)),
			Float(3.0),
		},
		{
			Call("FLOOR", Float(-3.1)),
			Float(-4.0),
		},
		{
			Call("FLOOR", Float(-3.9)),
			Float(-4.0),
		},
		{
			Call("CEIL", Float(3.1)),
			Float(4.0),
		},
		{
			Call("CEIL", Float(3.9)),
			Float(4.0),
		},
		{
			Call("CEIL", Float(-3.1)),
			Float(-3.0),
		},
		{
			Call("CEIL", Float(-3.9)),
			Float(-3.0),
		},
		{
			// canonicalization:
			//   3 < x -> x > 3
			Compare(Less, Integer(3), path("x")),
			Compare(Greater, path("x"), Integer(3)),
		},
		{
			// "foo%bar" LIKE z.name -> z.name LIKE "foo%bar"
			// XXX: is the above even valid SQL?
			Compare(Like, String("foo%bar"), path("z.name")),
			Compare(Like, path("z.name"), String("foo%bar")),
		},
		//#region Case-insensitive contains
		{
			// CONTAINS(UPPER(z.name), "FRED") -> CONTAINS_CI(z.name, "FRED")
			Call("CONTAINS", Call("UPPER", path("z.name")), String("FRED")),
			Call("CONTAINS_CI", path("z.name"), String("FRED")),
		},
		{
			// CONTAINS(UPPER(z.name), "fred") -> FALSE
			Call("CONTAINS", Call("UPPER", path("z.name")), String("fred")),
			Bool(false),
		},
		{
			// CONTAINS(LOWER(z.name), "fred") -> CONTAINS_CI(z.name, "fred")
			Call("CONTAINS", Call("LOWER", path("z.name")), String("fred")),
			Call("CONTAINS_CI", path("z.name"), String("fred")),
		},
		{
			// CONTAINS(LOWER(z.name), "FRED") -> FALSE
			Call("CONTAINS", Call("LOWER", path("z.name")), String("FRED")),
			Bool(false),
		},
		{
			// UPPER(z.name) LIKE "%FRED%" -> CONTAINS_CI(z.name, "FRED")
			Compare(Like, Call("UPPER", path("z.name")), String("%FRED%")),
			Call("CONTAINS_CI", path("z.name"), String("FRED")),
		},
		{
			// UPPER(z.name) LIKE "%fred%" -> FALSE
			Compare(Like, Call("UPPER", path("z.name")), String("%fred%")),
			Bool(false),
		},
		{
			// LOWER(z.name) LIKE "%fred%" -> CONTAINS_CI(z.name, "fred")
			Compare(Like, Call("LOWER", path("z.name")), String("%fred%")),
			Call("CONTAINS_CI", path("z.name"), String("fred")),
		},
		{
			// LOWER(z.name) LIKE "%FRED%" -> FALSE
			Compare(Like, Call("LOWER", path("z.name")), String("%FRED%")),
			Bool(false),
		},
		//#endregion Case-insensitive contains
		{ // LTRIM(LTRIM(x)) -> LTRIM(x)
			Call("LTRIM", Call("LTRIM", path("z.name"))),
			Call("LTRIM", path("z.name")),
		},
		{ // LTRIM(RTRIM(x)) -> TRIM(x)
			Call("LTRIM", Call("RTRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // LTRIM(TRIM(x)) -> TRIM(x)
			Call("LTRIM", Call("TRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // RTRIM(LTRIM(x)) -> TRIM(x)
			Call("RTRIM", Call("LTRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // RTRIM(RTRIM(x)) -> RTRIM(x)
			Call("RTRIM", Call("RTRIM", path("z.name"))),
			Call("RTRIM", path("z.name")),
		},
		{ // RTRIM(RTRIM(x)) -> TRIM(x)
			Call("RTRIM", Call("TRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // TRIM(LTRIM(x)) -> TRIM(x)
			Call("RTRIM", Call("LTRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // TRIM(RTRIM(x)) -> TRIM(x)
			Call("RTRIM", Call("LTRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{ // TRIM(TRIM(x)) -> TRIM(x)
			Call("TRIM", Call("TRIM", path("z.name"))),
			Call("TRIM", path("z.name")),
		},
		{
			// canonicalization:
			//   (3 + 3.5) < x -> x > 6.5
			Compare(Less, Add(Integer(3), Float(3.5)), path("x", "y", "z")),
			Compare(Greater, path("x", "y", "z"), (*Rational)(big.NewRat(13, 2))),
		},
		{
			// x < 3 AND 3 > x -> x < 3
			And(Compare(Less, path("x"), Integer(3)), Compare(Greater, Integer(3), path("x"))),
			Compare(Less, path("x"), Integer(3)),
		},
		{
			// !(TRUE) -> FALSE
			&Not{Expr: Bool(true)},
			Bool(false),
		},
		{
			// !(x IS MISSING) -> x IS NOT MISSING
			&Not{Expr: Is(path("x"), IsNotMissing)},
			Is(path("x"), IsMissing),
		},
		{
			// TRUE IS NULL -> FALSE
			Is(Bool(true), IsNull),
			Bool(false),
		},
		{
			// "xyz" IS MISSING -> FALSE
			Is(String("xyz"), IsMissing),
			Bool(false),
		},
		{
			Is(String("syz"), IsNotMissing),
			Bool(true),
		},
		{
			Is(String("xyz"), IsNotFalse),
			Bool(true),
		},
		{
			Is(String("xyz"), IsTrue),
			Bool(false),
		},
		{
			// 1+2 IS NOT NULL -> TRUE
			Is(Add(Integer(1), Integer(2)), IsNotNull),
			Bool(true),
		},
		{
			Is(Missing{}, IsMissing),
			Bool(true),
		},
		{
			Is(Missing{}, IsNotMissing),
			Bool(false),
		},
		{
			Is(Missing{}, IsNotFalse),
			Bool(true),
		},
		{
			Is(Missing{}, IsNotTrue),
			Bool(true),
		},
		{
			Is(Bool(true), IsTrue),
			Bool(true),
		},
		{
			Is(Bool(false), IsTrue),
			Bool(false),
		},
		{
			Is(Bool(true), IsNotFalse),
			Bool(true),
		},
		{
			Is(Bool(false), IsNotTrue),
			Bool(true),
		},
		{
			// (x + y) IS FALSE -> FALSE
			Is(Add(path("x"), path("y")), IsFalse),
			Bool(false),
		},
		{
			// (x + y) IS NOT FALSE -> TRUE
			Is(Add(path("x"), path("y")), IsNotFalse),
			Bool(true),
		},
		{
			// NOT(x = y) IS MISSING -> x <> y IS MISSING
			Is(&Not{(Compare(Equals, path("x"), path("y")))}, IsMissing),
			Is(Compare(NotEquals, path("x"), path("y")), IsMissing),
		},
		{
			// in general, this cannot be optimized to FALSE, but see below
			And(Missing{}, path("x")),
			And(Missing{}, path("x")),
		},
		{
			// IS TRUE should trigger some additional logical optimizations
			// (see the test case above)
			Is(And(Missing{}, path("x")), IsTrue),
			Bool(false),
		},
		{
			// CASE WHEN TRUE THEN x ELSE y END -> x
			casen(Bool(true), path("x"), path("y")),
			path("x"),
		},
		{
			// CASE WHEN FALSE THEN x ELSE y END -> y
			casen(Bool(false), path("x"), path("y")),
			path("y"),
		},
		{
			// eliminate one FALSE limb and preserve the remaining order
			casen(Compare(Less, path("x"), path("y")), Integer(3), Bool(false), path("x"), String("foo")),
			casen(Compare(Less, path("x"), path("y")), Integer(3), String("foo")),
		},
		{
			// eliminate everything after WHEN TRUE THEN ...
			casen(Compare(Less, path("x"), path("y")), Integer(3), Bool(true), Integer(4), Integer(5)),
			casen(Compare(Less, path("x"), path("y")), Integer(3), Integer(4)),
		},
		{
			// immediates are never NULL, so a coalesce
			// with a constant in it should yield the
			// first constant
			coalesce(Integer(3), path("x")),
			Integer(3),
		},
		{
			coalesce(path("x"), String("foo")),
			casen(Is(path("x"), IsNotNull), path("x"), String("foo")),
		},
		{
			// since 1+"foo" is MISSING
			// and 1+MISSING is MISSING,
			// the expression 1+x is exactly equivalent
			// to the input expression
			Add(Integer(1), coalesce(path("x"), String("foo"))),
			Add(path("x"), Integer(1)),
		},
		{
			// similar to above
			Mul(coalesce(path("x"), Bool(false)), coalesce(path("y"), String("x"))),
			Mul(path("x"), path("y")),
		},
		{
			// when mixing CASE with IS,
			// the IS comparison should be pushed into
			// the CASE expression
			Is(coalesce(path("x"), path("y")), IsNull),
			// FIXME: this can be simplified to
			// (x IS NOT NULL OR y IS NOT NULL)
			casen(Is(path("x"), IsNotNull), Is(path("x"), IsNull), Is(path("y"), IsNotNull), Is(path("y"), IsNull), Bool(true)),
		},
		{
			// COALESCE(x, 1) -> CASE x IS NOT NULL THEN x ELSE 1
			Mul(coalesce(path("x"), Integer(1)), Integer(2)),
			Mul(casen(Is(path("x"), IsNotNull), path("x"), Integer(1)), Integer(2)),
		},
		{
			&Cast{From: Integer(3), To: IntegerType},
			Integer(3),
		},
		{
			&Cast{From: Float(3.5), To: FloatType},
			Float(3.5),
		},
		{
			&Cast{From: Float(3.7), To: IntegerType},
			Integer(3),
		},
		{
			&Cast{From: Integer(3), To: FloatType},
			Float(3.0),
		},
		{
			// expressions inside CAST should discard
			// any portions of the calculation that
			// are not convertible
			&Cast{From: coalesce(path("x"), String("foo")), To: IntegerType},
			&Cast{From: path("x"), To: IntegerType},
		},
		// test that <= and >= with constant
		// timestamps get turned into the correct
		// BEFORE() function application
		// (the target times should be adjusted up
		// or down one nanosecond)
		{
			Compare(LessEquals, path("x"), ts("2009-01-14T23:59:59Z")),
			Call("BEFORE", path("x"), ts("2009-01-14T23:59:59.000001Z")),
		},
		{
			Compare(Less, path("x"), ts("2009-01-14T23:59:59Z")),
			Call("BEFORE", path("x"), ts("2009-01-14T23:59:59Z")),
		},
		{
			Compare(GreaterEquals, path("x"), ts("2009-01-14T23:59:59Z")),
			Call("BEFORE", ts("2009-01-14T23:59:58.999999Z"), path("x")),
		},
		{
			Compare(Greater, path("x"), ts("2009-01-14T23:59:59Z")),
			Call("BEFORE", ts("2009-01-14T23:59:59Z"), path("x")),
		},
		{
			Compare(LessEquals, ts("2009-01-14T23:59:59Z"), path("x")),
			Call("BEFORE", ts("2009-01-14T23:59:58.999999Z"), path("x")),
		},
		{
			Compare(Less, ts("2009-01-14T23:59:59Z"), path("x")),
			Call("BEFORE", ts("2009-01-14T23:59:59Z"), path("x")),
		},
		{
			Compare(GreaterEquals, ts("2009-01-14T23:59:59Z"), path("x")),
			Call("BEFORE", path("x"), ts("2009-01-14T23:59:59.000001Z")),
		},
		{
			Compare(Greater, ts("2009-01-14T23:59:59Z"), path("x")),
			Call("BEFORE", path("x"), ts("2009-01-14T23:59:59Z")),
		},
		{
			DateExtract(Year, ts("2009-01-14T23:59:59Z")),
			Integer(2009),
		},
		{
			DateTrunc(Month, ts("2009-01-14T23:59:59Z")),
			ts("2009-01-01T00:00:00Z"),
		},
		{
			DateTrunc(Minute, ts("2009-01-14T23:59:59Z")),
			ts("2009-01-14T23:59:00Z"),
		},
		{
			In(String("foo"), Float(3.5), String("bar"), String("foo"), Bool(false)),
			Bool(true),
		},
		{
			// x||"suffix" IN (...)
			// could only possibly match string-typed constants
			In(CallOp(Concat, path("x"), String("suffix")), String("start-suffix"), Integer(3), String("second-suffix"), Bool(false)),
			In(CallOp(Concat, path("x"), String("suffix")), String("start-suffix"), String("second-suffix")),
		},
		{
			// when the list of possible comparisons shrinks to 1,
			// this should revert to a regular equals
			In(CallOp(Concat, path("x"), String("suffix")), String("start-suffix"), Integer(3), Bool(false)),
			Compare(Equals, CallOp(Concat, path("x"), String("suffix")), String("start-suffix")),
		},
		{
			// SIZE(path) is unchanged
			CallOp(ObjectSize, path("x")),
			CallOp(ObjectSize, path("x")),
		},
		{
			// SIZE(missing) => missing
			CallOp(ObjectSize, Missing{}),
			Missing{},
		},
		{
			// SIZE({foo:1, bar:42, baz:123}) => 3
			CallOp(ObjectSize, &Struct{Fields: []Field{
				Field{"foo", Integer(1)},
				Field{"bar", Integer(42)},
				Field{"baz", Integer(123)},
			}}),
			Integer(3),
		},
		{
			// SIZE(["a", "b", "c", "d") => 4
			CallOp(ObjectSize, &List{Values: []Constant{
				String("a"), String("b"), String("c"), String("d"),
			}}),
			Integer(4),
		},
		{
			// x+0 is *not* the same as x unless
			// x can be proven to always be a number,
			// (consider x = 'foo')
			Add(path("x"), Integer(0)),
			Add(path("x"), Integer(0)),
		},
		{
			// ('x' + 1) IS MISSING -> TRUE
			Is(Add(String("x"), Integer(1)), IsMissing),
			Bool(true),
		},
		{
			// (1 + 'x') IS MISSING -> TRUE
			Is(Add(Integer(1), String("x")), IsMissing),
			Bool(true),
		},
		{
			// ('x' + 1) IS NOT MISSING -> FALSE
			Is(Add(String("x"), Integer(1)), IsNotMissing),
			Bool(false),
		},
		{
			CallOp(Concat, String("xyz"), String("abc")),
			String("xyzabc"),
		},
		{
			// CASE WHEN x = 0 THEN 'is_zero' WHEN 'foo' = x THEN 0 END
			// -> HASH_LOOKUP(x, 0, 'is_zero', 'foo', 0)
			casen(Compare(Equals, path("x"), Integer(0)), String("is_zero"),
				Compare(Equals, String("foo"), path("x")), Integer(0)),
			Call("HASH_LOOKUP", path("x"), Integer(0), String("is_zero"), String("foo"), Integer(0)),
		},
		{
			Count(casen(Is(path("x"), IsNotMissing), Null{}, Missing{})),
			Count(casen(Is(path("x"), IsNotMissing), Null{}, Missing{})),
		},
		{
			// COUNT(...) FILTER (WHERE false) => 0
			&Aggregate{Op: OpCount, Inner: Star{}, Filter: Bool(false)},
			Integer(0),
		},
		{
			// COUNT(DISTINCT ...) FILTER (WHERE false) => NULL
			&Aggregate{Op: OpCountDistinct, Inner: Star{}, Filter: Bool(false)},
			Integer(0),
		},
		{
			// aggregate(...) FILTER (WHERE false) => NULL
			&Aggregate{Op: OpSum, Inner: Star{}, Filter: Bool(false)},
			Null{},
		},
		{
			// aggregate(...) FILTER (WHERE true) => aggregate(...)
			&Aggregate{Op: OpCount, Inner: Star{}, Filter: Bool(true)},
			&Aggregate{Op: OpCount, Inner: Star{}},
		},
		{
			// COUNT(...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpCount, Inner: Star{}, Filter: Null{}},
			Integer(0),
		},
		{
			// COUNT(DISTINCT ...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpCountDistinct, Inner: Star{}, Filter: Null{}},
			Integer(0),
		},
		{
			// aggregate(...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpSum, Inner: Star{}, Filter: Null{}},
			Null{},
		},
		{
			// COUNT(...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpCount, Inner: Star{}, Filter: Missing{}},
			Integer(0),
		},
		{
			// COUNT(DISTINCT ...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpCountDistinct, Inner: Star{}, Filter: Missing{}},
			Integer(0),
		},
		{
			// aggregate(...) FILTER (WHERE null) => NULL
			&Aggregate{Op: OpSum, Inner: Star{}, Filter: Missing{}},
			Null{},
		},
		{
			DateAdd(Microsecond, Integer(-1), ts("2017-01-02T03:04:05.000001Z")),
			ts("2017-01-02T03:04:05Z"),
		},
		{
			DateAdd(Millisecond, Integer(-1), ts("2017-01-02T03:04:05Z")),
			ts("2017-01-02T03:04:04.999Z"),
		},
		{
			DateAdd(Second, Integer(-1), ts("2017-01-02T03:04:00.999Z")),
			ts("2017-01-02T03:03:59.999Z"),
		},
		// the following are from here:
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-date.html
		{
			DateAdd(Year, Integer(5), ts("2010-01-01T00:00:00Z")),
			ts("2015-01-01T00:00:00Z"),
		},
		{

			DateAdd(Month, Integer(1), ts("2010-01-01T00:00:00Z")),
			ts("2010-02-01T00:00:00Z"),
		},
		{
			DateAdd(Day, Integer(-1), ts("2017-01-10T00:00:00Z")),
			ts("2017-01-09T00:00:00Z"),
		},
		{
			DateAdd(Hour, Integer(1), ts("2017-01-01T00:00:00Z")),
			ts("2017-01-01T01:00:00Z"),
		},
		{
			DateAdd(Hour, Integer(1), ts("2017-01-02T03:04:05Z")),
			ts("2017-01-02T04:04:05Z"),
		},
		{
			DateAdd(Minute, Integer(1), ts("2017-01-02T03:04:05.006Z")),
			ts("2017-01-02T03:05:05.006Z"),
		},
		{
			CallOp(Upper, String("sneller")),
			String("SNELLER"),
		},
		{
			CallOp(Lower, String("SNELLER")),
			String("sneller"),
		},
		{
			// LOWER(s) == "fred"
			Compare(Equals, CallOp(Lower, path("s")), String("fred")),
			CallOp(EqualsCI, path("s"), String("fred")),
		},
		{
			// "fred" == LOWER(s)
			Compare(Equals, String("fred"), CallOp(Lower, path("s"))),
			CallOp(EqualsCI, path("s"), String("fred")),
		},
		{
			// LOWER(s) != "fred"
			Compare(NotEquals, CallOp(Lower, path("s")), String("fred")),
			&Not{Expr: CallOp(EqualsCI, path("s"), String("fred"))},
		},
		{
			// "fred" != LOWER(s)
			Compare(NotEquals, String("fred"), CallOp(Lower, path("s"))),
			&Not{Expr: CallOp(EqualsCI, path("s"), String("fred"))},
		},
		{
			// LOWER(s) == "FRED"
			Compare(Equals, CallOp(Lower, path("s")), String("FRED")),
			Bool(false),
		},
		{
			// "FRED" == LOWER(s)
			Compare(Equals, String("FRED"), CallOp(Lower, path("s"))),
			Bool(false),
		},
		{
			// LOWER(s) != "FRED"
			Compare(NotEquals, CallOp(Lower, path("s")), String("FRED")),
			Bool(true),
		},
		{
			// "FRED" != LOWER(s)
			Compare(NotEquals, String("FRED"), CallOp(Lower, path("s"))),
			Bool(true),
		},
		{
			// UPPER(s) == "FRED"
			Compare(Equals, CallOp(Upper, path("s")), String("FRED")),
			CallOp(EqualsCI, path("s"), String("FRED")),
		},
		{
			// "FRED" == UPPER(s)
			Compare(Equals, String("FRED"), CallOp(Upper, path("s"))),
			CallOp(EqualsCI, path("s"), String("FRED")),
		},
		{
			// UPPER(s) != "FRED"
			Compare(NotEquals, CallOp(Upper, path("s")), String("FRED")),
			&Not{Expr: CallOp(EqualsCI, path("s"), String("FRED"))},
		},
		{
			// "FRED" != UPPER(s)
			Compare(NotEquals, String("FRED"), CallOp(Upper, path("s"))),
			&Not{Expr: CallOp(EqualsCI, path("s"), String("FRED"))},
		},
		{
			// UPPER(s) == "fred"
			Compare(Equals, CallOp(Upper, path("s")), String("fred")),
			Bool(false),
		},
		{
			// "fred" == UPPER(s)
			Compare(Equals, String("fred"), CallOp(Upper, path("s"))),
			Bool(false),
		},
		{
			// UPPER(s) != "fred"
			Compare(NotEquals, CallOp(Upper, path("s")), String("fred")),
			Bool(true),
		},
		{
			// "fred" != UPPER(s)
			Compare(NotEquals, String("fred"), CallOp(Upper, path("s"))),
			Bool(true),
		},
	}

	for i := range testcases {
		tc := testcases[i]

		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			before := tc.before
			after := tc.after
			opt := Simplify(before, HintFn(NoHint))
			if !opt.Equals(after) {
				t.Errorf("\noriginal   %q\nsimplified %q\nwanted     %q", ToString(before), ToString(opt), ToString(after))
			}
			testEquivalence(before, t)
			testEquivalence(after, t)
		})
	}
}

func testEquivalence(e Node, t *testing.T) {
	t.Helper()

	var buf ion.Buffer
	var st ion.Symtab
	e.Encode(&buf, &st)

	res, _, err := Decode(&st, buf.Bytes())
	if err != nil {
		t.Helper()
		t.Errorf("node in: %s - error %s", e, err)
		return
	}
	if !Equivalent(res, e) {
		t.Logf("json: %s", jsontxt(&st, &buf))
		t.Errorf("input : %s", e)
		t.Errorf("output: %s", res)
	}
}

func jsontxt(st *ion.Symtab, buf *ion.Buffer) string {
	var both ion.Buffer
	st.Marshal(&both, true)
	both.UnsafeAppend(buf.Bytes())
	var out strings.Builder
	ion.ToJSON(&out, bufio.NewReader(bytes.NewReader(both.Bytes())))
	return out.String()
}
