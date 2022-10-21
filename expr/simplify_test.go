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
	"math"
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

// apply op onto a list of arguments in a left-associative manner,
// i.e. apply2 op x y z produces (op (op x y) z)
func apply2(op BuiltinOp, left, right Node, rest ...Node) *Builtin {
	if len(rest) == 0 {
		return Call(op, left, right)
	}
	return apply2(op, Call(op, left, right), rest[0], rest[1:]...)
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
			Call(Round, Float(3.1)),
			Float(3.0),
		},
		{
			Call(Round, Float(3.5)),
			Float(4.0),
		},
		{
			Call(Round, Float(3.9)),
			Float(4.0),
		},
		{
			Call(Round, Float(-3.1)),
			Float(-3.0),
		},
		{
			Call(Round, Float(-3.5)),
			Float(-3.0),
		},
		{
			Call(Round, Float(-3.9)),
			Float(-4.0),
		},
		{
			Call(RoundEven, Float(3.1)),
			Float(3.0),
		},
		{
			Call(RoundEven, Float(3.5)),
			Float(4.0),
		},
		{
			Call(RoundEven, Float(3.9)),
			Float(4.0),
		},
		{
			Call(RoundEven, Float(-3.1)),
			Float(-3.0),
		},
		{
			Call(RoundEven, Float(-3.5)),
			Float(-4.0),
		},
		{
			Call(RoundEven, Float(-3.9)),
			Float(-4.0),
		},
		{
			Call(Trunc, Float(3.1)),
			Float(3.0),
		},
		{
			Call(Trunc, Float(3.9)),
			Float(3.0),
		},
		{
			Call(Trunc, Float(-3.1)),
			Float(-3.0),
		},
		{
			Call(Trunc, Float(-3.9)),
			Float(-3.0),
		},
		{
			Call(Floor, Float(3.1)),
			Float(3.0),
		},
		{
			Call(Floor, Float(3.9)),
			Float(3.0),
		},
		{
			Call(Floor, Float(-3.1)),
			Float(-4.0),
		},
		{
			Call(Floor, Float(-3.9)),
			Float(-4.0),
		},
		{
			Call(Ceil, Float(3.1)),
			Float(4.0),
		},
		{
			Call(Ceil, Float(3.9)),
			Float(4.0),
		},
		{
			Call(Ceil, Float(-3.1)),
			Float(-3.0),
		},
		{
			Call(Ceil, Float(-3.9)),
			Float(-3.0),
		},
		{
			// canonicalization:
			//   3 < x -> x > 3
			Compare(Less, Integer(3), path("x")),
			Compare(Greater, path("x"), Integer(3)),
		},
		{
			// "foo%bar" LIKE z.name cannot be canonicalize
			//
			// The semantic of `z.name LIKE "foo%bar"` would be different
			Compare(Like, String("foo%bar"), path("z.name")),
			Compare(Like, String("foo%bar"), path("z.name")),
		},
		//#region Case-insensitive contains
		{
			// CONTAINS(UPPER(z.name), "FRED") -> CONTAINS_CI(z.name, "FRED")
			Call(Contains, Call(Upper, path("z.name")), String("FRED")),
			Call(ContainsCI, path("z.name"), String("FRED")),
		},
		{
			// CONTAINS(UPPER(z.name), "fred") -> FALSE
			Call(Contains, Call(Upper, path("z.name")), String("fred")),
			Bool(false),
		},
		{
			// CONTAINS(LOWER(z.name), "fred") -> CONTAINS_CI(z.name, "fred")
			Call(Contains, Call(Lower, path("z.name")), String("fred")),
			Call(ContainsCI, path("z.name"), String("fred")),
		},
		{
			// CONTAINS(LOWER(z.name), "FRED") -> FALSE
			Call(Contains, Call(Lower, path("z.name")), String("FRED")),
			Bool(false),
		},
		{
			// UPPER(z.name) LIKE "%FRED%" -> CONTAINS_CI(z.name, "FRED")
			Compare(Like, Call(Upper, path("z.name")), String("%FRED%")),
			Call(ContainsCI, path("z.name"), String("FRED")),
		},
		{
			// UPPER(z.name) LIKE "%fred%" -> FALSE
			Compare(Like, Call(Upper, path("z.name")), String("%fred%")),
			Bool(false),
		},
		{
			// LOWER(z.name) LIKE "%fred%" -> CONTAINS_CI(z.name, "fred")
			Compare(Like, Call(Lower, path("z.name")), String("%fred%")),
			Call(ContainsCI, path("z.name"), String("fred")),
		},
		{
			// LOWER(z.name) LIKE "%FRED%" -> FALSE
			Compare(Like, Call(Lower, path("z.name")), String("%FRED%")),
			Bool(false),
		},
		//#endregion Case-insensitive contains
		{ // LTRIM(LTRIM(x)) -> LTRIM(x)
			Call(Ltrim, Call(Ltrim, path("z.name"))),
			Call(Ltrim, path("z.name")),
		},
		{ // LTRIM(RTRIM(x)) -> TRIM(x)
			Call(Ltrim, Call(Rtrim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // LTRIM(TRIM(x)) -> TRIM(x)
			Call(Ltrim, Call(Trim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // RTRIM(LTRIM(x)) -> TRIM(x)
			Call(Rtrim, Call(Ltrim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // RTRIM(RTRIM(x)) -> RTRIM(x)
			Call(Rtrim, Call(Rtrim, path("z.name"))),
			Call(Rtrim, path("z.name")),
		},
		{ // RTRIM(RTRIM(x)) -> TRIM(x)
			Call(Rtrim, Call(Trim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // TRIM(LTRIM(x)) -> TRIM(x)
			Call(Rtrim, Call(Ltrim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // TRIM(RTRIM(x)) -> TRIM(x)
			Call(Rtrim, Call(Ltrim, path("z.name"))),
			Call(Trim, path("z.name")),
		},
		{ // TRIM(TRIM(x)) -> TRIM(x)
			Call(Trim, Call(Trim, path("z.name"))),
			Call(Trim, path("z.name")),
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
			In(Call(Concat, path("x"), String("suffix")), String("start-suffix"), Integer(3), String("second-suffix"), Bool(false)),
			In(Call(Concat, path("x"), String("suffix")), String("start-suffix"), String("second-suffix")),
		},
		{
			// when the list of possible comparisons shrinks to 1,
			// this should revert to a regular equals
			In(Call(Concat, path("x"), String("suffix")), String("start-suffix"), Integer(3), Bool(false)),
			Compare(Equals, Call(Concat, path("x"), String("suffix")), String("start-suffix")),
		},
		{
			// SIZE(path) is unchanged
			Call(ObjectSize, path("x")),
			Call(ObjectSize, path("x")),
		},
		{
			// SIZE(missing) => missing
			Call(ObjectSize, Missing{}),
			Missing{},
		},
		{
			// SIZE(null) => null
			Call(ObjectSize, Null{}),
			Null{},
		},
		{
			// SIZE({foo:1, bar:42, baz:123}) => 3
			Call(ObjectSize, &Struct{Fields: []Field{
				Field{"foo", Integer(1)},
				Field{"bar", Integer(42)},
				Field{"baz", Integer(123)},
			}}),
			Integer(3),
		},
		{
			// SIZE(["a", "b", "c", "d") => 4
			Call(ObjectSize, &List{Values: []Constant{
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
			Call(Concat, String("xyz"), String("abc")),
			String("xyzabc"),
		},
		{
			// CASE WHEN x = 0 THEN 'is_zero' WHEN 'foo' = x THEN 0 END
			// -> HASH_LOOKUP(x, 0, 'is_zero', 'foo', 0)
			casen(Compare(Equals, path("x"), Integer(0)), String("is_zero"),
				Compare(Equals, String("foo"), path("x")), Integer(0)),
			Call(HashLookup, path("x"), Integer(0), String("is_zero"), String("foo"), Integer(0)),
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
			Call(Upper, String("sneller")),
			String("SNELLER"),
		},
		{
			Call(Lower, String("SNELLER")),
			String("sneller"),
		},
		{
			// LOWER(s) == "fred"
			Compare(Equals, Call(Lower, path("s")), String("fred")),
			Call(EqualsCI, path("s"), String("fred")),
		},
		{
			// "fred" == LOWER(s)
			Compare(Equals, String("fred"), Call(Lower, path("s"))),
			Call(EqualsCI, path("s"), String("fred")),
		},
		{
			// UPPER(s) == "FRED"
			Compare(Equals, Call(Upper, path("s")), String("FRED")),
			Call(EqualsCI, path("s"), String("FRED")),
		},
		{
			// "FRED" == UPPER(s)
			Compare(Equals, String("FRED"), Call(Upper, path("s"))),
			Call(EqualsCI, path("s"), String("FRED")),
		},
		{
			// UPPER(s) != "FRED"
			Compare(NotEquals, Call(Upper, path("s")), String("FRED")),
			&Not{Expr: Call(EqualsCI, path("s"), String("FRED"))},
		},
		{
			// "FRED" != UPPER(s)
			Compare(NotEquals, String("FRED"), Call(Upper, path("s"))),
			&Not{Expr: Call(EqualsCI, path("s"), String("FRED"))},
		},
		{
			// LOWER(x) || ' and ' || LOWER(y) => LOWER(x || ' and ' || y)
			apply2(Concat, Call(Lower, path("x")), String(" and "), Call(Lower, path("y"))),
			Call(Lower, apply2(Concat, path("x"), String(" and "), path("y"))),
		},
		{
			// LOWER(x) || ' AND ' || LOWER(y) => no change
			apply2(Concat, Call(Lower, path("x")), String(" AND "), Call(Lower, path("y"))),
			apply2(Concat, Call(Lower, path("x")), String(" AND "), Call(Lower, path("y"))),
		},
		{
			// 'x=' || LOWER(x) || ', y=' || LOWER(y) => LOWER('x=' || x || ', y=' || y)
			apply2(Concat, String("x="), Call(Lower, path("x")), String(", y="), Call(Lower, path("y"))),
			Call(Lower, apply2(Concat, String("x="), path("x"), String(", y="), path("y"))),
		},
		{
			// 'X=' || LOWER(x) || ', Y=' || LOWER(y) => no change
			apply2(Concat, String("X="), Call(Lower, path("x")), String(", Y="), Call(Lower, path("y"))),
			apply2(Concat, String("X="), Call(Lower, path("x")), String(", Y="), Call(Lower, path("y"))),
		},
		{
			// UPPER(x) || ' AND ' || UPPER(y) => UPPER(x || ' AND ' || y)
			apply2(Concat, Call(Upper, path("x")), String(" AND "), Call(Upper, path("y"))),
			Call(Upper, apply2(Concat, path("x"), String(" AND "), path("y"))),
		},
		{
			// UPPER(x) || ' and ' || UPPER(y) => no change
			apply2(Concat, Call(Upper, path("x")), String(" and "), Call(Upper, path("y"))),
			apply2(Concat, Call(Upper, path("x")), String(" and "), Call(Upper, path("y"))),
		},
		{
			// 'X=' || UPPER(x) || ', Y=' || UPPER(y) => UPPER('X=' || x || ', Y=' || y)
			apply2(Concat, String("X="), Call(Upper, path("x")), String(", Y="), Call(Upper, path("y"))),
			Call(Upper, apply2(Concat, String("X="), path("x"), String(", Y="), path("y"))),
		},
		{
			// 'X=' || UPPER(x) || ', Y=' || UPPER(y) => no change
			apply2(Concat, String("x="), Call(Upper, path("x")), String(", y="), Call(Upper, path("y"))),
			apply2(Concat, String("x="), Call(Upper, path("x")), String(", y="), Call(Upper, path("y"))),
		},
		{
			// LOWER(x) || UPPER(x) => no change
			Call(Concat, Call(Lower, path("x")), Call(Upper, path("x"))),
			Call(Concat, Call(Lower, path("x")), Call(Upper, path("x"))),
		},
		{
			// CHAR_LENGTH(LOWER(s)) => CHAR_LENGTH(s)
			Call(CharLength, Call(Lower, path("x"))),
			Call(CharLength, path("x")),
		},
		{
			// CHAR_LENGTH(UPPER(s)) => CHAR_LENGTH(s)
			Call(CharLength, Call(Upper, path("x"))),
			Call(CharLength, path("x")),
		},
		{
			// SUBSTRING(LOWER(s), p, l) => LOWER(SUBSTRING(s, p, l))
			Call(Substring, Call(Lower, path("s")), Integer(1), Integer(5)),
			Call(Lower, Call(Substring, path("s"), Integer(1), Integer(5))),
		},
		{
			// SUBSTRING(UPPER(s), p, l) => UPPER(SUBSTRING(s, p, l))
			Call(Substring, Call(Upper, path("s")), Integer(1), Integer(5)),
			Call(Upper, Call(Substring, path("s"), Integer(1), Integer(5))),
		},
		{
			// CHAR_LENGTH(x || "test" || y || "xyz" || "foo" || z) =>
			//   CHAR_LENGTH(x) + CHAR_LENGTH(y) + CHAR_LENGTH(z) + Integer(10)
			Call(CharLength, apply2(Concat, path("x"), String("test"), path("y"), String("xyz"), String("foo"), path("z"))),
			Add(Add(Call(CharLength, path("x")),
				Add(Call(CharLength, path("y")), Integer(10))),
				Call(CharLength, path("z"))),
		},
		{
			Call(Concat, Call(Concat, path("x"), String("a")), String("b")),
			Call(Concat, path("x"), String("ab")),
		},
		{
			Call(TypeBit, Integer(1)),
			Integer(JSONTypeBits(ion.IntType)),
		},
		{
			Call(TypeBit, Null{}),
			Integer(JSONTypeBits(ion.NullType)),
		},
		{
			Call(TypeBit, Missing{}),
			Integer(0),
		},
		{
			Call(TypeBit, Float(3.5)),
			Integer(JSONTypeBits(ion.FloatType)),
		},
		{
			Is(Count(path("c")), IsNotMissing),
			Bool(true),
		},
		{
			Call(Sqrt, Integer(4)),
			Float(2.0),
		},
		{
			Call(Cos, Integer(0)),
			Float(1.0),
		},
		{
			Call(Pow, Integer(2), Float(3.0)),
			Float(8.0),
		},
		{
			Call(Tan, Float(math.Pi/4)),
			Float(1.0),
		},
		{
			Call(Exp10, Integer(4)),
			Float(10000.0),
		},
		{
			Call(Atan2, Float(-42), Integer(42)),
			Float(-math.Pi / 4),
		},
		{
			Call(Least, Float(2), Integer(-8), Float(10)),
			Float(-8),
		},
		{
			Call(Greatest, Float(200), Integer(-8), Float(10)),
			Float(200),
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
