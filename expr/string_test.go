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
	"fmt"
	"testing"
)

// construct a path expression from components
func path(args ...string) Node {
	cur := Node(Ident(args[0]))
	args = args[1:]
	for i := range args {
		cur = &Dot{Inner: cur, Field: args[i]}
	}
	return cur
}

func TestString(t *testing.T) {
	testcases := []struct {
		in   Node
		want string
	}{
		{
			Is(path("t", "foo"), IsNull),
			"t.foo IS NULL",
		},
		{
			Is(path("t", "foo"), IsMissing),
			"t.foo IS MISSING",
		},
		{
			// parenthesize when left-associativity
			// would lead to a different expression
			And(path("x"), Or(path("y"), path("z"))),
			"x AND (y OR z)",
		},
		{
			Add(path("x"), Mul(path("y"), path("z"))),
			"x + (y * z)",
		},
		{
			Compare(Equals, path("isTrue"), Compare(Less, path("x"), path("y"))),
			"isTrue = (x < y)",
		},
		{
			And(Compare(Less, path("t", "foo"), Integer(3)), Compare(Greater, path("t", "foo"), Integer(0))),
			"t.foo < 3 AND t.foo > 0",
		},
		{
			Or(Compare(Less, Integer(3), Integer(4)), Compare(Less, Integer(4), Integer(5))),
			"3 < 4 OR 4 < 5",
		},
		{
			&StringMatch{Op: Like, Expr: path("t", "foo", "name"), Pattern: "F%Z%Y"},
			"t.foo.name LIKE 'F%Z%Y'",
		},
		{
			Between(path("t", "x"), Integer(0), Integer(5)),
			"t.x >= 0 AND t.x <= 5",
		},
		{
			In(path("t", "x"), String("foo"), String("bar")),
			"t.x IN ('bar', 'foo')",
		},
		{
			Compare(Less, Add(path("t", "x"), path("t", "y")), Integer(5)),
			"t.x + t.y < 5",
		},
		{
			Compare(Greater, Sub(path("", "a"), path("", "b")), Div(path("", "c"), path("", "d"))),
			".a - .b > .c / .d",
		},
		{
			&Not{Expr: Is(path("x", "y"), IsMissing)},
			"!(x.y IS MISSING)",
		},
		{
			And(path("a"), Compare(Equals, path("b"), path("c"))),
			"a AND b = c",
		},
		{
			// same tokens as above, but the expression tree is rotated
			Compare(Equals, And(path("a"), path("b")), path("c")),
			"(a AND b) = c",
		},
		{
			Compare(Equals, path("c"), And(path("a"), path("b"))),
			"c = (a AND b)",
		},
		{
			&StringMatch{Op: Ilike, Expr: path("x", "y"), Pattern: "%xyz%"},
			"x.y ILIKE '%xyz%'",
		},
		{
			&StringMatch{Op: Like, Expr: path("x"), Pattern: "%x%", Escape: "\\"},
			"x LIKE '%x%' ESCAPE '\\\\'",
		},
		{
			// test for valid PartiQL list literal syntax
			&List{Values: []Constant{Integer(3), Float(2.5), String("foo")}},
			"[3, 2.5, 'foo']",
		},
		{
			// test for valid PartiQL structure literal syntax
			&Struct{Fields: []Field{
				{Label: "foo", Value: Integer(3)},
				{Label: "bar", Value: &List{Values: []Constant{Integer(3), String("hello")}}},
				{Label: "baz", Value: String("quux")},
			}},
			"{'foo': 3, 'bar': [3, 'hello'], 'baz': 'quux'}",
		},
		{
			Call(MakeStruct, String("foo"), Identifier("x")),
			`{'foo': x}`,
		},
		{
			Call(MakeStruct, String("foo"), Identifier("x"), String("bar"), Integer(3)),
			`{'foo': x, 'bar': 3}`,
		},
	}
	for i := range testcases {
		got := ToString(testcases[i].in)
		want := testcases[i].want
		if got != want {
			t.Errorf("testcase %d: got  %q", i, got)
			t.Errorf("testcase %d: want %q", i, want)
		}
		testEquivalence(testcases[i].in, t)
	}
}

func TestSFWString(t *testing.T) {
	testcases := []struct {
		sfw *Select
		str string
	}{
		{
			sfw: &Select{
				Columns: []Binding{
					Bind(Call(Upper, path("outer", "x")), ""),
					Bind(Call(Lower, path("inner", "y")), ""),
				},
				From: &Join{
					Kind: CrossJoin,
					Left: &Table{
						Binding: Bind(path("data"), "outer"),
					},
					Right: Bind(path("outer", "lst"), "inner"),
				},
				Where: Compare(Equals, path("outer", "foo"), path("inner", "bar")),
			},
			str: "SELECT UPPER(outer.x), LOWER(\"inner\".y) FROM data AS outer CROSS JOIN outer.lst AS \"inner\" WHERE outer.foo = \"inner\".bar",
		},
		{
			sfw: &Select{
				Columns: []Binding{
					Bind(path("foo", "x"), "x"),
					Bind(path("foo", "y"), "y"),
				},
				From: &Table{Binding: Bind(path("data"), "foo")},
				OrderBy: []Order{
					{Column: path("x"), Desc: true, NullsLast: false},
				},
			},
			str: "SELECT foo.x AS x, foo.y AS y FROM data AS foo ORDER BY x DESC NULLS FIRST",
		},
	}

	for i := range testcases {
		got := testcases[i].sfw.Text()
		want := testcases[i].str
		if got != want {
			t.Errorf("testcase %d: want %s", i, want)
			t.Errorf("testcase %d: got  %s", i, got)
		}
		testEquivalence(testcases[i].sfw, t)
	}
}

func TestParsePath(t *testing.T) {
	tcs := []struct {
		str  string
		want Node
	}{
		{
			str:  "x",
			want: Ident("x"),
		},
		{
			str:  "x.y",
			want: &Dot{Ident("x"), "y"},
		},
		{
			str:  "x[0]",
			want: &Index{Ident("x"), 0},
		},
		{
			str:  "first.second[100]",
			want: &Index{&Dot{Ident("first"), "second"}, 100},
		},
		{
			str:  "first[1][2]",
			want: &Index{&Index{Ident("first"), 1}, 2},
		},
		{
			str:  "first.foo[2].bar",
			want: &Dot{&Index{&Dot{Ident("first"), "foo"}, 2}, "bar"},
		},
	}
	for i := range tcs {
		tc := tcs[i]

		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			p, err := ParsePath(tc.str)
			if err != nil {
				t.Error(err)
				return
			}
			if !p.Equals(tc.want) {
				t.Logf("got:  %s", ToString(p))
				t.Logf("want: %s", ToString(tc.want))
				t.Error("wrong result")
			}
		})
	}
}

func TestParsePathErrors(t *testing.T) {
	bad := []string{
		"",
		"field.",
		".field",
		"[2]",
		"x..y",
		"x.[2]",
		"x....",
		"x[[",
		"x[foo]",
		"x[2][]",
		"x[2].[]",
		"x[2].[3]",
		"x[2][3].",
	}
	for i := range bad {
		p, err := ParsePath(bad[i])
		t.Log(err)
		if err == nil {
			t.Errorf("%q - expected error but got back %s", bad[i], p)
		}
	}
}

func TestParseBindings(t *testing.T) {
	tcs := []struct {
		str  string
		want []Binding
	}{
		{
			str: "x as foo, y",
			want: []Binding{
				Bind(Ident("x"), "foo"),
				Bind(Ident("y"), ""),
			},
		},
		{
			str: "x.y as x, x.z as z",
			want: []Binding{
				Bind(&Dot{Ident("x"), "y"}, "x"),
				Bind(&Dot{Ident("x"), "z"}, "z"),
			},
		},
	}
	for i := range tcs {
		b, err := ParseBindings(tcs[i].str)
		if err != nil {
			t.Errorf("case %d: %s", i, err)
			continue
		}
		want := tcs[i].want
		if len(b) != len(want) {
			t.Errorf("case %d: got %#v", i, b)
			continue
		}
		for j := range b {
			if !b[j].Expr.Equals(want[j].Expr) {
				t.Errorf("%#v != %#v", b[j].Expr, want[j].Expr)
			}
			if b[j].Result() != want[j].Result() {
				t.Errorf("%s != %s", b[j].Result(), want[j].Result())
			}
		}
	}
}

func TestAutoBindings(t *testing.T) {
	tcs := []struct {
		in, out string
	}{
		{"x", "x"},
		{"x.y", "y"},
		{"x.y.z", "z"},
		{"x[0]", "x_0"},
		{"x.y[2]", "y_2"},
	}
	for i := range tcs {
		e, err := ParsePath(tcs[i].in)
		if err != nil {
			t.Errorf("case %d: %s", i, err)
			continue
		}
		got := DefaultBinding(e)
		if got != tcs[i].out {
			t.Errorf("case %d: got  %q", i, got)
			t.Errorf("case %d: want %q", i, tcs[i].out)
		}
		testEquivalence(e, t)
	}
}
