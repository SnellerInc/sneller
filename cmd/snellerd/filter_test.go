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

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestCompileFilter(t *testing.T) {
	// Test basic expressions.
	for _, c := range []struct {
		node   expr.Node
		expect ternary
	}{
		{node: expr.Bool(false), expect: never},
		{node: parsePath("foo"), expect: maybe},
		{node: expr.Bool(true), expect: always},
	} {
		var sparse blockfmt.SparseIndex
		f, ok := compileFilter(c.node)
		got := toMaybe(f, ok)(&sparse, 0)
		if got != c.expect {
			t.Errorf("%v: want %d, got %d", c.node, c.expect, got)
		}
	}

	// Test truth tables for logical operators,
	// arranged like so:
	//
	//     F U T
	//   F 0 1 2
	//   U 3 4 5
	//   T 6 7 8
	//
	type truthTable [9]ternary
	const F, U, T = never, maybe, always
	for _, c := range []struct {
		op expr.LogicalOp
		tt truthTable
	}{{
		op: expr.OpAnd,
		tt: truthTable{
			F, F, F,
			F, U, U,
			F, U, T,
		},
	}, {
		op: expr.OpOr,
		tt: truthTable{
			F, U, T,
			U, U, T,
			T, T, T,
		},
	}, {
		op: expr.OpXnor,
		tt: truthTable{
			T, U, F,
			U, U, U,
			F, U, T,
		},
	}, {
		op: expr.OpXor,
		tt: truthTable{
			F, U, T,
			U, U, U,
			T, U, F,
		},
	}} {
		exprs := []expr.Node{
			expr.Bool(false), // F
			parsePath("foo"), // U
			expr.Bool(true),  // T
		}
		t.Run(fmt.Sprint(c.op), func(t *testing.T) {
			var sparse blockfmt.SparseIndex
			le := &expr.Logical{Op: c.op}
			for i := range c.tt {
				le.Left, le.Right = exprs[i/3], exprs[i%3]
				got, want := toMaybe(compileFilter(le))(&sparse, 0), c.tt[i]
				if got != want {
					t.Errorf("%v: want %d, got %d", le, want, got)
				}
			}
		})
	}

	// Test some path expressions against ranges.
	now := date.Now().Truncate(time.Second)
	type check struct {
		ranges []blockfmt.Range
		expect ternary
	}
	cases := []struct {
		expr   expr.Node
		checks []check
	}{{
		expr: parseExpr("BEFORE(%s, foo.bar)", now),
		checks: []check{{
			// No ranges
			ranges: nil,
			expect: maybe,
		}, {
			// Within the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: maybe,
		}, {
			// Right at the min
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: maybe,
		}, {
			// Right at the max; before(now, now) is false,
			// but we use inclusive ranges, so "maybe"
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now),
			)},
			expect: maybe,
		}, {
			// Before the range -> always
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: always,
		}, {
			// After the range -> never
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour)),
			)},
			expect: never,
		}, {
			// Extra ranges, before the range -> maybe
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"baz"},
				ion.Int(100),
				ion.Int(200),
			), blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: always,
		}, {
			// Different path
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar", "foo"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: maybe,
		}, {
			// Non-applicable types
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Int(100),
				ion.Int(200),
			)},
			expect: maybe,
		}},
	}, {
		expr: parseExpr("BEFORE(foo.bar, %s)", now),
		checks: []check{{
			// Within the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: maybe,
		}, {
			// Right at the min; before(min, now) is false
			// and before(max, now) is false, but we use
			// inclusive ranges, so maybe
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: maybe,
		}, {
			// Right at the max; before(max, now) is false
			// but before(min, now) is true
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now),
			)},
			expect: maybe,
		}, {
			// Before the range;
			// BEFORE(foo.bar, now) is always false
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: never,
		}, {
			// After the range;
			// BEFORE(foo.bar, now) is always true
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour)),
			)},
			expect: always,
		}},
	}, {
		expr: parseExpr("BEFORE(%s, foo.bar, %s)",
			now.Add(-time.Hour), now.Add(time.Hour)),
		checks: []check{{
			// Smaller range; result always
			// fits within bounds
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Minute)),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: always,
		}, {
			// Exact range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: maybe,
		}, {
			// Disjoint range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour-1)),
			)},
			expect: never,
		}},
	}, {
		expr: parseExpr("BEFORE(%s, foo, %s, bar, %s)",
			now.Add(-time.Hour), now, now.Add(time.Hour)),
		checks: []check{{
			// Both smaller ranges
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			), blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(1*time.Minute)),
				ion.Timestamp(now.Add(2*time.Minute)),
			)},
			expect: always,
		}, {
			// foo always matches
			// bar never matches
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			), blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(1*time.Hour+time.Millisecond)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: never,
		}, {
			// Missing range for bar
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: maybe,
		}},
	}, {
		expr: parseExpr("TO_UNIX_EPOCH(foo) >= %d", now.Unix()),
		checks: []check{{
			// Within range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-1*time.Minute)),
				ion.Timestamp(now.Add(+1*time.Minute)),
			)},
			expect: maybe,
		}, {
			// After the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: always,
		}, {
			// Before the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(1*time.Minute)),
				ion.Timestamp(now.Add(2*time.Minute)),
			)},
			expect: never,
		}, {
			// Right at min
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: maybe,
		}},
	}, {
		expr: parseExpr("%d < TO_UNIX_EPOCH(bar)", now.Unix()),
		checks: []check{{
			// Within range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(-1*time.Minute)),
				ion.Timestamp(now.Add(+1*time.Minute)),
			)},
			expect: maybe,
		}, {
			// Right below min
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(time.Millisecond)),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: never,
		}, {
			// After the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: always,
		}},
	}, {
		expr: parseExpr("TO_UNIX_MICRO(foo) = %d", now.UnixMicro()),
		checks: []check{{
			// Within range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-1*time.Minute)),
				ion.Timestamp(now.Add(+1*time.Minute)),
			)},
			expect: maybe,
		}, {
			// min = max
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now),
				ion.Timestamp(now),
			)},
			expect: maybe,
		}, {
			// Outside range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: never,
		}},
	}, {
		expr:   expr.Bool(false),
		checks: []check{{expect: never}},
	}, {
		// include an un-indexed expression
		expr: parseExpr("foo = 'bar' AND timestamp BETWEEN %s AND %s",
			now.Add(-2*time.Minute), now.Add(2*time.Minute)),
		checks: []check{
			{
				// strictly before
				ranges: []blockfmt.Range{
					blockfmt.NewRange(
						[]string{"userIdentity", "sessionContext", "creationDate"},
						ion.Timestamp(now.Add(11*time.Minute)),
						ion.Timestamp(now.Add(12*time.Minute)),
					),
					blockfmt.NewRange(
						[]string{"timestamp"},
						ion.Timestamp(now.Add(-12*time.Minute)),
						ion.Timestamp(now.Add(-11*time.Minute)),
					),
				},
				expect: never,
			},
			{
				// within
				ranges: []blockfmt.Range{
					blockfmt.NewRange(
						[]string{"timestamp"},
						ion.Timestamp(now.Add(-time.Minute)),
						ion.Timestamp(now.Add(time.Minute)),
					),
					blockfmt.NewRange(
						[]string{"userIdentity", "sessionContext", "creationDate"},
						ion.Timestamp(now.Add(-time.Hour)),
						ion.Timestamp(now.Add(time.Hour)),
					),
				},
				expect: maybe,
			},
			{
				// during
				ranges: []blockfmt.Range{blockfmt.NewRange(
					[]string{"timestamp"},
					ion.Timestamp(now.Add(-2*time.Minute)),
					ion.Timestamp(now.Add(2*time.Minute)),
				)},
				expect: maybe,
			},
			{
				// after
				ranges: []blockfmt.Range{
					blockfmt.NewRange(
						[]string{"timestamp"},
						ion.Timestamp(now.Add(11*time.Minute)),
						ion.Timestamp(now.Add(12*time.Minute)),
					),
					blockfmt.NewRange(
						[]string{"userIdentity", "sessionContext", "creationDate"},
						ion.Timestamp(now.Add(-time.Hour)),
						ion.Timestamp(now.Add(time.Hour)),
					),
				},
				expect: never,
			},
		},
	}}
	for i := range cases {
		c := cases[i]
		t.Run(fmt.Sprint(expr.ToString(c.expr)), func(t *testing.T) {
			f := toMaybe(compileFilter(c.expr))
			for i, c := range c.checks {
				var sparse blockfmt.SparseIndex
				if len(c.ranges) > 0 {
					sparse.Push(c.ranges)
				}
				got := f(&sparse, 0)
				if got != c.expect {
					t.Errorf("check %d did not match: %v != %v",
						i, c.expect, got)
				}
			}
		})
	}
}

func parsePath(s string) *expr.Path {
	p, err := expr.ParsePath(s)
	if err != nil {
		panic(err)
	}
	return p
}

func parseExpr(str string, args ...interface{}) expr.Node {
	for i := range args {
		if t, ok := args[i].(date.Time); ok {
			args[i] = expr.ToString(&expr.Timestamp{Value: t})
		}
	}
	str = fmt.Sprintf(str, args...)
	q, err := partiql.Parse([]byte(fmt.Sprintf("SELECT * FROM x WHERE %s", str)))
	if err != nil {
		println("couldn't parse", str)
		panic(err)
	}
	return q.Body.(*expr.Select).Where
}

func BenchmarkExecuteFilter(b *testing.B) {
	now := date.Now().Truncate(time.Microsecond)
	cases := []struct {
		Expr   expr.Node
		Ranges []blockfmt.Range
	}{
		{
			Expr: parseExpr(`(foo = 'baz' OR foo = 'bar') AND BEFORE(x, %s)`, now),
			Ranges: []blockfmt.Range{
				blockfmt.NewRange([]string{"quux"}, ion.Int(0), ion.Int(100)),
				blockfmt.NewRange([]string{"x"}, ion.Timestamp(now.Add(-time.Minute)), ion.Timestamp(now.Add(time.Minute))),
			},
		},
	}
	for i := range cases {
		e := cases[i].Expr
		rng := cases[i].Ranges
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			f := toMaybe(compileFilter(e))
			var sparse blockfmt.SparseIndex
			sparse.Push(rng)
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(1024 * 1024) // 1MB per block
			for i := 0; i < b.N; i++ {
				_ = f(&sparse, 0)
			}
		})
	}

}
