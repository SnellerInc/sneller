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

package sneller

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
		expect Ternary
	}{
		{node: expr.Bool(false), expect: Never},
		{node: parsePath("foo"), expect: Maybe},
		{node: expr.Bool(true), expect: Always},
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
	type truthTable [9]Ternary
	const F, U, T = Never, Maybe, Always
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
		expect Ternary
	}
	cases := []struct {
		expr   expr.Node
		checks []check
	}{{
		expr: parseExpr("%s < foo.bar", now),
		checks: []check{{
			// No ranges
			ranges: nil,
			expect: Maybe,
		}, {
			// Within the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Right at the min
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Right at the max; `now < now` is false,
			// but we use inclusive ranges, so "maybe"
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now),
			)},
			expect: Maybe,
		}, {
			// Before the range -> always
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: Always,
		}, {
			// After the range -> never
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour)),
			)},
			expect: Never,
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
			expect: Always,
		}, {
			// Different path
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar", "foo"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Non-applicable types
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Int(100),
				ion.Int(200),
			)},
			expect: Maybe,
		}},
	}, {
		expr: parseExpr("foo.bar < %s", now),
		checks: []check{{
			// Within the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Right at the min; `min < now` is false
			// and `max < now`) is false, but we use
			// inclusive ranges, so maybe
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Right at the max; before(max, now) is false
			// but before(min, now) is true
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now),
			)},
			expect: Maybe,
		}, {
			// Before the range;
			// BEFORE(foo.bar, now) is always false
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(time.Hour)),
				ion.Timestamp(now.Add(2*time.Hour)),
			)},
			expect: Never,
		}, {
			// After the range;
			// BEFORE(foo.bar, now) is always true
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour)),
			)},
			expect: Always,
		}},
	}, {
		expr: parseExpr("%s < foo.bar AND foo.bar < %s",
			now.Add(-time.Hour), now.Add(time.Hour)),
		checks: []check{{
			// Smaller range; result always fits within bounds
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Minute)),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: Always,
		}, {
			// Exact range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-time.Hour)),
				ion.Timestamp(now.Add(time.Hour)),
			)},
			expect: Maybe,
		}, {
			// Disjoint range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo", "bar"},
				ion.Timestamp(now.Add(-2*time.Hour)),
				ion.Timestamp(now.Add(-time.Hour-1)),
			)},
			expect: Never,
		}},
	}, {
		expr: parseExpr("%s < foo AND foo < %s AND %s < bar AND bar < %s",
			now.Add(-time.Hour), now, now, now.Add(time.Hour)),
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
			expect: Always,
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
			expect: Never,
		}, {
			// Missing range for bar
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: Maybe,
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
			expect: Maybe,
		}, {
			// After the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: Never,
		}, {
			// Before the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(1*time.Minute)),
				ion.Timestamp(now.Add(2*time.Minute)),
			)},
			expect: Always,
		}, {
			// Right at min
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: Always,
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
			expect: Maybe,
		}, {
			// Before the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(time.Millisecond)),
				ion.Timestamp(now.Add(time.Minute)),
			)},
			expect: Always,
		}, {
			// After the range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"bar"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: Never,
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
			expect: Maybe,
		}, {
			// min = max
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now),
				ion.Timestamp(now),
			)},
			expect: Maybe,
		}, {
			// Outside range
			ranges: []blockfmt.Range{blockfmt.NewRange(
				[]string{"foo"},
				ion.Timestamp(now.Add(-2*time.Minute)),
				ion.Timestamp(now.Add(-1*time.Minute)),
			)},
			expect: Never,
		}},
	}, {
		expr:   expr.Bool(false),
		checks: []check{{expect: Never}},
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
				expect: Never,
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
				expect: Maybe,
			},
			{
				// during
				ranges: []blockfmt.Range{blockfmt.NewRange(
					[]string{"timestamp"},
					ion.Timestamp(now.Add(-2*time.Minute)),
					ion.Timestamp(now.Add(2*time.Minute)),
				)},
				expect: Maybe,
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
				expect: Never,
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
					t.Errorf("check %d did not match: want '%s', got '%s'",
						i, c.expect.String(), got.String())
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
			Expr: parseExpr(`(foo = 'baz' OR foo = 'bar') AND x < %s`, now),
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
