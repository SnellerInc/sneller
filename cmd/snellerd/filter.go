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
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// A filter returns a ternary truth value indicating
// whether rows constrained by the given ranges always
// match, maybe match, or never match the expression
// the filter was compiled from.
type filter func([]blockfmt.Range) ternary

type ternary int8

const (
	always ternary = 1
	maybe  ternary = 0
	never  ternary = -1
)

func alwaysMatches([]blockfmt.Range) ternary { return always }
func maybeMatches([]blockfmt.Range) ternary  { return maybe }
func neverMatches([]blockfmt.Range) ternary  { return never }

// compileFilter compiles a filter expression.
func compileFilter(e expr.Node) filter {
	switch e := e.(type) {
	case expr.Bool:
		if e {
			return alwaysMatches
		}
		return neverMatches
	case *expr.Not:
		f := compileFilter(e.Expr)
		return func(r []blockfmt.Range) ternary {
			return -f(r)
		}
	case *expr.Logical:
		return compileLogicalFilter(e)
	case *expr.Comparison:
		return compileComparisonFilter(e)
	case *expr.Builtin:
		return compileBuiltin(e)
	}
	return maybeMatches
}

// compileLogicalFilter compiles a filter from a
// logical expression.
func compileLogicalFilter(e *expr.Logical) filter {
	left := compileFilter(e.Left)
	right := compileFilter(e.Right)
	return logical(left, e.Op, right)
}

// logical returns a filter applying op to the results
// of the left and right filters.
func logical(left filter, op expr.LogicalOp, right filter) filter {
	switch op {
	case expr.OpAnd:
		return func(r []blockfmt.Range) ternary {
			a, b := left(r), right(r)
			if a == never || b == never {
				return never
			}
			if a == always && b == always {
				return always
			}
			return maybe
		}
	case expr.OpOr:
		return func(r []blockfmt.Range) ternary {
			a, b := left(r), right(r)
			if a == always || b == always {
				return always
			}
			if a == never && b == never {
				return never
			}
			return maybe
		}
	case expr.OpXnor:
		return func(r []blockfmt.Range) ternary {
			a, b := left(r), right(r)
			if a == maybe || b == maybe {
				return maybe
			}
			if a == b {
				return always
			}
			return never
		}
	case expr.OpXor:
		return func(r []blockfmt.Range) ternary {
			a, b := left(r), right(r)
			if a == maybe || b == maybe {
				return maybe
			}
			if a == b {
				return never
			}
			return always
		}
	}
	return maybeMatches
}

// compileComparisonFilter compiles a filter from a
// comparison expression.
func compileComparisonFilter(e *expr.Comparison) filter {
	fn, ok1 := e.Left.(*expr.Builtin)
	im, ok2 := e.Right.(expr.Integer)
	op := e.Op
	if !ok1 || !ok2 {
		fn, ok1 = e.Right.(*expr.Builtin)
		im, ok2 = e.Left.(expr.Integer)
		if !ok1 || !ok2 {
			return maybeMatches
		}
		op = e.Op.Flip()
	}
	if len(fn.Args) != 1 {
		return maybeMatches
	}
	path, ok := fn.Args[0].(*expr.Path)
	if !ok {
		return maybeMatches
	}
	cmp := compareFunc(op)
	if cmp == nil {
		return maybeMatches
	}
	switch fn.Func {
	case expr.DateToUnixEpoch:
		return timeFilter(path, func(min, max time.Time) ternary {
			return cmp(int64(im), min.Unix(), max.Unix())
		})
	case expr.DateToUnixMicro:
		return timeFilter(path, func(min, max time.Time) ternary {
			return cmp(int64(im), min.UnixMicro(), max.UnixMicro())
		})
	}
	return maybeMatches
}

// compareFunc returns a function that returns whether
// "min op v" always, maybe, or never evaluates to true
// for any value v in the range [min, max] (inclusive).
// This returns nil if op is not applicable to integers
// (LIKE or ILIKE).
func compareFunc(op expr.CmpOp) func(n, min, max int64) ternary {
	switch op {
	case expr.Equals:
		return func(n, min, max int64) ternary {
			if n == min && n == max {
				return always
			}
			if n < min || n > max {
				return never
			}
			return maybe
		}
	case expr.NotEquals:
		return func(n, min, max int64) ternary {
			if n == min && n == max {
				return never
			}
			if n < min || n > max {
				return always
			}
			return maybe
		}
	case expr.Less:
		return func(n, min, max int64) ternary {
			if n < min {
				return always
			}
			if n < max {
				return maybe
			}
			return never
		}
	case expr.LessEquals:
		return func(n, min, max int64) ternary {
			if n <= min {
				return always
			}
			if n <= max {
				return maybe
			}
			return never
		}
	case expr.Greater:
		return func(n, min, max int64) ternary {
			if n > max {
				return always
			}
			if n > min {
				return maybe
			}
			return never
		}
	case expr.GreaterEquals:
		return func(n, min, max int64) ternary {
			if n >= max {
				return always
			}
			if n >= min {
				return maybe
			}
			return never
		}
	}
	return nil
}

// compileBuiltin compiles a filter from a builtin
// expression.
func compileBuiltin(e *expr.Builtin) filter {
	switch e.Func {
	case expr.Before:
		return compileBefore(e.Args)
	}
	return maybeMatches
}

// compileBefore compiles a filter from a BEFORE
// expression.
func compileBefore(args []expr.Node) filter {
	if len(args) < 2 {
		return maybeMatches
	}
	if len(args) > 2 {
		f1 := compileBefore(args[:2])
		f2 := compileBefore(args[1:])
		return logical(f1, expr.OpAnd, f2)
	}
	switch lhs := args[0].(type) {
	case *expr.Timestamp:
		switch rhs := args[1].(type) {
		case *expr.Path:
			// BEFORE(ts, path)
			return timeFilter(rhs, func(min, max time.Time) ternary {
				if lhs.Value.Before(min) {
					return always
				}
				if lhs.Value.Before(max) {
					return maybe
				}
				return never
			})
		}
	case *expr.Path:
		switch rhs := args[1].(type) {
		case *expr.Timestamp:
			// BEFORE(path, ts)
			return timeFilter(lhs, func(min, max time.Time) ternary {
				if max.Before(rhs.Value) {
					return always
				}
				if min.Before(rhs.Value) {
					return maybe
				}
				return never
			})
		}
	}
	return maybeMatches
}

// timeFilter returns a filter that finds a time range
// matching the given path and applies fn to it. If a
// range for the given path was found, but it is not a
// time range, the filter returns maybe.
func timeFilter(path *expr.Path, fn func(min, max time.Time) ternary) filter {
	return pathFilter(path, func(r blockfmt.Range) ternary {
		rt, ok := r.(*blockfmt.TimeRange)
		if !ok {
			return maybe
		}
		return fn(rt.MinTime(), rt.MaxTime())
	})
}

// pathFilter returns a filter that finds a range
// matching the given path and applies fn to it.
func pathFilter(path *expr.Path, fn func(blockfmt.Range) ternary) filter {
	return func(r []blockfmt.Range) ternary {
		for i := range r {
			if !pathMatches(path, r[i].Path()) {
				continue
			}
			return fn(r[i])
		}
		return maybe
	}
}

func pathMatches(e *expr.Path, p []string) bool {
	if len(p) == 0 || e.First != p[0] {
		return false
	}
	p = p[1:]
	for n := e.Rest; n != nil; n = n.Next() {
		if len(p) == 0 {
			return false
		}
		d, ok := n.(*expr.Dot)
		if !ok || d.Field != p[0] {
			return false
		}
		p = p[1:]
	}
	return len(p) == 0
}
