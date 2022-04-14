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
	"github.com/SnellerInc/sneller/date"
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

// compileFilter compiles a filter expression;
// if it returns (nil, false), then the result
// of the expression is intederminate
func compileFilter(e expr.Node) (filter, bool) {
	switch e := e.(type) {
	case expr.Bool:
		if e {
			return alwaysMatches, true
		}
		return neverMatches, true
	case *expr.Not:
		f, ok := compileFilter(e.Expr)
		if ok {
			return func(r []blockfmt.Range) ternary {
				return -f(r)
			}, true
		}
		return nil, false
	case *expr.Logical:
		return compileLogicalFilter(e)
	case *expr.Comparison:
		return compileComparisonFilter(e)
	case *expr.Builtin:
		return compileBuiltin(e)
	}
	return nil, false
}

func toMaybe(f filter, ok bool) filter {
	if ok {
		return f
	}
	return maybeMatches
}

// compileLogicalFilter compiles a filter from a
// logical expression.
func compileLogicalFilter(e *expr.Logical) (filter, bool) {
	left, okl := compileFilter(e.Left)
	right, okr := compileFilter(e.Right)
	if !okl && !okr {
		return nil, false
	}
	// if we can evaluate one side but
	// not the other, we can still search
	// for short-circuiting
	if !okl || !okr {
		prim := left
		if !okl {
			prim = right
		}
		return logical1Arm(prim, e.Op)
	}
	return logical(left, e.Op, right)
}

// evaluate a logical expression when
// only one side of the expression has
// an interesting result
func logical1Arm(prim filter, op expr.LogicalOp) (filter, bool) {
	switch op {
	case expr.OpAnd:
		return func(r []blockfmt.Range) ternary {
			if prim(r) == never {
				return never
			}
			return maybe
		}, true
	case expr.OpOr:
		return func(r []blockfmt.Range) ternary {
			if prim(r) == always {
				return always
			}
			return maybe
		}, true
	default:
		return nil, false
	}
}

// logical returns a filter applying op to the results
// of the left and right filters.
func logical(left filter, op expr.LogicalOp, right filter) (filter, bool) {
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
		}, true
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
		}, true
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
		}, true
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
		}, true
	}
	return nil, false
}

// compileComparisonFilter compiles a filter from a
// comparison expression.
func compileComparisonFilter(e *expr.Comparison) (filter, bool) {
	fn, ok1 := e.Left.(*expr.Builtin)
	im, ok2 := e.Right.(expr.Integer)
	op := e.Op
	if !ok1 || !ok2 {
		fn, ok1 = e.Right.(*expr.Builtin)
		im, ok2 = e.Left.(expr.Integer)
		if !ok1 || !ok2 {
			return nil, false
		}
		op = e.Op.Flip()
	}
	if len(fn.Args) != 1 {
		return nil, false
	}
	path, ok := fn.Args[0].(*expr.Path)
	if !ok {
		return nil, false
	}
	cmp := compareFunc(op)
	if cmp == nil {
		return nil, false
	}
	switch fn.Func {
	case expr.DateToUnixEpoch:
		return timeFilter(path, func(min, max date.Time) ternary {
			return cmp(int64(im), min.Unix(), max.Unix())
		}), true
	case expr.DateToUnixMicro:
		return timeFilter(path, func(min, max date.Time) ternary {
			return cmp(int64(im), min.UnixMicro(), max.UnixMicro())
		}), true
	}
	return nil, false
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
func compileBuiltin(e *expr.Builtin) (filter, bool) {
	switch e.Func {
	case expr.Before:
		return compileBefore(e.Args)
	}
	return nil, false
}

// compileBefore compiles a filter from a BEFORE
// expression.
func compileBefore(args []expr.Node) (filter, bool) {
	if len(args) < 2 {
		return nil, false
	}
	if len(args) > 2 {
		f1, ok1 := compileBefore(args[:2])
		f2, ok2 := compileBefore(args[1:])
		if ok1 || ok2 {
			return logical(toMaybe(f1, ok1), expr.OpAnd, toMaybe(f2, ok2))
		}
		return nil, false
	}
	switch lhs := args[0].(type) {
	case *expr.Timestamp:
		switch rhs := args[1].(type) {
		case *expr.Path:
			// BEFORE(ts, path)
			return timeFilter(rhs, func(min, max date.Time) ternary {
				if lhs.Value.Before(min) {
					return always
				}
				if lhs.Value.Before(max) {
					return maybe
				}
				return never
			}), true
		}
	case *expr.Path:
		switch rhs := args[1].(type) {
		case *expr.Timestamp:
			// BEFORE(path, ts)
			return timeFilter(lhs, func(min, max date.Time) ternary {
				if max.Before(rhs.Value) {
					return always
				}
				if min.Before(rhs.Value) {
					return maybe
				}
				return never
			}), true
		}
	}
	return nil, false
}

// timeFilter returns a filter that finds a time range
// matching the given path and applies fn to it. If a
// range for the given path was found, but it is not a
// time range, the filter returns maybe.
func timeFilter(path *expr.Path, fn func(min, max date.Time) ternary) filter {
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
