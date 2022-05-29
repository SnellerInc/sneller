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

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// A filter returns a ternary truth value indicating
// whether rows constrained by the given ranges always
// match, maybe match, or never match the expression
// the filter was compiled from.
type filter func(*blockfmt.SparseIndex, int) ternary

type ternary int8

const (
	always ternary = 1
	maybe  ternary = 0
	never  ternary = -1
)

func alwaysMatches(*blockfmt.SparseIndex, int) ternary { return always }
func maybeMatches(*blockfmt.SparseIndex, int) ternary  { return maybe }
func neverMatches(*blockfmt.SparseIndex, int) ternary  { return never }

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
			return func(s *blockfmt.SparseIndex, n int) ternary {
				return -f(s, n)
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
		return func(r *blockfmt.SparseIndex, n int) ternary {
			if prim(r, n) == never {
				return never
			}
			return maybe
		}, true
	case expr.OpOr:
		return func(r *blockfmt.SparseIndex, n int) ternary {
			if prim(r, n) == always {
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
		return func(s *blockfmt.SparseIndex, n int) ternary {
			a, b := left(s, n), right(s, n)
			if a == never || b == never {
				return never
			}
			if a == always && b == always {
				return always
			}
			return maybe
		}, true
	case expr.OpOr:
		return func(s *blockfmt.SparseIndex, n int) ternary {
			a, b := left(s, n), right(s, n)
			if a == always || b == always {
				return always
			}
			if a == never && b == never {
				return never
			}
			return maybe
		}, true
	case expr.OpXnor:
		return func(s *blockfmt.SparseIndex, n int) ternary {
			a, b := left(s, n), right(s, n)
			if a == maybe || b == maybe {
				return maybe
			}
			if a == b {
				return always
			}
			return never
		}, true
	case expr.OpXor:
		return func(s *blockfmt.SparseIndex, n int) ternary {
			a, b := left(s, n), right(s, n)
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
	switch fn.Func {
	case expr.DateToUnixEpoch:
		when := date.Unix(int64(im), 0)
		cmp := compareFunc(op, when)
		if cmp == nil {
			return nil, false
		}
		return pathFilter(path, cmp), true
	case expr.DateToUnixMicro:
		when := date.UnixMicro(int64(im))
		cmp := compareFunc(op, when)
		if cmp == nil {
			return nil, false
		}
		return pathFilter(path, cmp), true
	}
	return nil, false
}

// compareFunc returns a function that returns whether
// "min op v" always, maybe, or never evaluates to true
// for any value v in the range [min, max] (inclusive).
// This returns nil if op is not applicable to integers
// (LIKE or ILIKE).
func compareFunc(op expr.CmpOp, when date.Time) func(*blockfmt.TimeIndex, int) ternary {
	const epsilon = time.Microsecond
	switch op {
	case expr.Equals:
		return func(i *blockfmt.TimeIndex, n int) ternary {
			if i.Contains(when) {
				return maybe
			}
			return never
		}
	case expr.NotEquals:
		// complicated due to partiql semantics
		return nil
	case expr.Less:
		// when < expr
		return pickAfter(when.Add(-epsilon))
	case expr.LessEquals:
		// when <= expr
		return pickAfter(when)
	case expr.Greater:
		// when > expr
		return pickBefore(when)
	case expr.GreaterEquals:
		// when >= expr
		return pickBefore(when.Add(epsilon))
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

// pickAfter returns a function that evaluates
// whether or not the provided block and time index
// contain a value greater than or equal to when
func pickAfter(when date.Time) func(*blockfmt.TimeIndex, int) ternary {
	return func(i *blockfmt.TimeIndex, n int) ternary {
		// exclude blocks to the left of the block
		// where max(block) < ts
		if n < i.Start(when) {
			return never
		}
		if n < i.End(when) {
			return maybe
		}
		return always
	}
}

// pickBefore returns a function that
// evaluates whether or not the provided block
// and time index contain a value less than when
func pickBefore(when date.Time) func(*blockfmt.TimeIndex, int) ternary {
	return func(i *blockfmt.TimeIndex, n int) ternary {
		if n < i.Start(when) {
			return always
		}
		if n < i.End(when) {
			return maybe
		}
		return never
	}
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
			return pathFilter(rhs, pickAfter(lhs.Value)), true
		}
	case *expr.Path:
		switch rhs := args[1].(type) {
		case *expr.Timestamp:
			// BEFORE(path, ts)
			return pathFilter(lhs, pickBefore(rhs.Value)), true
		}
	}
	return nil, false
}

func flatpath(path *expr.Path) ([]string, bool) {
	ret := []string{path.First}
	for d := path.Rest; d != nil; d = d.Next() {
		dot, ok := d.(*expr.Dot)
		if !ok {
			return nil, false
		}
		ret = append(ret, dot.Field)
	}
	return ret, true
}

// pathFilter returns a filter that finds a range
// matching the given path and applies fn to it.
func pathFilter(path *expr.Path, fn func(*blockfmt.TimeIndex, int) ternary) filter {
	flat, ok := flatpath(path)
	if !ok {
		return nil
	}
	return func(s *blockfmt.SparseIndex, i int) ternary {
		tr := s.Get(flat)
		if tr == nil {
			return maybe
		}
		return fn(tr, i)
	}
}
