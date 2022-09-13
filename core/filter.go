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

package core

import (
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// A Filter returns a ternary truth value indicating
// whether rows constrained by the given ranges always
// match, maybe match, or never match the expression
// the Filter was compiled from.
type Filter func(*blockfmt.SparseIndex, int) Ternary

type Ternary int8

const (
	Always Ternary = 1
	Maybe  Ternary = 0
	Never  Ternary = -1
)

func (t Ternary) String() string {
	switch t {
	case Always:
		return "always"
	case Maybe:
		return "maybe"
	case Never:
		return "never"
	default:
		return "<unknown>"
	}
}

func alwaysMatches(*blockfmt.SparseIndex, int) Ternary { return Always }
func maybeMatches(*blockfmt.SparseIndex, int) Ternary  { return Maybe }
func neverMatches(*blockfmt.SparseIndex, int) Ternary  { return Never }

// compileFilter compiles a filter expression;
// if it returns (nil, false), then the result
// of the expression is intederminate
func compileFilter(e expr.Node) (Filter, bool) {
	switch e := e.(type) {
	case expr.Bool:
		if e {
			return alwaysMatches, true
		}
		return neverMatches, true
	case *expr.Not:
		f, ok := compileFilter(e.Expr)
		if ok {
			return func(s *blockfmt.SparseIndex, n int) Ternary {
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

func toMaybe(f Filter, ok bool) Filter {
	if ok {
		return f
	}
	return maybeMatches
}

// compileLogicalFilter compiles a filter from a
// logical expression.
func compileLogicalFilter(e *expr.Logical) (Filter, bool) {
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
func logical1Arm(prim Filter, op expr.LogicalOp) (Filter, bool) {
	switch op {
	case expr.OpAnd:
		return func(r *blockfmt.SparseIndex, n int) Ternary {
			if prim(r, n) == Never {
				return Never
			}
			return Maybe
		}, true
	case expr.OpOr:
		return func(r *blockfmt.SparseIndex, n int) Ternary {
			if prim(r, n) == Always {
				return Always
			}
			return Maybe
		}, true
	default:
		return nil, false
	}
}

// logical returns a filter applying op to the results
// of the left and right filters.
func logical(left Filter, op expr.LogicalOp, right Filter) (Filter, bool) {
	switch op {
	case expr.OpAnd:
		return func(s *blockfmt.SparseIndex, n int) Ternary {
			a, b := left(s, n), right(s, n)
			if a == Never || b == Never {
				return Never
			}
			if a == Always && b == Always {
				return Always
			}
			return Maybe
		}, true
	case expr.OpOr:
		return func(s *blockfmt.SparseIndex, n int) Ternary {
			a, b := left(s, n), right(s, n)
			if a == Always || b == Always {
				return Always
			}
			if a == Never && b == Never {
				return Never
			}
			return Maybe
		}, true
	case expr.OpXnor:
		return func(s *blockfmt.SparseIndex, n int) Ternary {
			a, b := left(s, n), right(s, n)
			if a == Maybe || b == Maybe {
				return Maybe
			}
			if a == b {
				return Always
			}
			return Never
		}, true
	case expr.OpXor:
		return func(s *blockfmt.SparseIndex, n int) Ternary {
			a, b := left(s, n), right(s, n)
			if a == Maybe || b == Maybe {
				return Maybe
			}
			if a == b {
				return Never
			}
			return Always
		}, true
	}
	return nil, false
}

// compileComparisonFilter compiles a filter from a
// comparison expression.
func compileComparisonFilter(e *expr.Comparison) (Filter, bool) {
	op := e.Op
	left := e.Left
	right := e.Right

	// if there is a literal on the left side, flip the expression and swap
	// left/right so we always compare with literals on the right side.
	if op.Ordinal() {
		switch left.(type) {
		case expr.Integer, expr.Float, expr.String, *expr.Timestamp:
			left, right = right, left
			op = op.Flip()
		}
	}

	leftPath, leftIsPath := left.(*expr.Path)
	if leftIsPath {
		switch right := right.(type) {
		case *expr.Timestamp:
			cmp := compareFunc(op, right.Value)
			if cmp == nil {
				return nil, false
			}
			return pathFilter(leftPath, cmp), true
		}
	}

	fn, ok1 := left.(*expr.Builtin)
	im, ok2 := right.(expr.Integer)
	if !ok1 || !ok2 {
		return nil, false
	}

	if len(fn.Args) != 1 {
		return nil, false
	}
	path, ok := fn.Args[0].(*expr.Path)
	if !ok {
		return nil, false
	}

	switch fn.Func {
	case expr.ToUnixEpoch:
		when := date.Unix(int64(im), 0)
		cmp := compareFunc(op, when)
		if cmp == nil {
			return nil, false
		}
		return pathFilter(path, cmp), true
	case expr.ToUnixMicro:
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
func compareFunc(op expr.CmpOp, when date.Time) func(*blockfmt.TimeIndex, int) Ternary {
	const epsilon = time.Microsecond
	switch op {
	case expr.Equals:
		return func(i *blockfmt.TimeIndex, n int) Ternary {
			if i.Contains(when) {
				return Maybe
			}
			return Never
		}
	case expr.NotEquals:
		// complicated due to partiql semantics
		return nil
	case expr.Less:
		// when < expr
		return pickBefore(when)
	case expr.LessEquals:
		// when <= expr
		return pickBefore(when.Add(epsilon))
	case expr.Greater:
		// when > expr
		return pickAfter(when)
	case expr.GreaterEquals:
		// when >= expr
		return pickAfter(when.Add(-epsilon))
	}
	return nil
}

// compileBuiltin compiles a filter from a builtin
// expression.
func compileBuiltin(e *expr.Builtin) (Filter, bool) {
	return nil, false
}

// pickAfter returns a function that evaluates
// whether or not the provided block and time index
// contain a value greater than or equal to when
func pickAfter(when date.Time) func(*blockfmt.TimeIndex, int) Ternary {
	return func(i *blockfmt.TimeIndex, n int) Ternary {
		// exclude blocks to the left of the block
		// where max(block) < ts
		if n < i.Start(when) {
			return Never
		}
		if n < i.End(when) {
			return Maybe
		}
		return Always
	}
}

// pickBefore returns a function that
// evaluates whether or not the provided block
// and time index contain a value less than when
func pickBefore(when date.Time) func(*blockfmt.TimeIndex, int) Ternary {
	return func(i *blockfmt.TimeIndex, n int) Ternary {
		if n < i.Start(when) {
			return Always
		}
		if n < i.End(when) {
			return Maybe
		}
		return Never
	}
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
func pathFilter(path *expr.Path, fn func(*blockfmt.TimeIndex, int) Ternary) Filter {
	flat, ok := flatpath(path)
	if !ok {
		return nil
	}
	return func(s *blockfmt.SparseIndex, i int) Ternary {
		tr := s.Get(flat)
		if tr == nil {
			return Maybe
		}
		return fn(tr, i)
	}
}
