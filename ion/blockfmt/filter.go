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

package blockfmt

import (
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"

	"golang.org/x/exp/slices"
)

// Filter represents a compiled condition that
// can be evaluated against a SparseIndex to
// produce intervals that match the compiled condition.
type Filter struct {
	// functions that produce time ranges;
	// executing these in a loop should produce
	// the intervals
	exprs []func(f *Filter, si *SparseIndex, pc int)
	paths []string

	// union of intervals to traverse;
	// by convention these are always non-empty
	intervals [][2]int
}

func (f *Filter) path(p *expr.Path) []string {
	l := len(f.paths)
	more := flatpath(p, f.paths)
	if more != nil {
		f.paths = more
		return f.paths[l:]
	}
	return nil
}

// Compile sets the expression that the filter should evaluate.
// A call to Compile erases any previously-compiled expression.
func (f *Filter) Compile(e expr.Node) {
	f.exprs = f.exprs[:0]
	f.paths = f.paths[:0]
	f.compile(e)
}

// Trivial returns true if the compiled filter
// condition will never select non-trivial
// subranges of the input slice in Visit.
// (In other words, a trivial filter will
// always visit all the blocks in a sparse index.)
func (f *Filter) Trivial() bool { return len(f.exprs) == 0 }

func (f *Filter) popRange() [2]int {
	ret := f.intervals[len(f.intervals)-1]
	f.intervals = f.intervals[:len(f.intervals)-1]
	return ret
}

func (f *Filter) pushIntersect() bool {
	last := f.popRange()
	prev := f.popRange()
	lo, hi := last[0], last[1]
	if prev[0] > lo {
		lo = prev[0]
	}
	if prev[1] < hi {
		hi = prev[1]
	}
	if lo < hi {
		f.push(lo, hi)
		return true
	}
	return false
}

func (f *Filter) pushExpr(fn func(f *Filter, si *SparseIndex, pc int)) {
	f.exprs = append(f.exprs, fn)
}

func (f *Filter) cont(si *SparseIndex, pc int) {
	if pc >= len(f.exprs) {
		return
	}
	f.exprs[pc](f, si, pc+1)
}

// compile left and right, then replace the compiled
// expressions with a single expression that computes
// the intersection of the two ranges computed by
// left and right
func (f *Filter) intersect(left, right expr.Node) bool {
	if !f.compile(left) {
		// any AND right -> right
		return f.compile(right)
	}
	if !f.compile(right) {
		return true // left AND any -> left
	}
	f.pushExpr(func(f *Filter, si *SparseIndex, pc int) {
		if f.pushIntersect() {
			f.cont(si, pc)
		}
	})
	return true
}

func (f *Filter) evalAny(blocks int) {
	f.push(0, blocks)
}

func (f *Filter) push(start, end int) {
	if end <= start {
		panic("blockfmt.Filter: push() of invalid range")
	}
	f.intervals = append(f.intervals, [2]int{start, end})
}

// filter where p <= when
func (f *Filter) beforeeq(p *expr.Path, when date.Time) bool {
	path := f.path(p)
	if path == nil {
		return false
	}
	f.exprs = append(f.exprs, func(f *Filter, si *SparseIndex, pc int) {
		ti := si.Get(path)
		if ti == nil {
			f.evalAny(si.Blocks())
		} else {
			end := ti.End(when)
			if end == 0 {
				return
			}
			f.push(0, end)
		}
		f.cont(si, pc)
	})
	return true
}

// filter where p >= when
func (f *Filter) aftereq(p *expr.Path, when date.Time) bool {
	path := f.path(p)
	if path == nil {
		return false
	}
	f.exprs = append(f.exprs, func(f *Filter, si *SparseIndex, pc int) {
		ti := si.Get(path)
		if ti == nil {
			f.evalAny(si.Blocks())
		} else {
			start := ti.Start(when)
			if start == ti.Blocks() {
				return
			}
			f.push(start, ti.Blocks())
		}
		f.cont(si, pc)
	})
	return true
}

func (f *Filter) within(p *expr.Path, when date.Time) bool {
	path := f.path(p)
	if path == nil {
		return false
	}
	f.exprs = append(f.exprs, func(f *Filter, si *SparseIndex, pc int) {
		ti := si.Get(path)
		if ti == nil {
			f.evalAny(si.Blocks())
		} else {
			start, end := ti.Start(when), ti.End(when.Add(time.Microsecond))
			if start == end {
				return
			}
			f.push(start, end)
		}
		f.cont(si, pc)
	})
	return true
}

// filter where !e
func (f *Filter) negate(e expr.Node) bool {
	// we expect DNF ("disjunctive normal form"),
	// so if we have a negation of a disjunction we
	// need to turn it into a conjunction instead
	if or, ok := e.(*expr.Logical); ok && or.Op == expr.OpOr {
		// !(A OR B) -> !A AND !B
		// which in turn becomes
		//   (A-left OR A-right) AND (B-left OR B-right)
		return f.intersect(&expr.Not{or.Left}, &expr.Not{or.Right})
	}
	if !f.compile(e) {
		return false
	}
	// !range = left-of-min OR right-of-max
	f.pushExpr(func(f *Filter, si *SparseIndex, pc int) {
		iv := f.popRange()
		if iv[0] > 0 {
			f.push(0, iv[0])
			f.cont(si, pc)
		}
		if iv[1] < si.Blocks() {
			f.push(iv[1], si.Blocks())
			f.cont(si, pc)
		}
	})
	return true
}

func toUnixEpoch(e expr.Node) *expr.Timestamp {
	if i, ok := e.(expr.Integer); ok {
		return &expr.Timestamp{date.Unix(int64(i), 0)}
	}
	return nil
}

func toUnixMicro(e expr.Node) *expr.Timestamp {
	if i, ok := e.(expr.Integer); ok {
		return &expr.Timestamp{date.UnixMicro(int64(i))}
	}
	return nil
}

func (f *Filter) union(a, b expr.Node) bool {
	if !f.compile(a) {
		return f.compile(b)
	}
	if !f.compile(b) {
		return true
	}
	// execute the rest of the program twice
	// (each invocation should pop 1 value)
	f.exprs = append(f.exprs, func(f *Filter, si *SparseIndex, pc int) {
		save := f.popRange()
		f.cont(si, pc)
		f.push(save[0], save[1])
		f.cont(si, pc)
	})
	return true
}

func (f *Filter) compile(e expr.Node) bool {
	switch e := e.(type) {
	case *expr.Not:
		return f.negate(e.Expr)
	case *expr.Logical:
		switch e.Op {
		case expr.OpAnd:
			return f.intersect(e.Left, e.Right)
		case expr.OpOr:
			return f.union(e.Left, e.Right)
		}
	case *expr.Comparison:
		conv := func(e expr.Node) *expr.Timestamp {
			ts, _ := e.(*expr.Timestamp)
			return ts
		}
		// note: expr normalizes constant comparisons
		// such that the constant appears on the rhs
		p, ok := e.Left.(*expr.Path)
		if !ok {
			if b, ok := e.Left.(*expr.Builtin); ok {
				switch b.Func {
				case expr.ToUnixEpoch:
					p, ok = b.Args[0].(*expr.Path)
					if !ok {
						return false
					}
					conv = toUnixEpoch
				case expr.ToUnixMicro:
					p, ok = b.Args[0].(*expr.Path)
					if !ok {
						return false
					}
					conv = toUnixMicro
				default:
					return false
				}
			} else {
				return false
			}
		}
		ts := conv(e.Right)
		if ts == nil {
			return false
		}
		const epsilon = time.Microsecond
		switch e.Op {
		case expr.Equals:
			return f.within(p, ts.Value)
		case expr.Less:
			return f.beforeeq(p, ts.Value.Add(-epsilon))
		case expr.LessEquals:
			return f.beforeeq(p, ts.Value)
		case expr.Greater:
			return f.aftereq(p, ts.Value.Add(epsilon))
		case expr.GreaterEquals:
			return f.aftereq(p, ts.Value)
		}
	}
	return false
}

func (f *Filter) compress() {
	// sort by start, then by end
	slices.SortFunc(f.intervals, func(x, y [2]int) bool {
		if x[0] == y[0] {
			return x[1] < y[1]
		}
		return x[0] < y[0]
	})
	// remove duplicate ranges
	f.intervals = slices.Compact(f.intervals)

	// compress overlapping ranges
	oranges := f.intervals[:0]
	for i := 0; i < len(f.intervals); i++ {
		merged := 0
		// while the next-highest start range
		// starts below the current ranges' max,
		// collapse the ranges together
		for j := i + 1; j < len(f.intervals); j++ {
			if f.intervals[j][0] > f.intervals[i][1] {
				break
			}
			// expand interval (sort guarantees [i][1] < [j][1])
			f.intervals[i][1] = f.intervals[j][1]
			merged++
		}
		oranges = append(oranges, f.intervals[i])
		i += merged
	}
	f.intervals = oranges
}

func (f *Filter) eval(si *SparseIndex) {
	f.intervals = f.intervals[:0]
	f.cont(si, 0)
	f.compress()
}

// Overlaps returns whether or not the sparse
// index matches the predicate within the half-open
// interval [start, end)
//
// The behavior of Overlaps when start >= end is unspecified.
func (f *Filter) Overlaps(si *SparseIndex, start, end int) bool {
	// no known bounds:
	if len(f.exprs) == 0 {
		return true
	}
	if si.Blocks() == 0 { // hmm...
		return start == 0
	}
	f.eval(si)
	for i := range f.intervals {
		// ends before start: doesn't overlap
		if f.intervals[i][1] <= start {
			continue
		}
		// starts after end: done
		if f.intervals[i][0] >= end {
			break
		}
		// we know f.intervals[i][1] > start
		//      or f.intervals[i][0] < end
		return true
	}
	return false
}

// MatchesAny returns true if f matches any non-empty
// intervals in si, or false otherwise.
func (f *Filter) MatchesAny(si *SparseIndex) bool {
	if f.Trivial() || si.Blocks() == 0 {
		return true
	}
	f.eval(si)
	return len(f.intervals) > 0
}

// Visit visits distinct (non-overlapping) intervals
// within si that correspond to the compiled filter.
//
// When no range within si matches the filter,
// interval will be called once with (0, 0).
func (f *Filter) Visit(si *SparseIndex, interval func(start, end int)) {
	// no known bounds:
	if len(f.exprs) == 0 || si.Blocks() == 0 {
		interval(0, si.Blocks())
		return
	}
	f.eval(si)
	if len(f.intervals) == 0 {
		// no non-empty intervals
		interval(0, 0)
		return
	}
	for i := range f.intervals {
		start := f.intervals[i][0]
		end := f.intervals[i][1]
		interval(start, end)
	}
}
