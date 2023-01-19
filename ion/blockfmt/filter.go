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
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
)

// Filter represents a compiled condition that
// can be evaluated against a SparseIndex to
// produce intervals that match the compiled condition.
type Filter struct {
	eval  evalfn // entry point
	grow  cont   // return point
	paths []string

	// union of intervals to traverse;
	// by convention these are always non-empty
	intervals [][2]int
}

type evalfn func(f *Filter, si *SparseIndex, rest cont)
type cont func(start, end int)

// Compile sets the expression that the filter should evaluate.
// A call to Compile erases any previously-compiled expression.
func (f *Filter) Compile(e expr.Node) {
	f.paths = f.paths[:0]
	f.eval = f.compile(e)
}

// Trivial returns true if the compiled filter
// condition will never select non-trivial
// subranges of the input slice in Visit.
// (In other words, a trivial filter will
// always visit all the blocks in a sparse index.)
func (f *Filter) Trivial() bool { return f.eval == nil }

// compile left and right, then replace the compiled
// expressions with a single expression that computes
// the intersection of the two ranges computed by
// left and right
func (f *Filter) intersect(left, right expr.Node) evalfn {
	lhs := f.compile(left)
	rhs := f.compile(right)
	if lhs == nil {
		return rhs
	} else if rhs == nil {
		return lhs
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		lhs(f, si, func(lo, hi int) {
			rhs(f, si, func(rlo, rhi int) {
				nlo := lo
				if rlo > lo {
					nlo = rlo
				}
				nhi := hi
				if rhi < hi {
					nhi = rhi
				}
				if nlo < nhi {
					rest(nlo, nhi)
				}
			})
		})
	}
}

// filter where p <= when
func (f *Filter) beforeeq(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(0, si.Blocks())
			return
		}
		end := ti.End(when)
		if end == 0 {
			return
		}
		rest(0, end)
	}
}

// filter where p >= when
func (f *Filter) aftereq(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(0, si.Blocks())
			return
		}
		start := ti.Start(when)
		if start == ti.Blocks() {
			return
		}
		rest(start, ti.Blocks())
	}
}

func (f *Filter) within(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(0, si.Blocks())
			return
		}
		start, end := ti.Start(when), ti.End(when.Add(time.Microsecond))
		if start == end {
			return
		}
		rest(start, end)
	}
}

func (f *Filter) eqstring(p []string, str expr.String) evalfn {
	if len(p) != 1 {
		return nil
	}
	eq := func(s expr.String, d ion.Datum) bool {
		s2, ok := d.String()
		return ok && string(s) == s2
	}
	name := p[0]
	return func(f *Filter, si *SparseIndex, rest cont) {
		field, ok := si.consts.FieldByName(name)
		if !ok || eq(str, field.Datum) {
			rest(0, si.Blocks())
		}
	}
}

func (f *Filter) eqint(p []string, n expr.Integer) evalfn {
	if len(p) != 1 {
		return nil
	}
	name := p[0]
	eq := func(n expr.Integer, d ion.Datum) bool {
		n2, ok := d.Int()
		if ok {
			return int64(n) == n2
		}
		u2, ok := d.Uint()
		return ok && n >= 0 && uint64(n) == u2
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		field, ok := si.consts.FieldByName(name)
		if !ok || eq(n, field.Datum) {
			rest(0, si.Blocks())
		}
	}
}

// filter where !e
func (f *Filter) negate(e expr.Node) evalfn {
	// we expect DNF ("disjunctive normal form"),
	// so if we have a negation of a disjunction we
	// need to turn it into a conjunction instead
	if or, ok := e.(*expr.Logical); ok && or.Op == expr.OpOr {
		// !(A OR B) -> !A AND !B
		// which in turn becomes
		//   (A-left OR A-right) AND (B-left OR B-right)
		// which is then
		//   (A-left AND B-left) OR (A-left AND B-right) OR
		//   (A-right AND B-left) OR (A-right AND B-right)
		return f.intersect(&expr.Not{or.Left}, &expr.Not{or.Right})
	}
	inner := f.compile(e)
	if inner == nil {
		return nil
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		inner(f, si, func(x, y int) {
			if x > 0 {
				rest(0, x)
			}
			if y < si.Blocks() {
				rest(y, si.Blocks())
			}
		})
	}
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

func (f *Filter) union(a, b expr.Node) evalfn {
	part0 := f.compile(a)
	part1 := f.compile(b)
	if part0 == nil {
		return part1
	} else if part1 == nil {
		return part0
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		part0(f, si, rest)
		part1(f, si, rest)
	}
}

func (f *Filter) compile(e expr.Node) evalfn {
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
		p, ok := expr.FlatPath(e.Left)
		if !ok {
			if b, ok := e.Left.(*expr.Builtin); ok {
				switch b.Func {
				case expr.ToUnixEpoch:
					p, ok = expr.FlatPath(b.Args[0])
					if !ok {
						return nil
					}
					conv = toUnixEpoch
				case expr.ToUnixMicro:
					p, ok = expr.FlatPath(b.Args[0])
					if !ok {
						return nil
					}
					conv = toUnixMicro
				default:
					return nil
				}
			} else {
				return nil
			}
		} else if e.Op == expr.Equals {
			// special handling for row constants
			switch rhs := e.Right.(type) {
			case *expr.Timestamp:
				// continue on to timestamp handling
			case expr.String:
				return f.eqstring(p, rhs)
			case expr.Integer:
				// TODO: support more than just
				// equality comparisons
				return f.eqint(p, rhs)
			default:
				return nil
			}
		}
		ts := conv(e.Right)
		if ts == nil {
			return nil
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
	return nil
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

func (f *Filter) run(si *SparseIndex) {
	f.intervals = f.intervals[:0]
	if f.eval == nil {
		return
	}
	if f.grow == nil {
		f.grow = func(x, y int) {
			f.intervals = append(f.intervals, [2]int{x, y})
		}
	}
	f.eval(f, si, f.grow)
	f.compress()
}

// Overlaps returns whether or not the sparse
// index matches the predicate within the half-open
// interval [start, end)
//
// The behavior of Overlaps when start >= end is unspecified.
func (f *Filter) Overlaps(si *SparseIndex, start, end int) bool {
	// no known bounds:
	if f.eval == nil {
		return true
	}
	if si.Blocks() == 0 { // hmm...
		return start == 0
	}
	f.run(si)
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
	f.run(si)
	return len(f.intervals) > 0
}

// Visit visits distinct (non-overlapping) intervals
// within si that correspond to the compiled filter.
//
// When no range within si matches the filter,
// interval will be called once with (0, 0).
func (f *Filter) Visit(si *SparseIndex, interval func(start, end int)) {
	// no known bounds:
	if f.eval == nil || si.Blocks() == 0 {
		interval(0, si.Blocks())
		return
	}
	f.run(si)
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
