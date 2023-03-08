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
	eval evalfn // entry point

	// union of intervals to traverse;
	// by convention these are always non-empty
	intervals [][2]int
}

type evalfn func(f *Filter, si *SparseIndex, rest cont)
type cont func(f *Filter, start, end int)

// Compile sets the expression that the filter should evaluate.
// A call to Compile erases any previously-compiled expression.
func (f *Filter) Compile(e expr.Node) {
	f.eval = filtcompile(e)
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
func filtintersect(left, right expr.Node) evalfn {
	lhs := filtcompile(left)
	rhs := filtcompile(right)
	if lhs == nil {
		return rhs
	} else if rhs == nil {
		return lhs
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		lhs(f, si, func(f *Filter, lo, hi int) {
			rhs(f, si, func(f *Filter, rlo, rhi int) {
				nlo := lo
				if rlo > lo {
					nlo = rlo
				}
				nhi := hi
				if rhi < hi {
					nhi = rhi
				}
				if nlo < nhi {
					rest(f, nlo, nhi)
				}
			})
		})
	}
}

// filter where p <= when
func filtbeforeeq(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(f, 0, si.Blocks())
			return
		}
		end := ti.End(when)
		if end == 0 {
			return
		}
		rest(f, 0, end)
	}
}

// filter where p >= when
func filtaftereq(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(f, 0, si.Blocks())
			return
		}
		start := ti.Start(when)
		if start == ti.Blocks() {
			return
		}
		rest(f, start, ti.Blocks())
	}
}

func filtwithin(path []string, when date.Time) evalfn {
	return func(f *Filter, si *SparseIndex, rest cont) {
		ti := si.Get(path)
		if ti == nil {
			rest(f, 0, si.Blocks())
			return
		}
		start, end := ti.Start(when), ti.End(when.Add(time.Microsecond))
		if start == end {
			return
		}
		rest(f, start, end)
	}
}

func filteqstring(p []string, str expr.String) evalfn {
	if len(p) != 1 {
		return nil
	}
	eq := func(s expr.String, d ion.Datum) bool {
		if d.IsSymbol() {
			s2, _ := d.String()
			return string(s) == s2
		}
		if d.IsString() {
			s2, _ := d.StringShared()
			return string(s) == string(s2)
		}
		return false
	}
	name := p[0]
	return func(f *Filter, si *SparseIndex, rest cont) {
		field, ok := si.consts.FieldByName(name)
		if !ok || eq(str, field.Datum) {
			rest(f, 0, si.Blocks())
		}
	}
}

func filteqint(p []string, n expr.Integer) evalfn {
	if len(p) != 1 {
		return nil
	}
	name := p[0]
	eq := func(n expr.Integer, d ion.Datum) bool {
		if d.IsInt() {
			n2, _ := d.Int()
			return int64(n) == n2
		}
		if d.IsUint() && n >= 0 {
			u2, _ := d.Uint()
			return uint64(n) == u2
		}
		return false
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		field, ok := si.consts.FieldByName(name)
		if !ok || eq(n, field.Datum) {
			rest(f, 0, si.Blocks())
		}
	}
}

func filtcontains(p []string, set *ion.Bag) evalfn {
	if len(p) != 1 {
		return nil
	}
	name := p[0]
	match := func(d ion.Datum) bool {
		any := false
		set.Each(func(val ion.Datum) bool {
			if d.Equal(val) {
				any = true
				return false
			}
			return !any
		})
		return any
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		field, ok := si.consts.FieldByName(name)
		if !ok || match(field.Datum) {
			rest(f, 0, si.Blocks())
		}
	}
}

// filter where !e
func filtnegate(e expr.Node) evalfn {
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
		return filtintersect(&expr.Not{or.Left}, &expr.Not{or.Right})
	}
	inner := filtcompile(e)
	if inner == nil {
		return nil
	}
	return func(f *Filter, si *SparseIndex, rest cont) {
		inner(f, si, func(f *Filter, x, y int) {
			if x > 0 {
				rest(f, 0, x)
			}
			if y < si.Blocks() {
				rest(f, y, si.Blocks())
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

func filtunion(a, b expr.Node) evalfn {
	part0 := filtcompile(a)
	part1 := filtcompile(b)
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

func filtcompile(e expr.Node) evalfn {
	switch e := e.(type) {
	case *expr.Member:
		p, ok := expr.FlatPath(e.Arg)
		if ok {
			return filtcontains(p, &e.Set)
		}
	case *expr.Not:
		return filtnegate(e.Expr)
	case *expr.Logical:
		switch e.Op {
		case expr.OpAnd:
			return filtintersect(e.Left, e.Right)
		case expr.OpOr:
			return filtunion(e.Left, e.Right)
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
				return filteqstring(p, rhs)
			case expr.Integer:
				// TODO: support more than just
				// equality comparisons
				return filteqint(p, rhs)
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
			return filtwithin(p, ts.Value)
		case expr.Less:
			return filtbeforeeq(p, ts.Value.Add(-epsilon))
		case expr.LessEquals:
			return filtbeforeeq(p, ts.Value)
		case expr.Greater:
			return filtaftereq(p, ts.Value.Add(epsilon))
		case expr.GreaterEquals:
			return filtaftereq(p, ts.Value)
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
	f.eval(f, si, func(f *Filter, start, end int) {
		f.intervals = append(f.intervals, [2]int{start, end})
	})
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
