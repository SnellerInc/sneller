// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package blockfmt

import (
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
)

// Filter represents a compiled condition that
// can be evaluated against a SparseIndex to
// produce intervals that match the compiled condition.
type Filter struct {
	eval evalfn // entry point

	// union of intervals to traverse;
	// by convention these are always non-empty
	intervals ints.Intervals
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

func (f *Filter) run(si *SparseIndex) {
	f.intervals = f.intervals[:0]
	if f.eval == nil {
		return
	}
	f.eval(f, si, func(f *Filter, start, end int) {
		f.intervals = append(f.intervals, ints.Interval{start, end})
	})
	f.intervals.Compress()
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
	return f.intervals.Overlaps(start, end)
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

// Intervals returns the intervals within [si]
// that match the filter.
func (f *Filter) Intervals(si *SparseIndex) ints.Intervals {
	f.run(si)
	in := f.intervals
	f.intervals = nil
	return in
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
	f.intervals.Visit(interval)
}
