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

package pir

import (
	"github.com/SnellerInc/sneller/expr"
)

func filterelim(b *Trace) {
	var child Step
	for s := b.top; s != nil; s = s.parent() {
		f, ok := s.(*Filter)
		if !ok {
			child = s
			continue
		}
		if f.Where == expr.Bool(true) {
			// splice out this filter entirely
			if child == nil {
				b.top = s.parent()
			} else {
				child.setparent(s.parent())
			}
		} else if f.Where == expr.Bool(false) {
			b.top = NoOutput{}
			return
		}
		child = s
	}
}

func conjoin(x, y expr.Node, at Step) expr.Node {
	if x == nil {
		return y
	}
	if y == nil {
		return x
	}
	return expr.SimplifyLogic(expr.And(x, y), &stepHint{parent: at.parent()})
}

// determine if 'e' has no references to 'step'
//
// n.b. false negatives are okay here; they will
// just end up inhibiting optimizations
func doesNotReference(e expr.Node, bind string) bool {
	ref := false
	visit := expr.WalkFunc(func(e expr.Node) bool {
		if ref {
			return false
		}
		id, ok := e.(expr.Ident)
		if ok && string(id) == bind {
			ref = true
		}
		return !ok
	})
	expr.Walk(visit, e)
	return !ref
}

// determine if 'e' only references one binding
func onlyReferences(e expr.Node, bind string) bool {
	ref := true
	visit := expr.WalkFunc(func(e expr.Node) bool {
		if !ref {
			return false
		}
		id, ok := e.(expr.Ident)
		if ok && string(id) != bind {
			ref = false
		}
		return !ok
	})
	expr.Walk(visit, e)
	return ref
}

// tables accept filters directly
func (i *IterTable) filter(e expr.Node, scope *Trace) {
	i.Filter = conjoin(i.Filter, e, i)
}

// filters accept filters directly (duh)
func (f *Filter) filter(e expr.Node, scope *Trace) {
	f.Where = conjoin(f.Where, e, f)
}

// bindings can be filtered by replacing
// the output bindings in 'e' with the
// expressions that produce the bindings in 'b'
//
// NOTE: it's possible this is a pessimization
// if the binding ends up computing a value we
// need inside the WHERE, because we effectively
// end up computing the value twice; the assumption
// here is that the WHERE clause eliminates a large
// percentage of the input rows, and therefore the
// redundant computation is negligible compared to
// the the reduction in total projection size
func (b *Bind) filter(e expr.Node, scope *Trace) {
	e = expr.Rewrite(&bindflattener{from: b.bind}, e)
	f := new(Filter)
	f.Where = expr.SimplifyLogic(e, &stepHint{parent: b.parent()})
	f.setparent(b.parent())
	b.setparent(f)
}

func forcepush(where expr.Node, at Step, scope *Trace) Step {
	type filterer interface {
		filter(e expr.Node, s *Trace)
	}
	if fi, ok := at.(filterer); ok {
		fi.filter(where, scope)
		return at
	}
	f := new(Filter)
	f.Where = where
	f.setparent(at)
	return f
}

// unconditionally push ordinary filters ahead of DISTINCT,
// since DISTINCT is almost always more expensive to evaluate
func (d *Distinct) filter(e expr.Node, _ *Trace) {
	f := new(Filter)
	f.Where = e
	f.setparent(d.parent())
	d.setparent(f)
}

// push a filter into its parent step
func push(f *Filter, dst Step, s *Trace) bool {
	type filterer interface {
		filter(e expr.Node, s *Trace)
	}

	// handle filtering through an EquiJoin;
	// most of this is handled by EquiJoin.filterOne
	if eqj, ok := dst.(*EquiJoin); ok {
		conj := conjunctions(f.Where, nil)
		var remaining expr.Node
		for j := range conj {
			if eqj.filterOne(conj[j], s) {
				continue
			}
			if remaining == nil {
				remaining = conj[j]
			} else {
				remaining = conjoin(remaining, conj[j], eqj)
			}
		}
		if remaining == nil {
			return true
		}
		f.Where = remaining
		return false
	}

	// this is an unusual case because we
	// can only push down *part* of the filter:
	if iv, ok := dst.(*IterValue); ok {
		conj := conjunctions(f.Where, nil)
		par := iv.parent()
		newparent := false
		var remaining expr.Node
		for j := range conj {
			if doesNotReference(conj[j], iv.Result) {
				par = forcepush(conj[j], par, s)
				newparent = true
			} else {
				if remaining == nil {
					remaining = conj[j]
				} else {
					remaining = conjoin(remaining, conj[j], iv)
				}
			}
		}
		if newparent {
			iv.setparent(par)
		}
		if remaining == nil {
			return true
		}
		f.Where = remaining
		return false
	}

	// in some cases we can always push:
	if fi, ok := dst.(filterer); ok {
		fi.filter(f.Where, s)
		return true
	}
	return false
}

// simple filter push-down:
// merge adjacent filter steps into single ones,
// and merge filters into table iteration steps
func filterpushdown(b *Trace) {
	var child Step
	for s := b.top; s != nil; s = s.parent() {
		f, ok := s.(*Filter)
		if !ok {
			child = s
			continue
		}
		parent := f.parent()
		if push(f, parent, b) {
			if child == nil {
				b.top = parent
			} else {
				child.setparent(parent)
			}
		} else {
			child = s
		}
	}
}
