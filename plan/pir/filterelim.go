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

func conjoin(x, y expr.Node, scope *Trace) expr.Node {
	if x == nil {
		return y
	}
	if y == nil {
		return x
	}
	return expr.SimplifyLogic(expr.And(x, y), scope)
}

type visitfn func(expr.Node) bool

func (v visitfn) Visit(e expr.Node) expr.Visitor {
	if v(e) {
		return v
	}
	return nil
}

// determine if 'e' has no references to 'step'
//
// n.b. false negatives are okay here; they will
// just end up inhibiting optimizations
func doesNotReference(e expr.Node, step Step, scope *Trace) bool {
	ref := false
	visit := func(e expr.Node) bool {
		if ref {
			return false
		}
		p, ok := e.(*expr.Path)
		if !ok {
			return true
		}
		if scope.origin(p) == step {
			ref = true
		}
		return false
	}
	expr.Walk(visitfn(visit), e)
	return !ref
}

// tables accept filters directly
func (i *IterTable) filter(e expr.Node, scope *Trace) {
	i.Filter = conjoin(i.Filter, e, scope)
}

// filters accept filters directly (duh)
func (f *Filter) filter(e expr.Node, scope *Trace) {
	f.Where = conjoin(f.Where, e, scope)
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
	f.Where = expr.SimplifyLogic(e, scope)
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
	f.setparent(at.parent())
	at.setparent(f)
	return f
}

// IterValue accepts filters directly,
// but we push down parts of the filter that
// do not reference the inner values into
// a parent node (which may be subsequently pushed down)
func (i *IterValue) filter(e expr.Node, scope *Trace) {
	conj := conjunctions(e, nil)
	par := i.parent()
	for j := range conj {
		if doesNotReference(conj[j], i, scope) {
			par = forcepush(conj[j], par, scope)
		} else {
			i.Filter = conjoin(i.Filter, conj[j], scope)
		}
	}
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
