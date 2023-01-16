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
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/exp/maps"
)

func deleteAt[T any](x []T, i int) []T {
	x[i] = x[len(x)-1]
	return x[:len(x)-1]
}

func filterSlice[T any](x []T, keep func(*T) bool) []T {
	for i := 0; i < len(x); i++ {
		if !keep(&x[i]) {
			x = deleteAt(x, i)
			i--
		}
	}
	return x
}

type walkfn func(e expr.Node)

func (w walkfn) Visit(e expr.Node) expr.Visitor {
	w(e)
	return w
}

func columns(b []expr.Binding) []expr.Node {
	out := make([]expr.Node, len(b))
	for i := range b {
		out[i] = b[i].Expr
	}
	return out
}

// for each Bind step, eliminate bindings
// that are not referenced in subsequent steps
func projectelim(b *Trace) {
	// build a reverse-lookup from step
	// to path bindings that have been resolved
	// from other subsequent steps
	used := make(map[string]struct{})

	// make each path expression we see "used"
	walk := func(e expr.Node) {
		p, ok := e.(expr.Ident)
		if ok {
			used[string(p)] = struct{}{}
		}
	}

	// eliminiate unused bindings for each bind pass
	first := true
	var parent Step
loop:
	for s := b.top; s != nil; s = s.parent() {
		switch s := s.(type) {
		case *Bind:
			if first {
				first = false
				break
			}
			s.bind = filterSlice(s.bind, func(b *expr.Binding) bool {
				_, ok := used[b.Result()]
				return ok
			})
			maps.Clear(used)
			if len(s.bind) == 0 {
				// inconsequential PROJECT; usually we are
				// being passed to COUNT(*)
				parent.setparent(s.parent())
				continue loop
			}
			first = false
		case *Aggregate:
			if first {
				first = false
				break
			}
			s.Agg = filterSlice(s.Agg, func(ab *vm.AggBinding) bool {
				_, ok := used[ab.Result]
				return ok
			})
			if len(s.Agg) == 0 {
				if len(s.GroupBy) == 0 {
					parent.setparent(DummyOutput{})
				} else {
					// GROUP BY ... -> DISTINCT ...
					d := &Distinct{Columns: columns(s.GroupBy)}
					d.setparent(s.parent())
					b := &Bind{binds: binds{s.GroupBy}, complete: true}
					b.setparent(d)
					parent.setparent(b)
				}
			}
			maps.Clear(used)
			first = false
		case *IterTable:
			if first {
				first = false
				break
			}

			s.trim(used)
			first = false

		case *IterValue:
			if first {
				first = false
				break
			}
			if _, ok := used[s.Result]; !ok {
				// cross-join result isn't used
				parent.setparent(s.parent())
				continue loop
			}
			// we are *not* clearing used because
			// we only introduce 1 new binding;
			// we do not overwrite existing bindings
			first = false
		case *Unpivot, *UnpivotAtDistinct:
			return // all incoming fields are used
		default:
			// nothing
		}
		s.walk(walkfn(walk))
		parent = s
	}
}

type bindflattener struct {
	from []expr.Binding
}

// turn expr.Ident -> associated bound value
func (b *bindflattener) Rewrite(e expr.Node) expr.Node {
	id, ok := e.(expr.Ident)
	if !ok {
		return e
	}
	var into expr.Node
	for i := range b.from {
		if b.from[i].Result() == string(id) {
			into = b.from[i].Expr
			break
		}
	}
	if into == nil {
		return e // probably shouldn't happen
	}
	return into
}

func (b *bindflattener) Walk(e expr.Node) expr.Rewriter {
	return b
}

// simple projection push-down:
// merge adjacent projections into
// a single extended projection
func projectpushdown(scope *Trace) {
outer:
	for s := scope.top; s != nil; s = s.parent() {
		var rewrite func(bf *bindflattener)
		switch s := s.(type) {
		case *Bind:
			rewrite = func(bf *bindflattener) {
				h := &stepHint{s.parent()}
				for i := range s.bind {
					s.bind[i].Expr = expr.Simplify(expr.Rewrite(bf, s.bind[i].Expr), h)
				}
			}
		case *Aggregate:
			rewrite = func(bf *bindflattener) {
				h := &stepHint{s.parent()}
				for i := range s.Agg {
					s.Agg[i].Expr = expr.Simplify(expr.Rewrite(bf, s.Agg[i].Expr), h).(*expr.Aggregate)
				}
				for i := range s.GroupBy {
					s.GroupBy[i].Expr = expr.Simplify(expr.Rewrite(bf, s.GroupBy[i].Expr), h)
				}
			}
		default:
			continue
		}
		// while the parent node is a Bind,
		// merge the results into a single
		// extended projection
		for {
			p := s.parent()
			pb, ok := p.(*Bind)
			if !ok {
				continue outer
			}
			rw := bindflattener{from: pb.bind}
			rewrite(&rw)
			s.setparent(pb.parent())
		}
	}
}

func bindings(s Step) []expr.Binding {
	switch s := s.(type) {
	case *Bind:
		return s.binds.bind
	default:
		// TODO: disallow aggregate references
		// to columns that are produced as part
		// of the aggregation?
		return nil
	}
}

func flatten(b *Trace) {
	for s := b.top; s != nil; s = s.parent() {
		bind := bindings(s)
		if bind == nil {
			continue
		}
		flattenBind(bind)
	}
}

// if we have a Bind that follows
// an Aggregate and the binding is
// just performing trivial re-naming,
// then push the names into the aggregate
// and eliminate the Bind
func liftprojectagg(b *Trace) {
	var child Step
outer:
	for s := b.top; s != nil; s = s.parent() {
		bi, ok := s.(*Bind)
		if !ok {
			child = s
			continue
		}
		par := bi.parent()
		ag, ok := par.(*Aggregate)
		if !ok {
			child = s
			continue
		}
		// in order for the binding to be trivial,
		// we must have 1:1 matches of aggregate columns
		// and grouping columns, since that's what we get
		// out of the core
		if len(bi.bind) != len(ag.Agg)+len(ag.GroupBy) {
			child = s
			continue
		}
		agg2bind := make([]int, len(bi.bind))
		for i := range agg2bind {
			agg2bind[i] = -1
		}
		// try to match each binding
		// to one of the outputs of the aggregate
		for i := range bi.bind {
			id, ok := bi.bind[i].Expr.(expr.Ident)
			if !ok {
				child = s
				continue outer
			}
			matched := false
			for j := range ag.Agg {
				if agg2bind[j] != -1 {
					continue
				}
				if ag.Agg[j].Result == string(id) {
					agg2bind[j] = i
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			for j := range ag.GroupBy {
				if agg2bind[len(ag.Agg)+j] != -1 {
					continue
				}
				if ag.GroupBy[j].Result() == string(id) {
					agg2bind[len(ag.Agg)+j] = i
					matched = true
					break
				}
			}
			if !matched {
				child = s
				continue outer
			}
		}

		for i, bidx := range agg2bind {
			if i < len(ag.Agg) {
				ag.Agg[i].Result = bi.bind[bidx].Result()
			} else {
				ag.GroupBy[i-len(ag.Agg)].As(bi.bind[bidx].Result())
			}
		}

		// splice out the bind node
		if child == nil {
			b.top = ag
		} else {
			child.setparent(ag)
		}
	}
}
