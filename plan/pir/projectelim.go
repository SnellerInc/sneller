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

// for each Bind step, eliminate bindings
// that are not referenced in subsequent steps
func projectelim(b *Trace) {
	// build a reverse-lookup from step
	// to path bindings that have been resolved
	// from other subsequent steps
	used := make(map[expr.Node]bool)
	final := b.final
	for i := range final {
		used[final[i].Expr] = true
	}
	for _, info := range b.scope {
		if info.node != nil {
			used[info.node] = true
		}
	}

	// eliminiate unused bindings for each bind pass
	var child Step
	for s := b.top; s != nil; s = s.parent() {
		bind, ok := s.(*Bind)
		if !ok {
			child = s
			continue
		}
		binds := bind.bind[:0]
		for i := range bind.bind {
			if used[bind.bind[i].Expr] {
				binds = append(binds, bind.bind[i])
			}
		}
		bind.bind = binds
		// this can happen if we are doing
		// a count(*) of a projection
		if len(bind.bind) == 0 {
			if child != nil {
				child.setparent(s.parent())
			} else {
				// we should be succeeded by at least
				// a count(*) operation, so we should
				// never be Builder.top
				panic("elimination of top projection?")
			}
		}
	}
}

type bindreplacer struct {
	fromstep Step
	scope    *Trace
}

// concatenate orig with rest, i.e.
//   joinpath("x.y", ".z") -> "x.y.z"
//   joinpath("x[0]", ".y") -> "x[0].y"
// and so forth
// (does not currently handle "x.*", etc.)
func joinpath(orig *expr.Path, rest expr.PathComponent) *expr.Path {
	n := &expr.Path{First: orig.First}
	bp := &n.Rest
	r := orig.Rest
	for r != nil {
		switch n := r.(type) {
		case *expr.Dot:
			nv := &expr.Dot{Field: n.Field}
			*bp = nv
			bp = &nv.Rest
			r = n.Rest
		case *expr.LiteralIndex:
			nv := &expr.LiteralIndex{Field: n.Field}
			*bp = nv
			bp = &nv.Rest
			r = n.Rest
		default:
			// *, etc.
			return nil
		}
	}
	*bp = rest
	return n
}

func (b *bindreplacer) Rewrite(e expr.Node) expr.Node {
	p, ok := e.(*expr.Path)
	if !ok {
		return e
	}
	if b.scope.origin(p) != b.fromstep {
		return e
	}
	out := b.scope.get(p)
	if out == nil {
		return e
	}
	if p.Rest != nil {
		// update this path expression
		// so that it begins with the lhs
		// path expression
		if pout, ok := out.(*expr.Path); ok {
			if out := joinpath(pout, p.Rest); out != nil {
				p.First = out.First
				p.Rest = out.Rest
			}
		}
		// otherwise, we don't know how
		// to flatten these results... yikes
		return e
	}
	return out
}

func (b *bindreplacer) Walk(e expr.Node) expr.Rewriter {
	return b
}

// simple projection push-down:
// merge adjacent projections into
// a single extended projection
func projectpushdown(scope *Trace) {
outer:
	for s := scope.top; s != nil; s = s.parent() {
		b, ok := s.(*Bind)
		if !ok {
			continue
		}

		// while the parent node is a Bind,
		// merge the results into a single
		// extended projection
		for {
			p := b.parent()
			pb, ok := p.(*Bind)
			if !ok {
				continue outer
			}
			// now we have Bind(Bind(...)),
			// so we can just take the results
			// of the inner bind and replace the
			// outer binding references with those
			// expressions
			rw := bindreplacer{scope: scope, fromstep: p}
			for i := range b.bind {
				b.bind[i].Expr = expr.Simplify(expr.Rewrite(&rw, b.bind[i].Expr), scope)
			}
			// ... and splice out the inner Bind
			b.setparent(pb.parent())
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
		// replace references to bindings produced
		// in this step with the expanded expression
		// (this will get CSE'd in the core SSA, so
		// there isn't really any cost to duplicating the expression)
		rw := bindreplacer{scope: b, fromstep: s}
		for i := range bind {
			bind[i].Expr = expr.Simplify(expr.Rewrite(&rw, bind[i].Expr), b)
		}
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
			p, ok := bi.bind[i].Expr.(*expr.Path)
			if !ok || b.origin(p) != ag {
				child = s
				continue outer
			}
			agr := b.get(p)
			if agr == nil {
				child = s
				continue outer
			}
			matched := false
			for j := range ag.Agg {
				if agg2bind[j] != -1 {
					continue
				}
				if agr == ag.Agg[j].Expr {
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
				if agr == ag.GroupBy[j].Expr {
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
