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

	"golang.org/x/exp/slices"
)

func isPartition(s Step, e expr.Node, it *IterTable) (expr.Ident, *IterTable, bool) {
	id, ok := e.(expr.Ident)
	if !ok {
		return "", nil, false
	}
	step, _ := s.parent().get(string(id))
	it2, ok := step.(*IterTable)
	if !ok || (it != nil && it2 != it) {
		return "", nil, false
	}
	return id, it2, it2.Index != nil && it2.Index.HasPartition(string(id))
}

type partRewriter struct {
	node  Step
	it    *IterTable
	parts []string
}

func (p *partRewriter) Walk(e expr.Node) expr.Rewriter { return p }

func (p *partRewriter) Rewrite(e expr.Node) expr.Node {
	id, ok := e.(expr.Ident)
	if !ok {
		return e
	}
	from := p.node
	if from != p.it {
		from = from.parent()
		if from == nil {
			return e
		}
		from, _ = from.get(string(id))
		if from == nil || from != p.it {
			return e
		}
	}
	for i, part := range p.parts {
		if string(id) == part {
			return expr.Call(expr.PartitionValue, expr.Integer(i))
		}
	}
	return e
}

func rewriteParts(parts []string, s Step, it *IterTable) {
	pw := &partRewriter{parts: parts, it: it}
	rw := func(e expr.Node, _ bool) expr.Node {
		return expr.Rewrite(pw, e)
	}
	for s := s; s != nil; s = s.parent() {
		pw.node = s
		s.rewrite(rw)
	}
}

// is lst exactly the list of results produced by bind
func matchesBindings(bind []expr.Binding, lst []string) bool {
	if len(lst) != len(bind) {
		return false
	}
	for i := range bind {
		if !slices.Contains(lst, bind[i].Result()) {
			return false
		}
	}
	return true
}

func joinByPartition(b *Trace, s *IterValue) (*UnionMap, bool) {
	// match
	//
	//   HASH_REPLACEMENT(id, 'joinlist', k, MAKE_LIST(lst...))
	//
	// in this IterValue and
	//
	//   MAKE_LIST(matched...) AS k
	//
	// for b.Replacements[id]
	//
	hr, ok := s.Value.(*expr.Builtin)
	if !ok || hr.Func != expr.HashReplacement {
		return nil, false
	}
	if str, ok := hr.Args[1].(expr.String); !ok || string(str) != "joinlist" {
		return nil, false
	}
	lst, ok := hr.Args[3].(*expr.Builtin)
	if !ok || lst.Func != expr.MakeList {
		return nil, false
	}
	id := int(hr.Args[0].(expr.Integer))
	k := string(hr.Args[2].(expr.String))
	sub := b.Replacements[id]
	whence, from := sub.top.get(k)
	matched, ok := from.(*expr.Builtin)
	if !ok || matched.Func != expr.MakeList || len(matched.Args) != len(lst.Args) {
		return nil, false
	}

	// for each pair of matched expressions,
	// see if we can do a partition match instead
	var um *UnionMap
	var outert, innert *IterTable
	for i := 0; i < len(lst.Args) && len(lst.Args) > 1; i++ {
		var oid, iid expr.Ident
		oid, outert, ok = isPartition(s, lst.Args[i], outert)
		if !ok {
			continue
		}
		iid, innert, ok = isPartition(whence, matched.Args[i], innert)
		if !ok {
			continue
		}
		lst.Args = slices.Delete(lst.Args, i, i+1)
		matched.Args = slices.Delete(matched.Args, i, i+1)
		innert.Partitioned = true
		innert.OnEqual = append(innert.OnEqual, string(iid))
		if um == nil {
			um = &UnionMap{
				Inner: outert,
				Child: &Trace{
					Parent:       b,
					Replacements: []*Trace{sub},
					top:          s,
				},
			}
		}
		um.PartitionBy = append(um.PartitionBy, string(oid))
	}
	if um == nil {
		return nil, false
	}
	if len(lst.Args) == 0 {
		panic("should be impossible")
	}
	// if we only have 1 arg remaining in each MAKE_LIST,
	// then we can eliminate the MAKE_LIST entirely
	if len(lst.Args) == 1 {
		// these were matched at the beginning, so they should still match:
		if len(matched.Args) != 1 {
			panic("mis-matched arg lengths")
		}
		// HASH_REPLACEMENT(..., MAKE_LIST(x)) -> HASH_REPLACEMENT(.., x)
		hr.Args[3] = lst.Args[0]
		rewrite := func(e expr.Node, _ bool) expr.Node {
			if e == matched {
				return matched.Args[0]
			}
			return e
		}
		// MAKE_LIST(matched...) -> matched[0]
		whence.rewrite(rewrite)
	}
	rewriteParts(um.PartitionBy, s.parent(), outert)
	rewriteParts(innert.OnEqual, sub.top, innert)
	b.Replacements[id] = nil // will be removed by mergereplacements
	return um, true
}

func distinctByPartition(b *Trace, s *Bind) (*UnionMap, bool) {
	d, ok := s.parent().(*Distinct)
	if !ok {
		return nil, false
	}
	var parts []string
	var keep []expr.Node
	var it *IterTable
	for _, col := range d.Columns {
		var id expr.Ident
		id, it, ok = isPartition(d, col, it)
		if ok {
			parts = append(parts, string(id))
		} else {
			keep = append(keep, col)
		}
	}
	if it == nil || len(parts) == 0 {
		return nil, false
	}

	if len(keep) == 0 {
		if matchesBindings(s.bind, parts) && d.parent() == it && it.Filter == nil {
			// if we eliminated everything, then we are outputting 1 row
			s.setparent(DummyOutput{})
		} else {
			// we can output the partition as soon as we have
			// one row produced by the input
			lim := &Limit{Count: 1}
			lim.setparent(d.parent())
			s.setparent(lim)
		}
	} else {
		d.Columns = keep
	}

	// replace all the references to the distinct partition(s)
	// with PARTITION_VALUE() expressions at the projection step
	bf := bindflattener{}
	for i := range parts {
		bf.from = append(bf.from, expr.Bind(expr.Call(expr.PartitionValue, expr.Integer(i)), parts[i]))
	}
	for i := range s.bind {
		s.bind[i].Expr = expr.Rewrite(&bf, s.bind[i].Expr)
	}
	rewriteParts(parts, s.parent(), it)
	return &UnionMap{
		Inner: it,
		Child: &Trace{
			Parent: b,
			top:    s,
		},
		PartitionBy: parts,
	}, true
}

func aggByPartition(b *Trace, agg *Aggregate) (*UnionMap, bool) {
	// if we have a window function that depends
	// on being able to see all the groups for the partition,
	// then we can't split this grouping operation:
	for i := range agg.Agg {
		if agg.Agg[i].Expr.Op.WindowOnly() {
			return nil, false
		}
	}
	// split the GROUP BY clause into
	// partition-specific and non-partition-specific results
	var parts, nonparts []expr.Binding
	var partnames []string
	var thetable *IterTable
	for i := range agg.GroupBy {
		id, ok := agg.GroupBy[i].Expr.(expr.Ident)
		if !ok {
			nonparts = append(nonparts, agg.GroupBy[i])
			continue
		}
		step, _ := agg.parent().get(string(id))
		it, ok := step.(*IterTable)
		if !ok || it.Index == nil {
			nonparts = append(nonparts, agg.GroupBy[i])
			continue
		}
		if thetable == nil {
			thetable = it
		} else if it != thetable {
			return nil, false // ...?
		}
		if it.Index.HasPartition(string(id)) {
			parts = append(parts, agg.GroupBy[i])
			partnames = append(partnames, string(id))
		} else {
			nonparts = append(nonparts, agg.GroupBy[i])
		}
	}
	if len(parts) == 0 {
		return nil, false
	}

	// just keep the regular parts
	agg.GroupBy = nonparts
	// don't allow the bind step to produce a row
	// for any partitions that do not reach the aggregate stage
	agg.NonEmpty = true

	thetable.Partitioned = true
	// partition all the preceding steps
	// and then introduce an aggregation after the partitioning
	// that inserts the identity bindings for the partition values
	proj := &Bind{complete: true}
	for i := range parts {
		e := expr.Call(expr.PartitionValue, expr.Integer(i))
		proj.bind = append(proj.bind, expr.Bind(e, parts[i].Result()))
	}
	for i := range nonparts {
		proj.bind = append(proj.bind, expr.Identity(nonparts[i].Result()))
	}
	for i := range agg.Agg {
		proj.bind = append(proj.bind, expr.Identity(agg.Agg[i].Result))
	}
	rewriteParts(partnames, agg.parent(), thetable)
	proj.setparent(agg)
	um := &UnionMap{
		// TODO: lift partitioned replacements
		Inner: thetable,
		Child: &Trace{
			Parent: b,
			top:    proj,
		},
		PartitionBy: partnames,
	}
	return um, true
}

func steps(b *Trace) []Step {
	var out []Step
	for s := b.top; s != nil; s = s.parent() {
		out = append(out, s)
	}
	return out
}

func trivialSplit(s Step) bool {
	switch s.(type) {
	case *Bind, *Filter, *IterValue: // not affected by grouping
		return true
	default:
		return false
	}
}

func partition(b *Trace) {
	lst := steps(b)
	for i := range lst {
		s := lst[i]
		var ok bool
		var self *UnionMap
		switch s := s.(type) {
		case *IterValue:
			self, ok = joinByPartition(b, s)
		case *Aggregate:
			if len(s.GroupBy) > 0 {
				self, ok = aggByPartition(b, s)
			}
		case *Bind:
			self, ok = distinctByPartition(b, s)
		}
		if !ok {
			continue
		}
		// push trivially-splitting steps
		// into the partitioned part of the query
		j := i - 1
		for ; j >= 0; j-- {
			if !trivialSplit(lst[j]) {
				break
			}
			lst[j].setparent(self.Child.top)
			self.Child.top = lst[j]
		}
		if j < 0 {
			b.top = self
		} else {
			lst[j].setparent(self)
		}
		filterpushdown(self.Child)
		projectpushdown(self.Child)
		projectelim(self.Child)
		return
	}
}
