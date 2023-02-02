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

func isPartition(s Step, id expr.Ident, it *IterTable) (*IterTable, bool) {
	step, _ := s.parent().get(string(id))
	it2, ok := step.(*IterTable)
	if !ok || (it != nil && it2 != it) {
		return nil, false
	}
	return it2, it2.Index != nil && it2.Index.HasPartition(string(id))
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

func distinctByPartition(b *Trace, s *Bind) (*UnionMap, bool) {
	d, ok := s.parent().(*Distinct)
	if !ok {
		return nil, false
	}
	var parts []string
	var keep []expr.Node
	var it *IterTable
	for _, col := range d.Columns {
		id, ok := col.(expr.Ident)
		if !ok {
			keep = append(keep, col)
			continue
		}
		it, ok = isPartition(d, id, it)
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
