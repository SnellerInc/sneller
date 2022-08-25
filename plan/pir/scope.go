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

type scopeinfo struct {
	origin Step
	node   expr.Node
}

// TypeOf returns the type of a node, taking into account
// the additional scope and schema information available
// inside the query planner.
func (b *Trace) TypeOf(e expr.Node) expr.TypeSet {
	if p, ok := e.(*expr.Path); ok {
		origin, node := b.resolve(p)
		if origin == nil {
			return expr.NoHint(e)
		}
		// if a node originates from a table,
		// see if we can get information from
		// the table schema
		if orig, ok := origin.(*IterTable); ok {
			schema := orig.Schema
			if schema == nil {
				schema = expr.HintFn(expr.NoHint)
			}
			return expr.TypeOf(e, schema)
		}
		// otherwise, recurse until the
		// expression type-info machinery
		// decides it has enough information
		if node != nil && node != e {
			return expr.TypeOf(node, b)
		}
		return expr.NoHint(e)
	}
	return expr.TypeOf(e, b)
}

// resolve returns the Step and expression that
// produces the binding referenced by p, or
// (nil, nil) if the path expression does not
// reference an explicit binding.
func (b *Trace) resolve(p *expr.Path) (Step, expr.Node) {
	info := b.scope[p]
	return info.origin, info.node
}

// get returns the expression that produces
// the binding references by p, or nil if the
// path expression does not reference an explicit
// binding.
func (b *Trace) get(p *expr.Path) expr.Node {
	_, node := b.resolve(p)
	return node
}

// origin returns the Step that produces
// the binding referenced by p, or nil if
// the path expression has an uknown origin.
func (b *Trace) origin(p *expr.Path) Step {
	origin, _ := b.resolve(p)
	return origin
}

// make equivalent paths the same object
// so that re-naming passes can efficiently
// rename all references at once
func pathunify(b *Trace) {
	// build reverse-lookup table so that
	// each reference to a path expression
	// can be easily unified later
	rev := make(map[*expr.Path]*expr.Path)
	for k, si := range b.scope {
		p, ok := si.node.(*expr.Path)
		if !ok {
			continue
		}
		rev[p] = k
	}

	// FIXME: this data structure ends up
	// being a significant part of total
	// parse + compile memory allocation
	//
	// let's find a more memory-efficient data structure
	byorigin := make(map[Step][]*expr.Path)

	// walk all path expressions and
	// compare them with other path expressions
	// from the same origin, unifying them
	// when we discover duplicates
	b.Rewrite(rewritefn(func(in expr.Node) expr.Node {
		p, ok := in.(*expr.Path)
		if !ok {
			return in
		}
		origin := b.origin(p)
		if origin == nil {
			return in
		}
		have := byorigin[origin]
		if have == nil {
			byorigin[origin] = []*expr.Path{p}
			return in
		}
		for i := range have {
			// the paths are only equivalent if:
			//   - they are the same path expression
			//   - they have the same origin step
			//   - they have the same lvalue in the origin
			//     (b/c a bind expression can re-bind the same path more than once)
			if have[i].Equals(p) && b.get(have[i]) == b.get(p) {
				// if so, then update the associated
				// scope entry so that the scope.info
				// that points to 'p' now points to have[i] instead
				// (assuming anything actually uses 'p' in practice...)
				rv, ok := rev[p]
				if ok {
					si := b.scope[rv]
					si.node = have[i]
					b.scope[rv] = si
				}
				return have[i]
			}
		}
		byorigin[origin] = append(have, p)
		return in
	}))
}
