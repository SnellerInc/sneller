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

package pir

import (
	"github.com/SnellerInc/sneller/expr"
)

// decorrelate rewrites a correlated subquery to be
// used with HASH_REPLACEMENT in the parent query. The
// subquery must meet the following conditions to be
// decorrelated:
//
//   - The subquery contains only one correlated
//     reference (x).
//   - x must be related to a column (y) by an equality
//     comparison (x = y).
//   - There must be no other conditions referencing x.
//
// If the subquery had a correlated reference and was
// successfully rewritten, This returns the name of the
// key field in the results (k) and the path of the
// correlated variable in the outer query (v).
//
// If v == nil and err == nil, the subquery did not
// contain a correlated reference and should be
// substituted using SCALAR_REPLACEMENT, etc.
//
// If err != nil, the subquery did contain a correlated
// reference, but decorrelation was unsuccessful and
// the trace may no longer be valid.
func (b *Trace) decorrelate() (k, v expr.Node, x string, err error) {
	// first we need to find a correlated variable
	// in the trace by checking its free variables
	// against the parent trace
	top := b.top
	p := top.parent()
	for p != nil {
		top = p
		p = top.parent()
	}
	it, ok := top.(*IterTable)
	if !ok || it.Filter == nil {
		return nil, nil, "", nil
	}
	for free := range it.free {
		if free == x {
			continue
		}
		_, node := b.Parent.top.get(free)
		if node == nil {
			continue
		}
		if _, ok := node.(*expr.Select); ok {
			continue
		}
		// multiple correlated references are
		// unsupported for now
		if x != "" {
			return nil, nil, "", decorrerr(node, free)
		}
		x = free
		v = node
		continue
	}
	if x == "" {
		return nil, nil, "", nil
	}
	// remove any limit steps in the child trace
	var prev Step
	for s := b.top; s != nil; s = s.parent() {
		li, ok := s.(*Limit)
		if !ok {
			prev = s
			continue
		}
		// FIXME: we can't support list results
		// unless we have a way to filter N
		// distinct results for a given column
		if li.Count > 1 {
			return nil, nil, "", decorrerr(v, x)
		}
		if b.top == s {
			b.top = s.parent()
		}
		if prev != nil {
			prev.setparent(s.parent())
		}
	}
	// find "x = y" in the WHERE clause
	y := b.decorrelateWhere(x, it)
	if y == nil {
		return nil, nil, "", decorrerr(v, x)
	}
	// the top step must either be a Bind or
	// Aggregate with at least one output
	switch s := b.top.(type) {
	case *Bind:
		if len(s.bind) == 0 {
			return nil, nil, "", decorrerr(v, x)
		}
		for i := range s.bind {
			if hasReference(x, s.bind[i].Expr) {
				return nil, nil, "", decorrerr(v, x)
			}
		}
		key := expr.Bind(y, gensym(0, 0))
		s.bind = append(s.bind, key)
		// insert "FILTER DISTINCT y" before
		// the bind step
		di := &Distinct{
			Columns: []expr.Node{y},
		}
		di.setparent(s.parent())
		s.setparent(di)
		k = expr.String(key.Result())
	case *Aggregate:
		if len(s.Agg) == 0 || s.GroupBy != nil || hasReference(x, s.Agg[0].Expr) {
			return nil, nil, "", decorrerr(v, x)
		}
		by := expr.Bind(y, gensym(0, 0))
		s.GroupBy = append(s.GroupBy, by)
		k = expr.String(by.Result())
	default:
		return nil, nil, "", decorrerr(v, x)
	}
	// do some bookkeeping
	delete(it.free, x)
	return k, v, x, nil
}

func decorrerr(e expr.Node, x string) error {
	return errorf(e, "cannot support correlated reference to %q", x)
}

// decorrelateWhere searches the top-level conjunctions
// in it.Filter for an expression matching "x = y" and,
// if no other expression in the filter reference x,
// removes "x = y" from the filter and returns y.
func (b *Trace) decorrelateWhere(x string, it *IterTable) (y expr.Node) {
	where := conjunctions(it.Filter, nil)
	for i := range where {
		if path, ok := decorrelateCmp(x, where[i]); ok {
			v := expr.MakePath(path)
			// reject "x = y AND x = z"
			if y != nil && !y.Equals(v) {
				return nil
			}
			y = v
			where[i] = nil
		} else if hasReference(x, where[i]) {
			return nil
		}
	}
	if y != nil {
		it.Filter = conjoinAll(where, it)
	}
	return y
}

// hasReference returns whether n references x.
func hasReference(x string, n expr.Node) bool {
	found := false
	visit := expr.WalkFunc(func(e expr.Node) bool {
		if !found {
			found = expr.IsIdentifier(e, x)
		}
		return !found
	})
	expr.Walk(visit, n)
	return found
}

func decorrelateCmp(x string, n expr.Node) ([]string, bool) {
	cmp, ok := n.(*expr.Comparison)
	if !ok || cmp.Op != expr.Equals {
		return nil, false
	}
	if expr.IsIdentifier(cmp.Left, x) {
		return expr.FlatPath(cmp.Right)
	}
	if expr.IsIdentifier(cmp.Right, x) {
		return expr.FlatPath(cmp.Left)
	}
	return nil, false
}
