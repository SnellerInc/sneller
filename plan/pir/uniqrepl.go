// Copyright (C) 2023 Sneller, Inc.
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

// uniqueReplacement scans the trace for joins having the
// right-hand-side query returning a unique set of tuples
// replaces it with hash lookup.
//
// Currently we look for the following sub-trace:
// - IterValue from a ListReplacement,
// - Filter with equality with the replacement unique columns.
func uniqueReplacement(filter *Filter, context *fpoContext) (Step, fpoStatus) {
	u := &uniqueReplacementAux{trace: context.trace}
	if !u.match(filter) {
		return nil, fpoIntact
	}

	if u.simplify() {
		context.trace.Rewrite(u)
		return filter, fpoUpdate
	}

	return nil, fpoIntact
}

// uniqueReplacementAux is a helper keeping current state of optimization
type uniqueReplacementAux struct {
	trace *Trace

	filter    *Filter
	itervalue *IterValue
	listrepl  *expr.Builtin
	columns   []string    // unique columns
	values    []expr.Node // corresponding lhs values
}

// match looks for a trace IterValue(with ListReplacement) -> Filter.
// (without detailed analysis)
func (u *uniqueReplacementAux) match(filter *Filter) bool {
	p := filter.parent()
	if p == nil {
		return false
	}

	iter, ok := p.(*IterValue)
	if !ok {
		return false
	}

	listrepl, ok := iter.Value.(*expr.Builtin)
	if !ok || listrepl.Func != expr.ListReplacement {
		return false
	}

	u.filter = filter
	u.itervalue = iter
	u.listrepl = listrepl
	return true
}

// simplify checks if the filter has equality condition on
// unique columns from the replacement.
func (u *uniqueReplacementAux) simplify() bool {
	id := u.listrepl.Args[0].(expr.Integer)
	u.columns = distinctcolumns(u.trace.Replacements[id])
	if len(u.columns) == 0 {
		return false
	}

	if len(u.columns) > 1 {
		// For now we handle only a simple case
		return false
	}

	u.values = make([]expr.Node, len(u.columns))
	conj := conjunctions(u.filter.Where, nil)
	for i := range conj {
		cmp, ok := conj[i].(*expr.Comparison)
		if !ok || cmp.Op != expr.Equals {
			continue
		}

		for j := range u.columns {
			column := expr.MakePath([]string{u.itervalue.Result, u.columns[j]})
			switch {
			case expr.Equal(column, cmp.Left):
				conj[i] = expr.Call(expr.InReplacement, cmp.Right, u.listrepl.Args[0])
				u.values[j] = cmp.Right

			case expr.Equal(column, cmp.Right):
				conj[i] = expr.Call(expr.InReplacement, cmp.Left, u.listrepl.Args[0])
				u.values[j] = cmp.Left
			}
		}
	}

	for _, n := range u.values {
		if n == nil { // didn't find equality for i-th column
			return false
		}
	}

	u.filter.Where = conj[0]
	for _, n := range conj[1:] {
		u.filter.Where = expr.And(u.filter.Where, n)
	}

	// Remove only itervalue step; the subsequent optimization passes
	// will process the filter, no extra work on our side is needed.
	u.filter.setparent(u.itervalue.parent())
	return true
}

func (u *uniqueReplacementAux) Walk(n expr.Node) expr.Rewriter {
	switch v := n.(type) {
	case *expr.Select:
		return nil

	case *expr.Table:
		return nil

	case *expr.Builtin:
		if v.Func == expr.InSubquery {
			return nil
		}
	}

	return u
}

// Rewrite replaces reference to the list replacement with hash replacements.
func (u *uniqueReplacementAux) Rewrite(n expr.Node) expr.Node {
	dot, ok := n.(*expr.Dot)
	if !ok {
		return n
	}

	inner, ok := dot.Inner.(expr.Ident)
	if !ok {
		return n
	}

	if string(inner) != u.itervalue.Result {
		return n
	}

	dot.Inner = u.hashReplacement()
	return n
}

func (u *uniqueReplacementAux) hashReplacement() expr.Node {
	return expr.Call(
		expr.HashReplacement,
		u.listrepl.Args[0],
		structkind,
		expr.String(u.columns[0]),
		u.values[0],
	)
}

// distinctcolumns returns the set of columns that form unique tuples.
func distinctcolumns(b *Trace) []string {
	var result []string

	final := make(map[string]struct{})
	binds := b.FinalBindings()
	for i := range binds {
		final[binds[i].Result()] = struct{}{}
	}

	find := func(n expr.Node) *expr.Binding {
		for i := range binds {
			if expr.Equal(n, binds[i].Expr) {
				return &binds[i]
			}
		}
		return nil
	}

	for s := b.top; s != nil; s = s.parent() {
		switch v := s.(type) {
		case *Aggregate:
			for i := range v.GroupBy {
				bind := v.GroupBy[i].Result()
				if _, exists := final[bind]; exists {
					result = append(result, bind)
				}
			}
			return result

		case *Distinct:
			for i := range v.Columns {
				switch col := v.Columns[i].(type) {
				case expr.Ident:
					bind := string(col)
					if _, ok := final[bind]; ok {
						result = append(result, bind)
					} else if find(col) != nil {
						result = append(result, bind)
					}

				case *expr.Dot:
					if bind := find(col); bind != nil {
						result = append(result, bind.Result())
					}
				}
			}
			return result
		}
	}

	return result
}
