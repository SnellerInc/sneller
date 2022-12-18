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
	"fmt"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

type reftracker interface {
	strip(p *expr.Path) error
}

func check(parent Step, e expr.Node) error {
	if err := checkAggregateWorkInProgress(e); err != nil {
		return err
	}
	if parent == nil {
		return expr.Check(e)
	}
	return expr.CheckHint(e, &stepHint{parent: parent})
}

func (b *Trace) checkExpressions(n []expr.Node) error {
	for i := range n {
		err := check(b.top, n[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Trace) errorf(e expr.Node, f string, args ...interface{}) {
	b.err = append(b.err, errorf(e, f, args...))
}

func (b *Trace) combine() error {
	if len(b.err) == 1 {
		return b.err[0]
	}
	return fmt.Errorf("%w (and %d other errors)", b.err[0], len(b.err)-1)
}

func (b *Trace) Visit(e expr.Node) expr.Visitor {
	switch n := e.(type) {
	case *expr.Select:
		return b.visitSelect(n)
	case *expr.Path:
		return b.visitPath(n)
	case *expr.Unpivot:
		return b.visitUnpivot(n)
	default:
		return b
	}
}

func (b *Trace) visitSelect(e *expr.Select) expr.Visitor {
	// don't visit subqueries, we'll hoist those
	// into inputs in a later step
	return nil
}

func (b *Trace) visitPath(p *expr.Path) expr.Visitor {
	src, node := b.cur.get(p.First)
	if src == nil {
		b.errorf(p, "path %s references an unbound variable", expr.ToString(p))
		return nil
	}
	// if the source of a binding is an iterator,
	// add this path expression to the set of variable
	// references that originate from that table;
	// this lets us compute the set of bindings produced
	// from a table
	if rt, ok := src.(reftracker); ok {
		if err := rt.strip(p); err != nil {
			b.err = append(b.err, err)
		}
		// references to tables, etc.
		// do not need to be additionally
		// type-checked
		return nil
	}

	t := expr.TypeOf(node, &stepHint{src.parent()})
	if t == expr.AnyType || p.Rest == nil {
		return nil
	}
	// type-check the path expression against
	// the node that produces the value when
	// the path has multiple components
	switch p.Rest.(type) {
	case *expr.LiteralIndex:
		if !t.Contains(ion.ListType) {
			b.errorf(p, "path expression %q indexes a non-list object", p)
		}
	case *expr.Dot:
		if !t.Contains(ion.StructType) {
			b.errorf(p, "path expression %q dots a non-structure object", p)
		}
	}
	return nil
}

func (b *Trace) visitUnpivot(u *expr.Unpivot) expr.Visitor {
	b.errorf(u, "the UNPIVOT cross join case is not supported yet")
	return nil
}
