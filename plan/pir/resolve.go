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
)

type reftracker interface {
	strip(path []string) ([]string, error)
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

func (r *pathRewriter) errorf(e expr.Node, f string, args ...interface{}) {
	r.err = append(r.err, errorf(e, f, args...))
}

func (r *pathRewriter) combine() error {
	switch len(r.err) {
	case 0:
		return nil
	case 1:
		return r.err[0]
	default:
		return fmt.Errorf("%w (and %d other errors)", r.err[0], len(r.err)-1)
	}
}

type pathRewriter struct {
	cur Step
	err []error

	rewrote expr.Node
}

func (r *pathRewriter) Rewrite(e expr.Node) expr.Node {
	if id, ok := e.(expr.Ident); ok {
		return r.rewritePath(e, []string{string(id)})
	}
	// called immediately after Walk
	if r.rewrote != nil {
		e = r.rewrote
	}
	r.rewrote = nil
	return e
}

func (r *pathRewriter) Walk(e expr.Node) expr.Rewriter {
	switch n := e.(type) {
	case *expr.Select:
		return nil
	case *expr.Unpivot:
		r.visitUnpivot(n)
		return nil
	default:
		flat, ok := expr.FlatPath(e)
		if ok {
			r.rewrote = r.rewritePath(e, flat)
			return nil // don't traverse flat paths any further
		}
		return r
	}
}

func (r *pathRewriter) rewritePath(e expr.Node, path []string) expr.Node {
	src, _ := r.cur.get(path[0])
	if src == nil {
		r.errorf(e, "path %s references an unbound variable", expr.ToString(e))
		return e
	}
	// if the source of a binding is an iterator,
	// add this path expression to the set of variable
	// references that originate from that table;
	// this lets us compute the set of bindings produced
	// from a table
	if rt, ok := src.(reftracker); ok {
		newpath, err := rt.strip(path)
		if err != nil {
			r.err = append(r.err, err)
			return e
		}
		return expr.MakePath(newpath)
	}
	return e
}

func (r *pathRewriter) visitUnpivot(u *expr.Unpivot) expr.Visitor {
	r.errorf(u, "the UNPIVOT cross join case is not supported yet")
	return nil
}
