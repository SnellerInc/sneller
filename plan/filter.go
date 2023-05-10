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

package plan

import (
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// Filter is a plan that
// filters the input rows on
// some set of criteria
type Filter struct {
	Nonterminal
	Expr expr.Node
}

func (f *Filter) String() string {
	return "WHERE " + expr.ToString(f.Expr)
}

func (f *Filter) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	filt := ep.rewrite(f.Expr)
	if ep.Rewriter != nil {
		push(filt, f.From)
	}
	filter, err := vm.NewFilter(filt, dst)
	if err != nil {
		return err
	}
	return f.From.exec(filter, src, ep)
}

func (f *Filter) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("filter", dst, st)
	dst.BeginField(st.Intern("expr"))
	ep.rewrite(f.Expr).Encode(dst, st)
	dst.EndStruct()
	return nil
}

func (f *Filter) SetField(sf ion.Field) error {
	switch sf.Label {
	case "expr":
		e, err := expr.Decode(sf.Datum)
		if err != nil {
			return err
		}
		f.Expr = e
	default:
		return errUnexpectedField
	}
	return nil
}

// push a filter expression into op
func push(e expr.Node, op Op) {
	type filterer interface {
		filter(expr.Node)
	}
	if f, ok := op.(filterer); ok {
		f.filter(e)
	}
}

func (f *Filter) filter(e expr.Node)   { push(expr.And(f.Expr, e), f.From) }
func (o *OrderBy) filter(e expr.Node)  { push(e, o.From) }
func (u *UnionMap) filter(e expr.Node) { push(e, u.From) }
func (l *Leaf) filter(e expr.Node)     { l.Filter = e }
