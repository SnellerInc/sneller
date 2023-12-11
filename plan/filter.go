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
