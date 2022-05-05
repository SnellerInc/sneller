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
	"errors"
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/plan/pir"
	"github.com/SnellerInc/sneller/vm"
)

var (
	ErrNotSupported = errors.New("plan: query not supported")
)

// reject produces an ErrNotSupported error message
func reject(msg string) error {
	return fmt.Errorf("%w: %s", ErrNotSupported, msg)
}

func lowerIterValue(in *pir.IterValue, from Op) (Op, error) {
	if in.Wildcard() {
		return nil, reject("cannot project '*' from a cross-join")
	}
	pivot, ok := in.Value.(*expr.Path)
	if !ok {
		return nil, reject("cross-join on non-path expression")
	}
	return &Unnest{
		Nonterminal: Nonterminal{
			From: from,
		},
		PivotField:   pivot,
		InnerProject: vm.Selection(in.InnerBind()),
		OuterProject: vm.Selection(in.OuterBind()),
		InnerMatch:   in.Filter,
	}, nil
}

func lowerFilter(in *pir.Filter, from Op) (Op, error) {
	return &Filter{
		Nonterminal: Nonterminal{From: from},
		Expr:        in.Where,
	}, nil
}

func lowerDistinct(in *pir.Distinct, from Op) (Op, error) {
	return &Distinct{
		Nonterminal: Nonterminal{From: from},
		Fields:      in.Columns,
	}, nil
}

func lowerLimit(in *pir.Limit, from Op) (Op, error) {
	if in.Count == 0 {
		return NoOutput{}, nil
	}

	// some operations accept Limit natively
	switch f := from.(type) {
	case *HashAggregate:
		f.Limit = int(in.Count)
		if in.Offset != 0 {
			return nil, reject("non-zero OFFSET of hash aggregate result")
		}
		return f, nil
	case *OrderBy:
		f.Limit = int(in.Count)
		f.Offset = int(in.Offset)
		return f, nil
	case *Distinct:
		if in.Offset == 0 {
			f.Limit = in.Count
		}
		return f, nil
	}
	if in.Offset != 0 {
		return nil, reject("OFFSET without GROUP BY not implemented")
	}
	return &Limit{
		Nonterminal: Nonterminal{From: from},
		Num:         in.Count,
	}, nil
}

func iscountstar(e expr.Node) bool {
	agg, ok := e.(*expr.Aggregate)
	if ok {
		_, ok = agg.Inner.(expr.Star)
		return ok
	}
	return false
}

func lowerAggregate(in *pir.Aggregate, from Op) (Op, error) {
	if in.GroupBy == nil {
		// simple aggregate; check for COUNT(*) first
		if len(in.Agg) == 1 && iscountstar(in.Agg[0].Expr) {
			return &CountStar{
				Nonterminal: Nonterminal{From: from},
				As:          in.Agg[0].Result,
			}, nil
		}
		return &SimpleAggregate{
			Nonterminal: Nonterminal{From: from},
			Outputs:     in.Agg,
		}, nil
	}

	return &HashAggregate{
		Nonterminal: Nonterminal{From: from},
		Agg:         in.Agg,
		By:          in.GroupBy,
	}, nil
}

func lowerOrder(in *pir.Order, from Op) (Op, error) {
	if ha, ok := from.(*HashAggregate); ok {
		// hash aggregates can accept ORDER BY directly
	outer:
		for i := range in.Columns {
			ex := in.Columns[i].Column
			for col := range ha.Agg {
				if expr.IsIdentifier(ex, ha.Agg[col].Result) {
					ha.OrderBy = append(ha.OrderBy, HashOrder{
						Column:    col,
						Desc:      in.Columns[i].Desc,
						NullsLast: in.Columns[i].NullsLast,
					})
					continue outer
				}
			}
			for col := range ha.By {
				if expr.IsIdentifier(ex, ha.By[col].Result()) {
					ha.OrderBy = append(ha.OrderBy, HashOrder{
						Column:    len(ha.Agg) + col,
						Desc:      in.Columns[i].Desc,
						NullsLast: in.Columns[i].NullsLast,
					})
					continue outer
				}
			}
			return nil, fmt.Errorf("cannot ORDER BY expression %q", ex)
		}
		return ha, nil
	}

	// ordinary Order node
	columns := make([]OrderByColumn, 0, len(in.Columns))
	for i := range in.Columns {
		switch in.Columns[i].Column.(type) {
		case expr.Bool, expr.Integer, *expr.Rational, expr.Float, expr.String:
			// skip constant columns; they do not meaningfully apply a sort
			continue
		}

		columns = append(columns, OrderByColumn{
			Node:      in.Columns[i].Column,
			Desc:      in.Columns[i].Desc,
			NullsLast: in.Columns[i].NullsLast,
		})
	}

	// if we had ORDER BY "foo" or something like that,
	// then we don't need to do any ordering at all
	if len(columns) == 0 {
		return from, nil
	}

	// find possible duplicates
	for i := range columns {
		for j := i + 1; j < len(columns); j++ {
			if expr.Equivalent(columns[i].Node, columns[j].Node) {
				return nil, fmt.Errorf("duplicate order by expression %q", expr.ToString(columns[j].Node))
			}
		}
	}

	return &OrderBy{
		Nonterminal: Nonterminal{From: from},
		Columns:     columns,
	}, nil
}

func lowerBind(in *pir.Bind, from Op) (Op, error) {
	return (&Project{
		Nonterminal: Nonterminal{From: from},
		Using:       in.Bindings(),
	}).lowerApplications(1), nil
}

func lowerUnionMap(in *pir.UnionMap, env Env, split Splitter) (Op, error) {
	tbl := in.Inner.Table
	// NOTE: we're passing the same splitter
	// to the child here. We don't currently
	// produce nested split queries, so it isn't
	// meaningful at the moment, but it's possible
	// at some point we will need to indicate that
	// we are splitting an already-split query
	sub, err := walkBuild(in.Child.Final(), env, split)
	if err != nil {
		return nil, err
	}
	// don't call Stat; wait for it to be called
	// on the Leaf table and then grab the handle
	// returned from that Stat operation
	var handle TableHandle
	for o := sub; o != nil; o = o.input() {
		if l, ok := o.(*Leaf); ok && l.Expr == tbl {
			// the inner handle should never be
			// referenced directly; strip it
			// from the query plan
			handle, l.Handle = l.Handle, nil
			break
		}
	}
	if handle == nil {
		return nil, fmt.Errorf("lowerUnionMap: couldn't find %s", expr.ToString(tbl))
	}
	tbls, err := doSplit(split, in.Inner.Table.Expr, handle)
	if err != nil {
		return nil, err
	}
	// no subtables means no output
	if len(tbls) == 0 {
		return NoOutput{}, nil
	}
	return &UnionMap{
		Nonterminal: Nonterminal{From: sub},
		Orig:        in.Inner.Table,
		Sub:         tbls,
	}, nil
}

// doSplit calls s.Split(tbl, th) with special handling
// for tableHandles.
func doSplit(s Splitter, tbl expr.Node, th TableHandle) ([]Subtable, error) {
	hs, ok := th.(tableHandles)
	if !ok {
		return s.Split(tbl, th)
	}
	var out []Subtable
	for i := range hs {
		sub, err := doSplit(s, tbl, hs[i])
		if err != nil {
			return nil, err
		}
		out = append(out, sub...)
	}
	return out, nil
}

func walkBuild(in pir.Step, env Env, split Splitter) (Op, error) {
	// IterTable is the terminal node
	if it, ok := in.(*pir.IterTable); ok {
		handle, err := stat(env, it.Table.Expr, it.Filter)
		if err != nil {
			return nil, err
		}
		out := (Op)(&Leaf{Expr: it.Table, Handle: handle})
		if it.Filter != nil {
			out = &Filter{
				Nonterminal: Nonterminal{From: out},
				Expr:        it.Filter,
			}
		}
		return out, nil
	}
	// similarly, NoOutput is also terminal
	if _, ok := in.(pir.NoOutput); ok {
		return NoOutput{}, nil
	}
	if _, ok := in.(pir.DummyOutput); ok {
		return DummyOutput{}, nil
	}

	// ... and UnionMap as well
	if u, ok := in.(*pir.UnionMap); ok {
		return lowerUnionMap(u, env, split)
	}

	input, err := walkBuild(pir.Input(in), env, split)
	if err != nil {
		return nil, err
	}
	switch n := in.(type) {
	case *pir.IterValue:
		return lowerIterValue(n, input)
	case *pir.Filter:
		return lowerFilter(n, input)
	case *pir.Distinct:
		return lowerDistinct(n, input)
	case *pir.Bind:
		return lowerBind(n, input)
	case *pir.Aggregate:
		return lowerAggregate(n, input)
	case *pir.Limit:
		return lowerLimit(n, input)
	case *pir.Order:
		return lowerOrder(n, input)
	case *pir.OutputIndex:
		return nil, fmt.Errorf("INTO not yet supported")
	case *pir.OutputPart:
		return nil, fmt.Errorf("INTO not yet supported")
	default:
		return nil, fmt.Errorf("don't know how to lower %T", in)
	}
}

func autoname(e expr.Node, pos int) string {
	switch n := e.(type) {
	case *expr.Path:
		return n.Binding()
	case *expr.Builtin:
		return strings.ToLower(n.Func.String())
	default:
		return gensym(pos)
	}
}

// Result is a (field, type) tuple
// that indicates the possible output encoding
// of a particular field
type Result struct {
	Name string
	Type expr.TypeSet
}

// ResultSet is an ordered list of Results
type ResultSet []Result

func results(b *pir.Trace) ResultSet {
	final := b.FinalBindings()
	if len(final) == 0 {
		return nil
	}
	out := make(ResultSet, len(final))
	for i := range final {
		out[i] = Result{Name: final[i].Result(), Type: b.TypeOf(final[i].Expr)}
	}
	return out
}

func toTree(in *pir.Trace, env Env, split Splitter) (*Tree, error) {
	op, err := walkBuild(in.Final(), env, split)
	if err != nil {
		return nil, err
	}
	t := &Tree{
		Op:         op,
		OutputType: results(in),
	}
	for i := range in.Inputs {
		ct, err := toTree(in.Inputs[i], env, split)
		if err != nil {
			return nil, err
		}
		t.Children = append(t.Children, ct)
	}
	return t, nil
}

// New creates a new Tree from raw query AST.
func New(q *expr.Query, env Env) (*Tree, error) {
	bld, err := pir.Build(q, env)
	if err != nil {
		return nil, err
	}
	return toTree(bld, env, nil)
}

// NewSplit creates a new Tree from raw query AST.
func NewSplit(q *expr.Query, env Env, split Splitter) (*Tree, error) {
	b, err := pir.Build(q, env)
	if err != nil {
		return nil, err
	}
	reduce, err := pir.Split(b)
	if err != nil {
		return nil, err
	}
	return toTree(reduce, env, split)
}

// ShouldSplit walks the tables referenced in q
// and returns true if split.Split(table) returns
// no errors and a list of more than one target transport.
func ShouldSplit(q *expr.Query, env Env, split Splitter) (bool, error) {
	var visit visitfn
	var err error
	ret := false
	visit = visitfn(func(node expr.Node) expr.Visitor {
		if ret || err != nil {
			return nil
		}
		t, ok := node.(*expr.Table)
		if !ok {
			return visit
		}
		var handle TableHandle
		handle, err = stat(env, t.Expr, nil)
		if err != nil {
			return nil
		}
		var lst []Subtable
		lst, err = split.Split(t.Expr, handle)
		if err == nil && len(lst) > 1 {
			ret = true
			return nil
		}
		return visit
	})
	expr.Walk(visit, q.Body)
	return ret, err
}
