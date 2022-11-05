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

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
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
	return &Unnest{
		Nonterminal: Nonterminal{
			From: from,
		},
		Expr:   in.Value,
		Result: in.Result,
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
		if in.Offset != 0 {
			return nil, reject("non-zero OFFSET of distinct result")
		}
		f.Limit = in.Count
		return f, nil
	}
	if in.Offset != 0 {
		return nil, reject("OFFSET without GROUP BY/ORDER BY not implemented")
	}
	return &Limit{
		Nonterminal: Nonterminal{From: from},
		Num:         in.Count,
	}, nil
}

func iscountstar(a vm.Aggregation) bool {
	if len(a) != 1 {
		return false
	}

	agg := a[0]
	if agg.Expr.Filter != nil {
		return false
	}

	_, isstar := agg.Expr.Inner.(expr.Star)
	return isstar
}

func lowerAggregate(in *pir.Aggregate, from Op) (Op, error) {
	if in.GroupBy == nil {
		// simple aggregate; check for COUNT(*) first
		if iscountstar(in.Agg) {
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
			// there are cases where we ORDER BY an expression
			// that is composed of multiple aggregate results,
			// and in those cases we cannot merge these operations
			goto slowpath
		}
		return ha, nil
	}

slowpath:
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
	return &Project{
		Nonterminal: Nonterminal{From: from},
		Using:       in.Bindings(),
	}, nil
}

func lowerUnionMap(in *pir.UnionMap, env Env, split Splitter) (Op, error) {
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
	handle, err := stat(env, in.Inner.Table.Expr, &Hints{
		Filter:    in.Inner.Filter,
		Fields:    in.Inner.Fields(),
		AllFields: in.Inner.Wildcard(),
	})
	if err != nil {
		return nil, err
	}
	tbls, err := doSplit(split, in.Inner.Table.Expr, handle)
	if err != nil {
		return nil, err
	}
	// no subtables means no output
	if tbls.Len() == 0 {
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
func doSplit(s Splitter, tbl expr.Node, th TableHandle) (Subtables, error) {
	hs, ok := th.(tableHandles)
	if !ok {
		return s.Split(tbl, th)
	}
	var out Subtables
	for i := range hs {
		sub, err := doSplit(s, tbl, hs[i])
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = sub
		} else {
			out = out.Append(sub)
		}
	}
	return out, nil
}

// UploadFS is a blockfmt.UploadFS that can be encoded
// as part of a query plan.
type UploadFS interface {
	blockfmt.UploadFS
	// Encode encodes the UploadFS into the
	// provided buffer.
	Encode(dst *ion.Buffer, st *ion.Symtab) error
}

// UploadEnv is an Env that supports uploading objects
// which enables support for SELECT INTO.
type UploadEnv interface {
	// Uploader returns an UploadFS to use to
	// upload generated objects. This may return
	// nil if the envionment does not support
	// uploading despite implementing the
	// interface.
	Uploader() UploadFS
	// Key returns the key that should be used to
	// sign the index.
	Key() *blockfmt.Key
}

func lowerOutputPart(n *pir.OutputPart, env Env, input Op) (Op, error) {
	if e, ok := env.(UploadEnv); ok {
		if up := e.Uploader(); up != nil {
			op := &OutputPart{
				Basename: n.Basename,
				Store:    up,
			}
			op.From = input
			return op, nil
		}
	}
	return nil, fmt.Errorf("cannot handle INTO with Env that doesn't support UploadEnv")
}

func lowerOutputIndex(n *pir.OutputIndex, env Env, input Op) (Op, error) {
	if e, ok := env.(UploadEnv); ok {
		if up := e.Uploader(); up != nil {
			op := &OutputIndex{
				Table:    n.Table,
				Basename: n.Basename,
				Store:    up,
				Key:      e.Key(),
			}
			op.From = input
			return op, nil
		}
	}
	return nil, fmt.Errorf("cannot handle INTO with Env that doesn't support UploadEnv")
}

type input struct {
	table  *expr.Table
	hints  Hints
	handle TableHandle // if already statted
}

func (i *input) finish(env Env) (Input, error) {
	th, err := i.stat(env)
	if err != nil {
		return Input{}, err
	}
	return Input{
		Table:  i.table,
		Handle: th,
	}, nil
}

func (i *input) stat(env Env) (TableHandle, error) {
	if i.handle != nil {
		return i.handle, nil
	}
	th, err := stat(env, i.table.Expr, &i.hints)
	if err != nil {
		return nil, err
	}
	i.handle = th
	return th, nil
}

// conjunctions returns the list of top-level
// conjunctions from a logical expression
// by appending the results to 'lst'
//
// this is used for predicate pushdown so that
//
//	<a> AND <b> AND <c>
//
// can be split and evaluated as early as possible
// in the query-processing pipeline
func conjunctions(e expr.Node, lst []expr.Node) []expr.Node {
	a, ok := e.(*expr.Logical)
	if !ok || a.Op != expr.OpAnd {
		return append(lst, e)
	}
	return conjunctions(a.Left, conjunctions(a.Right, lst))
}

func conjoin(x []expr.Node) expr.Node {
	o := x[0]
	rest := x[1:]
	for _, n := range rest {
		o = expr.And(o, n)
	}
	return o
}

func isTimestamp(e expr.Node) bool {
	_, ok := e.(*expr.Timestamp)
	return ok
}

// canRemoveHint should return true if it
// is "safe" (i.e. likely to be profitable)
// to remove a hint from an input
//
// right now we avoid removing any expressions
// that contain timestamp comparisons
// (or logical compositions thereof)
func canRemoveHint(e expr.Node) bool {
	l, ok := e.(*expr.Logical)
	if ok {
		return canRemoveHint(l.Left) && canRemoveHint(l.Right)
	}
	cmp, ok := e.(*expr.Comparison)
	if !ok {
		return true
	}
	return !(isTimestamp(cmp.Left) || isTimestamp(cmp.Right))
}

func mergeFilterHint(x, y *input) bool {
	var xconj, yconj []expr.Node
	if x.hints.Filter != nil {
		xconj = conjunctions(x.hints.Filter, nil)
	}
	if y.hints.Filter != nil {
		yconj = conjunctions(y.hints.Filter, nil)
	}
	var overlap []expr.Node
	i := 0
outer:
	for ; i < len(xconj) && len(yconj) > 0; i++ {
		v := xconj[i]
		for j := range yconj {
			if expr.Equivalent(yconj[j], v) {
				yconj[j], yconj = yconj[len(yconj)-1], yconj[:len(yconj)-1]
				xconj[i], xconj = xconj[len(xconj)-1], xconj[:len(xconj)-1]
				overlap = append(overlap, v)
				i--
				continue outer
			}
		}
		// not part of an overlap, so
		// make sure we are allowed to
		// eliminate this hint
		if !canRemoveHint(v) {
			return false
		}
	}
	for _, v := range xconj[i:] {
		if !canRemoveHint(v) {
			return false
		}
	}
	// make sure any remaining rhs values
	// can be safely eliminated as well
	for _, v := range yconj {
		if !canRemoveHint(v) {
			return false
		}
	}
	if len(overlap) > 0 {
		x.hints.Filter = conjoin(overlap)
	} else {
		x.hints.Filter = nil
	}
	return true
}

func (i *input) merge(in *input) bool {
	if !i.table.Expr.Equals(in.table.Expr) {
		return false
	}
	if !mergeFilterHint(i, in) {
		return false
	}
	i.handle = nil
	if i.hints.AllFields {
		return true
	}
	if in.hints.AllFields {
		i.hints.Fields = nil
		i.hints.AllFields = true
		return true
	}
	i.hints.Fields = append(i.hints.Fields, in.hints.Fields...)
	slices.Sort(i.hints.Fields)
	i.hints.Fields = slices.Compact(i.hints.Fields)
	return true
}

// A walker is used when walking a pir.Trace to
// accumulate identical inputs so leaf nodes
// that reference the same inputs can be
// deduplicated.
type walker struct {
	inputs []input
}

func (w *walker) put(it *pir.IterTable) int {
	in := input{
		table: it.Table,
		hints: Hints{
			Filter:    it.Filter,
			Fields:    it.Fields(),
			AllFields: it.Wildcard(),
		},
	}
	for i := range w.inputs {
		if w.inputs[i].merge(&in) {
			return i
		}
	}
	i := len(w.inputs)
	w.inputs = append(w.inputs, in)
	return i
}

func walkBuild(in pir.Step, env Env, split Splitter) (Op, error) {
	w := walker{}
	return w.walkBuild(in, env, split)
}

func (w *walker) walkBuild(in pir.Step, env Env, split Splitter) (Op, error) {
	// IterTable is the terminal node
	if it, ok := in.(*pir.IterTable); ok {
		// TODO: we should handle table globs and
		// the ++ operator specially
		out := Op(&Leaf{
			Input: w.put(it),
		})
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

	input, err := w.walkBuild(pir.Input(in), env, split)
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
		return lowerOutputIndex(n, env, input)
	case *pir.OutputPart:
		return lowerOutputPart(n, env, input)
	case *pir.Unpivot:
		return lowerUnpivot(n, input)
	case *pir.UnpivotAtDistinct:
		return lowerUnpivotAtDistinct(n, input)
	default:
		return nil, fmt.Errorf("don't know how to lower %T", in)
	}
}

func (w *walker) finish(env Env) ([]Input, error) {
	if w.inputs == nil {
		return nil, nil
	}
	inputs := make([]Input, len(w.inputs))
	for i := range w.inputs {
		in, err := w.inputs[i].finish(env)
		if err != nil {
			return nil, err
		}
		inputs[i] = in
	}
	return inputs, nil
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
	w := walker{}
	t := &Tree{}
	err := w.toNode(&t.Root, in, env, split)
	if err != nil {
		return nil, err
	}
	inputs, err := w.finish(env)
	if err != nil {
		return nil, err
	}
	t.Inputs = inputs
	return t, nil
}

func (w *walker) toNode(t *Node, in *pir.Trace, env Env, split Splitter) error {
	op, err := w.walkBuild(in.Final(), env, split)
	if err != nil {
		return err
	}
	t.Op = op
	t.OutputType = results(in)
	t.Children = make([]*Node, len(in.Replacements))
	sub := walker{}
	for i := range in.Replacements {
		t.Children[i] = &Node{}
		err := sub.toNode(t.Children[i], in.Replacements[i], env, split)
		if err != nil {
			return err
		}
	}
	inputs, err := sub.finish(env)
	if err != nil {
		return err
	}
	t.Inputs = inputs
	return nil
}

type pirenv struct {
	env Env
}

func (e pirenv) Schema(tbl expr.Node) expr.Hint {
	s, ok := e.env.(Schemer)
	if !ok {
		return nil
	}
	return s.Schema(tbl)
}

func (e pirenv) Index(tbl expr.Node) (pir.Index, error) {
	idx, ok := e.env.(Indexer)
	if !ok {
		return nil, nil
	}
	return index(idx, tbl)
}

// New creates a new Tree from raw query AST.
func New(q *expr.Query, env Env) (*Tree, error) {
	return NewSplit(q, env, nil)
}

// NewSplit creates a new Tree from raw query AST.
func NewSplit(q *expr.Query, env Env, split Splitter) (*Tree, error) {
	b, err := pir.Build(q, pirenv{env})
	if err != nil {
		return nil, err
	}
	if split != nil {
		reduce, err := pir.Split(b)
		if err != nil {
			return nil, err
		}
		b = reduce
	} else {
		b = pir.NoSplit(b)
	}

	tree, err := toTree(b, env, split)
	if err != nil {
		return nil, err
	}

	if q.Explain == expr.ExplainNone {
		return tree, nil
	}

	// explain the query
	op := &Explain{
		Format: q.Explain,
		Query:  q,
		Tree:   tree,
	}

	res := &Tree{Root: Node{Op: op}}
	return res, nil

}

func lowerUnpivot(in *pir.Unpivot, from Op) (Op, error) {
	u := &Unpivot{
		Nonterminal: Nonterminal{From: from},
		As:          in.Ast.As,
		At:          in.Ast.At,
	}
	return u, nil
}

func lowerUnpivotAtDistinct(in *pir.UnpivotAtDistinct, from Op) (Op, error) {
	u := &UnpivotAtDistinct{
		Nonterminal: Nonterminal{From: from},
		At:          *in.Ast.At,
	}
	return u, nil
}
