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
	"io"
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan/pir"
	"github.com/SnellerInc/sneller/vm"
)

// Op represents a single node in
// the query plan tree.
// The root of the plan tree is the
// final output node, and the leaves
// are the tables from which the data
// will be queried.
type Op interface {
	fmt.Stringer
	ion.FieldSetter

	// input returns the input to Op,
	// or nil if this is a terminal input op
	input() Op
	// setinput sets the input to Op
	setinput(o Op)

	// exec should write the contents of src into dst,
	// taking care to honor the rewrite rules in ep
	exec(dst vm.QuerySink, src *Input, ep *ExecParams) error

	// encode should write the op as an ion structure
	// to 'dst'; the first field of the structure
	// should have the label "type" and produce
	// a symbol that corresponds to the name of
	// the type of the plan op (see decode.go)
	encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error
}

// Nonterminal is embedded in every
// Op that has an input Op.
type Nonterminal struct {
	From Op
}

func (s *Nonterminal) input() Op {
	return s.From
}

func (s *Nonterminal) setinput(p Op) {
	s.From = p
}

// Hints describes a set of hints passed
// to Env.Stat that can be used to optimize
// the access to a table.
type Hints struct {
	// Filter, if non-nil, is a predicate
	// that is applied to every row of the table.
	// (Env.Stat may use this to produce a *Input
	// that produces fewer rows than it otherwise would
	// due to the presence of some secondary indexing information.)
	Filter expr.Node
	// Fields is a list of top-level record fields explicitly
	// referenced by the query. The list of fields will always
	// be in lexicographical order.
	Fields []string
	// AllFields is set to true if all of the fields
	// are implicitly referenced in the query (i.e. via "*");
	// otherwise it is set to false.
	AllFields bool
}

// Env represents the global binding environment
// at the time that the query was compiled
type Env interface {
	// Stat returns a *Input
	// associated with the given PartiQL expression.
	// The Hints provided in the second
	// argument can be used to constrain the set of
	// rows and columns that are present in the returned *Input.
	// The information provided by the *Input
	// is used by the query planner to make query-splitting
	// decisions.
	Stat(tbl expr.Node, h *Hints) (*Input, error)
}

// Geometry represents the shape of a distributed query
// in terms of the peers that are available for dispatching
// partial queries.
type Geometry struct {
	Peers []Transport

	// TODO: weights, etc.
}

func decodeGeometry(d ion.Datum) (*Geometry, error) {
	g := new(Geometry)
	err := d.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "peers":
			return f.UnpackList(func(d ion.Datum) error {
				t, err := DecodeTransport(d)
				if err != nil {
					return err
				}
				g.Peers = append(g.Peers, t)
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Geometry) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("peers"))
	dst.BeginList(-1)
	for i := range g.Peers {
		if err := EncodeTransport(g.Peers[i], st, dst); err != nil {
			return err
		}
	}
	dst.EndList()
	dst.EndStruct()
	return nil
}

// SplitEnv is an Env that can be used for planning
// distributed queries by supplying a Geometry.
type SplitEnv interface {
	Env
	Geometry() *Geometry
}

// stat handles calling env.Stat(tbl, flt), with
// special handling for certain table expressions
// (TABLE_GLOB, TABLE_PATTERN, ++ operator).
func stat(env Env, tbl expr.Node, h *Hints) (*Input, error) {
	switch e := tbl.(type) {
	case *expr.Appended:
		var ret *Input
		for i := range e.Values {
			input, err := stat(env, e.Values[i], h)
			if err != nil {
				return nil, err
			}
			if ret == nil {
				ret = input
			} else {
				ret.Append(input)
			}
		}
		return ret, nil
	case *expr.Builtin:
		switch e.Func {
		case expr.TableGlob, expr.TablePattern:
			tl, ok := env.(TableLister)
			if !ok {
				return nil, fmt.Errorf("listing not supported")
			}
			return statGlob(tl, env, e, h)
		}
	}
	return env.Stat(tbl, h)
}

// Schemer may optionally be implemented by Env to
// provide type hints for a table.
type Schemer interface {
	// Schema returns type hints associated
	// with a particular table expression.
	// In the event that there is no available
	// type information, Schema may return nil.
	Schema(expr.Node) expr.Hint
}

// Indexer may optionally be implemented by Env to
// provide an index for a table.
type Indexer interface {
	// Index returns the index for the given table
	// expression. This may return (nil, nil) if
	// the index for the table is not available.
	Index(expr.Node) (Index, error)
}

// An Index may be returned by Indexer.Index to provide
// additional table metadata that may be used during
// optimization.
type Index = pir.Index

// index calls idx.Index(tbl), with special handling
// for certain table expressions.
func index(idx Indexer, tbl expr.Node) (Index, error) {
	switch e := tbl.(type) {
	case *expr.Appended:
		mi := make(multiIndex, 0, len(e.Values))
		for i := range e.Values {
			idx, err := index(idx, e.Values[i])
			if err != nil {
				return nil, err
			} else if idx != nil {
				mi = append(mi, idx)
			}
		}
		return mi, nil
	case *expr.Builtin:
		switch e.Func {
		case expr.TableGlob, expr.TablePattern:
			tl, ok := idx.(TableLister)
			if !ok {
				return nil, fmt.Errorf("listing not supported")
			}
			return indexGlob(tl, idx, e)
		}
	}
	return idx.Index(tbl)
}

// SimpleAggregate computes aggregates
// for a list of fields
type SimpleAggregate struct {
	Nonterminal
	Outputs  vm.Aggregation
	NonEmpty bool
}

func encodeBindings(lst []expr.Binding, dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) {
	lst = ep.rewriteBind(lst)
	dst.BeginList(-1)
	for i := range lst {
		dst.BeginStruct(-1)
		dst.BeginField(st.Intern("expr"))
		lst[i].Expr.Encode(dst, st)
		if lst[i].Explicit() {
			dst.BeginField(st.Intern("bind"))
			dst.WriteString(lst[i].Result())
		}
		dst.EndStruct()
	}
	dst.EndList()
}

func encodeAggregation(lst vm.Aggregation, dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) {
	lst = ep.rewriteAgg(lst)
	dst.BeginList(-1)
	for i := range lst {
		lst[i].Expr.Encode(dst, st)
		dst.WriteString(lst[i].Result)
	}
	dst.EndList()
}

func decodeAggregation(dst *vm.Aggregation, d ion.Datum) error {
	var out vm.Aggregation
	i, err := d.Iterator()
	if err != nil {
		return err
	}
	for !i.Done() {
		// decode expression + string pairs
		var b vm.AggBinding
		var e expr.Node

		d, err := i.Next()
		if err == nil {
			e, err = expr.Decode(d)
		}
		if err != nil {
			return fmt.Errorf("decoding aggregate expression: %w", err)
		}
		ag, ok := e.(*expr.Aggregate)
		if !ok {
			return fmt.Errorf("decoding aggregate: invalid expression %q", expr.ToString(e))
		}
		b.Expr = ag
		b.Result, err = i.String()
		if err != nil {
			return fmt.Errorf("decoding aggregate result: %w", err)
		}
		out = append(out, b)
	}
	*dst = out
	return nil
}

func (s *SimpleAggregate) String() string {
	str := "AGGREGATE " + s.Outputs.String()
	if s.NonEmpty {
		str += " NONEMPTY"
	}
	return str
}

func (s *SimpleAggregate) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	var sysagg expr.AggregateOp
	system := 0
	regular := 0
	for i := range s.Outputs {
		switch op := s.Outputs[i].Expr.Op; op {
		case expr.OpSystemDatashape, expr.OpSystemDatashapeMerge:
			sysagg = op
			system += 1
		default:
			regular += 1
		}
	}

	if system > 0 {
		if regular > 0 {
			return fmt.Errorf("mixing system and regular aggregates is not supported")
		}

		if system > 1 {
			return fmt.Errorf("using more than one system aggregate is not supported")
		}

		switch sysagg {
		case expr.OpSystemDatashape:
			return s.From.exec(vm.NewSystemDatashape(dst), src, ep)

		case expr.OpSystemDatashapeMerge:
			return s.From.exec(vm.NewSystemDatashapeMerge(dst), src, ep)
		}
	}

	a, err := vm.NewAggregate(ep.rewriteAgg(s.Outputs), dst)
	if err != nil {
		return err
	}
	a.SetSkipEmpty(s.NonEmpty)
	return s.From.exec(a, src, ep)
}

func settype(name string, dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern(name))
}

func (s *SimpleAggregate) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("agg", dst, st)
	dst.BeginField(st.Intern("agg"))
	encodeAggregation(s.Outputs, dst, st, ep)
	dst.BeginField(st.Intern("nonempty"))
	dst.WriteBool(s.NonEmpty)
	dst.EndStruct()
	return nil
}

func (s *SimpleAggregate) SetField(f ion.Field) error {
	switch f.Label {
	case "nonempty":
		var err error
		s.NonEmpty, err = f.Bool()
		return err
	case "agg":
		return decodeAggregation(&s.Outputs, f.Datum)
	}
	return errUnexpectedField
}

// Leaf is the leaf of a plan tree,
// and just contains the table from
// which the data will be processed.
type Leaf struct {
	// Orig is the original table expression;
	// this exists mostly for presentation purposes.
	Orig *expr.Table
	// Filter is pushed down before exec if the
	// parent node supports filter pushdown.
	Filter expr.Node
	// OnEqual is a filtering operation that applies
	// specifically to partitions. The table will
	// only be iterated for partitions where
	//
	//   OnEqual[i] = PARTITION_VALUE(i)
	//
	// Since OnEqual depends on PARTITION_VALUE(i) being rewritten,
	// OnEqual is only present when Leaf is part of a sub-query
	// for a partitioned query.
	OnEqual   []string
	EqualExpr []expr.Node
}

func (l *Leaf) String() string { return l.describe() }
func (l *Leaf) input() Op      { return nil }
func (l *Leaf) setinput(o Op) {
	panic("Leaf: cannot setinput")
}

func (l *Leaf) describe() string {
	s := expr.ToString(l.Orig)
	if len(l.OnEqual) > 0 {
		s += " ON "
		for i := range l.OnEqual {
			if i > 0 {
				s += ", "
			}
			s += l.OnEqual[i] + "=" + expr.ToString(l.EqualExpr[i])
		}
	}
	return s
}

func (l *Leaf) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	if ep.Runner == nil {
		return fmt.Errorf("can't execute query: ExecParams.Runner is nil")
	}
	filt := ep.rewrite(l.Filter)
	var partvals []ion.Datum
	if len(l.EqualExpr) > 0 {
		partvals = make([]ion.Datum, len(l.EqualExpr))
		for i := range partvals {
			c, ok := ep.rewrite(l.EqualExpr[i]).(expr.Constant)
			if !ok {
				return fmt.Errorf("missing PARTITION_VALUE constant rewrite %d", i)
			}
			partvals[i] = c.Datum()
		}
		groups, ok := src.Partition(l.OnEqual)
		if !ok {
			return fmt.Errorf("cannot partition on %T %v", src, l.OnEqual)
		}
		src = groups.Get(partvals)
		if src == nil {
			return dst.Close()
		}
	}
	// apply any last-minute filtering
	if filt != nil {
		src = src.Filter(filt)
	}
	err := ep.Runner.Run(dst, src, ep)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	err2 := dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

func (l *Leaf) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("leaf", dst, st)
	if l.Orig != nil {
		dst.BeginField(st.Intern("orig"))
		ep.rewrite(l.Orig).Encode(dst, st)
	}
	if l.Filter != nil {
		dst.BeginField(st.Intern("filter"))
		ep.rewrite(l.Filter).Encode(dst, st)
	}
	if len(l.OnEqual) > 0 {
		dst.BeginField(st.Intern("on_equal"))
		dst.BeginList(-1)
		for i := range l.OnEqual {
			dst.WriteString(l.OnEqual[i])
		}
		dst.EndList()
		dst.BeginField(st.Intern("equal_expr"))
		dst.BeginList(-1)
		for i := range l.EqualExpr {
			ep.rewrite(l.EqualExpr[i]).Encode(dst, st)
		}
		dst.EndList()
	}
	dst.EndStruct()
	return nil
}

func (l *Leaf) SetField(f ion.Field) error {
	switch f.Label {
	case "orig":
		n, err := expr.Decode(f.Datum)
		if err != nil {
			return err
		}
		l.Orig = n.(*expr.Table)
	case "filter":
		f, err := expr.Decode(f.Datum)
		if err != nil {
			return err
		}
		l.Filter = f
	case "on_equal":
		return f.Datum.UnpackList(func(d ion.Datum) error {
			str, err := d.String()
			if err != nil {
				return err
			}
			l.OnEqual = append(l.OnEqual, str)
			return nil
		})
	case "equal_expr":
		return f.Datum.UnpackList(func(d ion.Datum) error {
			e, err := expr.Decode(d)
			if err != nil {
				return err
			}
			l.EqualExpr = append(l.EqualExpr, e)
			return nil
		})
	default:
		return errUnexpectedField
	}
	return nil
}

type NoOutput struct{}

func (n NoOutput) input() Op      { return nil }
func (n NoOutput) String() string { return "NONE" }
func (n NoOutput) setinput(o Op) {
	panic("NoOutput: cannot setinput()")
}

func writeIon(b *ion.Buffer, dst vm.QuerySink) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	_, err = w.Write(b.Bytes())
	if err != nil && !errors.Is(err, io.EOF) {
		w.Close()
		return err
	}
	err = w.Close()
	err2 := dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

func (n NoOutput) exec(dst vm.QuerySink, _ *Input, ep *ExecParams) error {
	// just output an empty symbol table
	// and no data following it
	var b ion.Buffer
	var st ion.Symtab
	st.Marshal(&b, true)
	return writeIon(&b, dst)
}

func (n NoOutput) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("none", dst, st)
	dst.EndStruct()
	return nil
}

func (n NoOutput) SetField(f ion.Field) error {
	return errUnexpectedField
}

type DummyOutput struct{}

func (n DummyOutput) input() Op      { return nil }
func (n DummyOutput) String() string { return "[{}]" }
func (n DummyOutput) setinput(o Op) {
	panic("DummyOutput: cannot setinput()")
}

func (n DummyOutput) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	// just output an empty symbol table
	// plus an empty structure
	//
	// NOTE: we are triggering a vm copy here,
	// but it's just for a few bytes
	var b ion.Buffer
	var st ion.Symtab
	st.Marshal(&b, true)
	empty := ion.Struct{}
	empty.Encode(&b, &st)
	return writeIon(&b, dst)
}

func (n DummyOutput) encode(dst *ion.Buffer, st *ion.Symtab, _ *ExecParams) error {
	dst.BeginStruct(-1)
	settype("dummy", dst, st)
	dst.EndStruct()
	return nil
}

func (n DummyOutput) SetField(f ion.Field) error {
	return errUnexpectedField
}

type Limit struct {
	Nonterminal
	Num int64
}

func (l *Limit) String() string {
	return fmt.Sprintf("LIMIT %d", l.Num)
}

func (l *Limit) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	return l.From.exec(vm.NewLimit(l.Num, dst), src, ep)
}

func (l *Limit) encode(dst *ion.Buffer, st *ion.Symtab, _ *ExecParams) error {
	dst.BeginStruct(-1)
	settype("limit", dst, st)
	dst.BeginField(st.Intern("limit"))
	dst.WriteInt(l.Num)
	dst.EndStruct()
	return nil
}

func (l *Limit) SetField(f ion.Field) error {
	switch f.Label {
	case "limit":
		i, err := f.Int()
		if err != nil {
			return err
		}
		l.Num = i
	default:
		return errUnexpectedField
	}
	return nil
}

// CountStar implements COUNT(*)
type CountStar struct {
	Nonterminal
	As       string // output count name
	NonEmpty bool   // don't output count=0
}

func (c *CountStar) name() string {
	if c.As != "" {
		return c.As
	}
	return "_1"
}

func (c *CountStar) String() string {
	if c.NonEmpty {
		return "NONEMPTY COUNT(*) AS " + c.name()
	}
	return "COUNT(*) AS " + c.name()
}

func (c *CountStar) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	qs := countSink{dst: dst, as: c.name(), nonempty: c.NonEmpty}
	return c.From.exec(&qs, src, ep)
}

type countSink struct {
	dst      vm.QuerySink
	c        vm.Count
	as       string
	nonempty bool
}

func (c *countSink) Open() (io.WriteCloser, error) {
	return c.c.Open()
}

func (c *countSink) Close() error {
	if err := c.c.Close(); err != nil {
		return err
	}
	var b ion.Buffer
	var st ion.Symtab

	field := st.Intern(c.as)
	st.Marshal(&b, true)
	if c.nonempty && c.c.Value() == 0 {
		return writeIon(&b, c.dst)
	}

	b.BeginStruct(-1)
	b.BeginField(field)
	b.WriteInt(c.c.Value())
	b.EndStruct()
	w, err := c.dst.Open()
	if err != nil {
		return err
	}
	// NOTE: we are triggering a vm copy here;
	// the buffer is small so it's fine
	_, err = w.Write(b.Bytes())
	err2 := w.Close()
	err3 := c.dst.Close()
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	return err
}

func (c *CountStar) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("count(*)", dst, st)
	dst.BeginField(st.Intern("as"))
	dst.WriteString(c.As)
	dst.BeginField(st.Intern("nonempty"))
	dst.WriteBool(c.NonEmpty)
	dst.EndStruct()
	return nil
}

func (c *CountStar) SetField(f ion.Field) error {
	switch f.Label {
	case "nonempty":
		var err error
		c.NonEmpty, err = f.Bool()
		return err
	case "as":
		n, err := f.String()
		if err != nil {
			return err
		}
		c.As = n
	default:
		return errUnexpectedField
	}
	return nil
}

type HashAggregate struct {
	Nonterminal
	Agg      vm.Aggregation
	By       vm.Selection
	Windows  vm.Aggregation
	Limit    int
	OrderBy  []HashOrder
	NonEmpty bool
}

type HashOrder struct {
	Column   int
	Ordering vm.SortOrdering
}

func (h *HashAggregate) String() string {
	b := &strings.Builder{}

	fmt.Fprintf(b, "HASH AGGREGATE %s ", h.Agg)
	if len(h.Windows) > 0 {
		fmt.Fprintf(b, "WINDOWS %s ", h.Windows)
	}
	fmt.Fprintf(b, "GROUP BY %s", h.By)
	if h.OrderBy != nil {
		b.WriteString(" ORDER BY ")
		for i := range h.OrderBy {
			col := h.OrderBy[i].Column
			if col < len(h.Agg) {
				fmt.Fprintf(b, "%s", h.Agg[col].String())
			} else {
				b.WriteString(expr.ToString(&h.By[col-len(h.Agg)]))
			}
			if i != len(h.OrderBy)-1 {
				b.WriteString(", ")
			}
		}
	}
	if h.Limit > 0 {
		fmt.Fprintf(b, " LIMIT %d", h.Limit)
	}
	if h.NonEmpty {
		b.WriteString(" NONEMPTY")
	}
	return b.String()
}

func (h *HashAggregate) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("hashagg", dst, st)
	dst.BeginField(st.Intern("agg"))
	encodeAggregation(h.Agg, dst, st, ep)
	dst.BeginField(st.Intern("by"))
	encodeBindings(h.By, dst, st, ep)
	if h.Limit > 0 {
		dst.BeginField(st.Intern("limit"))
		dst.WriteInt(int64(h.Limit))
	}
	if len(h.OrderBy) > 0 {
		dst.BeginField(st.Intern("order"))
		dst.BeginList(-1)
		for i := range h.OrderBy {
			dst.BeginList(-1)
			dst.WriteInt(int64(h.OrderBy[i].Column))
			dst.WriteBool(h.OrderBy[i].Ordering.Direction == vm.SortDescending)
			dst.WriteBool(h.OrderBy[i].Ordering.NullsOrder == vm.SortNullsLast)
			dst.EndList()
		}
		dst.EndList()
	}
	if len(h.Windows) > 0 {
		dst.BeginField(st.Intern("windows"))
		encodeAggregation(h.Windows, dst, st, ep)
	}
	dst.BeginField(st.Intern("nonempty"))
	dst.WriteBool(h.NonEmpty)
	dst.EndStruct()
	return nil
}

func (h *HashAggregate) SetField(f ion.Field) error {
	switch f.Label {
	case "agg":
		return decodeAggregation(&h.Agg, f.Datum)
	case "windows":
		return decodeAggregation(&h.Windows, f.Datum)
	case "by":
		return decodeSel(&h.By, f.Datum)
	case "limit":
		i, err := f.Int()
		if err != nil {
			return err
		}
		h.Limit = int(i)
	case "order":
		return f.UnpackList(func(d ion.Datum) error {
			var o HashOrder
			var err error
			var i int64
			it, err := d.Iterator()
			if err != nil {
				return err
			}
			i, err = it.Int()
			if err != nil {
				return fmt.Errorf("reading \"OrderBy.Column\": %w", err)
			}
			o.Column = int(i)

			var desc bool
			desc, err = it.Bool()
			if err != nil {
				return fmt.Errorf("reading \"OrderBy.Desc\": %w", err)
			}
			if desc {
				o.Ordering.Direction = vm.SortDescending
			} else {
				o.Ordering.Direction = vm.SortAscending
			}

			var nullsLast bool
			nullsLast, err = it.Bool()
			if err != nil {
				return fmt.Errorf("reading \"OrderBy.NullsLast\": %w", err)
			}
			if nullsLast {
				o.Ordering.NullsOrder = vm.SortNullsLast
			} else {
				o.Ordering.NullsOrder = vm.SortNullsFirst
			}

			h.OrderBy = append(h.OrderBy, o)
			return nil
		})
	case "nonempty":
		var err error
		h.NonEmpty, err = f.Bool()
		return err
	default:
		return errUnexpectedField
	}
	return nil
}

func (h *HashAggregate) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	ha, err := vm.NewHashAggregate(ep.rewriteAgg(h.Agg), ep.rewriteAgg(h.Windows), ep.rewriteBind(h.By), dst)
	if err != nil {
		return err
	}
	if h.Limit > 0 {
		ha.Limit(h.Limit)
	}
	ha.SetSkipEmpty(h.NonEmpty)
	for i := range h.OrderBy {
		col := h.OrderBy[i].Column
		ordering := h.OrderBy[i].Ordering

		if col < len(h.Agg) {
			ha.OrderByAggregate(col, ordering)
		} else if col < len(h.Agg)+len(h.By) {
			ha.OrderByGroup(col-len(h.Agg), ordering)
		} else {
			ha.OrderByWindow(col-len(h.Agg)-len(h.By), ordering)
		}
	}
	return h.From.exec(ha, src, ep)
}

// OrderBy implements ORDER BY clause (without GROUP BY).
type OrderBy struct {
	Nonterminal
	Columns []vm.SortColumn
	Limit   int
	Offset  int
}

func (o *OrderBy) String() string {
	b := &strings.Builder{}

	b.WriteString("ORDER BY ")
	for i, column := range o.Columns {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString(expr.ToString(column.Node))
		b.WriteRune(' ')
		b.WriteString(column.Ordering.String())
	}

	if o.Limit > 0 {
		fmt.Fprintf(b, " LIMIT %d", o.Limit)
	}

	if o.Offset > 0 {
		fmt.Fprintf(b, " OFFSET %d", o.Offset)
	}

	return b.String()
}

func (o *OrderBy) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	writer, err := dst.Open()
	if err != nil {
		return err
	}

	orderBy := make([]vm.SortColumn, len(o.Columns))
	for i := range orderBy {
		orderBy[i].Node = ep.rewrite(o.Columns[i].Node)
		orderBy[i].Ordering = o.Columns[i].Ordering
	}

	var limit *vm.SortLimit
	if o.Offset > 0 && o.Limit > 0 {
		limit = &vm.SortLimit{
			Offset: o.Offset,
			Limit:  o.Limit,
		}
	} else if o.Limit > 0 {
		limit = &vm.SortLimit{
			Limit: o.Limit,
		}
	}
	ord, err := vm.NewOrder(writer, orderBy, limit, ep.Parallel)
	if err != nil {
		return err
	}
	// NOTE: vm.Order does not accept an
	// io.WriteCloser and thus cannot close the
	// passed writer, so we have to do it
	// ourselves. If that ever changes, we can
	// remove orderSink and return a *vm.Order
	// directly.
	sorter := &orderSink{
		Order: ord,
		w:     writer,
		dst:   dst,
	}
	return o.From.exec(sorter, src, ep)
}

type orderSink struct {
	*vm.Order
	w, dst io.Closer
}

func (s *orderSink) Close() error {
	err := s.Order.Close()
	err2 := s.w.Close()
	err3 := s.dst.Close()
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	return err
}

func (o *OrderBy) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("order", dst, st)

	dst.BeginField(st.Intern("columns"))
	dst.BeginList(-1)
	for i := range o.Columns {
		dst.BeginList(-1)
		ep.rewrite(o.Columns[i].Node).Encode(dst, st)
		dst.WriteBool(o.Columns[i].Ordering.Direction == vm.SortDescending)
		dst.WriteBool(o.Columns[i].Ordering.NullsOrder == vm.SortNullsLast)
		dst.EndList()
	}
	dst.EndList()

	if o.Limit > 0 {
		dst.BeginField(st.Intern("limit"))
		dst.WriteInt(int64(o.Limit))
	}
	if o.Offset > 0 {
		dst.BeginField(st.Intern("offset"))
		dst.WriteInt(int64(o.Offset))
	}

	dst.EndStruct()
	return nil
}

func (o *OrderBy) SetField(f ion.Field) error {
	switch f.Label {
	case "columns":
		return f.UnpackList(func(v ion.Datum) error {
			var col vm.SortColumn
			var err error
			i, err := v.Iterator()
			if err != nil {
				return err
			}
			v, err = i.Next()
			if err == nil {
				col.Node, err = expr.Decode(v)
			}
			if err != nil {
				return err
			}
			var desc bool
			desc, err = i.Bool()
			if err != nil {
				return err
			}
			if desc {
				col.Ordering.Direction = vm.SortDescending
			} else {
				col.Ordering.Direction = vm.SortAscending
			}

			var nullsLast bool
			nullsLast, err = i.Bool()
			if err != nil {
				return err
			}
			if nullsLast {
				col.Ordering.NullsOrder = vm.SortNullsLast
			} else {
				col.Ordering.NullsOrder = vm.SortNullsFirst
			}

			o.Columns = append(o.Columns, col)
			return nil
		})
	case "limit":
		i, err := f.Int()
		if err != nil {
			return err
		}
		o.Limit = int(i)
	case "offset":
		i, err := f.Int()
		if err != nil {
			return err
		}
		o.Offset = int(i)
	default:
		return errUnexpectedField
	}
	return nil
}

type Distinct struct {
	Nonterminal
	Fields []expr.Node
	Limit  int64
}

func (d *Distinct) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	df, err := vm.NewDistinct(ep.rewriteAll(d.Fields), dst)
	if err != nil {
		return err
	}
	if d.Limit > 0 {
		df.Limit(d.Limit)
	}
	return d.From.exec(df, src, ep)
}

func (d *Distinct) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("distinct", dst, st)
	dst.BeginField(st.Intern("fields"))
	dst.BeginList(-1)
	for i := range d.Fields {
		ep.rewrite(d.Fields[i]).Encode(dst, st)
	}
	dst.EndList()
	if d.Limit > 0 {
		dst.BeginField(st.Intern("limit"))
		dst.WriteInt(d.Limit)
	}
	dst.EndStruct()
	return nil
}

func (d *Distinct) SetField(f ion.Field) error {
	switch f.Label {
	case "fields":
		return f.UnpackList(func(v ion.Datum) error {
			e, err := expr.Decode(v)
			if err != nil {
				return err
			}
			d.Fields = append(d.Fields, e)
			return nil
		})
	case "limit":
		var err error
		d.Limit, err = f.Int()
		return err
	default:
		return errUnexpectedField
	}
}

func (d *Distinct) String() string {
	var str strings.Builder
	str.WriteString("DISTINCT ")
	for i := range d.Fields {
		if i != 0 {
			str.WriteString(", ")
		}
		str.WriteString(expr.ToString(d.Fields[i]))
	}
	if d.Limit > 0 {
		str.WriteString(" LIMIT ")
		fmt.Fprintf(&str, "%d", d.Limit)
	}
	return str.String()
}

type Unpivot struct {
	Nonterminal
	As *string
	At *string
}

func (u *Unpivot) String() string {
	var str strings.Builder
	str.WriteString("UNPIVOT")
	if u.As != nil {
		fmt.Fprintf(&str, " AS %s", *u.As)
	}
	if u.At != nil {
		fmt.Fprintf(&str, " AT %s", *u.At)
	}
	return str.String()
}

func (u *Unpivot) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("unpivot", dst, st)
	if u.As != nil {
		dst.BeginField(st.Intern("As"))
		dst.WriteString(*u.As)
	}
	if u.At != nil {
		dst.BeginField(st.Intern("At"))
		dst.WriteString(*u.At)
	}
	dst.EndStruct()
	return nil
}

func (u *Unpivot) SetField(f ion.Field) error {
	var err error
	switch f.Label {
	case "As":
		var x string
		x, err = f.String()
		u.As = &x
	case "At":
		var x string
		x, err = f.String()
		u.At = &x
	default:
		return errUnexpectedField
	}
	return err
}

func (u *Unpivot) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	vmu, err := vm.NewUnpivot(u.As, u.At, dst)
	if err != nil {
		return err
	}
	return u.From.exec(vmu, src, ep)
}

func encoderec(p Op, dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	// encode the parent(s) of this op first
	if parent := p.input(); parent != nil {
		err := encoderec(parent, dst, st, ep)
		if err != nil {
			return err
		}
		return p.encode(dst, st, ep)
	}
	// nodes without parents
	type encodable interface {
		encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error
	}

	if n, ok := p.(encodable); ok {
		return n.encode(dst, st, ep)
	}

	return fmt.Errorf("cannot encode %T", p)
}

// Encode encodes a plan tree
// for later decoding using Decode.
func (t *Tree) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return t.encode(dst, st, &ExecParams{})
}

func (t *Tree) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("id"))
	dst.WriteString(t.ID)
	dst.BeginField(st.Intern("inputs"))
	dst.BeginList(-1)
	for i := range t.Inputs {
		t.Inputs[i].Encode(dst, st)
	}
	dst.EndList()
	if !t.Data.IsEmpty() {
		dst.BeginField(st.Intern("data"))
		t.Data.Encode(dst, st)
	}
	dst.BeginField(st.Intern("root"))
	if err := t.Root.encode(dst, st, ep); err != nil {
		return err
	}
	dst.EndStruct()
	return nil
}

func (n *Node) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("input"))
	dst.WriteInt(int64(n.Input))
	dst.BeginField(st.Intern("op"))
	dst.BeginList(-1)
	err := encoderec(n.Op, dst, st, ep)
	if err != nil {
		return err
	}
	dst.EndList()
	dst.EndStruct()
	return nil
}

type UnpivotAtDistinct struct {
	Nonterminal
	At string
}

func (u *UnpivotAtDistinct) String() string {
	return fmt.Sprintf("UNPIVOT_AT_DISTINCT %s", u.At)
}

func (u *UnpivotAtDistinct) encode(dst *ion.Buffer, st *ion.Symtab, _ *ExecParams) error {
	dst.BeginStruct(-1)
	settype("unpivotatdistinct", dst, st)
	dst.BeginField(st.Intern("At"))
	dst.WriteString(u.At)
	dst.EndStruct()
	return nil
}

func (u *UnpivotAtDistinct) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	vmu, err := vm.NewUnpivotAtDistinct(u.At, dst)
	if err != nil {
		return err
	}
	return u.From.exec(vmu, src, ep)
}

func (u *UnpivotAtDistinct) SetField(f ion.Field) error {
	var err error
	switch f.Label {
	case "At":
		u.At, err = f.String()
	default:
		return errUnexpectedField
	}
	return err
}

// Explain is leaf executor for explaining queries
type Explain struct {
	Format expr.ExplainFormat
	Query  *expr.Query
	Tree   *Tree
}

func (e *Explain) String() string { return "EXPLAIN QUERY" }
func (e *Explain) input() Op      { return nil }
func (e *Explain) setinput(Op)    { panic("Explain: cannot setinput()") }

func (e *Explain) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("explain", dst, st)
	dst.BeginField(st.Intern("format"))
	dst.WriteInt(int64(e.Format))
	dst.BeginField(st.Intern("query"))
	// NOTE: we are *not* applying a rewrite
	// because presumably the query here is
	// for presentation purposes only
	e.Query.Encode(dst, st)
	dst.BeginField(st.Intern("tree"))
	e.Tree.encode(dst, st, ep)
	dst.EndStruct()
	return nil
}

func (e *Explain) SetField(f ion.Field) error {
	switch f.Label {
	case "format":
		k, err := f.Int()
		if err != nil {
			return err
		}
		e.Format = expr.ExplainFormat(k)
	case "query":
		q, err := expr.DecodeQuery(f.Datum)
		if err != nil {
			return err
		}
		e.Query = q
	case "tree":
		tree, err := DecodeDatum(f.Datum)
		if err != nil {
			return err
		}

		e.Tree = tree

	default:
		return errUnexpectedField
	}

	return nil
}

func (e *Explain) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	var b ion.Buffer
	var st ion.Symtab

	// Build the following structure:
	// "query": textual form of query being explained
	// "plan": text or
	// "plan-lines": list of plan lines or
	// "graphviz": graphviz
	fieldName := func() string {
		switch e.Format {
		case expr.ExplainDefault, expr.ExplainText:
			return "plan"

		case expr.ExplainList:
			return "plan-lines"

		case expr.ExplainGraphviz:
			return "graphviz"
		}

		return ""
	}

	st.Intern("query")
	st.Intern(fieldName())
	st.Marshal(&b, true)

	b.BeginStruct(-1)
	b.BeginField(st.Intern("query"))
	b.WriteString(expr.ToString(e.Query))

	b.BeginField(st.Intern(fieldName()))

	switch e.Format {
	case expr.ExplainDefault, expr.ExplainText:
		b.WriteString(e.Tree.String())

	case expr.ExplainList:
		b.BeginList(-1)
		for _, line := range strings.Split(e.Tree.String(), "\n") {
			if len(line) > 0 {
				b.WriteString(line)
			}
		}
		b.EndList()

	case expr.ExplainGraphviz:
		var sb strings.Builder
		err := Graphviz(e.Tree, &sb)
		if err != nil {
			return err
		}
		b.WriteString(sb.String())
	}
	b.EndStruct()
	return writeIon(&b, dst)
}
