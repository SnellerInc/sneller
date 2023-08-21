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

// Package testquery provides common functions used in query tests.
package testquery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/ion/zion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tests"
	"github.com/SnellerInc/sneller/vm"
)

type Input interface {
	Run(dst vm.QuerySink, blocks ints.Intervals, parallel int) error
	Size() int64
	Trailer() *blockfmt.Trailer
}

type RawInput []byte

func (b RawInput) Run(dst vm.QuerySink, _ ints.Intervals, parallel int) error {
	return vm.BufferTable([]byte(b), len(b)).WriteChunks(dst, parallel)
}

func (b RawInput) Size() int64 {
	return int64(len(b))
}

func (b RawInput) Trailer() *blockfmt.Trailer {
	tr := &blockfmt.Trailer{
		BlockShift: bits.Len(uint(len(b))),
		Blocks:     []blockfmt.Blockdesc{{Chunks: 1}},
	}
	tr.Sparse.Push(nil)
	return tr
}

type chunksInput struct {
	chunks [][]byte
	fields []string
}

func (c *chunksInput) Size() int64 {
	n := int64(0)
	for i := range c.chunks {
		n += int64(len(c.chunks[i]))
	}
	return n
}

func (c *chunksInput) Trailer() *blockfmt.Trailer {
	tr := &blockfmt.Trailer{
		BlockShift: bits.Len(uint(len(c.chunks[0]))),
	}
	var off int64
	for range c.chunks {
		tr.Blocks = append(tr.Blocks, blockfmt.Blockdesc{
			Offset: off,
			Chunks: 1,
		})
		off += int64(len(c.chunks))
		tr.Sparse.Push(nil)
	}
	return tr
}

func (c *chunksInput) writeZion(dst io.WriteCloser, blocks ints.Intervals) error {
	var mem []byte
	var enc zion.Encoder
	err := blocks.EachErr(func(blk int) error {
		var err error
		mem, err = enc.Encode(c.chunks[blk], mem[:0])
		if err != nil {
			dst.Close()
			return err
		}
		_, err = dst.Write(mem)
		if err != nil {
			dst.Close()
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if shw, ok := dst.(vm.EndSegmentWriter); ok {
		shw.EndSegment()
	} else {
		dst.Close()
		return fmt.Errorf("%T not an EndSegmentWriter?", dst)
	}
	return dst.Close()
}

func (c *chunksInput) Run(dst vm.QuerySink, blocks ints.Intervals, _ int) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	if zw, ok := w.(blockfmt.ZionWriter); ok && zw.ConfigureZion(int64(len(c.chunks[0])), c.fields) {
		return c.writeZion(w, blocks)
	}
	tmp := vm.Malloc()
	defer vm.Free(tmp)
	err = blocks.EachErr(func(blk int) error {
		buf := c.chunks[blk]
		if len(buf) > len(tmp) {
			return fmt.Errorf("chunk len %d > PageSize", len(buf))
		}
		for i := range tmp {
			tmp[i] = byte(i & 255)
		}
		size := copy(tmp, buf)
		_, err = w.Write(tmp[:size:size])
		if err != nil {
			closerr := w.Close()
			if errors.Is(err, io.EOF) {
				return closerr
			}
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if shw, ok := w.(vm.EndSegmentWriter); ok {
		shw.EndSegment()
	} else {
		w.Close()
		return fmt.Errorf("%T not an EndSegmentWriter?", w)
	}
	return w.Close()
}

type rowgroup = plan.PartGroups[ion.Datum]

type parallelInput struct {
	chunks     [][]byte
	fields     []string
	extraParts bool

	rg *rowgroup
}

func (p *parallelInput) Size() int64 {
	n := int64(0)
	for i := range p.chunks {
		n += int64(len(p.chunks[i]))
	}
	return n
}

func (p *parallelInput) Trailer() *blockfmt.Trailer {
	tr := &blockfmt.Trailer{
		BlockShift: bits.Len(uint(len(p.chunks[0]))),
	}
	var off int64
	for range p.chunks {
		tr.Blocks = append(tr.Blocks, blockfmt.Blockdesc{
			Offset: off,
			Chunks: 1,
		})
		off += int64(len(p.chunks))
		tr.Sparse.Push(nil)
	}
	return tr
}

// MaxAutoPartitions is the maximum number of distinct
// partitions produced when attempting to satisfy requests
// from the query planner to partition the input table(s).
// This limit exists to keep unit tests from becoming too slow
// due to the expense of dynamically partitioning the data on-demand.
const MaxAutoPartitions = 100

func (p *parallelInput) partition(parts []string) (*rowgroup, error) {
	if p.rg != nil && slices.Equal(p.rg.Fields(), parts) {
		return p.rg, nil
	}
	okType := func(d ion.Datum) bool {
		switch d.Type() {
		// we allow ion.SymbolType here because
		// it is always flattened into a string
		case ion.StringType, ion.IntType, ion.UintType, ion.FloatType, ion.SymbolType:
			return true
		default:
			return false
		}
	}
	expand := func(d ion.Datum) ion.Datum {
		switch d.Type() {
		case ion.SymbolType:
			str, _ := d.String()
			return ion.NewString(str)
		default:
			return d
		}
	}
	getconst := func(d ion.Datum, p string) (ion.Datum, bool) {
		s, err := d.Struct()
		if err != nil {
			return ion.Empty, false
		}
		f, ok := s.FieldByName(p)
		if !ok || !okType(f.Datum) {
			return ion.Empty, false
		}
		return expand(f.Datum), true
	}

	var st ion.Symtab
	var d ion.Datum
	var err error
	var lst []ion.Datum
	for i := range p.chunks {
		buf := p.chunks[i]
		for len(buf) > 0 {
			d, buf, err = ion.ReadDatum(&st, buf)
			if err != nil {
				panic(err)
			}
			lst = append(lst, d)
		}
	}
	rg, ok := plan.Partition(lst, parts, getconst)
	if !ok {
		return nil, fmt.Errorf("cannot split on %v", parts)
	}
	// don't allow an enormous number of partitions
	if rg.Groups() > MaxAutoPartitions {
		return nil, fmt.Errorf("%d groups total for partition on %v exceeds max %d", rg.Groups(), parts, MaxAutoPartitions)
	}
	p.rg = rg
	return rg, nil
}

func (p *parallelInput) Run(dst vm.QuerySink, blocks ints.Intervals, _ int) error {
	outputs := make([]io.WriteCloser, blocks.Len())
	errlist := make([]error, blocks.Len())
	var wg sync.WaitGroup
	for i := range outputs {
		w, err := dst.Open()
		if err != nil {
			return err
		}
		outputs[i] = w
	}
	wg.Add(len(outputs))
	i := 0
	blocks.Each(func(blk int) {
		go func(w io.WriteCloser, mem []byte, errp *error) {
			defer wg.Done()
			seterr := func(e error) {
				if e != nil && !errors.Is(e, io.EOF) && *errp == nil {
					*errp = e
				}
			}
			var err error
			if zw, ok := w.(blockfmt.ZionWriter); ok && zw.ConfigureZion(int64(len(mem)), p.fields) {
				var enc zion.Encoder
				mem, err = enc.Encode(mem, nil)
				if err != nil {
					seterr(err)
					w.Close()
					return
				}
			}
			_, err = w.Write(mem)
			if err != nil {
				seterr(err)
				w.Close()
				return
			}
			if shw, ok := w.(vm.EndSegmentWriter); ok {
				shw.EndSegment()
			} else {
				seterr(fmt.Errorf("%T not an EndSegmentWriter?", w))
				w.Close()
				return
			}
			seterr(w.Close())
		}(outputs[i], p.chunks[blk], &errlist[i])
		i++
	})
	wg.Wait()
	for i := range errlist {
		if errlist[i] != nil {
			return errlist[i]
		}
	}
	return nil
}

// Env is an implementation of plan.Env uses for evaluating test cases.
type Env struct {
	// In is the set of tables in the environment.
	// If [len(In) == 1], then `In[0]` is the table [input].
	// Otherwise tables [input0], [input1], etc. correspond
	// to [In[0]], [In[1]], and so forth.
	In   []Input
	tags map[string]string
}

func (e *Env) input(p string) (Input, bool) {
	if p == "input" && len(e.In) == 1 {
		return e.In[0], true
	}
	var i int
	if n, _ := fmt.Sscanf(p, "input%d", &i); n > 0 && i >= 0 && i < len(e.In) {
		return e.In[i], true
	}
	return nil, false
}

func (e *Env) Geometry() *plan.Geometry {
	return &plan.Geometry{
		Peers: []plan.Transport{
			&plan.LocalTransport{},
			&plan.LocalTransport{},
		},
	}
}

// Stat implements plan.Env.Stat
func (e *Env) Stat(t expr.Node, h *plan.Hints) (*plan.Input, error) {
	id, ok := t.(expr.Ident)
	if !ok {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	in, ok := e.input(string(id))
	if !ok {
		return nil, fmt.Errorf("invalid input %q", string(id))
	}
	var blocks ints.Intervals
	tr := in.Trailer()
	if tr != nil {
		blocks = ints.Intervals{{0, len(tr.Blocks)}}
	}
	var fields []string
	if !h.AllFields {
		fields = h.Fields
		if fields == nil {
			fields = make([]string, 0)
		}
	}
	desc := blockfmt.Descriptor{
		ObjectInfo: blockfmt.ObjectInfo{
			Path: string(id),
			Size: in.Size(),
		},
	}
	if tr != nil {
		desc.Trailer = *tr
	}
	return &plan.Input{
		Descs: []plan.Descriptor{{
			Descriptor: desc,
			Blocks:     blocks,
		}},
		Fields: fields,
	}, nil
}

func (e *Env) Run(dst vm.QuerySink, in *plan.Input, ep *plan.ExecParams) error {
	for i := range in.Descs {
		input, ok := e.input(in.Descs[i].Path)
		if !ok {
			return fmt.Errorf("bad input: %s", in.Descs[i].Path)
		}
		// NOTE: not all input types support blocks
		err := input.Run(dst, in.Descs[i].Blocks, ep.Parallel)
		if err != nil {
			return err
		}
	}
	return nil
}

var _ plan.Indexer = &Env{}

type handleIndex struct {
	in   any
	part bool
}

func (h *handleIndex) TimeRange(path []string) (min, max date.Time, ok bool) {
	return // unimplemented for now
}

func (h *handleIndex) HasPartition(x string) bool {
	if !h.part {
		return false
	}
	// FIXME: we need to calculate row consts
	// ahead of time
	return false
	pi, ok := h.in.(*parallelInput)
	if ok {
		// XXX very slow:
		_, err := pi.partition([]string{x})
		return err == nil
	}
	return false
}

func (e *Env) Index(t expr.Node) (plan.Index, error) {
	id, ok := t.(expr.Ident)
	if !ok {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	in, ok := e.input(string(id))
	if !ok {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	return &handleIndex{
		in:   in,
		part: e.tags == nil || e.tags["partition"] != "false",
	}, nil
}

var _ plan.TableLister = (*Env)(nil)

func (e *Env) ListTables(db string) ([]string, error) {
	if db != "" {
		return nil, fmt.Errorf("no databases")
	}
	if len(e.In) == 1 {
		return []string{"input"}, nil
	}
	ts := make([]string, len(e.In))
	for i := range e.In {
		ts[i] = fmt.Sprintf("input%d", i)
	}
	return ts, nil
}

// Case is a test case.
type Case struct {
	// QueryStr is the raw query string.
	QueryStr []byte
	// Query is the parsed query.
	Query *expr.Query
	// SymbolTable is the symbol table used
	// for the input datums.
	SymbolTable *ion.Symtab
	// Input is the raw input data for the query,
	// grouped by table input. If [len(Input)] is 1,
	// then datums in [Input[0]] correspond to the table [input].
	// Otherwise, [Input[0]] corresponds to [input0],
	// [Input[1]] corresponds to [input1], and so forth.
	Input [][]ion.Datum
	// Output is the expected output of the query when
	// it is run on the provided input(s).
	Output []ion.Datum
	// Tags is the set of tags from the testcase file.
	// See also [tests.CaseSpec.Tags].
	Tags map[string]string
}

func (q *Case) extraParts() bool {
	return strings.EqualFold(q.Tags["extra-parts"], "TRUE")
}

// NeedShuffleOutput determines whether the output
// need to be shuffled along with the input
func NeedShuffleOutput(q *expr.Query) bool {
	sel, ok := q.Body.(*expr.Select)
	if !ok {
		return false
	}
	// ORDER BY, GROUP BY, and DISTINCT
	// all have output orderings that are
	// independent of the input
	return sel.OrderBy == nil && sel.GroupBy == nil && !sel.Distinct
}

// CanShuffleSymtab determines whether symtab can
// be safely shuffled
func CanShuffleSymtab(q *expr.Query) bool {
	allowed := true
	fn := expr.WalkFunc(func(n expr.Node) bool {
		if !allowed {
			return false
		}

		s, ok := n.(*expr.Select)
		if ok {
			// sorting fails if a symtab change (sort & limit prevents symtab changes)
			if !(s.OrderBy == nil || (s.OrderBy != nil && s.Limit != nil)) {
				allowed = false
				return false
			}
		}

		return true
	})

	expr.Walk(fn, q.Body)

	return allowed
}

// CanShuffleInput determines whether the inputs to the query can be shuffled
func CanShuffleInput(q *expr.Query) bool {
	sel, ok := q.Body.(*expr.Select)
	if !ok {
		return false
	}
	if sel.OrderBy != nil {
		// FIXME: not always true; sorting is not stable...
		return true
	}
	// any aggregate produces only one output row,
	// so the result can be shuffled trivially
	if anyHasAggregate(sel.Columns) && len(sel.GroupBy) == 0 {
		return true
	}
	if _, ok := sel.From.(*expr.Join); ok {
		// cross-join permutes row order;
		// need an ORDER BY to make results deterministic
		return false
	}
	if sel.From != nil {
		tbls := sel.From.Tables()
		for _, tbl := range tbls {
			if _, ok := tbl.Expr.(*expr.Unpivot); ok {
				return false
			}
		}
	}
	if sel.GroupBy != nil || sel.Distinct {
		// these permute the output ordering by hash
		return false
	}
	return true
}

func anyHasAggregate(lst []expr.Binding) bool {
	for i := range lst {
		e := lst[i].Expr
		if hasAggregate(e) {
			return true
		}
	}
	return false
}

type walkfn func(e expr.Node) bool

func (w walkfn) Visit(e expr.Node) expr.Visitor {
	if w(e) {
		return w
	}
	return nil
}

func hasAggregate(e expr.Node) bool {
	any := false
	w := walkfn(func(e expr.Node) bool {
		_, ok := e.(*expr.Aggregate)
		if ok {
			any = true
			return false
		}
		return !any
	})
	expr.Walk(w, e)
	return any
}

// Shuffle shuffles if possible
func (q *Case) Shuffle() {
	// shuffle around the input (and maybe output)
	// lanes so that we increase the coverage of
	// lane-specific branches
	if len(q.Input) != 1 {
		// don't shuffle multiple inputs
	} else if in := q.Input[0]; NeedShuffleOutput(q.Query) && len(in) == len(q.Output) {
		rand.Shuffle(len(in), func(i, j int) {
			in[i], in[j] = in[j], in[i]
			q.Output[i], q.Output[j] = q.Output[j], q.Output[i]
		})
	} else {
		rand.Shuffle(len(in), func(i, j int) {
			in[i], in[j] = in[j], in[i]
		})
	}
}

type RunFlags int

const (
	FlagParallel    = 1 << iota // run in parallel
	FlagResymbolize             // resymbolize
	FlagShuffle                 // shuffle symbol table
	FlagSplit                   // use a split plan
)

func (q *Case) Execute(flags RunFlags) error {

	// fix up the symbols input lst so that they
	// match the associated symbols input symbolTable
	fixup := func(lst []ion.Datum, st *ion.Symtab) {
		// we reset the symbol elements of structure fields
		// inside Encode, so the easiest way to do this is
		// just encode the data and throw it away
		var dummy ion.Buffer
		for i := range lst {
			lst[i].Encode(&dummy, st)
			dummy.Reset()
		}
	}
	tags := q.Tags
	newparts := q.extraParts()
	// run a query on the given input table and yield the output list
	run := func(q *expr.Query, in [][]ion.Datum, st *ion.Symtab, flags RunFlags) ([]ion.Datum, error) {
		var unsymbolize func(d ion.Datum, st *ion.Symtab) ion.Datum
		unsymbolize = func(d ion.Datum, st *ion.Symtab) ion.Datum {
			switch d.Type() {
			case ion.StructType:
				d, _ := d.Struct()
				fields := d.Fields(nil)
				for i := range fields {
					if str, err := fields[i].String(); err == nil {
						fields[i].Datum = ion.String(str)
					} else {
						fields[i].Datum = unsymbolize(fields[i].Datum, st)
					}
				}
				return ion.NewStruct(st, fields).Datum()
			case ion.ListType:
				d, _ := d.List()
				items := d.Items(nil)
				for i := range items {
					if str, err := items[i].String(); err == nil {
						items[i] = ion.String(str)
					} else {
						items[i] = unsymbolize(items[i], st)
					}
				}
				return ion.NewList(st, items).Datum()
			}
			return d
		}

		// return a symbol table with the symbols
		// randomly shuffled
		shuffled := func(st *ion.Symtab) *ion.Symtab {
			ret := &ion.Symtab{}
			// if only one symbol is input the input corpus,
			// then just bump it up one symbol
			if st.MaxID() == 11 {
				ret.Intern("a-random-symbol")
				ret.Intern(st.Get(11))
				return ret
			}

			// first 10 symbols are "pre-interned"
			symbolmap := make([]ion.Symbol, st.MaxID()-10)
			for i := range symbolmap {
				symbolmap[i] = ion.Symbol(i) + 10
			}
			rand.Shuffle(len(symbolmap), func(i, j int) {
				symbolmap[i], symbolmap[j] = symbolmap[j], symbolmap[i]
			})

			// force symbols to be multi-byte sequences:
			for i := 0; i < 117; i++ {
				ret.Intern(fmt.Sprintf("..garbage%d", i))
			}

			for _, s := range symbolmap {
				ret.Intern(st.Get(s))
			}
			return ret
		}

		input := make([]Input, len(in))
		maxp := 0
		for i, in := range in {
			if (flags&FlagResymbolize) != 0 && len(in) > 1 {
				half := len(in) / 2
				first := flatten(in[:half], st)

				var second []byte
				if flags&FlagShuffle != 0 {
					second = flatten(in[half:], shuffled(st))
				} else {
					second = flatten(in[half:], st)
				}
				if flags&FlagParallel != 0 {
					maxp = 2
					input[i] = &parallelInput{chunks: [][]byte{first, second}, extraParts: newparts}
				} else {
					input[i] = &chunksInput{chunks: [][]byte{first, second}}
				}
			} else {
				input[i] = RawInput(flatten(in, st))
			}
		}
		env := &Env{In: input, tags: tags}
		var tree *plan.Tree
		var err error
		if flags&FlagSplit != 0 {
			tree, err = plan.NewSplit(q, env)
		} else {
			tree, err = plan.New(q, env)
		}
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		lp := plan.LocalTransport{Threads: maxp}
		params := plan.ExecParams{
			Plan:     tree,
			Output:   &out,
			Parallel: maxp,
			Context:  context.Background(),
			Runner:   env,
		}
		err = lp.Exec(&params)
		if err != nil {
			return nil, err
		}
		outbuf := out.Bytes()
		var datum ion.Datum
		var outlst []ion.Datum
		st.Reset()
		for len(outbuf) > 0 {
			datum, outbuf, err = ion.ReadDatum(st, outbuf)
			if err != nil {
				return nil, err
			}
			if datum.IsEmpty() {
				continue // just a symbol table
			}
			datum = unsymbolize(datum, st)
			outlst = append(outlst, datum)
		}
		return outlst, nil
	}

	gotout, err := run(q.Query, q.Input, q.SymbolTable, flags)
	if err != nil {
		return err
	}
	q.SymbolTable.Reset()
	fixup(gotout, q.SymbolTable)
	fixup(q.Output, q.SymbolTable)
	if len(q.Output) != len(gotout) {
		err = fmt.Errorf("%d rows output; expected %d", len(gotout), len(q.Output))
	}
	nErrors := 0
	if err != nil {
		nErrors++
	}
	if flags&FlagSplit != 0 {
		// FIXME: split queries cannot be made
		// deterministic, so just check the number
		// of results for now
		return err
	}
	for i := range q.Output {
		if i >= len(gotout) {
			err = errors.Join(err, fmt.Errorf("missing %s", toJSON(q.SymbolTable, q.Output[i])))
			continue
		}
		if !ion.Equal(q.Output[i], gotout[i]) {
			nErrors++
			err = errors.Join(err, fmt.Errorf("row %d: got  %srow %d: want %s",
				i, toJSON(q.SymbolTable, gotout[i]), i, toJSON(q.SymbolTable, q.Output[i])))
		}
		if nErrors > 10 {
			err = errors.Join(err, fmt.Errorf("... and more %d", nErrors))
			break
		}
	}
	return err
}

// parseCase parses the provided query,
// input and output strings into a test case
func parseCase(queryStr []string, inputsStr [][]string, outputStr []string, tags map[string]string) (tci *Case, err error) {

	// stripInlineComment removes comment from **correctly** formatted line.
	// For example: stripInlineComment(`{"x": 5} # sample data`) => `{"x": 5}`
	stripInlineComment := func(line []byte) []byte {
		pos := bytes.LastIndexByte(line, '}')
		if pos == -1 {
			return line
		}
		trimmed := bytes.TrimSpace(line[pos+1:])
		if len(trimmed) == 0 || trimmed[0] == '#' {
			return line[:pos+1]
		}
		return line
	}

	part2bytes := func(part []string) []byte {
		var res []byte
		for i := range part {
			res = append(res, []byte(part[i])...)
			res = append(res, '\n')
		}

		return res
	}

	jsonrl2bytes := func(part []string) []byte {
		var res []byte
		for i := range part {
			res = append(res, stripInlineComment([]byte(part[i]))...)
			res = append(res, '\n')
		}

		return res
	}

	var inputs [][]byte
	for i := 0; i < len(inputsStr); i++ {
		inputs = append(inputs, jsonrl2bytes(inputsStr[i]))
	}

	output := jsonrl2bytes(outputStr)

	var inst ion.Symtab
	inputRows := make([][]ion.Datum, len(inputs))
	r := rand.New(rand.NewSource(0))
	for i := range inputRows {
		rows, err := IonizeRow(inputs[i], &inst, func() bool { return r.Intn(2) == 0 })
		if err != nil {
			return nil, fmt.Errorf("parsing input[%d] rows: %s", i, err)
		}
		inputRows[i] = rows
	}
	ouputRows, err := IonizeRow(output, &inst, func() bool { return false })
	if err != nil {
		return nil, fmt.Errorf("parsing output rows: %s", err)
	}

	query := part2bytes(queryStr)
	exprQuery, err := partiql.Parse(query)
	if err != nil {
		return nil, err
	}

	return &Case{
		QueryStr:    query,
		Query:       exprQuery,
		SymbolTable: &inst,
		Input:       inputRows,
		Output:      ouputRows,
		Tags:        tags,
	}, nil

}

// ReadCase reads a testcase file.
// The testcase must be in [tests.CaseSpec] format.
// See [tests.ReadSpec].
//
// Each test case must consist of at least 3 sections.
// The first section should be a SQL query, and the second and
// third sections should be sequences of JSON records.
// The second section is interpreted as table data and mapped
// to the table [input], and the third section is interpreted as output.
// If more than 3 sections are present, the middle sections are interpreted
// as inputs and mapped to the tables [input0], [input1], and so on.
func ReadCase(r io.Reader) (qtc *Case, err error) {
	spec, err := tests.ReadSpec(r)
	if err != nil {
		return nil, err
	}

	nSections := len(spec.Sections)
	if nSections < 3 {
		return nil, fmt.Errorf("expected at least 3 sections in testcase, got %d", nSections)
	}

	queryStr := spec.Sections[0]
	outputStr := spec.Sections[nSections-1]
	var inputsStr [][]string
	for i := 1; i < nSections-1; i++ {
		inputsStr = append(inputsStr, spec.Sections[i])
	}
	return parseCase(queryStr, inputsStr, outputStr, spec.Tags)
}

func ReadCaseFromFile(path string) (qtc *Case, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	return ReadCase(file)
}

type benchmarkSettings struct {
	Symbolizeprob float64 // symbolize probability: 0=never, 1=always
}

// ReadBenchmark reads a benchmark specification.
//
// Benchmark may contain either SQL query (text) and sample
// input (ND-JSON) separated with '---'.
//
// Alternatively, it may contain only an SQL query. But
// then the FROM part has to be a string which is treated
// as a filename and this file is read into the input (as NDJSON)
// relative to the given [root].
func ReadBenchmark(root fs.FS, r io.Reader) (*expr.Query, *benchmarkSettings, []byte, error) {
	spec, err := tests.ReadSpec(r)
	if err != nil {
		return nil, nil, nil, err
	}

	part2bytes := func(part []string) []byte {
		var res []byte
		for i := range part {
			res = append(res, []byte(part[i])...)
			res = append(res, '\n')
		}

		return res
	}

	text := part2bytes(spec.Sections[0])
	query, err := partiql.Parse(text)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse %q: %s", text, err)
	}

	var input []byte
	switch n := len(spec.Sections); n {
	case 1:
		// only query
		sel := query.Body.(*expr.Select)
		table := sel.From.(*expr.Table)
		file, ok := table.Expr.(expr.String)
		if !ok {
			return nil, nil, nil, fmt.Errorf("benchmark without input part has to refer an external JSONRL file")
		}

		f, err := root.Open(string(file))
		if err != nil {
			return nil, nil, nil, err
		}

		input, err = io.ReadAll(f)
		f.Close()
		if err != nil {
			return nil, nil, nil, err
		}

		// The testing framework expects path 'input'
		table.Expr = expr.Ident("input")

	case 2, 3:
		// query and inline input - note only the first input data is used,
		// output data would only be used by when testing this file, if the
		// benchmark file is compatible with a regular test case.
		input = part2bytes(spec.Sections[1])

	default:
		return nil, nil, nil, fmt.Errorf("expected at most 3 parts of benchmark, got %d", n)
	}

	bs := &benchmarkSettings{
		Symbolizeprob: 0.0,
	}
	v := spec.Tags["symbolize"]
	switch v {
	case "":
		// do nothing
	case "never":
		bs.Symbolizeprob = 0.0
	case "always":
		bs.Symbolizeprob = 1.0
	default:
		p, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("cannot parse symbolize=%q: %s", v, err)
		}
		if p < 0.0 || p > 1.0 {
			return nil, nil, nil, fmt.Errorf("symbolize=%f: wrong value, it has to be float in range [0, 1]", p)
		}
	}

	return query, bs, input, nil
}

// IonizeRow ionizes the row in b, give outgoing symbol table and symbolize function
func IonizeRow(b []byte, outst *ion.Symtab, symbolize func() bool) ([]ion.Datum, error) {

	var symbolizeRandomly func(d ion.Datum, st *ion.Symtab, symbolize func() bool) ion.Datum
	// walk d and replace 50% of the strings with stringSyms
	symbolizeRandomly = func(d ion.Datum, st *ion.Symtab, symbolize func() bool) ion.Datum {
		switch d.Type() {
		case ion.StructType:
			d, _ := d.Struct()
			fields := d.Fields(nil)
			for i := range fields {
				if str, err := fields[i].String(); err == nil {
					if symbolize() {
						fields[i].Datum = ion.Interned(st, str)
					}
				} else {
					fields[i].Datum = symbolizeRandomly(fields[i].Datum, st, symbolize)
				}
			}
			return ion.NewStruct(st, fields).Datum()
		case ion.ListType:
			d, _ := d.List()
			items := d.Items(nil)
			for i := range items {
				if str, err := items[i].String(); err == nil {
					if symbolize() {
						items[i] = ion.Interned(st, str)
					}
				} else {
					items[i] = symbolizeRandomly(items[i], st, symbolize)
				}
			}
			return ion.NewList(st, items).Datum()
		}
		return d
	}

	parseSpecialFPValues := func(s string) ion.Datum {
		const prefix = "float64:"
		if fn, ok := strings.CutPrefix(s, prefix); ok {
			switch fn {
			case "inf", "+inf":
				return ion.Float(math.Inf(+1))
			case "-inf":
				return ion.Float(math.Inf(-1))
			case "nan", "NaN":
				return ion.Float(math.NaN())
			case "-0":
				return ion.Float(math.Float64frombits(0x8000_0000_0000_0000))
			default:
				panic(fmt.Sprintf("unknown test function %q", s))
			}
		}
		return ion.Empty
	}

	var insertSpecialFPValues func(d ion.Datum, st *ion.Symtab) ion.Datum
	insertSpecialFPValues = func(d ion.Datum, st *ion.Symtab) ion.Datum {
		switch d.Type() {
		case ion.StructType:
			d, _ := d.Struct()
			fields := d.Fields(nil)
			for i := range fields {
				if str, err := fields[i].String(); err == nil {
					if v := parseSpecialFPValues(str); !v.IsEmpty() {
						fields[i].Datum = v
					}
				} else {
					fields[i].Datum = insertSpecialFPValues(fields[i].Datum, st)
				}
			}
			return ion.NewStruct(st, fields).Datum()
		case ion.ListType:
			d, _ := d.List()
			items := d.Items(nil)
			for i := range items {
				if str, err := items[i].String(); err == nil {
					if v := parseSpecialFPValues(str); !v.IsEmpty() {
						items[i] = v
					}
				} else {
					items[i] = insertSpecialFPValues(items[i], st)
				}
			}
			return ion.NewList(st, items).Datum()
		}
		return d
	}

	if len(b) == 0 {
		return nil, nil
	}
	d := json.NewDecoder(bytes.NewReader(b))
	var lst []ion.Datum
	for {
		d, err := ion.FromJSON(outst, d)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		d = insertSpecialFPValues(d, outst)
		if symbolize != nil {
			d = symbolizeRandomly(d, outst, symbolize)
		}
		lst = append(lst, d)
	}
	return lst, nil
}

func flatten(lst []ion.Datum, st *ion.Symtab) []byte {
	var outbuf ion.Buffer
	for i := range lst {
		lst[i].Encode(&outbuf, st)
	}
	tail := outbuf.Bytes()
	outbuf.Set(nil)
	st.Marshal(&outbuf, true)
	outbuf.UnsafeAppend(tail)
	return outbuf.Bytes()
}

func toJSON(st *ion.Symtab, d ion.Datum) string {
	if d.IsEmpty() {
		return "<nil>"
	}
	var ib ion.Buffer
	ib.StartChunk(st)
	d.Encode(&ib, st)
	br := bufio.NewReader(bytes.NewReader(ib.Bytes()))
	var sb strings.Builder
	_, err := ion.ToJSON(&sb, br)
	if err != nil {
		panic(err)
	}
	return sb.String()
}
