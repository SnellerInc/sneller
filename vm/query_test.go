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

package vm_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/ion/versify"
	"github.com/SnellerInc/sneller/ion/zion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tests"
	"github.com/SnellerInc/sneller/vm"
)

type bufhandle []byte

func (b bufhandle) Open(_ context.Context) (vm.Table, error) {
	return vm.BufferTable([]byte(b), len(b)), nil
}

func (b bufhandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected bufhandle.Encode")
}

func (b bufhandle) Size() int64 {
	return int64(len(b))
}

type chunkshandle struct {
	chunks [][]byte
	fields []string
}

func (c *chunkshandle) Open(_ context.Context) (vm.Table, error) {
	return c, nil
}

func (c *chunkshandle) Size() int64 {
	n := int64(0)
	for i := range c.chunks {
		n += int64(len(c.chunks[i]))
	}
	return n
}

func (c *chunkshandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected chunkshandle.Encode")
}

func (c *chunkshandle) writeZion(dst io.WriteCloser) error {
	var mem []byte
	var err error
	var enc zion.Encoder
	for i := range c.chunks {
		mem, err = enc.Encode(c.chunks[i], mem[:0])
		if err != nil {
			dst.Close()
			return err
		}
		_, err = dst.Write(mem)
		if err != nil {
			dst.Close()
			return err
		}
	}
	if shw, ok := dst.(vm.EndSegmentWriter); ok {
		shw.EndSegment()
	} else {
		dst.Close()
		return fmt.Errorf("%T not an EndSegmentWriter?", dst)
	}
	return dst.Close()
}

func (c *chunkshandle) WriteChunks(dst vm.QuerySink, parallel int) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	if zw, ok := dst.(blockfmt.ZionWriter); ok && zw.ConfigureZion(c.fields) {
		return c.writeZion(w)
	}
	tmp := vm.Malloc()
	defer vm.Free(tmp)
	for _, buf := range c.chunks {
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
	}
	if shw, ok := w.(vm.EndSegmentWriter); ok {
		shw.EndSegment()
	} else {
		w.Close()
		return fmt.Errorf("%T not an EndSegmentWriter?", w)
	}
	return w.Close()
}

type parallelchunks struct {
	chunks [][]byte
	fields []string
}

func (p *parallelchunks) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected parallelchunks.Encode")
}

func (p *parallelchunks) Size() int64 {
	n := int64(0)
	for i := range p.chunks {
		n += int64(len(p.chunks[i]))
	}
	return n
}

func (p *parallelchunks) SplitBy(parts []string) ([]plan.TablePart, error) {
	type part struct {
		keys, values []ion.Datum
	}

	okType := func(d ion.Datum) bool {
		switch d.Type() {
		case ion.StringType, ion.IntType, ion.UintType, ion.FloatType:
			return true
		default:
			return false
		}
	}

	partset := make(map[string]*part)

	var st ion.Symtab
	var tmp ion.Buffer
	var d ion.Datum
	var err error
	for i := range p.chunks {
		buf := p.chunks[i]
		for len(buf) > 0 {
			d, buf, err = ion.ReadDatum(&st, buf)
			if err != nil {
				panic(err)
			}
			s, err := d.Struct()
			if err != nil {
				panic(err)
			}
			var keys []ion.Datum
			for _, part := range parts {
				f, ok := s.FieldByName(part)
				if !ok {
					return nil, fmt.Errorf("record missing %q field", part)
				}
				if !okType(f.Datum) {
					return nil, fmt.Errorf("value for %q has unacceptable type", part)
				}
				keys = append(keys, f.Datum)
			}
			tmp.Reset()
			for _, key := range keys {
				key.Encode(&tmp, &st)
			}
			if p := partset[string(tmp.Bytes())]; p != nil {
				p.values = append(p.values, d)
			} else {
				partset[string(tmp.Bytes())] = &part{
					keys:   keys,
					values: []ion.Datum{d},
				}
			}
		}
	}

	ret := make([]plan.TablePart, 0, len(partset))
	for _, v := range partset {
		st.Reset()
		tmp.Reset()
		data := flatten(v.values, &st)
		ret = append(ret, plan.TablePart{
			Handle: bufhandle(data),
			Parts:  v.keys,
		})
	}
	return ret, nil
}

/* FIXME: uncomment this once BOOL_AND() is fixed
   so that partitions for which the condition is not satisfied
   do not cause the merged result to short-circuit to FALSE
func (p *parallelchunks) Split() (plan.Subtables, error) {
	tp := &plan.LocalTransport{Threads: 1}
	if len(p.chunks) == 1 {
		return plan.SubtableList{{Transport: tp, Handle: p}}, nil
	}
	first := p.chunks[:len(p.chunks)/2]
	second := p.chunks[len(p.chunks)/2:]
	return plan.SubtableList{
		{
			Transport: tp,
			Handle: &parallelchunks{chunks: first, fields: p.fields},
		},
		{
			Transport: tp,
			Handle: &parallelchunks{chunks: second, fields: p.fields},
		},
	}, nil
}
*/

func (p *parallelchunks) Open(_ context.Context) (vm.Table, error) {
	return p, nil
}

func (p *parallelchunks) WriteChunks(dst vm.QuerySink, parallel int) error {
	outputs := make([]io.WriteCloser, len(p.chunks))
	errlist := make([]error, len(p.chunks))
	var wg sync.WaitGroup
	for i := range outputs {
		w, err := dst.Open()
		if err != nil {
			return err
		}
		outputs[i] = w
	}
	wg.Add(len(outputs))
	for i := range outputs {
		go func(w io.WriteCloser, mem []byte, errp *error) {
			defer wg.Done()
			seterr := func(e error) {
				if e != nil && !errors.Is(e, io.EOF) && *errp == nil {
					*errp = e
				}
			}
			var err error
			if zw, ok := w.(blockfmt.ZionWriter); ok && zw.ConfigureZion(p.fields) {
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
		}(outputs[i], p.chunks[i], &errlist[i])
	}
	wg.Wait()
	for i := range errlist {
		if errlist[i] != nil {
			return errlist[i]
		}
	}
	return nil
}

type benchTable struct {
	buf   []byte
	count int64
}

func (b *benchTable) Open(_ context.Context) (vm.Table, error) {
	return b, nil
}

func (b *benchTable) Size() int64 {
	return b.count * int64(len(b.buf))
}

func (b *benchTable) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected benchTable.Encode")
}

func (b *benchTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	// FIXME: the memory being sent to the core here
	// is not from vm.Malloc, so it is going to be copied...
	return vm.SplitInput(dst, parallel, func(w io.Writer) error {
		for atomic.AddInt64(&b.count, -1) >= 0 {
			_, err := w.Write(b.buf)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type queryenv struct {
	in []plan.TableHandle
}

func (e *queryenv) handle(t expr.Node) (plan.TableHandle, bool) {
	p, ok := expr.FlatPath(t)
	if !ok {
		return nil, false
	}
	if len(p) != 1 {
		return nil, false
	}
	if p[0] == "input" && len(e.in) == 1 {
		return e.in[0], true
	}
	var i int
	if n, _ := fmt.Sscanf(p[0], "input%d", &i); n > 0 && i >= 0 && i < len(e.in) {
		return e.in[i], true
	}
	return nil, false
}

func setHints(h plan.TableHandle, hints *plan.Hints) {
	switch t := h.(type) {
	case *chunkshandle:
		t.fields = hints.Fields
	case *parallelchunks:
		t.fields = hints.Fields
	}
}

// Stat implements plan.Env.Stat
func (e *queryenv) Stat(t expr.Node, h *plan.Hints) (plan.TableHandle, error) {
	handle, ok := e.handle(t)
	if !ok {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	setHints(handle, h)
	return handle, nil
}

var _ plan.Indexer = &queryenv{}

type handleIndex struct {
	h plan.TableHandle
}

func (h *handleIndex) TimeRange(path []string) (min, max date.Time, ok bool) {
	return // unimplemented for now
}

func (h *handleIndex) HasPartition(x string) bool {
	sh, ok := h.h.(plan.PartitionHandle)
	if ok {
		// XXX very slow:
		_, err := sh.SplitBy([]string{x})
		return err == nil
	}
	return false
}

func (e *queryenv) Index(t expr.Node) (plan.Index, error) {
	handle, ok := e.handle(t)
	if !ok {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	return &handleIndex{handle}, nil
}

var _ plan.TableLister = (*queryenv)(nil)

func (e *queryenv) ListTables(db string) ([]string, error) {
	if db != "" {
		return nil, fmt.Errorf("no databases")
	}
	if len(e.in) == 1 {
		return []string{"input"}, nil
	}
	ts := make([]string, len(e.in))
	for i := range e.in {
		ts[i] = fmt.Sprintf("input%d", i)
	}
	return ts, nil
}

// walk d and replace 50% of the strings with stringSyms
func symbolizeRandomly(d ion.Datum, st *ion.Symtab, r *rand.Rand) ion.Datum {
	switch d.Type() {
	case ion.StructType:
		d, _ := d.Struct()
		fields := d.Fields(nil)
		for i := range fields {
			if str, err := fields[i].String(); err == nil {
				if r.Intn(2) == 0 {
					fields[i].Datum = ion.Interned(st, str)
				}
			} else {
				fields[i].Datum = symbolizeRandomly(fields[i].Datum, st, r)
			}
		}
		return ion.NewStruct(st, fields).Datum()
	case ion.ListType:
		d, _ := d.List()
		items := d.Items(nil)
		for i := range items {
			if str, err := items[i].String(); err == nil {
				if r.Intn(2) == 0 {
					items[i] = ion.Interned(st, str)
				}
			} else {
				items[i] = symbolizeRandomly(items[i], st, r)
			}
		}
		return ion.NewList(st, items).Datum()
	}
	return d
}

func parseSpecialFPValues(s string) ion.Datum {
	const prefix = "float64:"
	if strings.HasPrefix(s, prefix) {
		s = s[len(prefix):]
		switch s {
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

func insertSpecialFPValues(d ion.Datum, st *ion.Symtab) ion.Datum {
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

func rows(b []byte, outst *ion.Symtab, symbolize bool) ([]ion.Datum, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r := rand.New(rand.NewSource(0))
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
		if symbolize {
			d = symbolizeRandomly(d, outst, r)
		}
		lst = append(lst, d)
	}
	return lst, nil
}

// stripInlineComment removes comment from **correctly** formated line.
// For example: stripInlineComment(`{"x": 5} # sample data`) => `{"x": 5}`
func stripInlineComment(line []byte) []byte {
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

// return a symbol table with the symbols
// randomly shuffled
func shuffled(st *ion.Symtab) *ion.Symtab {
	ret := &ion.Symtab{}
	// if only one symbol is in the input corpus,
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

type runflags int

const (
	flagParallel    = 1 << iota // run in parallel
	flagResymbolize             // resymbolize
	flagShuffle                 // shuffle symbol table
	flagSplit                   // use a split plan
)

// run a query on the given input table and yield the output list
func run(t *testing.T, q *expr.Query, in [][]ion.Datum, st *ion.Symtab, flags runflags) []ion.Datum {
	input := make([]plan.TableHandle, len(in))
	maxp := 0
	for i, in := range in {
		if (flags&flagResymbolize) != 0 && len(in) > 1 {
			half := len(in) / 2
			first := flatten(in[:half], st)

			var second []byte
			if flags&flagShuffle != 0 {
				second = flatten(in[half:], shuffled(st))
			} else {
				second = flatten(in[half:], st)
			}
			if flags&flagParallel != 0 {
				maxp = 2
				input[i] = &parallelchunks{chunks: [][]byte{first, second}}
			} else {
				input[i] = &chunkshandle{chunks: [][]byte{first, second}}
			}
		} else {
			input[i] = bufhandle(flatten(in, st))
		}
	}
	env := &queryenv{in: input}
	var tree *plan.Tree
	var err error
	if flags&flagSplit != 0 {
		tree, err = plan.NewSplit(q, env)
	} else {
		tree, err = plan.New(q, env)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("plan:\n%s", tree.String())
	var out bytes.Buffer
	lp := plan.LocalTransport{Threads: maxp}
	params := plan.ExecParams{
		Output:   &out,
		Parallel: maxp,
		Context:  context.Background(),
	}
	err = lp.Exec(tree, &params)
	if err != nil {
		t.Fatal(err)
	}
	outbuf := out.Bytes()
	var datum ion.Datum
	var outlst []ion.Datum
	st.Reset()
	for len(outbuf) > 0 {
		datum, outbuf, err = ion.ReadDatum(st, outbuf)
		if err != nil {
			t.Fatal(err)
		}
		datum = unsymbolize(datum, st)
		outlst = append(outlst, datum)
	}
	return outlst
}

func unsymbolize(d ion.Datum, st *ion.Symtab) ion.Datum {
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

// fix up the symbols in lst so that they
// match the associated symbols in st
func fixup(lst []ion.Datum, st *ion.Symtab) {
	// we reset the symbol elements of structure fields
	// inside Encode, so the easiest way to do this is
	// just encode the data and throw it away
	var dummy ion.Buffer
	for i := range lst {
		lst[i].Encode(&dummy, st)
		dummy.Reset()
	}
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

// can the inputs to this query be shuffled?
func canShuffle(q *expr.Query) bool {
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
	if sel.GroupBy != nil || sel.Distinct {
		// these permute the output ordering by hash
		return false
	}
	return true
}

// does the output need to be shuffled along
// with the input?
func shuffleOutput(q *expr.Query) bool {
	sel, ok := q.Body.(*expr.Select)
	if !ok {
		return false
	}
	// ORDER BY, GROUP BY, and DISTINCT
	// all have output orderings that are
	// independent of the input
	return sel.OrderBy == nil && sel.GroupBy == nil && !sel.Distinct
}

// can symtab be safely shuffled?
func shuffleSymtab(q *expr.Query) bool {
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

const shufflecount = 10

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

func testInput(t *testing.T, query []byte, st *ion.Symtab, in [][]ion.Datum, out []ion.Datum) {
	var done bool
	for i := 0; i < shufflecount*2; i++ {
		name := fmt.Sprintf("shuffle-%d", i)
		split := false
		if i >= shufflecount {
			split = true
			name = fmt.Sprintf("shuffle-split-%d", i)
		}
		t.Run(name, func(t *testing.T) {
			st.Reset()
			q, err := partiql.Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			flags := runflags(0)
			// if the outputs are input-order-independent,
			// then we can test the query with parallel inputs:
			if i > 0 && len(out) <= 1 || !shuffleOutput(q) {
				flags |= flagParallel
			}
			if shuffleSymtab(q) {
				flags |= flagShuffle
			}
			if i > 0 {
				flags |= flagResymbolize
			}
			if split {
				flags |= flagSplit
			}
			gotout := run(t, q, in, st, flags)
			st.Reset()
			fixup(gotout, st)
			fixup(out, st)
			if len(out) != len(gotout) {
				t.Errorf("%d rows out; expected %d", len(gotout), len(out))
			}
			errors := 0
			for i := range out {
				if i >= len(gotout) {
					t.Errorf("missing %s", toJSON(st, out[i]))
					continue
				}
				if !ion.Equal(out[i], gotout[i]) {
					errors++
					t.Errorf("row %d: got  %s", i, toJSON(st, gotout[i]))
					t.Errorf("row %d: want %s", i, toJSON(st, out[i]))
				}
				if errors > 10 {
					t.Log("... and more errors")
					break
				}
			}
			if t.Failed() || !canShuffle(q) {
				done = true
				return
			}
			// shuffle around the input (and maybe output)
			// lanes so that we increase the coverage of
			// lane-specific branches
			if len(in) != 1 {
				// don't shuffle multiple inputs
			} else if in := in[0]; shuffleOutput(q) && len(in) == len(out) {
				rand.Shuffle(len(in), func(i, j int) {
					in[i], in[j] = in[j], in[i]
					out[i], out[j] = out[j], out[i]
				})
			} else {
				rand.Shuffle(len(in), func(i, j int) {
					in[i], in[j] = in[j], in[i]
				})
			}
		})
		if done {
			break
		}
	}
}

// readTestcase reads a testcase file, that contains three or more
// part sepearated with '---'.

// The first part is an SQL query (text), the last part is the
// expected rows (JSONRL) and the middle parts are inputs (also
// in the JSONRL format).
func readTestcase(t testing.TB, fname string) (query []byte, inputs [][]byte, output []byte) {
	parts, err := tests.ParseTestcase(fname)
	if err != nil {
		t.Fatal(err)
	}

	n := len(parts)
	if n < 3 {
		t.Fatalf("expected at least 3 parts of testcase, got %d", n)
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

	query = part2bytes(parts[0])
	output = jsonrl2bytes(parts[n-1])

	for i := 1; i < n-1; i++ {
		inputs = append(inputs, jsonrl2bytes(parts[i]))
	}

	return query, inputs, output
}

// readBenchmark reads a benchmark specification.
//
// Benchmark may contain either SQL query (text) and sample
// input (JSONRL) separarted with '---'.
//
// Alternatively, it may contain only an SQL query. But
// then the FROM part has to be a string which is treated
// as a filename and this file is read into the input (JSONRL).
func readBenchmark(t testing.TB, fname string) (*expr.Query, []byte) {
	parts, err := tests.ParseTestcase(fname)
	if err != nil {
		t.Fatal(err)
	}

	part2bytes := func(part []string) []byte {
		var res []byte
		for i := range part {
			res = append(res, []byte(part[i])...)
			res = append(res, '\n')
		}

		return res
	}

	text := part2bytes(parts[0])
	query, err := partiql.Parse(text)
	if err != nil {
		t.Fatalf("cannot parse %q: %s", text, err)
	}

	var input []byte
	switch n := len(parts); n {
	case 1: // only query
		sel := query.Body.(*expr.Select)
		table := sel.From.(*expr.Table)
		file, ok := table.Expr.(expr.String)
		if !ok {
			t.Fatal("benchark without input part has to refer an external JSONRL file")
		}

		path := filepath.Join(filepath.Dir(fname), string(file))
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		input, err = io.ReadAll(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}

		// The testing framework expects path 'input'
		table.Expr = expr.Ident("input")

	case 2: // query and inline input
		input = part2bytes(parts[1])

	default:
		t.Fatalf("expected at most two parts of benchmark, got %d", n)
	}

	return query, input
}

func testPath(t *testing.T, fname string) {
	query, inputs, output := readTestcase(t, fname)
	var inst ion.Symtab
	inrows := make([][]ion.Datum, len(inputs))
	for i := range inrows {
		rows, err := rows(inputs[i], &inst, true)
		if err != nil {
			t.Fatalf("parsing input[%d] rows: %s", i, err)
		}
		inrows[i] = rows
	}
	outrows, err := rows(output, &inst, false)
	if err != nil {
		t.Fatalf("parsing output rows: %s", err)
	}
	testInput(t, query, &inst, inrows, outrows)
}

func benchInput(b *testing.B, sel *expr.Query, inbuf []byte, rows int) {
	bt := &benchTable{
		count: int64(b.N),
		buf:   inbuf,
	}
	env := &queryenv{in: []plan.TableHandle{bt}}
	tree, err := plan.New(sel, env)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(inbuf)))
	b.ResetTimer()
	start := time.Now()
	var stats plan.ExecStats
	err = plan.Exec(tree, io.Discard, &stats)
	if err != nil {
		b.Fatal(err)
	}
	elapsed := time.Since(start)
	x := (float64(b.N) * float64(rows)) / (float64(elapsed) / float64(time.Second))
	b.ReportMetric(x, "rows/s")
}

func versifyGetter(inst *ion.Symtab, inrows []ion.Datum) func() ([]byte, int) {
	return func() ([]byte, int) {
		var u versify.Union
		for i := range inrows {
			if u == nil {
				u = versify.Single(inrows[i])
			} else {
				u = u.Add(inrows[i])
			}
		}
		src := rand.New(rand.NewSource(0))

		// generate a corpus that is larger than L3 cache
		// so that we actually measure the performance of
		// streaming the data in from DRAM
		const targetSize = 64 * 1024 * 1024
		var outbuf ion.Buffer
		inst.Marshal(&outbuf, true)
		rows := 0
		for {
			d := u.Generate(src)
			d.Encode(&outbuf, inst)
			rows++
			size := outbuf.Size()
			if size > targetSize {
				break
			}
		}
		return outbuf.Bytes(), rows
	}
}

func benchPath(b *testing.B, fname string) {
	query, input := readBenchmark(b, fname)
	var inst ion.Symtab

	inrows, err := rows(input, &inst, false)
	if err != nil {
		b.Fatalf("parsing input rows: %s", err)
	}
	if len(inrows) == 0 {
		b.Skip()
	}
	// don't actually versify unless b.Run runs
	// the inner benchmark; versification is expensive
	getter := versifyGetter(&inst, inrows)
	var rowmem []byte
	var rows int
	b.Run(fname, func(b *testing.B) {
		if rowmem == nil {
			rowmem, rows = getter()
		}
		benchInput(b, query, rowmem, rows)
	})
}

// symLinkFlag flag to toggle whether symlinks are crawled while searching for test cases.
// '-symlink=true' (or '-symlink') is default. To switch off symlink crawling use '-symlink=false'
var symLinkFlag = flag.Bool("symlink", true, "whether to crawl tests using symbolic links")

func BenchmarkTestQueries(b *testing.B) {
	for _, dir := range []string{"./testdata/queries/", "./testdata/benchmarks/"} {
		bench, err := findQueries(dir, ".bench", *symLinkFlag)
		if err != nil {
			b.Fatal(err)
		}

		for i := range bench {
			benchPath(b, bench[i].path)
		}
	}
}

// TestQueries runs all the tests
// in testdata/queries/*.test
//
// In order to run this test suite
// as quickly as possible, tests are
// run in parallel.
func TestQueries(t *testing.T) {
	test, err := findQueries("./testdata/queries/", ".test", *symLinkFlag)
	if err != nil {
		t.Fatal(err)
	}
	vm.Errorf = t.Logf
	defer func() {
		vm.Errorf = nil
	}()
	for i := range test {
		path := test[i].path
		t.Run(test[i].name, func(t *testing.T) {
			t.Parallel()
			testPath(t, path)
		})
	}
}

type queryTest struct {
	name, path string
}

func findQueries(dir, suffix string, symlink bool) ([]queryTest, error) {
	var tests []queryTest

	rootdir := filepath.Clean(dir)
	prefix := rootdir + "/"

	var walker fs.WalkDirFunc
	walker = func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if symlink && d.Type()&fs.ModeSymlink != 0 {
			path, _ = filepath.EvalSymlinks(path)
			return filepath.WalkDir(path, walker)
		}
		if !strings.HasSuffix(d.Name(), suffix) {
			return nil
		}

		name := strings.TrimPrefix(path, prefix)
		name = strings.TrimSuffix(name, suffix)
		name = strings.ReplaceAll(name, "/", "-")

		t := queryTest{
			name: name,
			path: path,
		}

		tests = append(tests, t)
		return nil
	}
	return tests, filepath.WalkDir(rootdir, walker)
}
