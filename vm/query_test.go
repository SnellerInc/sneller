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
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/versify"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/vm"
)

var sepdash = []byte("---")

type bufhandle []byte

func (b bufhandle) Open(_ context.Context) (vm.Table, error) {
	return vm.BufferTable([]byte(b), len(b)), nil
}

func (b bufhandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected bufhandle.Encode")
}

type chunkshandle [][]byte

func (c chunkshandle) Open(_ context.Context) (vm.Table, error) {
	return c, nil
}

func (c chunkshandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected chunkshandle.Encode")
}

func (c chunkshandle) Chunks() int { return len(c) }

func (c chunkshandle) WriteChunks(dst vm.QuerySink, parallel int) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	tmp := vm.Malloc()
	defer vm.Free(tmp)
	for _, buf := range c {
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
	return w.Close()
}

type parallelchunks [][]byte

func (p parallelchunks) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected parallelchunks.Encode")
}

func (p parallelchunks) Chunks() int { return len(p) }

func (p parallelchunks) Open(_ context.Context) (vm.Table, error) {
	return p, nil
}

func (p parallelchunks) WriteChunks(dst vm.QuerySink, parallel int) error {
	outputs := make([]io.WriteCloser, len(p))
	errors := make([]error, len(p))
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
			_, err := w.Write(mem)
			if err != nil {
				w.Close()
				*errp = err
			}
			*errp = w.Close()
		}(outputs[i], p[i], &errors[i])
	}
	wg.Wait()
	for i := range errors {
		if errors[i] != nil {
			return errors[i]
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

func (b *benchTable) Chunks() int { return int(b.count) }

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

func (e *queryenv) Stat(t, filter expr.Node) (plan.TableHandle, error) {
	p, ok := t.(*expr.Path)
	if !ok || p.Rest != nil {
		return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(t))
	}
	if p.First == "input" && len(e.in) == 1 {
		return e.in[0], nil
	}
	var i int
	if n, _ := fmt.Sscanf(p.First, "input%d", &i); n > 0 && i >= 0 && i < len(e.in) {
		return e.in[i], nil
	}
	return nil, fmt.Errorf("unexpected table expression %q", expr.ToString(p))
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
	switch d := d.(type) {
	case *ion.Struct:
		fields := d.Fields(nil)
		for i := range fields {
			if str, ok := fields[i].Value.(ion.String); ok {
				if r.Intn(2) == 0 {
					fields[i].Value = ion.Interned(str)
				}
			} else {
				fields[i].Value = symbolizeRandomly(fields[i].Value, st, r)
			}
		}
		return ion.NewStruct(st, fields)
	case *ion.List:
		items := d.Items(nil)
		for i := range items {
			if str, ok := items[i].(ion.String); ok {
				if r.Intn(2) == 0 {
					items[i] = ion.Interned(str)
				}
			} else {
				items[i] = symbolizeRandomly(items[i], st, r)
			}
		}
		return ion.NewList(st, items)
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
		if symbolize {
			d = symbolizeRandomly(d, outst, r)
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

func todash(rd *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		line, pre, err := rd.ReadLine()
		if err != nil {
			return out, err
		}
		if pre {
			return nil, fmt.Errorf("buffer not big enough to fit line beginning with %s", line)
		}
		if bytes.HasPrefix(line, sepdash) {
			return out, nil
		}
		// allow # line comments iff they begin the line
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
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

// run a query on the given input table and yield the output list
func run(t *testing.T, q *expr.Query, in [][]ion.Datum, st *ion.Symtab, resymbolize bool, parallel bool) []ion.Datum {
	input := make([]plan.TableHandle, len(in))
	for i, in := range in {
		if resymbolize && len(in) > 1 {
			half := len(in) / 2
			first := flatten(in[:half], st)
			second := flatten(in[half:], shuffled(st))
			if parallel {
				input[i] = parallelchunks{first, second}
			} else {
				input[i] = chunkshandle{first, second}
			}
		} else {
			input[i] = bufhandle(flatten(in, st))
		}
	}
	env := &queryenv{in: input}
	tree, err := plan.New(q, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("plan:\n%s", tree.String())
	var out bytes.Buffer
	var stats plan.ExecStats
	err = plan.Exec(tree, &out, &stats)
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
	switch d := d.(type) {
	case *ion.Struct:
		fields := d.Fields(nil)
		for i := range fields {
			if str, ok := fields[i].Value.(ion.Interned); ok {
				fields[i].Value = ion.String(str)
			} else {
				fields[i].Value = unsymbolize(fields[i].Value, st)
			}
		}
		return ion.NewStruct(st, fields)
	case *ion.List:
		items := d.Items(nil)
		for i := range items {
			if str, ok := items[i].(ion.Interned); ok {
				items[i] = ion.String(str)
			} else {
				items[i] = unsymbolize(items[i], st)
			}
		}
		return ion.NewList(st, items)
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

const shufflecount = 10

func toJSON(st *ion.Symtab, d ion.Datum) string {
	if d == nil {
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
	for i := 0; i < shufflecount; i++ {
		t.Run(fmt.Sprintf("shuffle-%d", i), func(t *testing.T) {
			st.Reset()
			q, err := partiql.Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			// when shuffling rows, split the input
			// into multiple chunks with different symbol
			// tables (but only if there's more than one symbol
			// that isn't part of the pre-interned set...)
			resymbolize := i > 0 && st.MaxID() > 11
			// if the outputs are input-order-independent,
			// then we can test the query with parallel inputs:
			parallel := len(out) <= 1 || !shuffleOutput(q)
			gotout := run(t, q, in, st, resymbolize, parallel)
			st.Reset()
			fixup(gotout, st)
			fixup(out, st)
			if len(out) != len(gotout) {
				t.Errorf("%d rows out; expected %d", len(gotout), len(out))
			}
			errors := 0
			for i := range out {
				if i >= len(gotout) {
					break
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

func readPath(t testing.TB, fname string) (query []byte, inputs [][]byte, output []byte) {
	f, err := os.Open(fname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	query, err = todash(rd)
	if err != nil {
		t.Fatal(err)
	}
	inputs = make([][]byte, 1)
	inputs[0], err = todash(rd)
	if err != nil {
		t.Fatal(err)
	}
	for first := true; ; first = false {
		b, err := todash(rd)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !first {
			inputs = append(inputs, output)
		}
		output = b
		if err == io.EOF {
			break
		}
	}
	return query, inputs, output
}

func testPath(t *testing.T, fname string) {
	query, inputs, output := readPath(t, fname)
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

func benchInput(b *testing.B, query, inbuf []byte, rows int) {
	bt := &benchTable{
		count: int64(b.N),
		buf:   inbuf,
	}
	sel, err := partiql.Parse(query)
	if err != nil {
		b.Fatal(err)
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
	err = plan.Exec(tree, ioutil.Discard, &stats)
	if err != nil {
		b.Fatal(err)
	}
	elapsed := time.Since(start)
	x := (float64(b.N) * float64(rows)) / (float64(elapsed) / float64(time.Second))
	b.ReportMetric(x, "rows/s")
}

func benchPath(b *testing.B, fname string) {
	query, inputs, _ := readPath(b, fname)
	var inst ion.Symtab
	if len(inputs) > 1 {
		// skip multi-table tests, for now
		b.Skip()
	}
	inrows, err := rows(inputs[0], &inst, false)
	if err != nil {
		b.Fatalf("parsing input rows: %s", err)
	}
	if len(inrows) == 0 {
		b.Skip()
	}
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
		d.Encode(&outbuf, &inst)
		rows++
		size := outbuf.Size()
		if size > targetSize {
			break
		}
	}
	benchInput(b, query, outbuf.Bytes(), rows)
}

func BenchmarkTestQueries(b *testing.B) {
	err := filepath.WalkDir("./testdata/queries/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".bench") {
			return nil
		}
		b.Run(strings.TrimSuffix(d.Name(), ".bench"), func(b *testing.B) {
			benchPath(b, path)
		})
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

// TestQueries runs all the tests
// in testdata/queries/*.test
//
// In order to run this test suite
// as quickly as possible, tests are
// run in parallel.
func TestQueries(t *testing.T) {
	start := vm.PagesUsed()
	vm.Errorf = t.Errorf
	t.Cleanup(func() {
		vm.Errorf = nil
	})
	t.Cleanup(func() {
		// detect a page leak
		end := vm.PagesUsed()
		if end != start {
			t.Errorf("pages used: %d -> %d", start, end)
		}
	})
	err := filepath.WalkDir("./testdata/queries/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".test") {
			t.Log("skip", d.Name())
			return nil
		}
		t.Run(strings.TrimSuffix(d.Name(), ".test"), func(t *testing.T) {
			t.Parallel()
			testPath(t, path)
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
