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
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/versify"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/testquery"
	"github.com/SnellerInc/sneller/vm"
)

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

func testInput(t *testing.T, query []byte, st *ion.Symtab, in [][]ion.Datum, out []ion.Datum) {
	var done bool
	for i := 0; i < testquery.Shufflecount*2; i++ {
		name := fmt.Sprintf("shuffle-%d", i)
		split := false
		if i >= testquery.Shufflecount {
			split = true
			name = fmt.Sprintf("shuffle-split-%d", i)
		}
		t.Run(name, func(t *testing.T) {
			st.Reset()
			q, err := partiql.Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			flags := testquery.RunFlags(0)
			// if the outputs are input-order-independent,
			// then we can test the query with parallel inputs:
			if i > 0 && len(out) <= 1 || !shuffleOutput(q) {
				flags |= testquery.FlagParallel
			}
			if shuffleSymtab(q) {
				flags |= testquery.FlagShuffle
			}
			if i > 0 {
				flags |= testquery.FlagResymbolize
			}
			if split {
				flags |= testquery.FlagSplit
			}
			if err = testquery.ExecuteQuery(q, st, in, out, flags); err != nil {
				t.Error(err)
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

func testPath(t *testing.T, fname string) {
	query, inputs, output, err := testquery.ReadTestcase(fname)
	if err != nil {
		t.Fatal(err)
	}
	var inst ion.Symtab
	inrows := make([][]ion.Datum, len(inputs))
	r := rand.New(rand.NewSource(0))
	for i := range inrows {
		rows, err := testquery.Rows(inputs[i], &inst, func() bool { return r.Intn(2) == 0 })
		if err != nil {
			t.Fatalf("parsing input[%d] rows: %s", i, err)
		}
		inrows[i] = rows
	}
	outrows, err := testquery.Rows(output, &inst, func() bool { return false })
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
	env := &testquery.Queryenv{In: []plan.TableHandle{bt}}
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

		slowProgress := false
		symtabSize := outbuf.Size()
		for {
			d := u.Generate(src)
			d.Encode(&outbuf, inst)
			rows++
			size := outbuf.Size()
			if rows == len(inrows) {
				coef := float64(targetSize) / float64(size)
				if coef > 300_000.0 {
					slowProgress = true
					break
				}
			}
			if size > targetSize {
				break
			}
		}

		if slowProgress {
			n := outbuf.Size() - symtabSize
			tmp := make([]byte, n)
			copy(tmp, outbuf.Bytes()[symtabSize:])
			for {
				outbuf.UnsafeAppend(tmp)
				size := outbuf.Size()
				if size > targetSize {
					break
				}
			}
		}

		return outbuf.Bytes(), rows
	}
}

func benchPath(b *testing.B, fname string) {
	b.Run(fname, func(b *testing.B) {
		query, bs, input, err := testquery.ReadBenchmark(fname)
		if err != nil {
			b.Fatal(err)
		}
		var inst ion.Symtab

		prob := bs.Symbolizeprob
		r := rand.New(rand.NewSource(0))
		symbolize := func() bool {
			return r.Float64() > prob
		}

		inrows, err := testquery.Rows(input, &inst, symbolize)
		if err != nil {
			b.Fatalf("parsing input rows: %s", err)
		}
		if len(inrows) == 0 {
			b.Skip()
		}
		getter := versifyGetter(&inst, inrows)
		rowmem, rows := getter()
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
