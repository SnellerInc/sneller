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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/ion/versify"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/testquery"
	"github.com/SnellerInc/sneller/vm"
)

var _ blockfmt.ZionWriter = &vm.TeeWriter{}

// symLinkFlag flag to toggle whether symlinks are crawled while searching for test cases.
// '-symlink=true' (or '-symlink') is default. To switch off symlink crawling use '-symlink=false'
var symLinkFlag = flag.Bool("symlink", true, "whether to crawl tests using symbolic links")

var traceBytecodeFlag = flag.Bool("trace", false, "print bytecode on stdout")

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

func testInput(t *testing.T, tci *testquery.Case, shuffleCount int) {
	var done bool

	for i := 0; i < shuffleCount*2; i++ {
		name := fmt.Sprintf("shuffle-%d", i)
		split := false
		if i >= shuffleCount {
			split = true
			name = fmt.Sprintf("shuffle-split-%d", i)
		}

		t.Run(name, func(t *testing.T) {
			tci.SymbolTable.Reset()

			var err error
			if tci.Query, err = partiql.Parse(tci.QueryStr); err != nil {
				t.Fatal(err)
			}

			flags := testquery.RunFlags(0)
			// if the outputs are input-order-independent,
			// then we can test the query with parallel inputs:
			if i > 0 && len(tci.Output) <= 1 || !testquery.NeedShuffleOutput(tci.Query) {
				flags |= testquery.FlagParallel
			}
			if testquery.CanShuffleSymtab(tci.Query) {
				flags |= testquery.FlagShuffle
			}
			if i > 0 {
				flags |= testquery.FlagResymbolize
			}
			if split {
				flags |= testquery.FlagSplit
			}
			if err := tci.Execute(flags); err != nil {
				t.Error(err)
			}
			if t.Failed() || !testquery.CanShuffleInput(tci.Query) {
				done = true
				return
			}
			tci.Shuffle()
		})
		if done {
			break
		}
	}
}

func benchInput(b *testing.B, sel *expr.Query, inbuf []byte, rows int) {
	bt := &benchTable{
		count: int64(b.N),
		buf:   inbuf,
	}

	env := &testquery.Env{In: []plan.TableHandle{bt}}
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

func versifyInput(inst *ion.Symtab, inrows []ion.Datum) ([]byte, int) {
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
	start := time.Now()
	for {
		d := u.Generate(src)
		d.Encode(&outbuf, inst)
		rows++
		size := outbuf.Size()
		if rows == len(inrows) {
			// After processing all the input rows,
			// try to predict how much time the rest of
			// generating data might take. If it would be
			// too long, just repeat the already generated
			// data.
			coef := float64(targetSize) / float64(size-symtabSize)
			elapsed := time.Since(start)
			estimation := time.Duration(float64(elapsed) * coef)
			if estimation > 3*time.Second {
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

func benchPath(b *testing.B, qt queryTest) {
	var query *expr.Query
	var rowmem []byte
	var rows int
	b.Run(qt.name, func(b *testing.B) {
		if query == nil {
			root := os.DirFS(filepath.Dir(qt.path))
			name := filepath.Base(qt.path)
			f, err := root.Open(name)
			if err != nil {
				b.Fatal(err)
			}
			q, bs, input, err := testquery.ReadBenchmark(root, f)
			f.Close()
			if err != nil {
				b.Fatal(err)
			}
			query = q
			var inst ion.Symtab

			prob := bs.Symbolizeprob
			r := rand.New(rand.NewSource(0))
			symbolize := func() bool {
				return r.Float64() > prob
			}

			inrows, err := testquery.IonizeRow(input, &inst, symbolize)
			if err != nil {
				b.Fatalf("parsing input rows: %s", err)
			}
			if len(inrows) == 0 {
				b.Skip()
			}
			rowmem, rows = versifyInput(&inst, inrows)
		}
		benchInput(b, query.Clone(), rowmem, rows)
	})
}

func BenchmarkTestQueries(b *testing.B) {
	for _, dir := range []string{"./testdata/queries/", "./testdata/benchmarks/"} {
		bench, err := findQueries(dir, ".bench", *symLinkFlag)
		if err != nil {
			b.Fatal(err)
		}

		for i := range bench {
			benchPath(b, bench[i])
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
	if *traceBytecodeFlag {
		vm.Trace(os.Stdout, vm.TraceSSAText)
	}

	defer func() {
		vm.Errorf = nil
	}()

	root := os.DirFS(".")
	for i := range test {
		path := test[i].path
		t.Run(test[i].name, func(t *testing.T) {
			t.Parallel()
			f, err := root.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			tci, err := testquery.ReadCase(f)
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			testInput(t, tci, 10)
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
