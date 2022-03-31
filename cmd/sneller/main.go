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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/vm"
)

var (
	dashf       bool
	dashj       bool
	dashN       bool
	dashg       bool
	dasho       string
	dashi       string
	printTime   bool
	printAllocs bool

	dst   io.WriteCloser
	stdin io.Reader
)

func init() {
	flag.BoolVar(&dashf, "f", false, "read arguments as files containing queries")
	flag.BoolVar(&dashg, "g", false, "just dump the query plan graphviz; do not execute")
	flag.BoolVar(&dashj, "j", false, "write output as JSON instead of ion")
	flag.BoolVar(&dashN, "N", false, "interpret input as NDJSON")
	flag.StringVar(&dasho, "o", "", "file for output (default is stdout)")
	flag.StringVar(&dashi, "i", "-", "file named stdin (default is stdin)")
	flag.BoolVar(&printTime, "t", false, "print execution time on stderr")
	flag.BoolVar(&printAllocs, "A", false, "print allocations stats on stderr")
	// mmap(2) is often slower than just some calls to pread(2), so
	// make mmap opt-in rather than opt-out
}

type memStats struct {
	mallocs uint64 // runtime.MemStats.Mallocs
	bytes   uint64 // runtime.MemStats.TotalAlloc
}

func (m *memStats) Start() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	m.mallocs = stats.Mallocs
	m.bytes = stats.TotalAlloc
}

func (m *memStats) Stop() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	m.mallocs = stats.Mallocs - m.mallocs
	m.bytes = stats.TotalAlloc - m.bytes
}

func formatSize(size uint64) string {
	res := fmt.Sprintf("%d B", size)
	if size > 1024*1024*1024 {
		res += fmt.Sprintf(" (%.2f GB)", float64(size)/(1024*1024*1024))
	} else if size > 1024*1024 {
		res += fmt.Sprintf(" (%.2f MB)", float64(size)/(1024*1024))
	} else if size > 1024 {
		res += fmt.Sprintf(" (%.2f kB)", float64(size)/1024)
	}

	return res
}

type eenv func(*expr.Table) (vm.Table, error)

type handle func() (vm.Table, error)

func (h handle) Open() (vm.Table, error) {
	return h()
}

func (e eenv) Stat(tbl *expr.Table) (plan.TableHandle, error) {
	return handle(func() (vm.Table, error) {
		return e(tbl)
	}), nil
}

func (e eenv) Schema(tbl *expr.Table) expr.Hint {
	return nil
}

func parse(arg string) *expr.Query {
	var buf []byte
	var err error
	if dashf {
		// arg is a file
		buf, err = ioutil.ReadFile(arg)
		if err != nil {
			exit(err)
		}
	} else {
		buf = []byte(arg)
	}
	q, err := partiql.Parse(buf)
	if err != nil {
		exit(err)
	}
	return q
}

func do(arg string) {
	tree, err := plan.New(parse(arg), eenv(func(tbl *expr.Table) (vm.Table, error) {
		str, ok := tbl.Expr.(expr.String)
		if !ok {
			return nil, fmt.Errorf("unexpected table expression %s", tbl.Expr)
		}
		fname := string(str)
		if strings.HasPrefix(fname, "s3://") {
			if dashN {
				return s3nd(fname)
			}
			return s3object(fname)
		}
		f, err := os.Open(fname)
		if err != nil {
			return nil, fmt.Errorf("inside query: %w", err)
		}
		i, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("inside query: %w", err)
		}
		if dashN {
			return &jstable{in: f, size: i.Size()}, nil
		}
		return srcTable(f, i.Size())
	}))
	if err != nil {
		exit(err)
	}

	if dashg {
		// -g -> just Graphviz
		plan.Graphviz(tree, dst)
		return
	}
	var stat plan.ExecStats
	err = plan.Exec(tree, dst, &stat)
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	flag.Parse()
	if dashg && dashj {
		// can't write graphviz output
		// as json, obviously...
		dashj = false
	}

	stdin = os.Stdin
	if dashi != "-" {
		f, err := os.Open(dashi)
		if err != nil {
			exit(err)
		}
		stdin = f
		defer f.Close()
	}
	dst = os.Stdout
	if dasho != "" {
		f, err := os.Create(dasho)
		if err != nil {
			exit(err)
		}
		dst = f
		defer f.Close()
	}
	var bg sync.WaitGroup
	if dashj {
		// if we are writing as JSON, have
		// the query data get written
		// asynchronously to the ion-to-JSON
		// translation layer
		r, w := io.Pipe()
		jsonout := dst
		dst = w
		bg.Add(1)
		go func() {
			defer bg.Done()
			_, err := ion.ToJSON(jsonout, bufio.NewReader(r))
			if err != nil {
				exit(err)
			}
		}()
	}

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	startTime := time.Now()
	var stats memStats
	stats.Start()

	for i := range args {
		do(args[i])
	}
	dst.Close()
	// wait for background threads
	bg.Wait()

	stats.Stop()
	elapsed := time.Since(startTime)
	if printTime {
		fmt.Fprintf(os.Stderr, "execution time: %v\n", elapsed)
	}
	if printAllocs {
		fmt.Fprintf(os.Stderr, "allocated memory: %s, allocations: %d\n",
			formatSize(stats.bytes), stats.mallocs)
	}
}
