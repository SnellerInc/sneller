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
	"context"
	"errors"
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
	"golang.org/x/sys/cpu"
)

var (
	dashf      bool
	dashj      bool
	dashN      bool
	dashg      bool
	dasho      string
	dashnommap bool
	printStats bool

	dst io.WriteCloser
)

func init() {
	flag.BoolVar(&dashf, "f", false, "read arguments as files containing queries")
	flag.BoolVar(&dashg, "g", false, "just dump the query plan graphviz; do not execute")
	flag.BoolVar(&dashj, "j", false, "write output as JSON instead of ion")
	flag.BoolVar(&dashN, "N", false, "interpret input as NDJSON")
	flag.StringVar(&dasho, "o", "", "file for output (default is stdout)")
	flag.BoolVar(&printStats, "S", false, "print exection statistics on stderr")
	flag.BoolVar(&dashnommap, "no-mmap", false, "do not mmap files (Linux only)")
}

type execStatistics struct {
	mallocs   uint64 // runtime.MemStats.Mallocs
	bytes     uint64 // runtime.MemStats.TotalAlloc
	startTime time.Time
	elapsed   time.Duration
}

func (e *execStatistics) Start() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	e.mallocs = stats.Mallocs
	e.bytes = stats.TotalAlloc
	e.startTime = time.Now()
}

func (e *execStatistics) Stop() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	e.mallocs = stats.Mallocs - e.mallocs
	e.bytes = stats.TotalAlloc - e.bytes
	e.elapsed = time.Since(e.startTime)
}

func (e *execStatistics) Print() {
	rate := (float64(allBytes) / float64(e.elapsed)) * 1000.0 / 1024.0 // bytes/ns ~= GB/s -> GiB/s*/

	fmt.Fprintf(os.Stderr, "%.3gGiB/s (scanned %s in %v), allocated memory: %s, allocations: %d\n",
		rate, formatSize(allBytes), e.elapsed, formatSize(e.bytes), e.mallocs)
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

type eenv func(expr.Node) (vm.Table, error)

type handle func() (vm.Table, error)

func (h handle) Open(_ context.Context) (vm.Table, error) {
	return h()
}

func (h handle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected call to handle.Encode")
}

// FIXME: use filter when we are reading ion data!
func (e eenv) Stat(tbl, filter expr.Node) (plan.TableHandle, error) {
	return handle(func() (vm.Table, error) {
		return e(tbl)
	}), nil
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
		var lexError *partiql.LexerError
		if errors.As(err, &lexError) {
			fmt.Printf("%s\n", buf)
			fmt.Printf("%s", strings.Repeat(" ", lexError.Position-1))
			n := lexError.Length
			if n == 0 {
				n = 2
			}

			fmt.Printf("%s\n", strings.Repeat("^", n))
		}
		exit(err)
	}
	return q
}

func do(arg string) {
	tree, err := plan.New(parse(arg), eenv(func(e expr.Node) (vm.Table, error) {
		str, ok := e.(expr.String)
		if !ok {
			return nil, fmt.Errorf("unexpected table expression %s", expr.ToString(e))
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

	if !cpu.X86.HasAVX512 {
		fmt.Fprintln(os.Stderr, "CPU doesn't support AVX-512")
		os.Exit(1)
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
	var stats execStatistics
	stats.Start()

	for i := range args {
		do(args[i])
	}
	dst.Close()
	// wait for background threads
	bg.Wait()

	stats.Stop()
	if printStats {
		stats.Print()
	}
}
