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
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/vm"
	"golang.org/x/sys/cpu"
)

var (
	dashauth     string
	dashd        string
	dashf        bool
	dashj        bool
	dashN        bool
	dashg        bool
	dashg2       bool
	dashg3       bool
	dasho        string
	dashr        string
	dashtoken    string
	cachedir     string
	dashnommap   bool
	printStats   bool
	printBuild   bool
	printVersion bool
	localTenant  bool

	dst io.WriteCloser
)

var (
	tmpdir string
)

func init() {
	sneller.CanVMOpen = true
	flagDefaultUsage = flag.CommandLine.Usage
	flag.CommandLine.Usage = printHelp
	tmpdir = os.TempDir()

	flag.StringVar(&dashauth, "auth", "", "authorization provider for database object storage")
	flag.StringVar(&dashd, "d", "", "default database name (requires -auth, -r or -local)")
	flag.BoolVar(&dashf, "f", false, "read arguments as files containing queries")
	flag.BoolVar(&dashg, "g", false, "just dump the query plan graphviz; do not execute")
	flag.BoolVar(&dashg2, "g2", false, "just dump DFA of first regex graphviz; do not execute")
	flag.BoolVar(&dashg3, "g3", false, "just dump data-structure of first regex; do not execute")
	flag.BoolVar(&dashj, "j", false, "write output as JSON instead of ion")
	flag.BoolVar(&dashN, "N", false, "interpret input as NDJSON")
	flag.StringVar(&dasho, "o", "", "file for output (default is stdout)")
	flag.StringVar(&dashr, "r", "", "root of database object storage (S3 only)")
	flag.BoolVar(&printStats, "S", false, "print execution statistics on stderr")
	flag.StringVar(&dashtoken, "token", "", "token for auth provider (default SNELLER_TOKEN from env)")
	flag.BoolVar(&dashnommap, "no-mmap", false, "do not mmap files (Linux only)")
	flag.StringVar(&cachedir, "cachedir", "/tmp", "cache directory")
	flag.BoolVar(&printBuild, "build", false, "print the build info of executable")
	flag.BoolVar(&printVersion, "version", false, "print the version of executable")
	flag.BoolVar(&localTenant, "local", false,
		fmt.Sprintf("read data from local storage (%s)\ndata has to be prepared by sdb invoked with -local option", tmpdir))
}

type execStatistics struct {
	mallocs   uint64 // runtime.MemStats.Mallocs
	bytes     int64  // runtime.MemStats.TotalAlloc
	startTime time.Time
	elapsed   time.Duration
}

func (e *execStatistics) Start() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	e.mallocs = stats.Mallocs
	e.bytes = int64(stats.TotalAlloc)
	e.startTime = time.Now()
}

func (e *execStatistics) Stop() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	e.mallocs = stats.Mallocs - e.mallocs
	e.bytes = int64(stats.TotalAlloc - uint64(e.bytes))
	e.elapsed = time.Since(e.startTime)
}

func (e *execStatistics) Print() {
	rate := (float64(allBytes) / float64(e.elapsed)) * 1000.0 / 1024.0 // bytes/ns ~= GB/s -> GiB/s*/

	fmt.Fprintf(os.Stderr, "%.3gGiB/s (scanned %s in %v), allocated memory: %s, allocations: %d\n",
		rate, formatSize(allBytes), e.elapsed, formatSize(e.bytes), e.mallocs)
}

func formatSize(size int64) string {
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

type eenv func(expr.Node, *plan.Hints) (vm.Table, error)

type handle func() (vm.Table, error)

func (h handle) Open(_ context.Context) (vm.Table, error) {
	return h()
}

func (h handle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("unexpected call to handle.Encode")
}

// FIXME: use hints!
func (e eenv) Stat(tbl expr.Node, h *plan.Hints) (plan.TableHandle, error) {
	return handle(func() (vm.Table, error) {
		return e(tbl, h)
	}), nil
}

func parse(arg string) *expr.Query {
	var buf []byte
	var err error
	if dashf {
		// arg is a file
		buf, err = os.ReadFile(expandUser(arg))
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
			position := lexError.Position
			length := lexError.Length
			if length == 0 {
				length = 2
			}

			underlineError(buf, position, length)
		}
		exit(err)
	}

	err = q.Check()
	if err != nil {
		exit(err)
	}

	return q
}

var newline = []byte{'\n'}

func underlineError(query []byte, position, length int) {
	lines := bytes.Split(query, newline)

	for i := range lines {
		line := lines[i]
		fmt.Printf("%s\n", line)
		if length > 0 && position <= len(line) {
			fmt.Printf("%s%s\n", strings.Repeat(" ", position), strings.Repeat("^", length))
			length = 0
		}

		position -= len(line) + 1
	}
}

func tenantEnv(tenant db.Tenant, db string) *sneller.TenantEnv {
	env, err := sneller.Environ(tenant, db)
	if err != nil {
		exit(err)
	}
	cache := filepath.Join(cachedir, tenant.ID())
	err = os.MkdirAll(cache, 0750)
	if err != nil {
		exit(err)
	}
	ret := &sneller.TenantEnv{
		FSEnv: env,
		Cache: dcache.New(cache, func() {}),
	}
	ret.Cache.Logger = log.New(os.Stderr, "", log.Lshortfile)
	return ret
}

func mkenv() plan.Env {
	token := dashtoken
	if token == "" {
		token = os.Getenv("SNELLER_TOKEN")
	}

	if localTenant {
		if dashr != "" {
			exitf("-r cannot be used with -local")
		}
		if dashauth != "" {
			exitf("-auth cannot be used with -local")
		}
		t := db.NewLocalTenantFromPath(tmpdir)
		return tenantEnv(t, dashd)
	}

	// database object storage via auth provider
	if dashauth != "" {
		if dashr != "" {
			exitf("-r cannot be used with -auth")
		}
		if token == "" {
			exitf("no token provided via -token or SNELLER_TOKEN")
		}

		prov, err := auth.Parse(dashauth)
		if err != nil {
			exit(err)
		}
		t, err := prov.Authorize(context.Background(), token)
		if err != nil {
			exit(err)
		}
		return tenantEnv(t, dashd)
	}
	if dashtoken != "" {
		exitf("-token can only be used with -auth")
	}
	// direct object storage access
	if dashr != "" {
		if strings.HasPrefix(dashr, "s3://") {
			root := s3fs(dashr)
			t := auth.S3Tenant(context.Background(), "localtenant", root, nil, nil)
			return tenantEnv(t, dashd)
		}
		exitf("-r can only be used with S3")
	}
	if dashd != "" {
		exitf("-d can only be used with -auth or -r")
	}
	// direct table access
	return eenv(func(e expr.Node, h *plan.Hints) (vm.Table, error) {
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
		fname = expandUser(fname)

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
		fields := h.Fields
		if h.AllFields {
			fields = nil
		} else if fields == nil {
			// len(fields)==0 but non-nil really means zero fields
			fields = []string{}
		}
		return srcTable(f, i.Size(), fields)
	})
}

func expandUser(path string) string {
	p := strings.TrimPrefix(path, "~/")
	if len(p) == len(path) {
		return path
	}

	dir, err := os.UserHomeDir()
	if err != nil {
		return path
	}

	return filepath.Join(dir, p)
}

func do(arg string) {
	query := parse(arg)
	tree, err := plan.New(query, mkenv())
	if err != nil {
		exit(err)
	}

	if dashg {
		// -g -> just Graphviz
		if err = plan.Graphviz(tree, dst); err != nil {
			exit(err)
		}
		return
	}
	if dashg2 || dashg3 {
		// -g2 -> dump DFA of first regex
		// -g3 -> dump data-structure for DFA of first regex
		if err = GraphvizDFA(query, dst, dashg2, dashg3); err != nil {
			exit(err)
		}
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

func exitf(f string, args ...any) {
	exit(fmt.Errorf(f, args...))
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printHelp() {
	helporder := []string{
		usagePlaceholder,
		"Query options",
		"N",
		"f",
		"Output target",
		"o",
		"S",
		"Output format",
		"j",
		"g",
		"g2",
		"g3",
		"Reading external database",
		"d",
		"auth",
		"token",
		"r",
		"local",
		"Other",
		"cachedir",
		"no-mmap",
		"version",
		"build",
	}

	PrintOrderedHelp(helporder)
}

func main() {
	flag.Parse()

	if printVersion {
		v, ok := sneller.Version()
		if ok {
			fmt.Println(v)
		} else {
			fmt.Println("version not available, please check -build")
		}
		return
	} else if printBuild {
		bi, ok := sneller.BuildInfo()
		if ok {
			fmt.Print(bi)
		} else {
			fmt.Println("build info not available")
		}
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		flag.CommandLine.Usage()
		os.Exit(1)
	}

	if (dashg || dashg2 || dashg3) && dashj {
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
