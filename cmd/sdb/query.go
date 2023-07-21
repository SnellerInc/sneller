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
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/sys/cpu"
)

// handle read_file('path/to/file')
func readFile(root fs.FS, args []expr.Node, hint *plan.Hints) (*plan.Input, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("read_file() should have 1 argument")
	}
	str, ok := args[0].(expr.String)
	if !ok {
		return nil, fmt.Errorf("read_file() should have a string argument")
	}
	f, err := root.Open(string(str))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ra, ok := f.(io.ReaderAt)
	if !ok {
		return nil, fmt.Errorf("%T doesn't implement io.ReaderAt", f)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	tr, err := blockfmt.ReadTrailer(ra, info.Size())
	if err != nil {
		return nil, err
	}
	blocks := ints.Intervals{{0, len(tr.Blocks)}}
	ret := &plan.Input{
		Descs: []plan.Descriptor{{
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{
					Path: string(str),
					Size: info.Size(),
				},
				Trailer: *tr,
			},
			Blocks: blocks,
		}},
		Fields: hint.Fields,
	}
	if hint.Filter != nil {
		ret = ret.Filter(hint.Filter)
	}
	return ret, nil
}

type cmdlineEnv struct {
	root fs.FS

	plan.Env // fallback environment
}

func (c *cmdlineEnv) Index(e expr.Node) (plan.Index, error) {
	if b, ok := e.(*expr.Builtin); ok && strings.EqualFold(b.Text, "read_file") {
		return nil, nil
	}
	if ie, ok := c.Env.(plan.Indexer); ok {
		return ie.Index(e)
	}
	return nil, nil
}

func (c *cmdlineEnv) Stat(tbl expr.Node, h *plan.Hints) (*plan.Input, error) {
	if b, ok := tbl.(*expr.Builtin); ok && strings.EqualFold(b.Text, "read_file") {
		return readFile(c.root, b.Args, h)
	}
	return c.Env.Stat(tbl, h)
}

func runner(cachedir string, root fs.FS) plan.Runner {
	switch root.(type) {
	case *db.DirFS:
		return &plan.FSRunner{root}
	case *db.S3FS:
		cachedir = filepath.Join(cachedir, "sneller-sdb")
		cache := dcache.New(cachedir, func() {})
		err := os.MkdirAll(cachedir, 0750)
		if err != nil {
			exitf("%s", err)
		}
		cache.Logger = log.New(os.Stderr, "", log.Lshortfile)
		return &sneller.TenantRunner{
			Cache: cache,
		}
	default:
		return nil
	}
}

func tenantEnv(root fs.FS) *sneller.TenantEnv {
	var tenant db.Tenant
	switch t := root.(type) {
	case *db.DirFS:
		tenant = db.NewLocalTenant(t)
	case *db.S3FS:
		tenant = auth.S3Tenant(context.Background(), "sneller", t, nil, nil)
	default:
		return nil
	}
	env, err := sneller.Environ(tenant, "")
	if err != nil {
		exitf("%s", err)
	}
	return &sneller.TenantEnv{
		FSEnv: env,
	}
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

func query(args []string) bool {
	var dashf string
	var dasho string
	var dashv bool
	var dashfmt string
	var dashtmp string
	var dashtrace string
	var dashtracefmt string

	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.StringVar(&dashf, "f", "", "sql input source (\"-\" implies stdin)")
	flags.StringVar(&dasho, "o", "-", "output (\"-\" implies stdout)")
	flags.BoolVar(&dashv, "v", false, "verbose diagnostics")
	flags.StringVar(&dashtrace, "trace", "", "trace output file (\"-\" implies stderr)")
	flags.StringVar(&dashtracefmt, "tracefmt", "text", "trace output (text, graphviz)")
	flags.StringVar(&dashfmt, "fmt", "ion", "output format (json, ion, ...)")
	flags.StringVar(&dashtmp, "tmp", os.TempDir(), "cache directory")
	flags.Parse(args[1:])
	args = flags.Args()

	var sql []byte
	var err error
	if len(args) == 0 && dashf != "" {
		if dashf == "-" {
			sql, err = io.ReadAll(os.Stdin)
		} else {
			sql, err = os.ReadFile(dashf)
		}
		if err != nil {
			exitf("%s", err)
		}
	} else if len(args) == 1 {
		sql = []byte(args[0])
	} else {
		return false
	}

	var stdout io.Writer
	if dasho == "-" {
		stdout = os.Stdout
	} else {
		f, err := os.Create(dasho)
		if err != nil {
			exitf("creating -o: %s", err)
		}
		stdout = f
		defer f.Close()
	}

	switch dashfmt {
	case "ion":
		// leave as-is
	case "json":
		stdout = ion.NewJSONWriter(stdout, '\n')
	default:
		exitf("unsupported output format %q", dashfmt)
	}

	sneller.CanVMOpen = true
	q, err := partiql.Parse(sql)
	if err != nil {
		var lexError *partiql.LexerError
		if errors.As(err, &lexError) {
			position := lexError.Position
			length := lexError.Length
			if length <= 0 {
				length = 2
			}
			underlineError(sql, position, length)
		}
		exitf("parsing query: %s", err)
	}
	err = q.Check()
	if err != nil {
		exitf("%s", err)
	}

	tenant := creds()
	rootfs := root(tenant)
	run := runner(dashtmp, rootfs)
	env := &cmdlineEnv{root: rootfs, Env: tenantEnv(rootfs)}
	tree, err := plan.New(q, env)
	if err != nil {
		exitf("planning query: %s", err)
	}
	if enc, ok := rootfs.(interface {
		Encode(*ion.Buffer, *ion.Symtab) error
	}); ok {
		var buf ion.Buffer
		var st ion.Symtab
		if err := enc.Encode(&buf, &st); err != nil {
			exitf("encoding file system: %s", err)
		}
		tree.Data, _, _ = ion.ReadDatum(&st, buf.Bytes())
	}

	if dashtrace != "" {
		w := os.Stderr
		if dashtrace != "-" {
			f, err := os.Create(dashtrace)
			if err != nil {
				exitf("creating %s: %s", dashtrace, err)
			}
			defer f.Close()
			w = f
		}
		var gv vm.TraceFlags
		switch dashtracefmt {
		case "text", "":
			gv = vm.TraceSSAText
		case "graphviz":
			gv = vm.TraceSSADot
		}
		vm.Trace(w, gv)
	}

	if !cpu.X86.HasAVX512 {
		exitf("cannot execute query without AVX512 support")
	}

	vm.Errorf = func(f string, args ...any) {
		fmt.Fprintf(os.Stderr, f, args...)
	}

	start := time.Now()
	ep := plan.ExecParams{
		FS:     rootfs,
		Plan:   tree,
		Output: stdout,
		Runner: run,
	}
	err = plan.Exec(&ep)
	if err != nil {
		exitf("%s", err)
	}
	if dashv {
		stats := ep.Stats
		elapsed := time.Since(start)
		rate := (float64(stats.BytesScanned) / float64(elapsed)) * 1000.0 / 1024.0 // bytes/ns ~= GB/s -> GiB/s*/
		fmt.Fprintf(os.Stderr, "%d bytes (%s) scanned in %s %.3gGiB/s\n",
			stats.BytesScanned, human(stats.BytesScanned), elapsed, rate)
	}
	return true
}

func init() {
	addApplet(applet{
		run:  query,
		name: "query",
		help: "[-v] [-o output] [-fmt json|ion] [-f query.sql]",
		desc: `run a query locally
The command
  $ sdb query <sql-text>
runs a sql query on the local machine.

The SQL query can read data either using a special read_file()
builtin function that can interpret zion packfiles, or it can
read tables directly from the hierarchy rooted at the path specified
by -root. (Note that read_file also reads files relative to -root.)

The -fmt flag can be used to change the output of the query engine.
The default behavior is to produce binary ion data, but -fmt=json can
be specified in order to produce JSON data.
`,
	})
}
