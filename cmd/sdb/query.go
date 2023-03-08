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
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/sys/cpu"
)

// plan.TableHandle implementation for a local file
type fileHandle struct {
	trailer *blockfmt.Trailer
	ra      io.ReaderAt
	fields  []string
}

func (f *fileHandle) Open(ctx context.Context) (vm.Table, error) {
	return &readerTable{
		t:      f.trailer,
		src:    f.ra,
		fields: f.fields,
	}, nil
}

func (f *fileHandle) Size() int64 {
	return f.trailer.Decompressed()
}

func (f *fileHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return fmt.Errorf("fileHandle.Encode unimplemented")
}

// handle read_file('path/to/file')
func readFileHandle(root fs.FS, args []expr.Node, hint *plan.Hints) (plan.TableHandle, error) {
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
	ra, ok := f.(io.ReaderAt)
	if !ok {
		f.Close()
		return nil, fmt.Errorf("%T doesn't implement io.ReaderAt", f)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	trailer, err := blockfmt.ReadTrailer(ra, info.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	fh := &fileHandle{
		trailer: trailer,
		ra:      ra,
	}
	if !hint.AllFields {
		fh.fields = hint.Fields
	}
	return fh, nil
}

type cmdlineEnv struct {
	root fs.FS

	plan.Env // fallback environment
}

func (c *cmdlineEnv) Stat(tbl expr.Node, h *plan.Hints) (plan.TableHandle, error) {
	if b, ok := tbl.(*expr.Builtin); ok && strings.EqualFold(b.Text, "read_file") {
		return readFileHandle(c.root, b.Args, h)
	}
	return c.Env.Stat(tbl, h)
}

func tenantEnv(cachedir string, root fs.FS) *sneller.TenantEnv {
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
	cache := filepath.Join(cachedir, tenant.ID())
	err = os.MkdirAll(cache, 0750)
	if err != nil {
		exitf("%s", err)
	}
	ret := &sneller.TenantEnv{
		FSEnv: env,
		Cache: dcache.New(cache, func() {}),
	}
	ret.Cache.Logger = log.New(os.Stderr, "", log.Lshortfile)
	return ret
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
		sql, err = os.ReadFile(dashf)
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
	rootfs := root(creds())
	env := &cmdlineEnv{root: rootfs, Env: tenantEnv(dashtmp, rootfs)}
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

	tree, err := plan.New(q, env)
	if err != nil {
		exitf("planning query: %s", err)
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
		var gv vm.TraceType
		switch dashtracefmt {
		case "text", "":
			gv = vm.TraceText
		case "graphviz":
			gv = vm.TraceDot
		}
		vm.Trace(w, gv)
	}

	if !cpu.X86.HasAVX512 {
		exitf("cannot execute query without AVX512 support")
	}

	var stats plan.ExecStats
	start := time.Now()
	err = plan.Exec(tree, stdout, &stats)
	if err != nil {
		exitf("%s", err)
	}
	if dashv {
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
		help: "[-f query.sql]",
		desc: `run a query locally
The command
  $ sdb -root=... query <sql-text>
runs a sql query on the local machine.
`,
	})
}
