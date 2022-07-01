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
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var (
	dashv        bool
	dashh        bool
	dashf        bool
	dashm        int64
	dashk        string
	dasho        string
	token        string
	authEndPoint string
)

const (
	mega = 1024 * 1024
	giga = 1024 * mega
)

func init() {
	flag.BoolVar(&dashv, "v", false, "verbose")
	flag.BoolVar(&dashh, "h", false, "show usage help")
	flag.BoolVar(&dashf, "f", false, "force rebuild")
	flag.Int64Var(&dashm, "m", 100*giga, "maximum input bytes read per index update")
	flag.StringVar(&dashk, "k", "", "key file to use for signing+authenticating indexes")
	flag.StringVar(&dasho, "o", "-", "output file (or - for stdin) for unpack")
	flag.StringVar(&token, "token", "", "JWT token or custom bearer token (default: fetch from SNELLER_TOKEN environment variable)")
	flag.StringVar(&authEndPoint, "a", "", "authorization specification (file://, http://, https://, empty uses environment)")
}

func exitf(f string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func root(creds db.Tenant) db.InputFS {
	root, err := creds.Root()
	if err != nil {
		exitf("creds.Root: %s", err)
	}
	return root
}

func outfs(creds db.Tenant) db.OutputFS {
	r := root(creds)
	ofs, ok := r.(db.OutputFS)
	if !ok {
		exitf("root %T does not support writing", r)
	}
	return ofs
}

func load(defpath string) *db.Definition {
	f, err := os.Open(defpath)
	if err != nil {
		exitf("%s\n", err)
	}
	defer f.Close()
	s, err := db.DecodeDefinition(f)
	if err != nil {
		exitf("%s\n", err)
	}
	return s
}

// entry point for 'sdb create ...'
func create(creds db.Tenant, dbname, defpath string) {
	ofs := outfs(creds)
	s := load(defpath)
	if dashv {
		logf("creating table %q in db %q", s.Name, dbname)
	}
	err := db.WriteDefinition(ofs, dbname, s)
	if err != nil {
		exitf("writing new definition: %s\n", err)
	}
}

func gc(creds db.Tenant, dbname, tblpat string) {
	ofs := root(creds)
	rmfs, ok := ofs.(db.RemoveFS)
	if !ok {
		exitf("GC unimplemented\n")
	}
	tables, err := db.Tables(ofs, dbname)
	if err != nil {
		exitf("listing db %s: %s\n", dbname, err)
	}
	conf := db.GCConfig{
		MinimumAge: 15 * time.Minute,
	}
	if dashv {
		conf.Logf = logf
	}
	key := creds.Key()
	for _, tab := range tables {
		match, err := path.Match(tblpat, tab)
		if err != nil {
			exitf("bad pattern %q: %s\n", tblpat, err)
		}
		if !match {
			continue
		}
		idx, err := db.OpenIndex(ofs, dbname, tab, key)
		if err != nil {
			exitf("opening index for %s/%s: %s\n", dbname, tab, err)
		}
		err = conf.Run(rmfs, dbname, idx)
		if err != nil {
			exitf("running gc on %s/%s: %s\n", dbname, tab, err)
		}
	}
}

func logf(f string, args ...interface{}) {
	if f[len(f)-1] != '\n' {
		f += "\n"
	}
	fmt.Fprintf(os.Stderr, f, args...)
}

// entry point for 'sdb sync ...'
func sync(dbname, tblpat string) {
	var err error
	for {
		b := db.Builder{
			Align:         1024 * 1024, // maximum alignment with current span size
			RangeMultiple: 100,         // metadata once every 100MB
			Force:         dashf,
			MaxScanBytes:  dashm,
			GCMinimumAge:  5 * time.Minute,
		}
		if dashv {
			b.Logf = logf
		}
		err = b.Sync(creds(), dbname, tblpat)
		if !errors.Is(err, db.ErrBuildAgain) {
			break
		}
	}
	if err != nil {
		exitf("sync: %s", err)
	}
}

var hsizes = []byte{'K', 'M', 'G', 'T', 'P'}

func human(size int64) string {
	dec := int64(0)
	trail := -1
	for size > 1024 {
		trail++
		// decimal component needs to be
		// converted from parts-per-1024
		dec = ((size % 1023) * 1024) / 1000
		size /= 1024
	}
	if trail < 0 {
		return fmt.Sprintf("%d", size)
	}
	return fmt.Sprintf("%d.%d %ciB", size, dec, hsizes[trail])
}

func describeTrailer(t *blockfmt.Trailer, compsize int64) {
	size := t.Decompressed()
	fmt.Printf("\ttrailer: %d blocks, %d bytes decompressed (%.2fx compression)\n", len(t.Blocks), size, float64(size)/float64(compsize))
	fmt.Printf("\tfields: %v\n", t.Sparse.FieldNames())
}

func describe(creds db.Tenant, dbname, table string) {
	ofs := root(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s\n", err)
	}
	descs, err := idx.Indirect.Search(ofs, nil)
	if err != nil {
		exitf("getting indirect blobs: %s\n", err)
	}
	totalComp := int64(0)
	totalDecomp := int64(0)
	blocks := 0
	nindirect := len(descs)
	descs = append(descs, idx.Inline...)
	for i := range descs {
		totalComp += descs[i].Size
		totalDecomp += descs[i].Trailer.Decompressed()
		blocks += len(descs[i].Trailer.Blocks)
		fmt.Printf("%s %s %s\n", descs[i].Path, descs[i].ETag, human(descs[i].Size))
		if i < nindirect {
			fmt.Printf("\t (indirect)\n")
		}
		describeTrailer(descs[i].Trailer, descs[i].Size)
	}
	fmt.Printf("total blocks:       %d\n", blocks)
	fmt.Printf("total compressed:   %s\n", human(totalComp))
	fmt.Printf("total decompressed: %s (%.2fx)\n", human(totalDecomp), float64(totalDecomp)/float64(totalComp))
}

func fetch(creds db.Tenant, files ...string) {
	ofs := root(creds)
	for i := range files {
		if dashv {
			logf("fetching %s...\n", files[i])
		}
		f, err := ofs.Open(files[i])
		if err != nil {
			exitf("%s\n", err)
		}
		dstf, err := os.Create(path.Base(files[i]))
		if err != nil {
			exitf("creating %s: %s\n", path.Base(files[i]), err)
		}
		_, err = io.Copy(dstf, f)
		f.Close()
		dstf.Close()
		if err != nil {
			os.Remove(dstf.Name())
			exitf("copying bytes: %s\n", err)
		}
	}
}

func inputs(creds db.Tenant, dbname, table string) {
	ofs := outfs(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s\n", err)
	}
	idx.Inputs.Backing = ofs
	err = idx.Inputs.Walk("", func(name, etag string, id int) bool {
		fmt.Printf("%s %s %d\n", name, etag, id)
		return true
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func creds() db.Tenant {
	activeToken := token
	if activeToken == "" {
		activeToken = os.Getenv("SNELLER_TOKEN")
	}
	if activeToken == "" {
		exitf("no token provided via -token or $SNELLER_TOKEN")
	}

	provider, err := auth.Parse(authEndPoint)
	if err != nil {
		exitf("can't initialize authorization: %s", err)
	}

	awsCreds, err := provider.Authorize(context.Background(), activeToken)
	if err != nil {
		exitf("authorization: %s", err)
	}
	return awsCreds
}

type errorWriter struct {
	any bool
}

func (e *errorWriter) Write(p []byte) (int, error) {
	e.any = true
	return os.Stderr.Write(p)
}

func validate(creds db.Tenant, dbname, table string) {
	ofs := root(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s\n", err)
	}
	e := errorWriter{}

	descs, err := idx.Indirect.Search(ofs, nil)
	if err != nil {
		exitf("populating indirect descriptors: %s\n", err)
	}
	descs = append(descs, idx.Inline...)
	for i := range descs {
		if dashv {
			fmt.Printf("checking %s\n", descs[i].Path)
		}
		f, err := ofs.Open(descs[i].Path)
		if err != nil {
			exitf("opening %s: %s", descs[i].Path, err)
		}
		blockfmt.Validate(f, descs[i].Trailer, &e)
		f.Close()
	}
	// TODO: validate idx.Indirect
	if e.any {
		os.Exit(1)
	}
}

type packed interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

func s3split(name string) (string, string) {
	out := strings.TrimPrefix(name, "s3://")
	split := strings.IndexByte(out, '/')
	if split == -1 || split == len(out) {
		exitf("invalid s3 path spec %q", out)
	}
	bucket := out[:split]
	object := out[split+1:]
	return bucket, object
}

func openarg(name string) (packed, *blockfmt.Trailer) {
	if strings.HasPrefix(name, "s3://") {
		bucket, key := s3split(name)
		k, err := aws.AmbientKey("s3", s3.DeriveForBucket(bucket))
		if err != nil {
			exitf("deriving key for %q: %s", name, err)
		}
		f, err := s3.Open(k, bucket, key)
		if err != nil {
			exitf("opening arg: %s", err)
		}
		trailer, err := blockfmt.ReadTrailer(f, f.Size())
		if err != nil {
			f.Close()
			exitf("reading trailer from %s: %s", name, err)
		}
		return f, trailer
	}
	f, err := os.Open(name)
	if err != nil {
		exitf("opening arg: %s", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		exitf("stat arg: %s", err)
	}
	trailer, err := blockfmt.ReadTrailer(f, info.Size())
	if err != nil {
		f.Close()
		exitf("reading trailer from %s: %s", name, err)
	}
	return f, trailer
}

func unpack(args []string) {
	var out io.WriteCloser
	var err error
	if dasho == "-" {
		out = os.Stdout
	} else {
		out, err = os.Create(dasho)
		if err != nil {
			exitf("creating output: %s", err)
		}
	}
	defer out.Close()
	var d blockfmt.Decoder
	for i := range args {
		src, trailer := openarg(args[i])
		d.Set(trailer, len(trailer.Blocks))
		data := io.LimitReader(src, trailer.Offset)
		_, err := d.Copy(out, data)
		if err != nil {
			exitf("blockfmt.Decoder.Copy: %s", err)
		}
	}
}

type applet struct {
	name string // command name
	help string // list of options
	desc string // text description of what the command does

	run func(args []string) bool // execute command, returns false if args are invalid
}

type appletList []applet

var applets = appletList{
	{
		name: "create",
		help: "<db> <definition.json>",
		desc: `create a new table from a def
The command
  $ sdb create <db> definition.json
uploads a copy of definition.json to
the tenant root file system at
  /db/<db>/<name>/definition.json
using the table name given in the definition.json file

The definition.json is expected to be a JSON
document with the following structure:

  {
    "name": "<table-name>",
    "inputs": [
      {"pattern": "s3://bucket/path/to/*.json", "format": "json"},
      {"pattern": "s3://another/path/*.json.gz", "format": "json.gz"}
    ]
  }

`,
		run: func(args []string) bool {
			if len(args) != 3 {
				return false
			}
			create(creds(), args[1], args[2])
			return true
		},
	},
	{
		name: "sync",
		help: "<db> <table-pattern?>",
		desc: `sync a table index based on an existing def
the command
  $ sdb sync <db> <pattern>
synchronizes all the tables that match <pattern> within
the database <db> against the list of objects specified
in the associated definition.json files (see also "create")
`,
		run: func(args []string) bool {
			if len(args) < 2 || len(args) > 3 {
				return false
			}
			if len(args) == 2 {
				args = append(args, "*")
			}
			sync(args[1], args[2])
			return true
		},
	},
	{
		name: "gc",
		help: "<db> <table-pattern?>",
		desc: `gc old objects from a db (+ table-pattern)
The command
  $ sdb gc <db> <table-pattern>
will perform garbage collection of all the objects
in the set of tables that match the glob pattern <table-pattern>.

A file is a candidate for garbage collection if
it is not pointed to by the current index file
and it was created more than 15 minutes ago.
`,
		run: func(args []string) bool {
			if len(args) < 2 || len(args) > 3 {
				return false
			}
			if len(args) == 2 {
				args = append(args, "*")
			}
			gc(creds(), args[1], args[2])
			return true
		},
	},
	{
		name: "describe",
		help: "<db> <table>",
		desc: `describe a table index
The command
  $ sdb describe <db> <table>
will output a textual description
of the index file associated with
the given database+table.
`,
		run: func(args []string) bool {
			if len(args) != 3 {
				return false
			}
			describe(creds(), args[1], args[2])
			return true
		},
	},
	{
		name: "inputs",
		help: "<db> <table>",
		desc: `inputs <db> <table>
The command
  $ sdb inputs <db> <table>
lists all of the input files that have
been recorded by the index file belonging
to the specified database and table.

The output of this command is a series
of newline-delimited JSON records describing
the name of the file and whether or not it
was successfully read into the table.
`,
		run: func(args []string) bool {
			if len(args) != 3 {
				return false
			}
			inputs(creds(), args[1], args[2])
			return true
		},
	},
	{
		name: "validate",
		help: "<db> <table>",
		desc: `iteratively validate each packed-*.ion.zst file in an index
The command
  $ sdb validate <db> <table>
loads the index file associated with the
provided db and table and walks each of
the packed-*.ion.zst files comprising the index.
Any errors discovered will be reported on stderr.

NOTE: validation walks every byte of data in a table.
It may take a long time for this command to run on large tables.
`,
		run: func(args []string) bool {
			if len(args) != 3 {
				return false
			}
			validate(creds(), args[1], args[2])
			return true
		},
	},
	{
		name: "unpack",
		help: "<file> ...",
		desc: `unpack 1 or more *.ion.zst files into ion
The command
  $ sdb unpack <file> ...
unpacks each of the listed files (from the local filesystem)
and outputs the decompressed ion data from within the file.

If the -o <output> flag is set, then the output of this
command will be directed to that file.
Otherwise, the output is written to stdout.

See the "fetch" command for downloading files
from the tenant rootfs to the local filesystem.
`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			unpack(args[1:])
			return true
		},
	},
	{
		name: "fetch",
		help: "<file> ...",
		desc: `download 1 or more files from the tenant rootfs
The command
  $ sdb fetch <file> ...
fetches the associated file from the tenant rootfs
and stores it on the local filesystem in a file with
the same basename as the respective remote file.

For example,
  $ sdb fetch db/foo/bar/baz.ion.zst
would create a local file called baz.ion.zst.

For unpacking downloaded *.ion.zst files,
see the unpack command.
`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			fetch(creds(), args[1:]...)
			return true
		},
	},
}

func (a *appletList) find(cmd string) *applet {
	for i := range applets {
		if cmd == applets[i].name {
			return &applets[i]
		}
	}
	return nil
}

func main() {
	prog := os.Args[0]

	showAppletHelp := func(app *applet, indent string, short bool) {
		fmt.Fprintf(os.Stderr, "%s%s %s %s\n",
			indent, prog, app.name, app.help)
		desc := app.desc
		if short {
			desc = strings.Split(desc, "\n")[0]
		}
		fmt.Fprintf(os.Stderr, "%s    %s\n", indent, desc)
	}

	originalUsage := flag.Usage

	showHelp := func() {
		fmt.Fprintf(os.Stderr, "Usage:\n  sdb [-a auth-spec] [-token token] command args...\n")
		fmt.Fprintf(os.Stderr, "  -a     auth-spec: an http:// or file:// URI\n")
		fmt.Fprintf(os.Stderr, "         pointing to the token validation server or local credentials\n")
		fmt.Fprintf(os.Stderr, "  -token token: the token to pass to the auth server\n")
		fmt.Fprintf(os.Stderr, "Available commands:\n")
		for i := range applets {
			showAppletHelp(&applets[i], "  ", true)
		}

		fmt.Fprintf(os.Stderr, "\n")
		originalUsage()
	}

	flag.Usage = showHelp

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		showHelp()
		os.Exit(1)
	}

	for i := range args {
		if args[i] == "-h" {
			dashh = true
		}
	}

	cmd := args[0]
	applet := applets.find(cmd)
	if applet == nil {
		fmt.Fprintf(os.Stderr, "commands: ")
		for i := range applets {
			if i > 0 {
				fmt.Fprintf(os.Stderr, ", ")
			}
			fmt.Fprintf(os.Stderr, applets[i].name)
		}

		fmt.Fprintf(os.Stderr, "\n")
		os.Exit(1)
	}

	if dashh {
		showAppletHelp(applet, "", false)
		return
	}

	validArgs := applet.run(args)
	if !validArgs {
		exitf("usage: %s %s\n", cmd, applet.help)
	}
}
