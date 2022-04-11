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
	"path/filepath"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var (
	dashv        bool
	dashh        bool
	dashf        bool
	dashunsafe   bool
	dashm        int64
	dashk        string
	dasho        string
	token        string
	tenant       string
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
	flag.BoolVar(&dashunsafe, "unsafe", false, "use unsafe index signing key")
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
	s, err := db.DecodeDefinition(f, filepath.Ext(defpath))
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
	fallback := func(name string) blockfmt.RowFormat {
		if dashunsafe && (strings.HasSuffix(name, ".10n") || strings.HasSuffix(name, ".ion")) {
			if dashv {
				logf("using unsafe ion format for %s\n", name)
			}
			return blockfmt.UnsafeION()
		}
		return nil
	}
	var err error
	for {
		b := db.Builder{
			Align:         1024 * 1024, // maximum alignment with current span size
			RangeMultiple: 100,         // metadata once every 100MB
			Fallback:      fallback,
			Force:         dashf,
			MaxInputBytes: dashm,
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

func describeBlob(b blob.Interface, compsize int64) {
	if c, ok := b.(*blob.Compressed); ok {
		t := c.Trailer
		size := t.Decompressed()
		ranges := 0
		indices := make(map[string]struct{})
		for i := range t.Blocks {
			ranges += len(t.Blocks[i].Ranges)
			for j := range t.Blocks[i].Ranges {
				field := strings.Join(t.Blocks[i].Ranges[j].Path(), ".")
				indices[field] = struct{}{}
			}
		}
		fields := make([]string, 0, len(indices))
		for k := range indices {
			fields = append(fields, k)
		}
		fmt.Printf("\ttrailer: %d blocks, %d bytes decompressed (%.2fx compression)\n", len(t.Blocks), size, float64(size)/float64(compsize))
		fmt.Printf("\tranges: %d total, fields: %v\n", ranges, fields)
	}
}

func describe(creds db.Tenant, dbname, table string) {
	ofs := root(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s\n", err)
	}
	blobs, err := db.Blobs(ofs.(db.FS), idx)
	if err != nil {
		exitf("getting blobs: %s\n", err)
	}
	for i := range idx.Contents {
		fmt.Printf("%s %s %d %s\n", idx.Contents[i].Path, idx.Contents[i].ETag, idx.Contents[i].Size, idx.Contents[i].Format)
		describeBlob(blobs.Contents[i], idx.Contents[i].Size)
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
	for i := range idx.Contents {
		if dashv {
			fmt.Printf("checking %s\n", idx.Contents[i].Path)
		}
		f, err := ofs.Open(idx.Contents[i].Path)
		if err != nil {
			exitf("opening %s: %s", idx.Contents[i].Path, err)
		}
		blockfmt.Validate(f, idx.Contents[i].Trailer, &e)
		f.Close()
	}
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
		d.Trailer = trailer
		data := io.LimitReader(src, trailer.Offset)
		_, err := d.Copy(out, data)
		if err != nil {
			exitf("blockfmt.Decoder.Copy: %s", err)
		}
	}
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 || dashh {
		fmt.Fprintf(os.Stderr, "usage:\n")
		fmt.Fprintf(os.Stderr, "    %s [-token <token>] create <db> <def.json>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "        create a new table from a def\n")
		fmt.Fprintf(os.Stderr, "    %s [-token <token>] sync <db> <table>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "        sync a table index based on an existing def\n")
		fmt.Fprintf(os.Stderr, "	%s [-token <token>] describe <db> <table>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "		describe a table index\n")
		fmt.Fprintf(os.Stderr, "	%s [-token <token>] gc <db> <table-pattern?>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "		gc old objects from a db (+ table-pattern)\n")
		fmt.Fprintf(os.Stderr, "    %s [-token <token>] [-o <output>] unpack <file>...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "        unpack a packed .ion.zst file into ion")
		fmt.Fprintf(os.Stderr, "flag usage:\n")
		flag.Usage()
		os.Exit(1)
	}

	switch args[0] {
	case "create":
		if len(args) != 3 {
			exitf("usage: create <db> <definition.json|definition.yaml>")
		}
		create(creds(), args[1], args[2])
	case "sync":
		if len(args) < 2 || len(args) > 3 {
			exitf("usage: sync <db> <table-pattern?>")
		}
		if len(args) == 2 {
			args = append(args, "*")
		}
		sync(args[1], args[2])
	case "gc":
		if len(args) < 2 || len(args) > 3 {
			exitf("usage: gc <db> <table-pattern?>")
		}
		if len(args) == 2 {
			args = append(args, "*")
		}
		gc(creds(), args[1], args[2])
	case "describe":
		if len(args) != 3 {
			exitf("usage: describe <db> <table>")
		}
		describe(creds(), args[1], args[2])
	case "inputs":
		if len(args) != 3 {
			exitf("usage: inputs <db> <table>")
		}
		inputs(creds(), args[1], args[2])
	case "validate":
		if len(args) != 3 {
			exitf("usage: validate <db> <table>")
		}
		validate(creds(), args[1], args[2])
	case "unpack":
		if len(args) < 2 {
			exitf("usage: unpack <file> ...")
		}
		unpack(args[1:])
	default:
		exitf("commands: create, sync\n")
	}
}
