// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

type errorWriter struct {
	any bool
}

func (e *errorWriter) Write(p []byte) (int, error) {
	e.any = true
	return os.Stderr.Write(p)
}

func validateFiles(creds db.Tenant, files []string) {
	ofs := root(creds)
	e := errorWriter{}
	for i := range files {
		if dashv {
			fmt.Printf("checking %s\n", files[i])
		}
		f, err := ofs.Open(files[i])
		if err != nil {
			exitf("opening %s: %s", files[i], err)
		}
		info, err := f.Stat()
		if err != nil {
			exitf("stat %s: %s", files[i], err)
		}
		ra, ok := f.(io.ReaderAt)
		if !ok {
			exitf("%T doesn't implement io.ReaderAt", f)
		}
		t, err := blockfmt.ReadTrailer(ra, info.Size())
		if err != nil {
			exitf("reading trailer for %s: %s", files[i], err)
		}
		blockfmt.Validate(f, t, &e)
		f.Close()
	}
	if e.any {
		os.Exit(1)
	}
}

func validate(creds db.Tenant, dbname, table string) {
	ofs := root(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s", err)
	}
	e := errorWriter{}

	descs, err := idx.Indirect.Search(ofs, nil)
	if err != nil {
		exitf("populating indirect descriptors: %s", err)
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
		blockfmt.Validate(f, &descs[i].Trailer, &e)
		f.Close()
	}
	// TODO: validate idx.Indirect
	if e.any {
		os.Exit(1)
	}
}

func init() {
	addApplet(applet{
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
	})
	addApplet(applet{
		name: "validate-file",
		help: "path-to-files...",
		desc: `validate files within root
The command
  $ sdb validate-file /path/to/file.zion
interprets its arguments as a list of files to
be validated. (Paths are interpreted as being relative
to -root=...)

See also: validate
`,
		run: func(args []string) bool {
			validateFiles(creds(), args[1:])
			return true
		},
	})
}
