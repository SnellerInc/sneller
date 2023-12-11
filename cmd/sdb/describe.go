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
	"strings"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var hsizes = []byte{'K', 'M', 'G', 'T', 'P', 'E'}

func human(size int64) string {
	dec := int64(0)
	trail := -1
	for size >= 1024 {
		trail++
		// decimal component needs to be
		// converted from parts-per-1024
		dec = ((size%1024)*1000 + 512) / 1024
		size /= 1024
	}
	if trail < 0 {
		return fmt.Sprintf("%d", size)
	}
	return fmt.Sprintf("%d.%03d %ciB", size, dec, hsizes[trail])
}

func describeTrailer(t *blockfmt.Trailer, compsize int64) {
	size := t.Decompressed()
	fmt.Printf("\ttrailer: %d blocks, %d bytes decompressed (%.2fx compression, %s)\n", len(t.Blocks), size, float64(size)/float64(compsize), t.Algo)
	names := t.Sparse.FieldNames()
	for i := range names {
		ti := t.Sparse.Get(strings.Split(names[i], "."))
		if ti == nil {
			continue
		}
		min, ok := ti.Min()
		if !ok {
			continue
		}
		max, ok := ti.Max()
		if !ok {
			continue
		}
		left := ti.StartIntervals()
		right := ti.EndIntervals()
		fmt.Printf("\tindex %s %d left %d right [%s to %s]\n",
			names[i], left, right, min.Time().Format(time.RFC3339), max.Time().Format(time.RFC3339))
	}
}

func descriptors(ofs db.InputFS, files []string) []blockfmt.Descriptor {
	out := make([]blockfmt.Descriptor, 0, len(files))
	for i := range files {
		f, err := ofs.Open(files[i])
		if err != nil {
			exitf("opening %s: %s", files[i], err)
		}
		info, err := f.Stat()
		if err != nil {
			exitf("stat %s: %s", files[i], err)
		}
		var etag string
		// skip populating the ETag if this is a local file;
		// the DirFS ETags are computed via hashing
		if _, ok := ofs.(*db.DirFS); !ok {
			etag, _ = ofs.ETag(files[i], info)
		}
		ra, ok := f.(io.ReaderAt)
		if !ok {
			exitf("%T doesn't implement io.ReaderAt", f)
		}
		t, err := blockfmt.ReadTrailer(ra, info.Size())
		if err != nil {
			exitf("reading trailer for %s: %s", files[i], err)
		}
		f.Close()
		out = append(out, blockfmt.Descriptor{
			ObjectInfo: blockfmt.ObjectInfo{
				Path:         files[i],
				ETag:         etag,
				LastModified: date.FromTime(info.ModTime()),
				Size:         info.Size(),
			},
			Trailer: *t,
		})
	}
	return out
}

func describeFiles(creds db.Tenant, files []string) {
	ofs := root(creds)
	descs := descriptors(ofs, files)
	describeDescs(ofs, descs, 0)
}

func describe(creds db.Tenant, dbname, table string) {
	ofs := root(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s", err)
	}
	descs, err := idx.Indirect.Search(ofs, nil)
	if err != nil {
		exitf("getting indirect blobs: %s", err)
	}
	nindirect := len(descs)
	descs = append(descs, idx.Inline...)
	describeDescs(ofs, descs, nindirect)
}

func describeDescs(src blockfmt.InputFS, descs []blockfmt.Descriptor, indirect int) {
	totalComp := int64(0)
	totalDecomp := int64(0)
	blocks := 0
	for i := range descs {
		totalComp += descs[i].Size
		totalDecomp += descs[i].Trailer.Decompressed()
		blocks += len(descs[i].Trailer.Blocks)
		fmt.Printf("%s%s %s %s\n", src.Prefix(), descs[i].Path, descs[i].ETag, human(descs[i].Size))
		if i < indirect {
			fmt.Printf("\t (indirect)\n")
		}
		describeTrailer(&descs[i].Trailer, descs[i].Size)
	}
	fmt.Printf("total blocks:       %d\n", blocks)
	fmt.Printf("total compressed:   %s\n", human(totalComp))
	fmt.Printf("total decompressed: %s (%.2fx)\n", human(totalDecomp), float64(totalDecomp)/float64(totalComp))
}

func init() {
	addApplet(applet{
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
	})
	addApplet(applet{
		name: "describe-file",
		help: "files...",
		desc: `describe a file
The command
  $ sdb describe-file file...
will output a textual description of the packfile(s)
specified by the arguments. Arguments are interpreted as
paths relative to -root=...
`,
		run: func(args []string) bool {
			describeFiles(creds(), args[1:])
			return true
		},
	})
}
