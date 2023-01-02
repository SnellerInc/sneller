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
	"fmt"
	"strings"
	"time"

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
	totalComp := int64(0)
	totalDecomp := int64(0)
	blocks := 0
	nindirect := len(descs)
	descs = append(descs, idx.Inline...)
	for i := range descs {
		totalComp += descs[i].Size
		totalDecomp += descs[i].Trailer.Decompressed()
		blocks += len(descs[i].Trailer.Blocks)
		fmt.Printf("%s%s %s %s\n", ofs.Prefix(), descs[i].Path, descs[i].ETag, human(descs[i].Size))
		if i < nindirect {
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
}
