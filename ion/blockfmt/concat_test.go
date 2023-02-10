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

package blockfmt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"golang.org/x/exp/slices"
)

func TestConcat(t *testing.T) {
	dfs := NewDirFS(t.TempDir())
	align := 32 * 1024
	var descs []Descriptor
	for i := 0; i < 5; i++ {
		var inputs []Input
		f, err := os.Open("../../testdata/cloudtrail.json")
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, Input{
			R: f,
			F: MustSuffixToFormat(".json"),
		})
		path := fmt.Sprintf("part-%d", i)
		up, err := dfs.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		c := Converter{
			Output:    up,
			Comp:      "zion",
			Inputs:    inputs,
			Align:     align,
			FlushMeta: 2 * align,
		}
		err = c.Run()
		if err != nil {
			t.Fatal(err)
		}
		etag, err := ETag(dfs, c.Output, path)
		if err != nil {
			t.Fatal(err)
		}
		descs = append(descs, Descriptor{
			ObjectInfo: ObjectInfo{
				Path: path,
				ETag: etag,
				Size: c.Output.Size(),
			},
			Trailer: *c.Trailer(),
		})
	}
	// sort largest desc first
	slices.SortFunc(descs, func(x, y Descriptor) bool {
		return x.Trailer.Offset > y.Trailer.Offset
	})
	// force buffering of final desc
	dfs.MinPartSize = int(descs[len(descs)-1].Trailer.Offset) + 1

	var conc concat
	for i := range descs {
		if !conc.add(&descs[i]) {
			t.Fatalf("couldn't add descriptor %d?", i)
		}
	}
	err := conc.run(dfs, "all")
	if err != nil {
		t.Fatal(err)
	}

	// now validate the concatenated file
	f, err := dfs.Open("all")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	trailer, err := ReadTrailer(f.(io.ReaderAt), info.Size())
	if err != nil {
		t.Fatal(err)
	}
	var errlog bytes.Buffer
	n := Validate(f, trailer, &errlog)
	if errlog.Len() > 0 {
		t.Fatal(errlog.String())
	}
	if n != len(descs)*1000 {
		t.Errorf("found %d items?", n)
	}
}
