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

package blockfmt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"
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
	slices.SortFunc(descs, func(x, y Descriptor) int {
		return int(y.Trailer.Offset - x.Trailer.Offset)
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
