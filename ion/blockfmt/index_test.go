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
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func init() {
	debugFree = true
}

// drop all cached data
func (f *FileTree) dropAll() {
	for i := range f.toplevel {
		if !f.dirty[i] {
			f.toplevel[i].contents = nil
		}
	}
}

// find index files in testdata/ and
// test that they decode correctly; some of these
// were encoded with older versions of the software
func TestIndexCompat(t *testing.T) {
	run := func(p string) {
		t.Run(p, func(t *testing.T) {
			buf, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			idx, err := DecodeIndex(nil, buf, 0)
			if err != nil {
				t.Fatal(err)
			}
			var key Key
			rand.Read(key[:])
			res, err := Sign(&key, idx)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d -> %d bytes", len(buf), len(res))
			t.Logf("%d indirect fields", idx.Indirect.Sparse.Fields())
			idx2, err := DecodeIndex(&key, res, 0)
			if err != nil {
				t.Fatal(err)
			}
			// second encode/decode operation
			// should yield an identical index
			if !reflect.DeepEqual(idx, idx2) {
				t.Fatal("not reproducible after second encode")
			}
		})
	}
	fn := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == "index" {
			run(p)
		}
		return nil
	}
	filepath.WalkDir("./testdata", fn)
}

func TestLargeIndexEncoding(t *testing.T) {
	time0 := date.Now().Truncate(time.Microsecond)
	idx := Index{
		Name:     "the-index",
		Created:  time0,
		Algo:     "zstd",
		Scanning: true,
		Cursors:  []string{"a/b/c", "x/y/z"},
		LastScan: time0,
		Inline: []Descriptor{
			{
				ObjectInfo: ObjectInfo{
					Path:         "foo/bar/baz.10n.c",
					ETag:         "baz-etag",
					LastModified: time0.Add(-time.Second),
					Format:       Version,
					Size:         12000,
				},
				Trailer: &Trailer{
					Version:    1,
					Offset:     100,
					Algo:       "lz4",
					BlockShift: 20,
					Blocks: []Blockdesc{{
						Offset: 0,
						ranges: []Range{&TimeRange{
							[]string{"a", "b"},
							time0,
							time0,
						}},
					}, {
						Offset: 1 << 20,
						ranges: nil,
					}},
				},
			},
		},
	}

	dfs := NewDirFS(t.TempDir())
	// generate enough input data that the list
	// of subtrees will be compressed
	for i := 0; i < 50000; i++ {
		name := fmt.Sprintf("file-%d", i)
		_, err := idx.Inputs.Append(name, name, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx.Inputs.Backing = dfs
	idx.SyncInputs(path.Join("db", "foo", "bar"), 0)

	var key Key
	rand.Read(key[:])

	// reset input state to appear decoded
	idx.Inputs.Backing = nil
	idx.Inputs.dropAll()

	buf, err := Sign(&key, &idx)
	if err != nil {
		t.Fatal(err)
	}
	// drop all ranges, since they
	// are stripped on decode:
	for i := range idx.Inline {
		for j := range idx.Inline[i].Trailer.Blocks {
			idx.Inline[i].Trailer.Blocks[j].ranges = nil
		}
	}

	ret, err := DecodeIndex(&key, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&idx, ret) {
		t.Errorf("input:  %#v", idx)
		t.Errorf("output: %#v", ret)
		t.Fatal("input and output not equal")
	}
}

func TestIndexEncoding(t *testing.T) {
	time0 := date.Now().Truncate(time.Duration(1000))

	timen := func(n int) date.Time {
		return time0.Add(-time.Duration(n) * 20 * time.Minute)
	}

	dfs := NewDirFS(t.TempDir())

	idxs := []Index{
		{
			Name:    "my-view",
			Created: date.Now().Truncate(time.Duration(1000)),
			Algo:    "zstd",
			ToDelete: []Quarantined{
				{Path: "/foo/bar/deleteme.ion.zst", Expiry: date.Now().Truncate(time.Microsecond).Add(time.Minute)},
				{Path: "/foo/bar/deleteme2.ion.zst", Expiry: date.Now().Truncate(time.Microsecond).Add(2 * time.Minute)},
			},
			Inline: []Descriptor{
				{
					ObjectInfo: ObjectInfo{
						Path:         "foo/bar/baz.10n.c",
						ETag:         "baz-etag",
						LastModified: timen(1),
						Format:       Version,
						Size:         12000,
					},
					Trailer: &Trailer{
						Version:    1,
						Offset:     100,
						Algo:       "lz4",
						BlockShift: 20,
						Blocks: []Blockdesc{{
							Offset: 0,
							ranges: []Range{&TimeRange{
								[]string{"a", "b"},
								time0,
								time0,
							}},
						}, {
							Offset: 1 << 20,
							ranges: nil,
						}},
					},
				},
				{
					ObjectInfo: ObjectInfo{
						Path:         "foo/bar/quux.10n.c",
						ETag:         "quux-etag",
						Format:       Version,
						LastModified: timen(4),
						Size:         100000,
					},
				},
			},
		},
	}
	for i := range idxs {
		idx := &idxs[i]
		var key Key
		rand.Read(key[:])
		for i, oi := range []ObjectInfo{
			{
				Path:         "bucket/02/quux.json.zst",
				Format:       "json.zst",
				ETag:         "quux-etag-02",
				LastModified: timen(5),
			},
			{
				Path:         "bucket/03/quux.json.lz4",
				Format:       "json.lz4",
				ETag:         "quux-etag-03",
				LastModified: timen(6),
			},
			{
				Path:         "bucket/00/baz.json",
				Format:       "json",
				ETag:         "baz.json-etag-0",
				LastModified: timen(2),
				Size:         1234,
			},
			{
				Path:         "bucket/01/baz/json.gz",
				Format:       "json.gz",
				ETag:         "baz.json-etag-1",
				LastModified: timen(3),
				Size:         5678,
			},
		} {
			_, err := idx.Inputs.Append(oi.Path, oi.ETag, i)
			if err != nil {
				t.Fatal(err)
			}
		}
		idx.Inputs.Backing = dfs
		idx.SyncInputs(path.Join("db", "foo", idx.Name), 0)

		for i := range idx.Inline {
			if idx.Inline[i].Trailer != nil {
				idx.Inline[i].Trailer.syncRanges()
			}
		}
		// reset input state to appear decoded
		idx.Inputs.Backing = nil
		idx.Inputs.dropAll()

		buf, err := Sign(&key, idx)
		if err != nil {
			t.Fatal(err)
		}
		ret, err := DecodeIndex(&key, buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(idx, ret) {
			t.Errorf("input : %#v", idx)
			t.Errorf("output: %#v", ret)
			t.Fatal("input and output not equal")
		}
		idx.Inputs.Reset() // not decoded with FlagSkipInputs
		idx.ToDelete = nil // not decoded with FlagSkipInputs
		ret, err = DecodeIndex(&key, buf, FlagSkipInputs)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(idx, ret) {
			t.Errorf("input : %#v", idx)
			t.Errorf("output: %#v", ret)
			t.Fatal("input and output not equal")
		}
	}
}

// Benchmark for the allocation overhead of decoding an
// index with a lot of ranges.
func BenchmarkIndexDecodingAllocs(b *testing.B) {
	debugFree = false
	defer func() { debugFree = true }()
	now := date.Now().Truncate(time.Duration(1000))
	timen := func(n int) date.Time {
		return now.Add(time.Duration(n) * time.Second)
	}
	ranges := make([]Range, 10)
	for i := range ranges {
		ranges[i] = &TimeRange{
			path: []string{"foo", fmt.Sprintf("bar%d", i)},
			min:  timen(i),
			max:  timen(i + 1),
		}
	}
	blocks := make([]Blockdesc, 1000)
	for i := range blocks {
		blocks[i] = Blockdesc{
			Offset: int64(i) << 20,
			ranges: ranges,
		}
	}
	contents := make([]Descriptor, 100)
	for i := range contents {
		contents[i] = Descriptor{
			ObjectInfo: ObjectInfo{
				Path:         "foo/bar",
				ETag:         "f00b412",
				LastModified: now,
				Format:       Version,
			},
			Trailer: &Trailer{
				Version:    1,
				Offset:     100,
				Algo:       "lz4",
				BlockShift: 20,
				Blocks:     blocks,
			},
		}
	}
	idx := &Index{
		Name:    "index",
		Created: now,
		Algo:    "zstd",
		Inline:  contents,
	}

	// get ranges set early; we can't use syncRanges()
	// because it will clear the (shared) list of blocks
	for i := range contents {
		contents[i].Trailer.Sparse.setRanges(contents[i].Trailer.Blocks)
	}
	var key Key
	rand.Read(key[:])
	buf, err := Sign(&key, idx)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(len(buf)), "bytes")

	for i := 0; i < b.N; i++ {
		_, err := DecodeIndex(&key, buf, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
