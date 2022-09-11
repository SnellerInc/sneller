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

package db

import (
	"io/fs"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func mktree(t *testing.T, files []string) string {
	base := t.TempDir()
	for _, c := range files {
		dir, _ := path.Split(c)
		err := os.MkdirAll(path.Join(base, dir), 0750)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(path.Join(base, c))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return base
}

func TestList(t *testing.T) {
	checkFiles(t)
	tcs := []struct {
		contents []string
		want     []string
	}{
		{
			contents: []string{
				"db/db0/table0/index",
				"db/db0/table1/index",
				"db/db1/table0/index",
				"db/db2/foo",
				"db/x/y/z",
				"db/x/z/index",
			},
			want: []string{
				"db0",
				"db1",
				"x",
			},
		},
	}
	for i := range tcs {
		dir := mktree(t, tcs[i].contents)
		dfs := os.DirFS(dir)
		dbs, err := List(dfs)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(dbs)
		if !reflect.DeepEqual(dbs, tcs[i].want) {
			t.Errorf("got db list %#v", dbs)
			t.Errorf("want db list %#v", tcs[i].want)
		}
	}
}

func TestTables(t *testing.T) {
	checkFiles(t)
	base := mktree(t, []string{
		"db/db0/foo/index",
		"db/db0/bar/index",
		"db/db1/x",
		"db/db2/quux/index",
	})
	want := []struct {
		db     string
		tables []string
	}{
		{"db0", []string{"bar", "foo"}},
		{"db2", []string{"quux"}},
	}
	d := os.DirFS(base)
	for i := range want {
		tables, err := Tables(d, want[i].db)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(tables)
		if !reflect.DeepEqual(tables, want[i].tables) {
			t.Errorf("db %q got tables %#v", want[i].db, tables)
			t.Errorf("want tables %#v", want[i].tables)
		}
	}
}

func TestOpenIndex(t *testing.T) {
	checkFiles(t)
	var k blockfmt.Key
	rand.Read(k[:])
	base := mktree(t, []string{
		"db/db0/table/index",
	})

	idx := blockfmt.Index{
		Name:    "test-index",
		Created: date.Now().Truncate(time.Microsecond),
		Algo:    "zstd",
		Inline: []blockfmt.Descriptor{{
			ObjectInfo: blockfmt.ObjectInfo{
				Path: "path/to/object",
				ETag: "object-etag-1",
			},
		}},
	}
	buf, err := blockfmt.Sign(&k, &idx)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(path.Join(base, "db/db0/table/index"), buf, 0644)
	if err != nil {
		t.Fatal(err)
	}

	idx2, err := OpenPartialIndex(os.DirFS(base), "db0", "table", &k)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&idx, idx2) {
		t.Errorf("first index : %#v", &idx)
		t.Errorf("partial index: %#v", idx2)
		t.Fatal("index object differs")
	}
}

// run a full end-to-end conversion and index building
// and confirm that the list of blobs we get out of
// the index is what we expect.
func TestBuildBlobs(t *testing.T) {
	checkFiles(t)
	dir := t.TempDir()
	err := os.MkdirAll(filepath.Join(dir, "db/db0/table0"), 0750)
	if err != nil {
		t.Fatal(err)
	}

	newname := filepath.Join(dir, "parking.10n")
	oldname, err := filepath.Abs("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}

	dfs := NewDirFS(dir)
	defer dfs.Close()

	def := `{"name": "table0", "input": [{"pattern":"file://*.10n"}]}`
	err = os.WriteFile(filepath.Join(dir, "db/db0/table0/definition.json"), []byte(def), 0640)
	if err != nil {
		t.Fatal(err)
	}

	owner := newTenant(dfs)
	dfs.Log = t.Logf
	b := Builder{
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Align: 1024,
		Logf:  t.Logf,
	}
	err = b.Sync(owner, "db0", "table*")
	if err != nil {
		t.Fatal(err)
	}
	// now we should be able to load blobs
	idx, err := OpenIndex(dfs, "db0", "table0", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx.Objects() != 1 {
		t.Fatalf("index contents: %d", idx.Objects())
	}
	match, err := path.Match("db/db0/table0/packed*.ion.zst", idx.Inline[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if !match {
		t.Fatalf("unexpected contents[0] path %s", idx.Inline[0].Path)
	}
	lst, err := Blobs(dfs, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(lst.Contents) != 1 {
		t.Fatalf("got %d blobs back?", len(lst.Contents))
	}
	bc, ok := lst.Contents[0].(*blob.Compressed)
	if !ok {
		t.Fatalf("expected blobs.Compressed; got %T", bc)
	}
	urlb, ok := bc.From.(*blob.URL)
	if !ok {
		t.Fatalf("expected blobs.URL; got %T", bc.From)
	}
	info, err := fs.Stat(dfs, idx.Inline[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after Stat: %d", openFiles(t))

	// etag should match object etag
	inputETag, err := dfs.ETag(idx.Inline[0].Path, info)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after ETag: %d", openFiles(t))
	if urlb.Info.ETag != inputETag {
		t.Errorf("got ETag %q but wanted ETag %q", urlb.Info.ETag, inputETag)
	}

	// uri should match object path
	uri, err := url.Parse(urlb.Value)
	if err != nil {
		t.Fatalf("invalid url %q: %s", urlb.Value, err)
	}
	match, err = path.Match("/db/db0/table0/packed*.ion.zst", uri.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !match {
		t.Errorf("unexpected path %s", uri.Path)
	}
}
