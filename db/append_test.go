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
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

type testTenant struct {
	root OutputFS
	key  *blockfmt.Key
	ro   bool
}

func (t *testTenant) ID() string { return "test-tenant" }
func (t *testTenant) Root() (InputFS, error) {
	if t.ro {
		return &noOutputFS{t.root}, nil
	}
	return t.root, nil
}
func (t *testTenant) Key() *blockfmt.Key { return t.key }

func (t *testTenant) Split(pat string) (InputFS, string, error) {
	dr := dirResolver{t.root}
	return dr.Split(pat)
}

func randomKey() *blockfmt.Key {
	ret := new(blockfmt.Key)
	rand.Read(ret[:])
	return ret
}

func newTenant(root OutputFS) *testTenant {
	return &testTenant{
		root: root,
		key:  randomKey(),
	}
}

func collectGlob(ifs blockfmt.InputFS, fn func(string) blockfmt.RowFormat, pat string) ([]partition, error) {
	lst, err := blockfmt.CollectGlob(ifs, fn, pat)
	if err != nil {
		return nil, err
	}
	return mkparts(lst), nil
}

func mkparts(lst []blockfmt.Input) []partition {
	return []partition{{
		prepend: -1,
		lst:     lst,
	}}
}

func info(c *Config, owner Tenant, db, table string) *tableInfo {
	infs, err := owner.Root()
	if err != nil {
		panic(err)
	}
	def, err := OpenDefinition(infs, db, table)
	if errors.Is(err, fs.ErrNotExist) {
		def = &Definition{}
	} else if err != nil {
		panic(err)
	}
	return &tableInfo{
		state: tableState{
			def:   def,
			owner: owner,
			db:    db,
			table: table,
			ofs:   infs.(OutputFS),
			conf:  *c,
		},
	}
}

func TestAppend(t *testing.T) {
	checkFiles(t)
	tmpdir := t.TempDir()
	for _, dir := range []string{
		filepath.Join(tmpdir, "a-prefix"),
		filepath.Join(tmpdir, "b-prefix"),
	} {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			t.Fatal(err)
		}
	}

	dfs := newDirFS(t, tmpdir)
	owner := newTenant(dfs)
	c := Config{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf: t.Logf,
	}
	ti := info(&c, owner, "default", "parking")
	err := ti.append(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if empty.Objects() != 0 {
		t.Errorf("expected len(Contents)==0; got %#v", empty.Objects())
	}
	if len(empty.ToDelete) != 0 {
		t.Errorf("expected len(ToDelete)==0; got %#v", empty.ToDelete)
	}

	newname := filepath.Join(tmpdir, "a-prefix/parking.10n")
	oldname, err := filepath.Abs("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}
	newname = filepath.Join(tmpdir, "b-prefix/nyc-taxi.block")
	oldname, err = filepath.Abs("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}

	raw := func(string) blockfmt.RowFormat { return blockfmt.UnsafeION() }
	lst, err := collectGlob(dfs, raw, "a-prefix/*.10n")
	if err != nil {
		t.Fatal(err)
	}

	// now we should ingest some data
	err = ti.append(context.Background(), lst)
	if err != nil {
		t.Fatal(err)
	}
	before := len(ti.state.cache.value.Inline)

	// confirm that it doesn't do anything
	// a second time around
	lst, err = collectGlob(dfs, raw, "a-prefix/*.10n")
	if err != nil {
		t.Fatal(err)
	}

	owner.ro = true
	err = ti.append(context.Background(), lst)
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
	after := len(ti.state.cache.value.Inline)
	if before != after {
		t.Fatal("dropped entries from Inline in no-op")
	}

	lst, err = collectGlob(dfs, raw, "b-prefix/*.block")
	if err != nil {
		t.Fatal(err)
	}
	err = ti.append(context.Background(), lst)
	if err != nil {
		t.Fatal(err)
	}
	idx0, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx0.Objects() != 1 {
		t.Errorf("expected idx0.Objects()==1; got %#v", idx0.Objects())
	}
	checkContents(t, idx0, dfs)

	// link a new file into the parking
	// table and see that we update the index:
	newname = filepath.Join(tmpdir, "a-prefix/parking2.json")
	oldname, err = filepath.Abs("../testdata/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}

	lst, err = collectGlob(dfs, nil, "a-prefix/*.json")
	if err != nil {
		t.Fatal(err)
	}
	err = ti.append(context.Background(), lst)
	if err != nil {
		t.Fatal(err)
	}
	// there should still be one output object,
	// but it should now have compacted the two
	// inputs together
	idx1, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx1.Objects() != 1 {
		t.Errorf("got idx1.Objects() = %d", idx1.Objects())
	}
	idx1.Inputs.Backing = dfs
	if !contains(t, idx1, "file://a-prefix/parking.10n") {
		t.Error("missing a-prefix/parking.10n")
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)
	blobs, _, err := Blobs(dfs, idx1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs.Contents) == 0 {
		t.Fatal("no blobs?")
	}
	tr := blobs.Contents[0].(*blob.CompressedPart).Parent.Trailer
	ranges := tr.Sparse.Fields()
	if ranges == 0 {
		// the parking2.json file
		// will have range data
		// that should be picked up
		// during JSON parsing
		t.Fatal("no ranges")
	}

	// add a bad file and confirm that
	// on its second appearance, we simply ignore it

	badtext := `{"foo": barbazquux }`
	bad := blockfmt.Input{
		Path: "path/to/bad.json",
		ETag: "bad-ETag",
		Size: int64(len(badtext)),
		R:    io.NopCloser(strings.NewReader(badtext)),
		F:    blockfmt.MustSuffixToFormat(".json"),
	}
	mk := func(v blockfmt.Input) []partition {
		return mkparts([]blockfmt.Input{v})
	}

	err = ti.append(context.Background(), mk(bad))
	if err == nil {
		t.Fatal("expected an error")
	}
	// there should still be one output object
	idx1, err = OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx1.Objects() != 1 {
		t.Errorf("got idx1.Objects() = %d", idx1.Objects())
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)

	// try again; this should be a no-op
	owner.ro = true
	err = ti.append(context.Background(), mk(bad))
	if err != nil {
		t.Fatal("got an error re-inserting a bad item:", err)
	}
	owner.ro = false

	// try again with a new ETag;
	// this should succeed in inserting an item
	goodtext := `{"foo": "barbazquux"}`
	bad.ETag = "good-ETag"
	bad.Size = int64(len(goodtext))
	bad.R = io.NopCloser(strings.NewReader(goodtext))
	err = ti.append(context.Background(), mk(bad))
	if err != nil {
		t.Fatal(err)
	}

	// check that the good version is now the copy in the filetree
	idx, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	idx.Inputs.Backing = dfs
	saw := false
	err = idx.Inputs.Walk("path/to/bad.json", func(name, etag string, id int) bool {
		if name == bad.Path && etag == bad.ETag && id >= 0 {
			saw = true
			return false
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Errorf("didn't find good copy in the tree?")
	}
}

// test that Append operations on a new index
// eventually overcome bad inputs
func TestAppendBadScan(t *testing.T) {
	checkFiles(t)
	tmpdir := t.TempDir()
	for _, dir := range []string{
		filepath.Join(tmpdir, "a-prefix"),
	} {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			t.Fatal(err)
		}
	}

	dfs := newDirFS(t, tmpdir)
	owner := newTenant(dfs)

	err := WriteDefinition(dfs, "default", "foo", &Definition{
		Inputs: []Input{{
			Pattern: "file://a-prefix/*.json",
			Format:  "json",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, x := range []struct {
		name, text string
	}{
		{"a-prefix/bad.json", `{"foo": barbazquux}`},
		{"a-prefix/good0.json", `{"foo": "bar"}`},
		{"a-prefix/good1.json", `{"bar": "baz"}`},
	} {
		_, err := dfs.WriteFile(x.name, []byte(x.text))
		if err != nil {
			t.Fatal(err)
		}
	}
	c := Config{
		Align:          2048,
		NewIndexScan:   true,
		MaxScanObjects: 1,
	}
	ti := info(&c, owner, "default", "foo")
	err = ti.append(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !blockfmt.IsFatal(err) {
		t.Fatalf("expected error satisfying blockfmt.IsFatal; got %T: %[1]v", err)
	}
	if ti.state.cache.value != nil {
		t.Error("cache is populated after an error")
	}
	// there should still be one output object
	idx, err := OpenIndex(dfs, "default", "foo", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx.Objects() != 0 {
		t.Errorf("got idx1.Objects() = %d", idx.Objects())
	}
	idx.Inputs.Backing = dfs
	ok := contains(t, idx, "file://a-prefix/bad.json")
	if !ok {
		t.Error("inputs doesn't contain bad.json?")
	}
	if !idx.Scanning {
		t.Error("not scanning?")
	}
	checkContents(t, idx, dfs)
	checkNoGarbage(t, dfs, "db/default/foo", idx)
	err = ti.append(context.Background(), nil)
	if !errors.Is(err, ErrBuildAgain) {
		if err == nil {
			t.Fatal("nil error?")
		}
		t.Fatal(err)
	}
	if ti.state.cache.value != nil {
		t.Error("cache value is populated after ErrBuildAgain")
	}

	idx, err = OpenIndex(dfs, "default", "foo", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx.Objects() != 1 {
		t.Errorf("got idx.Objects() = %d", idx.Objects())
	}
	checkContents(t, idx, dfs)
	checkNoGarbage(t, dfs, "db/default/foo", idx)
	// now get the last object:
	err = ti.append(context.Background(), nil)
	if !errors.Is(err, ErrBuildAgain) {
		if err == nil {
			t.Fatal("nil error?")
		}
		t.Fatal(err)
	}
	idx, err = OpenIndex(dfs, "default", "foo", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx.Objects() != 1 {
		t.Errorf("got idx.Objects() = %d", idx.Objects())
	}
	idx.Inputs.Backing = dfs
	ok = contains(t, idx, "file://a-prefix/good1.json")
	if !ok {
		t.Error("doesn't contain file://a-prefix/good1.json?")
	}
	if !idx.Scanning {
		t.Error("no longer scanning?")
	}

	// this one should turn off scanning:
	err = ti.append(context.Background(), nil)
	if !errors.Is(err, ErrBuildAgain) {
		if err == nil {
			t.Fatal("nil error?")
		}
		t.Fatal(err)
	}
	idx, err = OpenIndex(dfs, "default", "foo", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx.Scanning {
		t.Error("still scanning?")
	}
	err = ti.append(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
}
