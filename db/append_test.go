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
	"errors"
	"io"
	"math/rand"
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

	dfs := NewDirFS(tmpdir)
	defer dfs.Close()
	owner := newTenant(dfs)
	dfs.Log = t.Logf
	b := Builder{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf:         t.Logf,
		GCLikelihood: 1,
	}
	err := b.Append(owner, "default", "parking", nil)
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
	lst, err := blockfmt.CollectGlob(dfs, raw, "a-prefix/*.10n")
	if err != nil {
		t.Fatal(err)
	}

	// now we should ingest some data
	err = b.Append(owner, "default", "parking", lst)
	if err != nil {
		t.Fatal(err)
	}

	// confirm that it doesn't do anything
	// a second time around
	lst, err = blockfmt.CollectGlob(dfs, raw, "a-prefix/*.10n")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = true
	err = b.Append(owner, "default", "parking", lst)
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false

	lst, err = blockfmt.CollectGlob(dfs, raw, "b-prefix/*.block")
	if err != nil {
		t.Fatal(err)
	}
	err = b.Append(owner, "default", "taxi", lst)
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
	for i := range idx0.Inline {
		if idx0.Inline[i].Trailer == nil {
			t.Errorf("no trailer in contents[%d]", i)
		}
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

	lst, err = blockfmt.CollectGlob(dfs, nil, "a-prefix/*.json")
	if err != nil {
		t.Fatal(err)
	}
	err = b.Append(owner, "default", "parking", lst)
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
	if idx1.Inline[0].Trailer == nil {
		t.Errorf("no trailer in contents[%d]", 0)
	}
	idx1.Inputs.Backing = dfs
	if !contains(t, idx1, "file://a-prefix/parking.10n") {
		t.Error("missing a-prefix/parking.10n")
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)
	blobs, err := Blobs(dfs, idx1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs.Contents) != 1 {
		t.Errorf("got back %d blobs?", len(blobs.Contents))
	}
	tr := blobs.Contents[0].(*blob.Compressed).Trailer
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
	bad := []blockfmt.Input{{
		Path: "path/to/bad.json",
		ETag: "bad-ETag",
		Size: int64(len(badtext)),
		R:    io.NopCloser(strings.NewReader(badtext)),
		F:    blockfmt.SuffixToFormat[".json"](),
	}}

	err = b.Append(owner, "default", "parking", bad)
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
	if idx1.Inline[0].Trailer == nil {
		t.Errorf("no trailer in contents[%d]", 0)
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)

	// try again; this should be a no-op
	owner.ro = true
	err = b.Append(owner, "default", "parking", bad)
	if err != nil {
		t.Fatal("got an error re-inserting a bad item:", err)
	}
	owner.ro = false

	// try again with a new ETag;
	// this should succeed in inserting an item
	goodtext := `{"foo": "barbazquux"}`
	bad[0].ETag = "good-ETag"
	bad[0].Size = int64(len(goodtext))
	bad[0].R = io.NopCloser(strings.NewReader(goodtext))
	err = b.Append(owner, "default", "parking", bad)
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
		if name == bad[0].Path && etag == bad[0].ETag && id >= 0 {
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

	dfs := NewDirFS(tmpdir)
	defer dfs.Close()
	owner := newTenant(dfs)
	dfs.Log = t.Logf

	err := WriteDefinition(dfs, "default", &Definition{
		Name: "foo",
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
	b := Builder{
		Align:          2048,
		NewIndexScan:   true,
		MaxScanObjects: 1,
	}
	err = b.Append(owner, "default", "foo", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !blockfmt.IsFatal(err) {
		t.Fatalf("expected error satisfying blockfmt.IsFatal; got %T", err)
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
	err = b.Append(owner, "default", "foo", nil)
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
	checkContents(t, idx, dfs)
	checkNoGarbage(t, dfs, "db/default/foo", idx)
	// now get the last object:
	err = b.Append(owner, "default", "foo", nil)
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
	err = b.Append(owner, "default", "foo", nil)
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
	err = b.Append(owner, "default", "foo", nil)
	if err != nil {
		t.Fatal(err)
	}
}
