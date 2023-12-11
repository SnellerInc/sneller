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

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// simple db.Resolver wrapping a DirFS
type dirResolver struct {
	OutputFS
}

func checkFiles(t *testing.T) {
	count := openFiles(t)
	if count > 0 {
		t.Cleanup(func() {
			http.DefaultClient.CloseIdleConnections()
			ret := openFiles(t)
			if ret > count {
				// there is a brief race window in which
				// os.File.Close has been called but the
				// runtime poller has not closed the file descriptor,
				// hence this can occasionally return a false positive leak
				t.Logf("started with %d open files; now have %d", count, ret)
			}
		})
	}
}

type noReadCloser struct {
	t *testing.T
}

func (n noReadCloser) Read(p []byte) (int, error) {
	n.t.Fatal("didn't expect a Read call")
	return 0, io.EOF
}

func (n noReadCloser) Close() error {
	return nil
}

func openFiles(t *testing.T) int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	for i := range entries {
		full := filepath.Join("/proc/self/fd", entries[i].Name())
		name, err := os.Readlink(full)
		if err != nil {
			t.Logf("readlink: %s", err)
		} else {
			t.Logf("file: %s -> %s", full, name)
		}
	}
	// there is always 1 extra file
	// open for the readdir operation itself
	return len(entries) - 1
}

func (d *dirResolver) Split(pattern string) (InputFS, string, error) {
	stripped, ok := strings.CutPrefix(pattern, "file://")
	if !ok {
		return nil, "", fmt.Errorf("bad pattern %q", pattern)
	}
	return d.OutputFS, stripped, nil
}

// noOutputFS is an OutputFS that
// errors on write operations
type noOutputFS struct {
	OutputFS
}

func (n *noOutputFS) WriteFile(path string, buf []byte) (string, error) {
	return "", fmt.Errorf("refusing write to %q", path)
}

func (n *noOutputFS) Create(path string) (blockfmt.Uploader, error) {
	return nil, fmt.Errorf("refusing to create %q", path)
}

func contains(t *testing.T, idx *blockfmt.Index, path string) bool {
	ret := false
	err := idx.Inputs.Walk(path, func(p, etag string, id int) bool {
		ret = p == path
		return false
	})
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return ret
}

func checkContents(t *testing.T, idx *blockfmt.Index, dir OutputFS) {
	// it should be safe to delete anything
	// in ToDelete as long as we aren't running
	// concurrent queries (which we aren't);
	// confirm that we really don't need these
	// objects around any more:
	rmfs := dir.(RemoveFS)
	for i := range idx.ToDelete {
		rmfs.Remove(idx.ToDelete[i].Path)
	}
	idx.ToDelete = nil

	for i := range idx.Inline {
		b := &idx.Inline[i]
		info, err := fs.Stat(dir, b.Path)
		if err != nil {
			t.Fatal(err)
		}
		etag, err := dir.ETag(b.Path, info)
		if err != nil {
			t.Fatal(err)
		}
		if etag != b.ETag {
			t.Errorf("%s: etag %s != %s", b.Path, etag, b.ETag)
		}
		if b.Size != info.Size() {
			t.Errorf("%s: size %d != %d", b.Path, info.Size(), b.Size)
		}
	}
	idx.Inputs.EachFile(func(name string) {
		_, err := fs.Stat(dir, name)
		if err != nil {
			t.Fatalf("stat %s: %s", name, err)
		}
	})
}

func checkNoGarbage(t *testing.T, root *DirFS, dir string, idx *blockfmt.Index) {
	t.Helper()
	entries, err := fs.ReadDir(root, dir)
	if err != nil {
		t.Fatal(err)
	}
	okfile := make(map[string]struct{})
	for i := range idx.Inline {
		okfile[path.Base(idx.Inline[i].Path)] = struct{}{}
	}
	for i := range idx.Indirect.Refs {
		okfile[path.Base(idx.Indirect.Refs[i].Path)] = struct{}{}
	}
	descs, err := idx.Indirect.Search(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range descs {
		okfile[path.Base(descs[i].Path)] = struct{}{}
	}

	idx.Inputs.EachFile(func(name string) {
		okfile[path.Base(name)] = struct{}{}
	})
	defer func() {
		if t.Failed() {
			for k := range okfile {
				t.Logf("ok file: %s", k)
			}
		}
	}()
	for i := range entries {
		if entries[i].IsDir() {
			continue
		}
		name := entries[i].Name()
		if name == "definition.json" || name == "index" {
			continue
		}
		if _, ok := okfile[name]; !ok {
			t.Errorf("unexpected file %s", name)
		}
	}
}

func TestSync(t *testing.T) {
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
	err := WriteDefinition(dfs, "default", "parking", &Definition{
		Inputs: []Input{
			{Pattern: "file://a-prefix/*.10n"},
			{Pattern: "file://a-prefix/*.json"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = WriteDefinition(dfs, "default", "taxi", &Definition{
		Inputs: []Input{
			{Pattern: "file://b-prefix/*.block"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := newTenant(dfs)
	c := Config{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf: t.Logf,

		// note: this is tuned so that the
		// first append produces an inline ref,
		// and the second append flushes the
		// inline ref to an indirect ref
		MaxInlineBytes: 150 * 1024,

		GCMinimumAge: 1 * time.Millisecond,
	}
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	empty, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Inline) != 0 {
		t.Errorf("expected len(Contents)==0; got %#v", empty.Inline)
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
	// now we should ingest some data
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	// test that a second Sync determines
	// that everything is up-to-date and does nothing
	owner.ro = true
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
	idx0, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx0.Objects() != 1 {
		t.Errorf("expected idx.Objects()==1; got %d", idx0.Objects())
	}
	idx0.Inputs.Backing = dfs
	if !contains(t, idx0, "file://a-prefix/parking.10n") {
		t.Error("missing file?")
	}
	checkContents(t, idx0, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx0)

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
	// for this sync, append to the previous data
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	// there should be two objects in the parking table,
	// since we did a regular append
	idx1, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	if idx1.Objects() != 1 {
		t.Fatalf("got idx1.Objects() = %d", idx1.Objects())
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)

	// check that changing the definition for a
	// table causes the index to be rewritten
	idx2, err := OpenIndex(dfs, "default", "taxi", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	hash2, err := idx2.UserData.Field("definition").Field("hash").BlobShared()
	if err != nil {
		t.Fatal("idx2 had no definition hash present:", err)
	}
	def3 := &Definition{
		Inputs: []Input{
			{Pattern: "file://b-prefix/*.block"},
		},
		Features: []string{"legacy-zstd"},
	}
	err = WriteDefinition(dfs, "default", "taxi", def3)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	idx3, err := OpenIndex(dfs, "default", "taxi", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	hash3, err := idx3.UserData.Field("definition").Field("hash").BlobShared()
	if err != nil {
		t.Fatal("idx3 had no definition hash present:", err)
	}
	if bytes.Equal(hash2, hash3) {
		t.Errorf("expected different hashes, both were: %x", hash2)
	}
	if !bytes.Equal(hash3, def3.Hash()) {
		t.Errorf("hashes don't match: %x != %x", hash3, def3.Hash())
	}

	// appending should do nothing:
	ino, err := fs.Stat(dfs, "a-prefix/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	etag, err := dfs.ETag("a-prefix/parking2.json", ino)
	if err != nil {
		t.Fatal(err)
	}
	lst := mkparts([]blockfmt.Input{{
		Path: "file://a-prefix/parking2.json",
		ETag: etag,
		// deliberately omit LastModified
		Size: ino.Size(),
		R:    noReadCloser{t},
		F:    blockfmt.MustSuffixToFormat(".json"),
	}})

	ti := info(&c, owner, "default", "parking")
	owner.ro = true
	err = ti.append(context.Background(), lst)
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
	blobs, blocks, _, err := idx1.Descs(dfs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) == 0 {
		t.Fatal("no blobs?")
	}
	if len(blocks) == 0 {
		t.Fatal("no blocks?")
	}
	tr := blobs[0].Trailer
	ranges := tr.Sparse.Fields()
	if ranges == 0 {
		// the parking2.json file
		// will have range data
		// that should be picked up
		// during JSON parsing
		t.Fatal("no ranges")
	}
	if tr.Sparse.Get([]string{"Issue", "Data"}) == nil {
		t.Fatal("no ranges for Issue.Data")
	}
}

func TestMaxBytesSync(t *testing.T) {
	checkFiles(t)
	tmpdir := t.TempDir()
	err := os.MkdirAll(filepath.Join(tmpdir, "a-prefix"), 0750)
	if err != nil {
		t.Fatal(err)
	}

	dfs := newDirFS(t, tmpdir)
	err = WriteDefinition(dfs, "default", "parking", &Definition{
		Inputs: []Input{
			{Pattern: "file://a-prefix/{filename}.10n"},
		},
		Partitions: []Partition{{
			Field: "filename",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := newTenant(dfs)
	c := Config{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf: t.Logf,
	}
	symlink := func(old, new string) {
		oldabs, err := filepath.Abs(old)
		if err != nil {
			t.Fatal(err)
		}
		err = os.Symlink(oldabs, filepath.Join(tmpdir, new))
		if err != nil {
			t.Fatal(err)
		}
	}
	symlink("../testdata/parking.10n", "a-prefix/parking0.10n")
	symlink("../testdata/parking.10n", "a-prefix/parking1.10n")
	symlink("../testdata/parking.10n", "a-prefix/parking2.10n")
	// this should fit 2 but not 3 copies of parking.10n
	c.MaxScanBytes = 200000
	// disable merging of non-trivial objects
	c.MinMergeSize = 1
	err = c.Sync(owner, "default", "*")
	if !errors.Is(err, ErrBuildAgain) {
		t.Fatalf("expected ErrBuildAgain; got %v", err)
	}
	idx, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	idx.Inputs.Backing = dfs
	if !contains(t, idx, "file://a-prefix/parking0.10n") {
		t.Error("don't have parking0.10n?")
	}
	if !contains(t, idx, "file://a-prefix/parking1.10n") {
		t.Error("don't have parking1.10n?")
	}
	if contains(t, idx, "file://a-prefix/parking2.10n") {
		t.Error("have parking2.10n?")
	}

	// this should bring everything fully up-to-date
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	idx, err = OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	idx.Inputs.Backing = dfs
	if !contains(t, idx, "file://a-prefix/parking2.10n") {
		t.Error("don't have parking2.10n?")
	}
	checkContents(t, idx, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx)

	// test that a second Sync determines
	// that everything is up-to-date and does nothing
	owner.ro = true
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
}

func TestSyncRetention(t *testing.T) {
	tmpdir := t.TempDir()
	dfs := newDirFS(t, tmpdir)
	now := date.Now()
	mksparse := func(ago ...time.Duration) blockfmt.SparseIndex {
		var s blockfmt.SparseIndex
		for i := 0; i < len(ago); i += 2 {
			a, z := now.Add(-ago[i+1]), now.Add(-ago[i])
			rng := blockfmt.NewRange([]string{"date"}, ion.Timestamp(a), ion.Timestamp(z))
			s.Push([]blockfmt.Range{rng})
		}
		return s
	}
	const day = 24 * time.Hour
	checkFiles(t)
	st := tableState{
		def: &Definition{
			Retention: &RetentionPolicy{
				Field:    "date",
				ValidFor: date.Duration{Day: 10},
			},
			Partitions: []Partition{{Field: "file"}},
		},
		conf: Config{
			Logf: t.Logf,
		},
		ofs: dfs,
	}
	// construct an index and relevant files
	// manually since it's not always possible to
	// control whether output files end up in the
	// indirect tree or inlined into the index
	//
	// TODO: it would be very nice of there was a
	// cleaner way of doing this...
	mkempty := func() []byte {
		var st ion.Symtab
		var buf ion.Buffer
		buf.BeginStruct(-1)
		buf.BeginField(st.Intern("contents"))
		buf.BeginList(-1)
		buf.EndList()
		buf.EndStruct()
		var stbuf ion.Buffer
		st.Marshal(&stbuf, true)
		full := append(stbuf.Bytes(), buf.Bytes()...)
		return compr.Compression("zstd").Compress(full, nil)
	}
	empty := mkempty()
	root := "db/default/table"
	err := os.MkdirAll(path.Join(tmpdir, root), 0750)
	if err != nil {
		t.Fatal(err)
	}
	testobjs := []struct {
		obj     blockfmt.ObjectInfo
		content []byte
	}{
		{blockfmt.ObjectInfo{Path: root + "/inline-expired"}, nil},
		{blockfmt.ObjectInfo{Path: root + "/inline-retained"}, nil},
		{blockfmt.ObjectInfo{Path: root + "/indirect-expired"}, empty},
		{blockfmt.ObjectInfo{Path: root + "/indirect-retained"}, empty},
	}
	for i := range testobjs {
		o := &testobjs[i]
		o.obj.ETag, err = dfs.WriteFile(o.obj.Path, o.content)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx := &blockfmt.Index{
		Inline: []blockfmt.Descriptor{{
			ObjectInfo: testobjs[0].obj,
			Trailer: blockfmt.Trailer{
				Sparse: mksparse(14*day, 17*day),
			},
		}, {
			ObjectInfo: testobjs[1].obj,
			Trailer: blockfmt.Trailer{
				Sparse: mksparse(8*day, 11*day),
			},
		}},
		Indirect: blockfmt.IndirectTree{
			Refs: []blockfmt.IndirectRef{
				{ObjectInfo: testobjs[2].obj},
				{ObjectInfo: testobjs[3].obj},
			},
			Sparse: mksparse(
				10*day, 13*day, // expired
				0*day, 3*day, // retained
			),
		},
	}

	purged := st.purgeExpired(idx)
	if !purged {
		t.Fatal("expected something to have happened")
	}

	// make sure the relevant files got
	// quarantined
	var got []string
	for i := range idx.ToDelete {
		part := path.Base(idx.ToDelete[i].Path)
		got = append(got, part)
	}
	slices.Sort(got)
	want := []string{"indirect-expired", "inline-expired"}
	if !slices.Equal(want, got) {
		t.Errorf("unexpected results: want %s, got %s", want, got)
	}
}
