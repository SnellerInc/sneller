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
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr/blob"
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
	if !strings.HasPrefix(pattern, "file://") {
		return nil, "", fmt.Errorf("bad pattern %q", pattern)
	}
	pattern = strings.TrimPrefix(pattern, "file://")
	return d.OutputFS, pattern, nil
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

	dfs := NewDirFS(tmpdir)
	defer dfs.Close()
	dfs.Log = t.Logf
	err := WriteDefinition(dfs, "default", &Definition{
		Name: "parking",
		Inputs: []Input{
			{Pattern: "file://a-prefix/*.10n"},
			{Pattern: "file://a-prefix/*.json"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = WriteDefinition(dfs, "default", &Definition{
		Name: "taxi",
		Inputs: []Input{
			{Pattern: "file://b-prefix/*.block"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := newTenant(dfs)
	b := Builder{
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

		GCLikelihood: 1,
		GCMinimumAge: 1 * time.Millisecond,
	}
	err = b.Sync(owner, "default", "*")
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
	err = b.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	// test that a second Sync determines
	// that everything is up-to-date and does nothing
	owner.ro = true
	err = b.Sync(owner, "default", "*")
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
	for i := range idx0.Inline {
		if idx0.Inline[i].Trailer == nil {
			t.Errorf("no trailer in contents[%d]", i)
		}
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
	err = b.Sync(owner, "default", "*")
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
	if idx1.Indirect.Objects() != 1 {
		t.Logf("inline size: %d", idx1.Inline[0].Trailer.Decompressed())
		t.Fatal("expected flush to indirect...?")
	}
	checkContents(t, idx1, dfs)
	checkNoGarbage(t, dfs, "db/default/parking", idx1)

	// check that changing the definition for a
	// table causes the index to be rewritten
	idx2, err := OpenIndex(dfs, "default", "taxi", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	hash2, ok := idx2.UserData.Field("definition").Field("hash").Blob()
	if !ok {
		t.Fatal("idx2 had no definition hash present")
	}
	def3 := &Definition{
		Name: "taxi",
		Inputs: []Input{
			{Pattern: "file://b-prefix/*.block"},
		},
		Features: []string{"legacy-zstd"},
	}
	err = WriteDefinition(dfs, "default", def3)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	idx3, err := OpenIndex(dfs, "default", "taxi", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	hash3, ok := idx3.UserData.Field("definition").Field("hash").Blob()
	if !ok {
		t.Fatal("idx3 had no definition hash present")
	}
	if bytes.Equal(hash2, hash3) {
		t.Errorf("expected different hashes, both were: %x", hash2)
	}
	if !bytes.Equal(hash3, def3.Hash()) {
		t.Errorf("hashes don't match: %x != %x", hash3, def3.Hash())
	}

	// appending should do nothing:
	info, err := fs.Stat(dfs, "a-prefix/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	etag, err := dfs.ETag("a-prefix/parking2.json", info)
	if err != nil {
		t.Fatal(err)
	}
	lst := []blockfmt.Input{{
		Path: "file://a-prefix/parking2.json",
		ETag: etag,
		// deliberately omit LastModified
		Size: info.Size(),
		R:    noReadCloser{t},
		F:    blockfmt.MustSuffixToFormat(".json"),
	}}

	owner.ro = true
	err = b.Append(owner, "default", "parking", lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
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

	dfs := NewDirFS(tmpdir)
	defer dfs.Close()
	dfs.Log = t.Logf
	err = WriteDefinition(dfs, "default", &Definition{
		Name: "parking",
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
	b := Builder{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf:         t.Logf,
		GCLikelihood: 1,
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
	b.MaxScanBytes = 200000
	// disable merging of non-trivial objects
	b.MinMergeSize = 1
	err = b.Sync(owner, "default", "*")
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
	err = b.Sync(owner, "default", "*")
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
	err = b.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
}
