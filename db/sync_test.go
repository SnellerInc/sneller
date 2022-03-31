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
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// simple db.Resolver wrapping a DirFS
type dirResolver struct {
	*DirFS
}

func checkFiles(t *testing.T) {
	count := openFiles()
	if count > 0 {
		t.Cleanup(func() {
			http.DefaultClient.CloseIdleConnections()
			ret := openFiles()
			if ret > count {
				t.Errorf("started with %d open files; now have %d", count, ret)
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

func openFiles() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
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
	return d.DirFS, pattern, nil
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
	ret, err := idx.Inputs.Contains(path)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return ret
}

func checkContents(t *testing.T, idx *blockfmt.Index, dir OutputFS) {
	for i := range idx.Contents {
		b := &idx.Contents[i]
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

		GCLikelihood: 50,
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
	if len(empty.Contents) != 0 {
		t.Errorf("expected len(Contents)==0; got %#v", empty.Contents)
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
	if len(idx0.Contents) != 1 {
		t.Errorf("expected len(Contents)==1; got %#v", idx0.Contents)
	}
	for i := range idx0.Contents {
		if idx0.Contents[i].Trailer == nil {
			t.Errorf("no trailer in contents[%d]", i)
		}
	}
	idx0.Inputs.Backing = dfs
	if !contains(t, idx0, "file://a-prefix/parking.10n") {
		t.Error("missing file?")
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
	if len(idx1.Contents) != 1 {
		t.Fatalf("got idx1 contents %#v", idx1.Contents)
	}
	if idx1.Contents[0].Trailer == nil {
		t.Errorf("no trailer in contents[%d]", 0)
	}
	checkContents(t, idx1, dfs)

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
		F:    blockfmt.SuffixToFormat[".json"](),
	}}
	owner.ro = true
	err = b.Append(owner, "default", "parking", lst)
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
	blobs, err := Blobs(dfs, idx1)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs.Contents) != 1 {
		t.Errorf("got back %d blobs?", len(blobs.Contents))
	}
	tr := blobs.Contents[0].(*blob.Compressed).Trailer
	ranges := 0
	for j := range tr.Blocks {
		ranges += len(tr.Blocks[j].Ranges)
	}
	if ranges == 0 {
		// the parking2.json file
		// will have range data
		// that should be picked up
		// during JSON parsing
		t.Fatal("no ranges")
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
			{Pattern: "file://a-prefix/*.10n"},
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

		GCLikelihood: 50,
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
	b.MaxInputBytes = 250000
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
	// test that a second Sync determines
	// that everything is up-to-date and does nothing
	owner.ro = true
	err = b.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
}
