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
	"io/fs"
	"math/rand"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func checkLevel(t *testing.T, fs UploadFS, f *level, load, dirtyok bool) {
	// dirty-ness is transitive: a dirty child means
	// that all its parent tree nodes must also be dirty
	if f.isDirty && !dirtyok {
		t.Error("dirty level with !dirty parent")
	}
	if f.isInner {
		checkInner(t, fs, f, load, dirtyok)
	} else {
		checkLeaf(t, f)
	}
}

func checkTree(t *testing.T, f *FileTree, load bool) {
	checkLevel(t, f.Backing, &f.root, load, f.root.isDirty)
	if t.Failed() {
		t.FailNow()
	}
}

func checkInner(t *testing.T, fs UploadFS, f *level, load, dirtyok bool) {
	last := f.last
	var prev []byte
	for i := range f.levels {
		if i < len(f.levels)-1 && bytes.Compare(f.levels[i].last, last) >= 0 {
			t.Errorf("inner[%d] last = %s; outer last = %s", i, f.levels[i].last, last)
		}
		if i == len(f.levels)-1 && !bytes.Equal(f.levels[i].last, last) {
			t.Errorf("inner last = %s, outer last = %s", f.levels[i].last, last)
		}
		if bytes.Compare(f.levels[i].last, prev) <= 0 {
			t.Errorf("inner[%d] = %s, inner[%d-1] = %s", i, f.levels[i].last, i-1, prev)
		}
		prev = f.levels[i].last
		if f.levels[i].levels == nil &&
			f.levels[i].contents == nil {
			// an empty entry is only legal if it
			// has been stored somewhere
			if f.levels[i].path == nil {
				t.Errorf("inner[%d] dirty but no contents %s %s (inner: %v)", i, f.levels[i].path, f.levels[i].last, f.levels[i].isInner)
			}
			if !load {
				continue
			}
			err := f.levels[i].load(fs)
			if err != nil {
				t.Fatal(err)
			}
		}
		checkLevel(t, fs, &f.levels[i], load, dirtyok && f.isDirty)
	}
}

func checkLeaf(t *testing.T, f *level) {
	// assert top-level is ordered
	if bytes.Compare(f.first(), f.last) > 0 {
		t.Errorf("first %q > last %q", f.first(), f.last)
	}
	// assert contents are ordered
	for j := range f.contents {
		ent := &f.contents[j]
		if j == 0 && !bytes.Equal(ent.path, f.first()) {
			t.Errorf("list %d entry 0 path %q != first %q", j, ent.path, f.first())
		}
		if j == len(f.contents)-1 && !bytes.Equal(ent.path, f.last) {
			t.Errorf("entry %d %q != %q", j, ent.path, f.last)
		}
		if j > 0 && bytes.Compare(ent.path, f.contents[j-1].path) <= 0 {
			t.Errorf("table %d entry %d out-of-order", j-1, j)
		}
	}
}

// adjust splitlevel for testing
func lowsplit(t *testing.T, fanout int) {
	old := splitlevel
	splitlevel = fanout
	t.Cleanup(func() {
		splitlevel = old
	})
}

func TestFiletreeInsert(t *testing.T) {

	// do super lower fan-out to cover
	// the splitting logic
	lowsplit(t, 16)

	const inserts = 5000
	triple := func() (string, string, int) {
		n := rand.Intn(inserts / 2) // 50% likelihood of collision
		name := fmt.Sprintf("data/random-name-%d", n)
		etag := fmt.Sprintf("etag-%d", rand.Intn(2)+n)
		return name, etag, n
	}
	type inserted struct {
		etag string
		desc int
	}
	dir := NewDirFS(t.TempDir())

	sofar := make(map[string]inserted)
	f := FileTree{
		Backing: dir,
	}
	f.Reset()

	nextpath := 0
	var synclock sync.Mutex
	sync := func(old string, buf []byte) (path, etag string, err error) {
		synclock.Lock()
		defer synclock.Unlock()
		if old != "" {
			f.Backing.(*DirFS).Remove(old)
		}
		name := fmt.Sprintf("orig/out-file-%d", nextpath)
		ret, err := f.Backing.WriteFile(name, buf)
		nextpath++
		return name, ret, err
	}

	var tmpbuf ion.Buffer
	var tmpst ion.Symtab

	var appended, overwritten int
	var resyncs int
	for i := 0; i < inserts; i++ {
		if i%1000 == 123 {
			checkTree(t, &f, false)
			resyncs++
			// evict everything
			// and let it get paged back in
			// incrementally
			err := f.sync(sync)
			if err != nil {
				t.Fatal(err)
			}
			tmpbuf.Set(nil) // should not alias previous iteration
			tmpst.Reset()
			f.encode(&tmpbuf, &tmpst)
			f.Reset()
			err = f.decode(&tmpst, tmpbuf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			checkTree(t, &f, false)
		}
		path, etag, desc := triple()
		ret, err := f.Append(path, etag, desc)
		if err != nil {
			overwritten++
			if err != ErrETagChanged {
				t.Fatal(err)
			}
			// should be old path with different etag
			sf, ok := sofar[path]
			if !ok {
				t.Fatalf("got overwrite, but %q not yet written", path)
			}
			if sf.etag == etag {
				t.Fatalf("etag %q matches; it should have changed", etag)
			}
			continue
		}
		if ret {
			if !f.root.isDirty {
				t.Fatalf("root !dirty after insert (appended=%d)", appended)
			}
			appended++
			_, ok := sofar[path]
			if ok {
				t.Fatalf("got append with path %q even though we have inserted it already!", path)
			}
			sofar[path] = inserted{etag, desc}
		} else {
			// should be old path with same etag
			sf, ok := sofar[path]
			if !ok {
				t.Fatalf("got !append for %q but not yet inserted?", path)
			}
			if sf.etag != etag {
				t.Fatalf("got !append with non-matching etags %q, %q", sf.etag, etag)
			}
		}
	}
	checkTree(t, &f, true)
	t.Logf("resyncs: %d", resyncs)
	t.Logf("%d appended, %d overwritten", appended, overwritten)

	err := f.Walk("", func(p, etag string, id int) bool {
		sf, ok := sofar[p]
		if !ok {
			t.Errorf("didn't find path %s", p)
			return false
		}
		if sf.etag != etag {
			t.Errorf("bad etag %s for path %s", etag, p)
			return false
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// test for garbage; the state of f should
	// precisely match the set of files that
	// are currently in the tempdir
	live, err := fs.Glob(dir, "orig/out-file-*")
	if err != nil {
		t.Fatal(err)
	}
	expect := make(map[string]struct{})
	for _, l := range live {
		expect[l] = struct{}{}
	}
	startExpect := len(expect)
	err = f.EachFile(func(file string) {
		_, ok := expect[file]
		if !ok {
			t.Fatalf("unexpected garbage file %s", file)
		}
		delete(expect, file)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expect) != 0 {
		t.Errorf("%d files of %d not accounted for by EachFile", len(expect), startExpect)
	}
}

func TestFiletreeOverwrite(t *testing.T) {
	lowsplit(t, 16)
	dir := NewDirFS(t.TempDir())
	f := FileTree{
		Backing: dir,
	}

	nextpath := 0
	sync := func(old string, buf []byte) (path, etag string, err error) {
		if old != "" {
			f.Backing.(*DirFS).Remove(old)
		}
		name := fmt.Sprintf("orig/out-file-%d", nextpath)
		ret, err := f.Backing.WriteFile(name, buf)
		nextpath++
		return name, ret, err
	}

	// insert should succeed
	ret, err := f.Append("foo/bar", "etag:foo/bar", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ret {
		t.Fatal("expected new insert")
	}
	if f.root.levels[0].path != nil {
		t.Fatal("entry[0] not dirty")
	}
	// success -> failure should be allowed for matching etag
	ret, err = f.Append("foo/bar", "etag:foo/bar", -1)
	if err != nil {
		t.Fatal(err)
	}
	if !ret {
		t.Fatal("expected new insert")
	}
	if f.root.levels[0].path != nil {
		t.Fatal("entry[0] not dirty")
	}

	err = f.sync(sync)
	if err != nil {
		t.Fatal(err)
	}
	// force reload
	f.root.levels[0].contents = nil

	// failure -> success should be disallowed
	// if the etag has not changed
	ret, err = f.Append("foo/bar", "etag:foo/bar", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ret {
		t.Fatal("expected no new entry")
	}

	// failure -> success should be allowed
	// if the etag has been changed
	ret, err = f.Append("foo/bar", "etag:foo/bar2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ret {
		t.Fatal("expected new entry when etag changed")
	}

	err = f.sync(sync)
	if err != nil {
		t.Fatal(err)
	}
	f.root.levels[0].contents = nil

	// ordinary re-insert
	ret, err = f.Append("foo/bar", "etag:foo/bar2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ret {
		t.Fatal("expeced to refuse duplicate entry")
	}

	checkTree(t, &f, true)
}
