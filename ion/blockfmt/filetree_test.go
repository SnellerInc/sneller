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
	"math/rand"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func checkTree(t *testing.T, f *FileTree, load bool) {
	for i := range f.toplevel {
		if f.toplevel[i].contents == nil {
			if f.dirty[i] {
				t.Errorf("toplevel[%d] dirty but no contents", i)
			}
			if !load {
				continue
			}
			err := f.load(&f.toplevel[i])
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if !f.dirty[i] && len(f.toplevel[i].path) == 0 {
				t.Errorf("entry %d not dirty, has contents, but no filesystem path?", i)
			}
		}
		// assert top-level is ordered
		if bytes.Compare(f.toplevel[i].first(), f.toplevel[i].last) > 0 {
			t.Errorf("first %q > last %q", f.toplevel[i].first(), f.toplevel[i].last)
		}
		if i > 0 && bytes.Compare(f.toplevel[i-1].last, f.toplevel[i].first()) >= 0 {
			t.Errorf("last %q [%d] >= first %q [%d]", f.toplevel[i-1].last, i-1, f.toplevel[i].first(), i)
		}
		// assert contents are ordered
		for j := range f.toplevel[i].contents {
			ent := &f.toplevel[i].contents[j]
			if j == 0 && !bytes.Equal(ent.path, f.toplevel[i].first()) {
				t.Errorf("list %d entry 0 path %q != first %q", i, ent.path, f.toplevel[i].first())
			}
			if j == len(f.toplevel[i].contents)-1 && !bytes.Equal(ent.path, f.toplevel[i].last) {
				t.Errorf("entry %d %q != %q", j, ent.path, f.toplevel[i].last)
			}
			if j > 0 && bytes.Compare(ent.path, f.toplevel[i].contents[j-1].path) <= 0 {
				t.Errorf("table %d entry %d out-of-order", i, j)
			}
		}
	}
}

func TestFiletreeInsert(t *testing.T) {
	const inserts = 30000
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
	for i := range f.toplevel {
		t.Logf("final level %d: %d", i, len(f.toplevel[i].contents))
	}
	checkTree(t, &f, true)
	t.Logf("resyncs: %d", resyncs)
	t.Logf("%d appended, %d overwritten", appended, overwritten)
}

func TestFiletreeOverwrite(t *testing.T) {
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
	if !f.dirty[0] {
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
	if !f.dirty[0] {
		t.Fatal("entry[0] not dirty")
	}

	err = f.sync(sync)
	if err != nil {
		t.Fatal(err)
	}
	// force reload
	f.toplevel[0].contents = nil

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
	f.toplevel[0].contents = nil

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
