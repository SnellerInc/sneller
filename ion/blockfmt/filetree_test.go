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
	"encoding/binary"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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
		if j == 0 && !bytes.Equal(f.entryPath(ent), f.first()) {
			t.Errorf("list %d entry 0 path %q != first %q", j, f.entryPath(ent), f.first())
		}
		if j == len(f.contents)-1 && !bytes.Equal(f.entryPath(ent), f.last) {
			t.Errorf("entry %d %q != %q", j, f.entryPath(ent), f.last)
		}
		if j > 0 && bytes.Compare(f.entryPath(ent), f.entryPath(&f.contents[j-1])) <= 0 {
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

	nextpath := 0
	var synclock sync.Mutex
	livefiles := 0
	maxsize := 0
	sync := func(old string, buf []byte) (path, etag string, err error) {
		synclock.Lock()
		defer synclock.Unlock()
		if old != "" {
			f.Backing.(*DirFS).Remove(old)
			livefiles--
		}
		name := fmt.Sprintf("orig/out-file-%d", nextpath)
		ret, err := f.Backing.WriteFile(name, buf)
		nextpath++
		livefiles++
		if livefiles > maxsize {
			maxsize = livefiles
		}
		return name, ret, err
	}
	reset := func(f *FileTree) {
		var st ion.Symtab
		var buf ion.Buffer
		err := f.sync(sync)
		if err != nil {
			t.Fatal(err)
		}
		f.encode(&buf, &st)
		f.Reset()
		f.decode(&st, buf.Bytes())
	}

	var appended, overwritten int
	var resyncs int
	for i := 0; i < inserts; i++ {
		if i%500 == 123 {
			checkTree(t, &f, false)
			resyncs++
			reset(&f)
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
	t.Logf("%d files live; %d max", livefiles, maxsize)

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

	reset := func(f *FileTree) {
		var st ion.Symtab
		var buf ion.Buffer
		err := f.sync(sync)
		if err != nil {
			t.Fatal(err)
		}
		f.encode(&buf, &st)
		f.Reset()
		f.decode(&st, buf.Bytes())
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

	// failure -> success should be disallowed
	// if the etag has not changed
	ret, err = f.Append("foo/bar", "etag:foo/bar", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ret {
		t.Fatal("expected no new entry")
	}

	reset(&f)

	// failure -> success should be allowed
	// if the etag has been changed
	ret, err = f.Append("foo/bar", "etag:foo/bar2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ret {
		t.Fatal("expected new entry when etag changed")
	}

	reset(&f)

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

func TestFiletreeShrink(t *testing.T) {
	likelihoods := []float64{
		0, 0.3, 0.5, 0.8, 1.0,
	}
	for _, f := range likelihoods {
		t.Run(fmt.Sprintf("reload-%g", f), func(t *testing.T) {
			testFiletreeShrink(t, f)
		})
	}
}

func testFiletreeShrink(t *testing.T, reloadLikelihood float64) {
	lowsplit(t, 1000)
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

	any := func(sofar map[string]inserted) string {
		for key := range sofar {
			return key
		}
		return ""
	}

	nextpath := 0
	var synclock sync.Mutex
	livefiles := 0
	maxsize := 0
	totalout := 0
	sync := func(old string, buf []byte) (path, etag string, err error) {
		synclock.Lock()
		defer synclock.Unlock()
		if old != "" {
			f.Backing.(*DirFS).Remove(old)
			livefiles--
		}
		name := fmt.Sprintf("orig/out-file-%d", nextpath)
		ret, err := f.Backing.WriteFile(name, buf)
		nextpath++
		livefiles++
		totalout++
		if livefiles > maxsize {
			maxsize = livefiles
		}
		return name, ret, err
	}
	reset := func(f *FileTree) {
		var st ion.Symtab
		var buf ion.Buffer
		err := f.sync(sync)
		if err != nil {
			t.Fatal(err)
		}
		f.encode(&buf, &st)
		if rand.Float64() < reloadLikelihood {
			f.Reset()
			f.decode(&st, buf.Bytes())
		}
	}

	var appended, overwritten int
	var resyncs int
	for i := 0; i < inserts; i++ {
		if i == 4000 {
			lowsplit(t, 100)
		}
		if i%123 == 0 {
			checkTree(t, &f, false)
			resyncs++
			reset(&f)
			t.Logf("%d files live; %d max; %d total written (%d items)", livefiles, maxsize, totalout, appended)
			checkTree(t, &f, false)
		}

		path, etag, desc := triple()

		f.Prefetch([]Input{
			{Path: path},
			{Path: any(sofar)},
			{Path: any(sofar)},
			{Path: any(sofar)},
		})

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

	reset(&f)
	t.Logf("%d files live; %d max; %d total written", livefiles, maxsize, totalout)
	// prefetch elements;
	// we should get a panic in checkTree
	// if we try to load anything
	keys := maps.Keys(sofar)
	slices.Sort(keys)
	f.root.prefetchInner(f.Backing, keys)

	f.Backing = nil
	checkTree(t, &f, true)

	t.Logf("resyncs: %d", resyncs)
	t.Logf("%d appended, %d overwritten", appended, overwritten)
	t.Logf("%d files live; %d max; %d total written", livefiles, maxsize, totalout)
}

// if BENCH_S3_BUCKET is set, try to get ambient credentials
// and perform some benchmarks against a real backing store
func BenchmarkFiletreeS3(b *testing.B) {
	bucket, ok := os.LookupEnv("BENCH_S3_BUCKET")
	if !ok {
		b.Skip()
	}
	key, err := aws.AmbientKey("s3", s3.DeriveForBucket(bucket))
	if err != nil {
		b.Fatal(err)
	}
	fs := &S3FS{
		s3.BucketFS{
			Key:    key,
			Bucket: bucket,
		},
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	prefix := fmt.Sprintf("bench/%d/", rng.Int())
	b.Logf("using prefix %s", prefix)
	b.Cleanup(func() {
		entries, err := fs.ReadDir(path.Clean(prefix))
		if err != nil {
			b.Logf("readdir: %s", err)
		}
		for i := range entries {
			if entries[i].IsDir() {
				continue
			}
			err := fs.Remove(path.Join(prefix, entries[i].Name()))
			if err != nil {
				b.Logf("error removing file: %s", err)
			}
		}
	})

	// number of objects to insert
	sizes := []int{10000}
	// number of objects to insert
	// per call to sync()
	batches := []int{10, 100, 1000}
	for _, size := range sizes {
		lst := make([]string, size)
		for i := range lst {
			lst[i] = fmt.Sprintf("s3://%s/%s/file-%d", bucket, prefix, rand.Int())
		}
		slices.Sort(lst)
		for _, batch := range batches {
			b.Run(fmt.Sprintf("batch=%d/size=%d", batch, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					ft := FileTree{
						Backing: fs,
					}
					tbd := lst
					for len(tbd) > 0 {
						ins := tbd
						if len(ins) > batch {
							ins = ins[:batch]
						}
						tbd = tbd[len(ins):]
						for _, p := range ins {
							_, err := ft.Append(p, p, 1)
							if err != nil {
								b.Fatal(err)
							}
						}
						err := ft.sync(func(_ string, buf []byte) (string, string, error) {
							p := path.Join(prefix, "inputs-"+uuid())
							etag, err := ft.Backing.WriteFile(p, buf)
							return p, etag, err
						})
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkFiletree(b *testing.B) {
	sizes := []int{
		5000,
		10000,
		500000,
	}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("insert-ordered-%d", size), func(b *testing.B) {
			var path, etag [8]byte
			var f FileTree
			b.ReportAllocs()
			reset := 0
			f.Reset()
			for i := 0; i < b.N; i++ {
				if reset == size {
					f.Reset()
					reset = 0
				}
				// big-endian ints will be sorted by path:
				binary.BigEndian.PutUint64(path[:], uint64(i))
				copy(etag[:], path[:])
				ret, err := f.Append(string(path[:]), string(etag[:]), 0)
				if err != nil {
					b.Fatal(err)
				}
				if !ret {
					b.Fatalf("failed to insert entry %d", i)
				}
				reset++
			}
		})
		b.Run(fmt.Sprintf("insert-%d", size), func(b *testing.B) {
			var path, etag [8]byte
			var f FileTree
			b.ReportAllocs()
			reset := 0
			f.Reset()
			for i := 0; i < b.N; i++ {
				if reset == size {
					f.Reset()
					reset = 0
				}
				// little-endian ints will be out-of-order
				// when sorted as bytes
				binary.LittleEndian.PutUint64(path[:], uint64(i))
				copy(etag[:], path[:])
				ret, err := f.Append(string(path[:]), string(etag[:]), 0)
				if err != nil {
					b.Fatal(err)
				}
				if !ret {
					b.Fatalf("failed to insert entry %d", i)
				}
				reset++
			}
		})
	}
}
