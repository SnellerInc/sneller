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

//go:build !windows
// +build !windows

package dcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SnellerInc/sneller/expr/blob"
)

type testSegment struct {
	all             []byte
	align, spansize int
	skip            blob.Bitmap

	// segment hash -> segment number
	seghashes map[string]int

	// injected error
	inject struct {
		err error // error to return
		seg int   // segment # at which to return the error
	}
}

type testLogger struct {
	lock sync.Mutex
	out  testing.TB
}

func (t *testLogger) Printf(f string, args ...interface{}) {
	t.lock.Lock()
	t.out.Logf(f, args...)
	t.lock.Unlock()
}

type testSegOutput struct {
	raw     int64
	from    *testSegment
	refs    int32
	endsegs int32
	segok   []int32
	rep     int

	inject struct {
		err    error
		seg    int
		unlink func()
	}
}

func (t *testSegOutput) Open() (io.WriteCloser, error) {
	atomic.AddInt32(&t.refs, 1)
	return t, nil
}

func (t *testSegOutput) Close() error {
	if c := atomic.AddInt32(&t.refs, -1); c < 0 {
		return fmt.Errorf("negative refcount %d", c)
	}
	return nil
}

func (t *testSegOutput) Write(p []byte) (int, error) {
	if unlink := t.inject.unlink; unlink != nil {
		unlink()
	}
	name := hashname(p)
	seg, ok := t.from.seghashes[name]
	if !ok {
		return 0, fmt.Errorf("unknown segment hash %s (len=%d)", name, len(p))
	}
	atomic.AddInt64(&t.raw, int64(len(p)))
	if c := atomic.AddInt32(&t.segok[seg], 1); int(c) > t.rep {
		return 0, fmt.Errorf("segment %d written %d times", seg, c)
	}
	if t.inject.err != nil && t.inject.seg == seg {
		return 0, t.inject.err
	}
	return len(p), nil
}

// implement vm.EndSegmentWriter so that
// we can test that we were notified correctly
func (t *testSegOutput) EndSegment() {
	atomic.AddInt32(&t.endsegs, 1)
}

func hashname(buf []byte) string {
	h := sha256.Sum256(buf)
	return hex.EncodeToString(h[:])
}

func (ts *testSegment) populate() {
	rand.Read(ts.all)
	ts.seghashes = make(map[string]int, (len(ts.all)+ts.align-1)/ts.align)

	// determine which segments of input we
	// expect to see in the output and their
	// precise lengths
	seg := 0
	for off := 0; off < len(ts.all); off += ts.align {
		mem := ts.all[off:]
		if len(mem) > ts.align {
			mem = mem[:ts.align]
		}
		ts.seghashes[hashname(mem)] = seg
		seg++
	}
}

func (ts *testSegment) Merge(other Segment) {
	ts2 := other.(*testSegment)
	if ts2.ETag() != ts.ETag() {
		panic("mis-matched segment merge")
	}
}

func (ts *testSegment) ETag() string {
	return hashname(ts.all)
}

func (ts *testSegment) Size() int64 {
	return int64(len(ts.all))
}

func (ts *testSegment) Open() (io.ReadCloser, error) {
	if ts.inject.err != nil {
		return nil, ts.inject.err
	}
	return io.NopCloser(bytes.NewReader(ts.all)), nil
}

func (ts *testSegment) Decode(dst io.Writer, src []byte) error {
	_, err := ts.decode(dst, src)
	return err
}

func (ts *testSegment) decode(dst io.Writer, src []byte) (int64, error) {
	if len(src) != len(ts.all) {
		panic("unexpected source length")
	}
	if ts.inject.err != nil {
		return 0, ts.inject.err
	}
	n := int64(0)
	for off := 0; off < len(ts.all); off += ts.align {
		if ts.skip.Get(off / ts.align) {
			continue
		}
		mem := ts.all[off:]
		if len(mem) > ts.align {
			mem = mem[:ts.align]
		}
		nn, err := dst.Write(mem)
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (ts *testSegment) testout() *testSegOutput {
	return &testSegOutput{
		from:  ts,
		segok: make([]int32, len(ts.seghashes)),
		rep:   1,
	}
}

func (ts *testSegment) testrep(count int) *testSegOutput {
	return &testSegOutput{
		from:  ts,
		segok: make([]int32, len(ts.seghashes)),
		rep:   count,
	}
}

func (t *testSegOutput) check() error {
	if t.refs != 0 {
		return fmt.Errorf("%d reference counts outstanding", t.refs)
	}
	for i := range t.segok {
		if int(t.segok[i]) != t.rep {
			return fmt.Errorf("segment %d written %d times, not %d", i, t.segok[i], t.rep)
		}
	}
	if int(t.endsegs) != t.rep {
		return fmt.Errorf("%d EndSegment calls, but written %d times", t.endsegs, t.rep)
	}
	return nil
}

func randseg(align, spanmult, size int) *testSegment {
	ts := &testSegment{
		all:      make([]byte, size),
		align:    align,
		spansize: align * spanmult,
	}
	ts.populate()
	return ts
}

func assertUnlocked(t *testing.T, c *Cache, seg *testSegment) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.inflight[seg.ETag()]; ok {
		t.Helper()
		t.Error("segment still registered as in-flight")
	}
}

func testFiles(t *testing.T) {
	now, err := os.ReadDir("/proc/self/fd")
	if err == nil {
		count := len(now) - 1
		t.Cleanup(func() {
			done, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Error(err)
			}
			if len(done)-1 > count {
				t.Errorf("file descriptor leak: started with %d, now have %d", count, len(done)-1)
			}
		})
	} else {
		t.Log("(can't do file descriptor tracking)")
	}
}

func testCache(t *testing.T, seg *testSegment, parallel int) {
	dir := t.TempDir()
	c := New(dir, func() {})
	c.Logger = &testLogger{out: t}
	tbl := c.Table(seg, 0)
	out := seg.testout()
	err := tbl.WriteChunks(out, parallel)
	if err != nil {
		t.Fatal(err)
	}
	err = out.check()
	if err != nil {
		t.Fatal(err)
	}
	if c.Misses() != 1 {
		t.Errorf("expected 1 miss but got %d", c.Misses())
	}
	if c.Misses() != tbl.Misses() {
		t.Errorf("cache.Misses = %d, table.Misses = %d", c.Misses(), tbl.Misses())
	}

	// now test the cache that ought to be cached
	out = seg.testout()
	err = tbl.WriteChunks(out, parallel)
	if err != nil {
		t.Fatal(err)
	}
	err = out.check()
	if err != nil {
		t.Fatal(err)
	}
	if c.Hits() != 1 {
		t.Errorf("expected 1 hit but got %d", c.Hits())
	}
	if c.Hits() != tbl.Hits() {
		t.Errorf("cache.Hits = %d, table.Hits = %d", c.Hits(), tbl.Hits())
	}

	// can't test locked/unlocked
	// until the cache is closed
	c.Close()
	assertUnlocked(t, c, seg)

	if c.LiveHits() != 0 {
		t.Errorf("%d mappings live?", c.LiveHits())
	}
	match, err := filepath.Glob(dir + "/*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(match) != 0 {
		t.Errorf("tempfiles left in directory: %v", match)
	}
}

func TestRandomized(t *testing.T) {
	testFiles(t)
	spansizes := []int{
		100,
		1000,
		1024,
		4096,
	}
	spanmults := []int{
		1, 3, 4, 8,
	}
	parallelism := []int{
		1, 2, 4, 6, 8,
	}
	sizes := []int{
		4000,
		8000,
		10000,
		12000,
		160000,
	}
	for _, span := range spansizes {
		for _, mult := range spanmults {
			for _, size := range sizes {
				for _, parallel := range parallelism {
					t.Run(fmt.Sprintf("span=%d/mult=%d/size=%d/parallel=%d", span, mult, size, parallel),
						func(t *testing.T) {
							seg := randseg(span, mult, size)
							testCache(t, seg, parallel)
							t.Run("read-error", func(t *testing.T) {
								seg := randseg(span, mult, size)
								testReadError(t, seg, parallel)
							})
							t.Run("write-error", func(t *testing.T) {
								testWriteError(t, seg, parallel)
							})
						})
				}
			}
		}
	}
}

func testWriteError(t *testing.T, seg *testSegment, parallel int) {
	// the error we're testing is io.EOF
	// because early-EOF behavior is the nastied
	// possible error we could enounter
	// in terms of special-case handling
	injected := io.EOF
	segments := (len(seg.all) + seg.spansize - 1) / seg.spansize
	dir := t.TempDir()
	c := New(dir, func() {})
	c.Logger = &testLogger{out: t}
	tbl := c.Table(seg, 0)
	out := seg.testout()
	out.inject.seg = segments / 2
	out.inject.err = injected
	err := tbl.WriteChunks(out, parallel)
	if err != nil {
		t.Fatal(err)
	}
	if c.Misses() != 1 {
		t.Errorf("expected 1 miss but got %d", c.Misses())
	}

	out = seg.testout()
	// test the cache backing disappearing
	// during the access; this shouldn't matter
	// because we've already got access to the
	// file handle
	var once sync.Once
	out.inject.unlink = func() {
		once.Do(func() {
			name := filepath.Join(dir, out.from.ETag())
			if os.Remove(name) == nil {
				t.Logf("unlinked %s", name)
			}
		})
	}
	tbl = c.Table(seg, 0)
	out.raw = 0
	err = tbl.WriteChunks(out, parallel)
	if err != nil {
		t.Fatal(err)
	}
	err = out.check()
	if err != nil {
		t.Fatal(err)
	}

	c.Close()
	if c.LiveHits() != 0 {
		t.Errorf("%d cache entries currently being accessed?", c.LiveHits())
	}
	assertUnlocked(t, c, seg)
	// the first query filled the cache
	// if and only if the span that errored happened
	// to be the last span that was populated;
	// that means this second access may or may not
	// be a cache hit
	misses := c.Misses()
	if misses != 1 {
		t.Errorf("expected 1 miss; got %d", misses)
	}
	// we did two accesses, so if we didn't
	// miss, then we got a hit
	hits := c.Hits()
	if hits+misses != 2 {
		t.Errorf("expected 2 hits+misses; got %d", misses+hits)
	}
	match, err := filepath.Glob(dir + "/*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(match) != 0 {
		t.Errorf("tempfiles left in directory: %v", match)
	}
}

func testReadError(t *testing.T, seg *testSegment, parallel int) {
	// the error we're testing is io.EOF
	// because early-EOF behavior is the nastied
	// possible error we could enounter
	// in terms of special-case handling
	seg.inject.err = io.EOF
	dir := t.TempDir()
	c := New(dir, func() {})
	c.Logger = &testLogger{out: t}
	tbl := c.Table(seg, 0)
	out := seg.testout()
	err := tbl.WriteChunks(out, parallel)
	if err == nil {
		t.Fatal("no error?")
	}
	// the rogue EOF should become an io.ErrUnexpectedEOF
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("unxpected error %v", err)
	}
	if c.Misses() != 1 {
		t.Errorf("expected 1 miss but got %d", c.Misses())
	}
	if c.LiveHits() != 0 {
		t.Errorf("%d cache entries currently being accessed?", c.LiveHits())
	}
	// encountering an error should not strand
	// tmpfiles in the cache directory
	match, err := filepath.Glob(dir + "/*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(match) != 0 {
		t.Errorf("tempfiles left in directory: %v", match)
	}

	// clear the error and confirm that things work
	seg.inject.err = nil
	tbl = c.Table(seg, 0)
	out = seg.testout()
	err = tbl.WriteChunks(out, parallel)
	if err != nil {
		t.Fatal(err)
	}
	err = out.check()
	if err != nil {
		t.Fatal(err)
	}
	if c.LiveHits() != 0 {
		t.Errorf("%d mappings live?", c.LiveHits())
	}

	c.Close()
	assertUnlocked(t, c, seg)
	match, err = filepath.Glob(dir + "/*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(match) != 0 {
		t.Errorf("tempfiles left in directory: %v", match)
	}
}

// concurrent-access torture-test:
// many goroutines accessing the same cache
// entry at once should not lead to any errors
// (also, they should end up sharing the same
// mapping under the hood; we need to confirm
// that it works as expected and doesn't lead
// to any memory leaks)
func TestConcurrentAccess(t *testing.T) {
	testFiles(t)
	parallel := 10
	cc := make(chan error, parallel)
	cache := New(t.TempDir(), func() {})
	cache.Logger = &testLogger{out: t}
	seg := randseg(100, 4000, 80927)
	for i := 0; i < parallel; i++ {
		go func() {
			out := seg.testrep(2)
			tbl := cache.MultiTable(context.Background(), []Segment{seg, seg}, 0)
			err := tbl.WriteChunks(out, parallel)
			if err != nil {
				cc <- err
				return
			}

			cc <- out.check()
		}()
	}
	for i := 0; i < parallel; i++ {
		err := <-cc
		if err != nil {
			t.Error(err)
		}
	}
	cache.Close()
	assertUnlocked(t, cache, seg)

	// the cache accesses should have been
	// coalesced so that we had at most
	// one miss; everything else should have
	// been coalesced into one or more hits
	miss := cache.Misses()
	if miss != 1 {
		t.Errorf("%d cache misses?", miss)
	}
	hits := cache.Hits()
	if hits < 1 {
		t.Errorf("%d cache hits?", hits)
	}
	if n := cache.LiveHits(); n != 0 {
		t.Errorf("%d mappings live?", n)
	}
}

func TestCancel(t *testing.T) {
	testFiles(t)
	parallel := 10
	cc := make(chan error, parallel)
	cache := New(t.TempDir(), func() {})
	cache.Logger = &testLogger{out: t}
	seg := randseg(100, 4000, 80927)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < parallel; i++ {
		go func() {
			out := seg.testrep(2)
			tbl := cache.MultiTable(ctx, []Segment{seg, seg}, 0)
			err := tbl.WriteChunks(out, parallel)
			if err != nil {
				if tbl.Hits() != 0 && tbl.Misses() != 0 {
					// we canceled before beginning any operations,
					// so this should scan 0 segments
					cc <- fmt.Errorf("%d hit %d misses?", tbl.Hits(), tbl.Misses())
					return
				}
				cc <- err
				return
			}
			cc <- out.check()
		}()
	}
	for i := 0; i < parallel; i++ {
		err := <-cc
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}
	cache.Close()
	assertUnlocked(t, cache, seg)
}

// even when allocating a cache entry fails,
// we should still succeed in reading the original data
// from the Segment
func TestBadDir(t *testing.T) {
	testFiles(t)
	seg := randseg(1000, 2, 3500)
	out := seg.testout()
	filled := int64(0)
	cache := New("not-a-directory!", func() {
		atomic.AddInt64(&filled, 1)
	})
	defer cache.Close()
	tbl := cache.Table(seg, 0)
	err := tbl.WriteChunks(out, 8)
	if err != nil {
		t.Fatal(err)
	}
	err = out.check()
	if err != nil {
		t.Fatal(err)
	}
	if filled != 1 {
		t.Errorf("expected 1 attempted fill; got %d", filled)
	}
	if f := cache.Failures(); f != 1 {
		t.Errorf("expected 1 cache failure; got %d", f)
	}
}

type multiOutput struct {
	possible []*testSegOutput
	endsegs  int32
}

func (mo *multiOutput) Open() (io.WriteCloser, error) {
	return mo, nil
}

func (mo *multiOutput) Close() error {
	return nil
}

func (mo *multiOutput) Write(p []byte) (int, error) {
	name := hashname(p)
	for i := range mo.possible {
		if _, ok := mo.possible[i].from.seghashes[name]; ok {
			return mo.possible[i].Write(p)
		}
	}
	return 0, fmt.Errorf("segment %s not recognized in any input", name)
}

func (mo *multiOutput) EndSegment() {
	atomic.AddInt32(&mo.endsegs, 1)
}

func (mo *multiOutput) check() error {
	for i := range mo.possible {
		mo.possible[i].endsegs = mo.endsegs / int32(len(mo.possible))
		if err := mo.possible[i].check(); err != nil {
			return err
		}
	}
	return nil
}

func TestMulti(t *testing.T) {
	for i, c := range []struct {
		flags        Flag
		hits, misses int64
	}{{
		hits:   4,
		misses: 4,
	}, {
		flags:  FlagNoFill,
		hits:   0,
		misses: 4,
	}} {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			testMulti(t, c.flags, c.hits, c.misses)
		})
	}
}

func testMulti(t *testing.T, flags Flag, hits, misses int64) {
	t.Helper()
	testFiles(t)
	seg0 := randseg(1000, 2, 3500)
	seg1 := randseg(1352, 3, 15872)
	seg2 := randseg(1352, 3, 15872)
	seg3 := randseg(1400, 3, 20000)
	mo := &multiOutput{possible: []*testSegOutput{
		seg0.testout(), seg1.testout(), seg2.testout(), seg3.testout(),
	}}
	c := New(t.TempDir(), func() {})
	defer c.Close()
	tbl := c.MultiTable(context.Background(), []Segment{seg0, seg1, seg2, seg3}, flags)
	err := tbl.WriteChunks(mo, 8)
	if err != nil {
		t.Fatal(err)
	}
	err = mo.check()
	if err != nil {
		t.Fatal(err)
	}
	if m := c.Misses(); m != misses {
		t.Errorf("got %d misses; expected %d", m, misses)
	}
	if tbl.Misses() != misses {
		t.Errorf("table has %d misses; cache has %d?", tbl.Misses(), c.Misses())
	}
	want := int64(0)
	for i := range mo.possible {
		want += mo.possible[i].raw
	}

	// now check what ought to be a cache hit
	mo = &multiOutput{possible: []*testSegOutput{
		seg0.testout(), seg1.testout(), seg2.testout(), seg3.testout(),
	}}
	err = tbl.WriteChunks(mo, 8)
	if err != nil {
		t.Fatal(err)
	}
	err = mo.check()
	if err != nil {
		t.Fatal(err)
	}
	if h := c.Hits(); h != hits {
		t.Errorf("got %d hits; expected %d", h, hits)
	}
	if tbl.Hits() != hits {
		t.Errorf("table has %d hits; cache has %d?", tbl.Hits(), c.Hits())
	}
	for i := range mo.possible {
		want += mo.possible[i].raw
	}
}
