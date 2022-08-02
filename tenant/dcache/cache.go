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

// Package dcache provides a cache
// for table data by storing files in a directory.
//
// Typically, a caller will arrange
// for a new cache to be set up in
// a directory with NewCache(dir),
// and then use Cache.Table as the
// vm.Table implementation to be
// returned to the query planner.
package dcache

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/SnellerInc/sneller/vm"
)

// Flag is an option flag which may be passed to
// methods in this package.
type Flag int

const (
	// FlagNoFill instructs the cache not to create
	// a new entry for missing segments.
	FlagNoFill Flag = 1 << iota
)

// Cache caches Segment data
// and provides a vm.Table implementation
// for a cache entry backed by a Segment.
// See: Segment, Cache.Table.
type Cache struct {
	// Logger, if non-nil, is used
	// to log errors encountered
	// by the cache.
	Logger Logger

	dir    string
	onFill func()

	// we don't allow concurrent cache fills;
	// a span can only be populated by one caller
	lock     sync.Mutex
	cond     sync.Cond
	inflight map[string]struct{}

	queue queue
	wg    sync.WaitGroup // waiting for c.worker()

	// we do, however, allow concurrent reads
	// from the same mapping; we keep mappings
	// open as long as there is at least one
	// active user; otherwise we remove them
	rocache map[string]*mapping

	// statistics; accessed atomically
	hits, misses, failures int64
}

type Logger interface {
	Printf(f string, args ...interface{})
}

func (c *Cache) errorf(f string, args ...interface{}) {
	if c.Logger != nil {
		c.Logger.Printf(f, args...)
	}
}

// LiveHits returns the number of mappings
// open for reading at the moment it is called.
// (Note that this is fundamentally racy; this
// is only here for telemetry and testing purposes.)
func (c *Cache) LiveHits() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.rocache)
}

// Accesses returns the total number
// of times the cache was accessed.
func (c *Cache) Accesses() int64 {
	return c.Hits() + c.Misses() + c.Failures()
}

// Hits returns the number of times
// the cache successfully substitued
// a request for Segment data with
// locally cached data.
func (c *Cache) Hits() int64 {
	return atomic.LoadInt64(&c.hits)
}

// Misses returns the number of times
// the cache failed to substitute Segment
// data with locally-cached data.
func (c *Cache) Misses() int64 {
	return atomic.LoadInt64(&c.misses)
}

// Failures returns the number of times
// the cache attempted to create a new
// entry for a Segment but failed to allocate
// the appropriate disk/memory backing due to
// resource exhaustion.
func (c *Cache) Failures() int64 {
	return atomic.LoadInt64(&c.failures)
}

type mapping struct {
	file       *os.File // file handle
	id, target string   // actual filepath of populated entry
	mem        []byte   // actual mapping
	populated  bool     // memory is populated

	// reference count; can only be accessed
	// when the parent cache lock is locked
	refcount int
}

// New makes a new cache that keeps
// cache data in files inside 'dir'.
// The provided onFill function will be
// called each time the cache is about
// to fill a new cache entry.
func New(dir string, onFill func()) *Cache {
	c := &Cache{
		dir:      dir,
		onFill:   onFill,
		inflight: make(map[string]struct{}),
		rocache:  make(map[string]*mapping),
	}
	c.queue.reserved = make(map[string]*reservation)
	parallel := runtime.GOMAXPROCS(0)
	extrafill := (parallel + 1) / 2
	c.queue.bgfill = make(chan struct{}, extrafill)
	for i := 0; i < extrafill; i++ {
		c.queue.bgfill <- struct{}{}
	}
	c.queue.out = make(chan *reservation, parallel)
	c.wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go c.worker()
	}
	c.cond.L = &c.lock
	return c
}

// acquire id exclusively;
// if a read-only mapping is already present
// for that ID, then return that mapping and
// unlock the entry immediately
func (c *Cache) lockID(id string) *mapping {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, ok := c.inflight[id]; ok; _, ok = c.inflight[id] {
		c.cond.Wait()
	}
	if mp := c.rocache[id]; mp != nil {
		mp.refcount++
		return mp
	}
	c.inflight[id] = struct{}{}
	return nil
}

// drop exclusive lock on id
func (c *Cache) unlockID(id string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	before := len(c.inflight)
	delete(c.inflight, id)
	if after := len(c.inflight); after != before-1 {
		panic("double unlock of id " + id)
	}
	c.cond.Broadcast()
}

// drop exclusive lock on id and set
// its read-only mapping to mp
func (c *Cache) unlockIDMapped(id string, mp *mapping) {
	if !mp.populated {
		panic("unlockIDMapped on non-populated mapping")
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	before := len(c.inflight)
	delete(c.inflight, id)
	after := len(c.inflight)
	if after != before-1 {
		panic("duplicate unlockID in unlockIDMapped " + id)
	}
	c.rocache[id] = mp
	c.cond.Broadcast()
}

func mkdir(name string, mode os.FileMode) bool {
	err := os.Mkdir(name, mode)
	return err == nil || errors.Is(err, fs.ErrExist)
}

// how this is *currently* implemented:
// we create a file "ID.tmp" that holds the
// cache data while the entry is being populated,
// and then we re-name it to just "ID" if we
// successfully populate the entire entry
// (this is all-or-nothing)
//
// we use Cache.lockID()/Cache.unlockID()
// to prevent multiple cache *fills* simultaneously
// (so, when we are populating a cache entry for
// a particular Segment, any other accesses of that
// Segment will block until we have populated the entire entry
// or otherwise aborted the query)
func (c *Cache) mmap(s Segment, flags Flag) *mapping {
	id := s.ETag()
	var target string
	var predir string
	if len(id) >= 2 {
		// add 1 level of indirection so that a subsequent
		// readdir opertion need not lock the entire directory
		predir = filepath.Join(c.dir, id[:1])
		target = filepath.Join(predir, id[1:])
	} else {
		target = filepath.Join(c.dir, id)
	}
	if m := c.lockID(id); m != nil {
		atomic.AddInt64(&c.hits, 1)
		return m
	}
	f, err := os.Open(target)
	if err == nil {
		fi, err := f.Stat()
		if err != nil {
			c.unlockID(id)
			f.Close()
			c.errorf("Cache.mmap: stat: %s", err)
			atomic.AddInt64(&c.failures, 1)
			return nil
		}
		buf, err := mmap(f, fi.Size(), true)
		if err != nil {
			f.Close()
			// should we os.Remove() here too?
			c.unlockID(id)
			c.errorf("Cache.mmap: mmap: %s", err)
			atomic.AddInt64(&c.failures, 1)
			return nil
		}
		atomic.AddInt64(&c.hits, 1)
		mp := &mapping{
			file:   f,
			id:     id,
			target: target,
			// have the slice arrange so that
			// cap(mem) = filesystem size,
			// len(mem) = range of actual data
			mem:       buf[:s.Size()],
			populated: true,
			refcount:  1,
		}
		c.unlockIDMapped(id, mp)
		return mp
	}
	if flags&FlagNoFill != 0 {
		atomic.AddInt64(&c.misses, 1)
		c.unlockID(id)
		return nil
	}
	c.onFill()
	// we are creating a new entry
	f, err = os.Create(target + ".tmp")
	if errors.Is(err, fs.ErrNotExist) &&
		predir != "" && mkdir(predir, 0750) {
		// we don't insert the mkdir in this path
		// ordinarily because this isn't something
		// we ever deliberately delete:
		f, err = os.Create(target + ".tmp")
	}
	if err != nil {
		// couldn't even create the file
		c.unlockID(id)
		c.errorf("Cache.mmap: couldn't create temporary backing: %s", err)
		atomic.AddInt64(&c.failures, 1)
		return nil
	}
	size := s.Size()
	err = resize(f, size+slack)
	if err != nil {
		// out of memory or disk space;
		// don't cache at all and make sure
		// the file doesn't stick around
		f.Close()
		os.Remove(f.Name())
		c.unlockID(id)
		atomic.AddInt64(&c.failures, 1)
		c.errorf("Cache.mmap: fallocate: %s", err)
		return nil
	}
	buf, err := mmap(f, size+slack, false)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		c.unlockID(id)
		atomic.AddInt64(&c.failures, 1)
		c.errorf("Cache.mmap: mapping new entry: %s", err)
		return nil
	}
	atomic.AddInt64(&c.misses, 1)
	return &mapping{
		file: f,
		id:   id,
		// cap(mem) = fallocated space,
		// len(mem) = size of data
		mem:       buf[:s.Size()],
		target:    target,
		populated: false,
		refcount:  1,
	}
}

// take a mapping that was not populated
// and relink it so that it is a populated mapping
func (c *Cache) finalize(mp *mapping, pop bool) {
	if mp.populated {
		panic("finalize of populated mapping")
	}
	name := mp.file.Name()
	if pop {
		// unpopulated -> populated means
		// renaming id.tmp -> id so that
		// it can be acquired directly from the filesystem
		if err := os.Rename(name, mp.target); err != nil {
			c.errorf("Cache.finalize: %s", err)
		}
	} else {
		if err := os.Remove(name); err != nil {
			c.errorf("Cache.finalize: deleting failed fill: %s", err)
		}
	}
	c.unlockID(mp.id)
}

func (c *Cache) unmap(mp *mapping) {
	if mp.mem == nil {
		return
	}
	if mp.populated {
		// this is a read-only cache entry;
		// have to decref and see if we can
		// really close the entry
		c.lock.Lock()
		rmp := c.rocache[mp.id]
		if rmp != mp {
			panic("cache entry changed?")
		}
		mp.refcount--
		dead := false
		if mp.refcount == 0 {
			dead = true
			delete(c.rocache, mp.id)
		}
		c.lock.Unlock()
		if !dead {
			return
		}
	}
	// we're going to panic here if unmap fails
	// because letting it simply fail would leak
	// mappings endlessly into our address space;
	// if we encounter this we've got a terrible bug
	if err := unmap(mp.file, mp.mem[:cap(mp.mem)]); err != nil {
		panic("dcache.Cache.unmap: " + err.Error())
	}
	if err := mp.file.Close(); err != nil {
		c.errorf("closing %s: %s", mp.file.Name(), err)
	}
	mp.file = nil
	mp.mem = nil
}

// Segment describes a particular region
// of data to be cached.
type Segment interface {
	// Merge is called when segments are coalesced
	// due to being accessed simultaneously, and
	// will only be called when the other segment
	// has an ETag that matches the target segment.
	Merge(other Segment)
	// ETag should return a unique identifier
	// associated with the data backing this segment.
	// Any unique identifier may be used, as long as
	// it does not contain '/' characters.
	ETag() string
	// Size should return the size of the segment.
	Size() int64
	// Open should open an io.ReadCloser
	// that reads the contents of the segment.
	// The return reader will be expected to
	// read at least Size bytes successfully.
	// Implementations are encouraged to return
	// an io.ReadCloser that also implements io.WriterTo.
	Open() (io.ReadCloser, error)
	// Decode should copy data from src
	// into dst. src is guaranteed to be
	// a slice with length equal to the
	// return value of Size and contents
	// equal to what a previous call to
	// Open() ended up returning
	Decode(dst io.Writer, src []byte) error
}

// Table is an implementation of vm.Table
// that wraps a Segment and attempts to provide
// cached data in place of data read from the Segment.
type Table struct {
	Stats
	cache *Cache
	seg   Segment
	flags Flag
}

// Stats is the a collection of
// statistics about a Table or MultiTable.
type Stats struct {
	hits, misses, bytes int64
}

// Reset zeros all of the stats fields.
//
// Note: Reset is not safe to call concurrently
// with other Stats methods.
// When embedded into other structures like
// Table or MultiTable, Reset is not safe to
// call simultaneously with other methods of
// the enclosing structure that may update
// Stats' fields.
func (s *Stats) Reset() {
	*s = Stats{}
}

func (s *Stats) hit() {
	atomic.AddInt64(&s.hits, 1)
}

func (s *Stats) miss() {
	atomic.AddInt64(&s.misses, 1)
}

func (s *Stats) addBytes(n int64) {
	atomic.AddInt64(&s.bytes, n)
}

// Bytes returns the number of bytes sent
// to a table. In the context of an individual
// Table, this is a running total of the number
// of bytes passed through WriteChunks.
func (s *Stats) Bytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

// Hits returns the accumulated total
// of the number of cache hits.
//
// A cache hit is defined as a cache
// access that does not result in a cache fill.
func (s *Stats) Hits() int64 { return atomic.LoadInt64(&s.hits) }

// Misses returns the accumulated total
// of the number of cache misses.
//
// A cache miss is any cache access
// that is not a cache hit.
// Cache fills and uncached read-through
// are both considered misses.
func (s *Stats) Misses() int64 { return atomic.LoadInt64(&s.misses) }

// Table returns a Table associated with
// the given segment. The returned Table
// implements vm.Table.
func (c *Cache) Table(s Segment, f Flag) *Table {
	return &Table{
		cache: c,
		seg:   s,
		flags: f,
	}
}

func (t *Table) write(w io.Writer) error {
	ret := make(chan error, 1)
	t.cache.queue.send(t.seg, w, t.flags, &t.Stats, ret)
	return <-ret
}

// slow-path: read data from the segment into the cache
// and write it out to the destination at the same time
func readThrough(seg Segment, mp *mapping, w io.Writer) (bool, error) {
	rd, err := seg.Open()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return false, fmt.Errorf("cache segment for read-through: %w", err)
	}
	defer rd.Close()
	var buf []byte
	if mp != nil {
		buf = mp.mem
	} else {
		if wt, ok := rd.(io.WriterTo); ok {
			_, err := wt.WriteTo(w)
			return false, err
		}
		size := seg.Size()
		// no backing; just use a regular buffer
		buf = make([]byte, size, size+16)
	}
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return false, err
	}
	return mp != nil, seg.Decode(w, buf)
}

// Chunks implements vm.Table.Chunks
func (t *Table) Chunks() int { return -1 }

// WriteChunks implements vm.Table.WriteChunks
//
// NOTE: the WriteChunks method is not safe to call
// from multiple goroutines simultaneously.
// However, it is safe to call WriteChunks more than once.
// Each call to WriteChunks accesses the cache a separate time,
// so it is safe to re-use a Table as long as it is accessed
// from a single goroutine at a time.
func (t *Table) WriteChunks(dst vm.QuerySink, parallel int) error {
	return vm.SplitInput(dst, 1, t.write)
}
