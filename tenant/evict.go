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

package tenant

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/heap"
)

// tenant cache eviction implementation
//
// The basic strategy here is to walk
// Manager.CacheDir an remove the least-recently-used
// files in that hierarchy.
// Since walking the whole hierarchy to
// find the oldest file could be expensive,
// we cache some of the results from each directory
// walk so that we have a list of candidate files
// to evict when a tenant indicates it would like
// to fill a cache entry.
// The way this is currently implemented is we
// walk the hierarchy and insert each file atime
// into a max heap, taking care to remove the
// most-recently-used file so that the size
// of the heap is fixed to some upper limit.
// Once we have completed the scan, we have
// collected a list of up to max-heap-size
// of the least-recently-used items which
// can be evicted.
// When a tenant indicates that it is performing
// a cache fill, we determine if there is enough
// slack space left in the cache, and if there
// isn't, then we evict files from the candidate
// list produced earlier until there is enough
// slack space left. (We check that the atime of
// the file has not changed since we performed the scan.)
// If we exhaust the candidate list before the slack
// space has been reclaimed, we re-scan to populate
// the candidate list and resume evictions.
// Since the atime cannot get *smaller* over time,
// the list of candidates that we keep in memory
// is the *best* list of candidates at any point in
// time as long as we refuse to consider candidates
// that have had their atimes jump forward.
// In other words, the behavior with the candidate heap
// is still "perfectly LRU" behavior.

// these functions are overridden for testing
var (
	usage func(dir string) (int64, int64)
	atime func(f fs.FileInfo) int64
)

type fprio struct {
	path  string // file path
	atime int64  // file atime
	size  int64  // file size
	score int64  // file "score" for eviction priority
}

type fileHeap struct {
	items []fprio // unordered items
}

func (f *fileHeap) reset() {
	f.items = f.items[:0]
}

func (f *fileHeap) size() int64 {
	sz := int64(0)
	for i := range f.items {
		sz += f.items[i].size
	}
	return sz
}

func (f fprio) orderMRU(other fprio) bool {
	return f.atime > other.atime
}

func (f fprio) orderScore(other fprio) bool {
	if f.score == other.score {
		return f.atime < other.atime
	}
	return f.score > other.score
}

func (f *fileHeap) trimMRU(max int) {
	for len(f.items) > max {
		heap.PopSlice(&f.items, (fprio).orderMRU)
	}
}

func (f *fileHeap) shouldAddMRU(atime int64, maxbuffered int) bool {
	return len(f.items) < maxbuffered || atime < f.items[0].atime
}

func (f *fileHeap) addMRU(path string, atime, size int64) {
	heap.PushSlice(&f.items, fprio{
		path:  path,
		atime: atime,
		size:  size,
	}, (fprio).orderMRU)
}

func (f *fileHeap) popMRU() fprio {
	return heap.PopSlice(&f.items, (fprio).orderMRU)
}

func (f *fileHeap) count() int {
	return len(f.items)
}

func (f *fileHeap) addScore(item fprio) {
	heap.PushSlice(&f.items, item, (fprio).orderScore)
}

func (f *fileHeap) popScore() fprio {
	return heap.PopSlice(&f.items, (fprio).orderScore)
}

type totalHeap struct {
	buffered  fileHeap
	sorted    []fprio          // sorted from buffered
	maxbuffer int              // maximum # of items to buffer
	minatime  int64            // minimum atime; delete everything older than this
	keepeph   time.Duration    // maximum duration for keeping "ephemeral" files
	now       func() time.Time // virtual clock (usually time.Now)

	// summary stats:
	runs     int64 // number of evict runs
	files    int64 // number of files evicted
	bytes    int64 // number of bytes evicted
	maxatime int64 // most-recently-used file removed
}

func (m *Manager) evict(t *totalHeap, size int64) {
	for size > 0 {
		if len(t.sorted) == 0 {
			m.fill(t, size)
			if len(t.sorted) == 0 {
				return // nothing to evict
			}
		}
	inner:
		for i := range t.sorted {
			f := &t.sorted[i]
			info, err := os.Stat(f.path)
			if err != nil || info.Size() != f.size || atime(info) != f.atime {
				// disregard stale objects
				continue inner
			}
			if os.Remove(f.path) == nil {
				t.files++         // track files evicted
				t.bytes += f.size // track bytes evicted
				size -= f.size
				if f.atime > t.maxatime {
					t.maxatime = f.atime
				}
				if size <= 0 {
					t.sorted = t.sorted[:copy(t.sorted, t.sorted[i+1:])]
					return
				}
			}
		}
		t.sorted = t.sorted[:0]
	}
}

// walk the tree and put eviction candidates
// in t.sorted in decreasing order of eviction quality
func (m *Manager) fill(t *totalHeap, wantsize int64) {
	t.buffered.reset()
	toplvl, err := os.ReadDir(m.CacheDir)
	if err != nil {
		m.errorf("cache eviction walk: %s", err)
		return
	}
	var local fileHeap
	var cursize int64
	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			// don't care about directories,
			// links, etc.
			return nil
		}
		info, err := d.Info()
		if err != nil {
			// we are racing with something else
			// that is removing the file
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		at := atime(info)
		// too old? remove
		if at < t.minatime {
			os.Remove(path)
			return nil
		}
		// ephemeral and too old? remove
		if strings.HasPrefix(d.Name(), "eph:") && time.Duration(m.eheap.now().UnixNano()-at) > t.keepeph {
			os.Remove(path)
			return nil
		}
		size := info.Size()
		cursize += size
		if local.shouldAddMRU(at, t.maxbuffer) {
			local.addMRU(path, at, size)
			local.trimMRU(t.maxbuffer)
		}
		return nil
	}
	for i := range toplvl {
		if !toplvl[i].IsDir() {
			continue
		}
		local.reset()
		cursize = 0
		err := filepath.WalkDir(filepath.Join(m.CacheDir, toplvl[i].Name()), walk)
		if err != nil {
			m.errorf("cache eviction walk: %s", err)
			return
		}
		// score the items as we insert
		// them into the global heap by computing
		// the number of bytes remaining in this
		// tenant's directory at the point that
		// it would be deleted (in atime-sorted order)
		bufsize := local.size()
		for local.count() > 0 {
			f := local.popMRU()
			bufsize -= f.size           // # bytes deleted up to this point
			f.score = cursize - bufsize // score is tenant's total - deleted
			t.buffered.addScore(f)
		}
	}
	// heap-sort the results into t.sorted,
	// taking care to produce either the desired
	// buffer size and also the desired number of
	// candidate bytes to be evicted (whichever is greater)
	t.sorted = t.sorted[:0]
	sortsize := int64(0)
	for t.buffered.count() > 0 &&
		(len(t.sorted) < t.maxbuffer || sortsize < wantsize) {
		next := t.buffered.popScore()
		sortsize += next.size
		t.sorted = append(t.sorted, next)
	}
	t.buffered.reset()
}

func (m *Manager) cacheEvict() {
	// by default, cache up to 50 items to evict
	if m.eheap.maxbuffer == 0 {
		m.eheap.maxbuffer = 50
	}
	// by default, use time.Now for time
	if m.eheap.now == nil {
		m.eheap.now = time.Now
	}
	// by default, keep ephemeral files for 6 seconds
	if m.eheap.keepeph == 0 {
		m.eheap.keepeph = 6 * time.Second
	}
	// target usage of 90% of the disk blocks;
	// this gives us a little headroom for polling delay
	used, avail := usage(m.CacheDir)
	target := (9 * avail) / 10
	if used < target {
		return
	}
	// set the minimum atime to within the last hour
	m.eheap.minatime = m.eheap.now().Add(-time.Hour).UnixNano()
	m.eheap.runs++
	m.evict(&m.eheap, used-target)
	if m.logger != nil && m.eheap.files > 0 &&
		time.Since(m.lastSummary) >= time.Minute {
		// log summary and reset
		sec := m.eheap.maxatime / 1e9
		nsec := m.eheap.maxatime % 1e9
		m.logger.Printf("evict stats: %d runs, %d files, %d bytes, min age %s",
			m.eheap.runs, m.eheap.files, m.eheap.bytes, time.Since(time.Unix(sec, nsec)))
		m.lastSummary = m.eheap.now()
		m.eheap.runs = 0
		m.eheap.files = 0
		m.eheap.bytes = 0
		m.eheap.maxatime = 0
	}
}
