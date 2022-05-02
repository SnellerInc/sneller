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

package tenant

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/exp/slices"
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
	path  string
	atime int64
	size  int64
}

type evictHeap struct {
	lst    []fprio
	sorted []fprio
}

// sort the final heap results by
// *least recently accessed time*
func (e *evictHeap) sort() {
	e.sorted = slices.Grow(e.sorted, len(e.lst))[:len(e.lst)]
	// reverse the output of calling pop() repeatedly
	for i := len(e.sorted) - 1; i >= 0; i-- {
		path, atime, size := e.pop()
		e.sorted[i] = fprio{path: path, atime: atime, size: size}
	}
}

func (e *evictHeap) push(path string, atime int64, size int64) {
	e.lst = append(e.lst, fprio{
		path:  path,
		atime: atime,
		size:  size,
	})
	e.siftUp(len(e.lst) - 1)
}

func (e *evictHeap) siftUp(at int) {
	f := e.lst[at]
	t := f.atime
	for at > 0 {
		p := (at - 1) / 2
		if e.lst[p].atime >= t {
			break
		}
		e.lst[at] = e.lst[p]
		at = p
	}
	e.lst[at] = f
}

func (e *evictHeap) siftDown(at int) {
	f := e.lst[at]
	t := f.atime
	for {
		left := (at * 2) + 1
		right := left + 1
		if left >= len(e.lst) {
			break
		}
		c := left
		if len(e.lst) > right && e.lst[right].atime > e.lst[left].atime {
			c = right
		}
		if t > e.lst[c].atime {
			break
		}
		e.lst[at] = e.lst[c]
		at = c
	}
	e.lst[at] = f
}

// shrink the heap while it is larger than max elements
func (e *evictHeap) shrink(max int) {
	for len(e.lst) > max {
		e.lst[0] = e.lst[len(e.lst)-1]
		e.lst = e.lst[:len(e.lst)-1]
		e.siftDown(0)
	}
}

// pop the *most recently accessed* file from
// the eviction heap
func (e *evictHeap) pop() (string, int64, int64) {
	f := e.lst[0]
	e.lst[0], e.lst = e.lst[len(e.lst)-1], e.lst[:len(e.lst)-1]
	if len(e.lst) > 1 {
		e.siftDown(0)
	}
	return f.path, f.atime, f.size
}

func (m *Manager) evict(e *evictHeap, size int64) {
	for {
		if len(e.sorted) == 0 {
			m.fill(e)
			e.sort()
			if len(e.sorted) == 0 {
				// nothing to evict...?
				return
			}
		}
		for i := range e.sorted {
			f := &e.sorted[i]
			fi, err := os.Stat(f.path)
			if err != nil || fi.Size() != f.size || atime(fi) != f.atime {
				continue
			}
			if os.Remove(f.path) == nil {
				size -= f.size
				if size <= 0 {
					// copy remaining entries to the front of the cache
					// so that we begin where we left off on the
					// next call to evict()
					e.sorted = e.sorted[:copy(e.sorted, e.sorted[i+1:])]
					return
				}
			}
		}
		// if we iterated the whole list,
		// anything that is left here must
		// be stale, so we should re-fill
		e.sorted = e.sorted[:0]
	}
}

func (m *Manager) fill(e *evictHeap) {
	// we limit the number of files considered
	// for eviction so that the number of files
	// present in the cache directory does not
	// affect the amount of memory we need to
	// consume in order to select good candidates
	const maxbuffered = 25
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
		e.push(path, atime(info), info.Size())
		e.shrink(maxbuffered)
		return nil
	}
	err := filepath.WalkDir(m.CacheDir, walk)
	if err != nil {
		m.errorf("cache eviction walk: %s", err)
		return
	}
}

func (m *Manager) cacheEvict() {
	// target usage of 90% of the disk blocks;
	// this gives us a little headroom for polling delay
	used, avail := usage(m.CacheDir)
	target := (9 * avail) / 10
	if used < target {
		return
	}
	m.evict(&m.eheap, used-target)
}
