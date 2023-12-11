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

package dcache

import (
	"io"
	"sync"

	"github.com/SnellerInc/sneller/vm"
)

type reservation struct {
	seg     Segment
	etag    string
	out     *vm.TeeWriter
	primary *Stats

	// guarded by queue.lock
	// until the reservation has
	// been deleted from queue.reserved
	flags Flag
}

type queue struct {
	lock     sync.Mutex
	reserved map[string]*reservation
	out      chan *reservation
	bgfill   chan struct{}
}

func (q *queue) endBackground() {
	q.bgfill <- struct{}{}
}

func (q *queue) tryBackground() bool {
	select {
	case <-q.bgfill:
		return true
	default:
		return false
	}
}

func (q *queue) send(seg Segment, dst io.Writer, flags Flag, stats *Stats, ret chan<- error) {
	etag := seg.ETag()
	done := func(pos int64, e error) {
		stats.addBytes(pos)
		select {
		case ret <- e:
			// ok
		default:
			// we always set cap(ret) == 1, and there should
			// only ever be 1 segment outstanding
			panic("queue response channel should never block")
		}
	}
	q.lock.Lock()
	// TODO: if len(q.reserved) is too large,
	// reject the query here
	if res, ok := q.reserved[etag]; ok {
		res.seg.Merge(seg)
		res.out.Add(dst, done)
		// treat this access as a hit, since it
		// is coalesced with a miss
		stats.hit()
		q.lock.Unlock()
		return
	}
	res := &reservation{
		seg:     seg,
		etag:    etag,
		out:     vm.NewTeeWriter(dst, done),
		primary: stats,
		flags:   flags,
	}
	q.reserved[etag] = res
	q.lock.Unlock()
	q.out <- res
}

// implements blockfmt.ZionWriter
func (r *reservation) ConfigureZion(blocksize int64, fields []string) bool {
	return r.out.ConfigureZion(blocksize, fields)
}

func (r *reservation) Write(p []byte) (int, error) {
	return r.out.Write(p)
}

func (r *reservation) close(err error) {
	if r.out == nil {
		return
	}
	if err == nil {
		r.out.Close()
	} else {
		r.out.CloseError(err)
	}
	r.out = nil
}

func (r *reservation) hit() {
	r.primary.hit()
}

func (r *reservation) miss() {
	r.primary.miss()
}

// Close closes the cache.
// Further use of the cache after
// a call to Close will cause panics.
func (c *Cache) Close() {
	close(c.queue.out)
	c.wg.Wait()
}

func (c *Cache) asyncReadThrough(res *reservation, mp *mapping) bool {
	if !c.queue.tryBackground() {
		return false
	}
	go func() {
		defer c.queue.endBackground()
		pop, err := readThrough(res.seg, mp, res)
		if mp != nil {
			c.finalize(mp, pop)
			c.unmap(mp)
		}
		res.close(err)
	}()
	return true
}

func (c *Cache) worker() {
	defer c.wg.Done()
	q := &c.queue
outer:
	for res := range q.out {
		mp := c.mmap(res.seg, res.flags)

		// remove from reserved map
		// so that res.out is safe to access
		q.lock.Lock()
		delete(q.reserved, res.etag)
		q.lock.Unlock()

		var err error
		pop := false
		if mp != nil && mp.populated {
			res.hit()
			err = res.seg.Decode(res, mp.mem)
			c.unmap(mp)
		} else {
			res.miss()
			if c.asyncReadThrough(res, mp) {
				// res.close() will be called elsewhere
				continue outer
			}
			pop, err = readThrough(res.seg, mp, res)
			if mp != nil {
				c.finalize(mp, pop)
				c.unmap(mp)
			}
		}
		res.close(err)
	}
}
