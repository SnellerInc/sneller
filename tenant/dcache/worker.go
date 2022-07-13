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

package dcache

import (
	"io"
	"sync"

	"github.com/SnellerInc/sneller/vm"
)

func (r *reservation) add(w io.Writer, ret chan<- error, stats *Stats) {
	done := func(pos int64, e error) {
		stats.addBytes(pos)
		ret <- e
	}
	if r.out == nil {
		r.out = vm.NewTeeWriter(w, done)
		return
	}
	r.out.Add(w, done)
}

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
	q.lock.Lock()
	// TODO: if len(q.reserved) is too large,
	// reject the query here
	if res, ok := q.reserved[etag]; ok {
		res.add(dst, ret, stats)
		// treat this access as a hit, since it
		// is coalesced with a miss
		stats.hit()
		q.lock.Unlock()
		return
	}
	res := &reservation{
		seg:  seg,
		etag: etag,
		out: vm.NewTeeWriter(dst, func(pos int64, e error) {
			stats.addBytes(pos)
			ret <- e
		}),
		primary: stats,
		flags:   flags,
	}
	q.reserved[etag] = res
	q.lock.Unlock()
	q.out <- res
}

func (r *reservation) Write(p []byte) (int, error) {
	return r.out.Write(p)
}

func (r *reservation) close(err error) {
	if err == nil {
		r.out.Close()
	} else {
		r.out.CloseError(err)
	}
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
		// so that res.aux is safe to access
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
