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

type reservation struct {
	seg     Segment
	etag    string
	primary output

	// guarded by queue.lock
	// until the reservation has
	// been deleted from queue.reserved
	flags Flag
	aux   []output
}

type output struct {
	dst   io.Writer
	ret   chan<- error
	stats *Stats
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
		// TODO: how should flags
		// affect res.flags?
		res.aux = append(res.aux, output{
			dst:   dst,
			ret:   ret,
			stats: stats,
		})
		q.lock.Unlock()
		return
	}
	res := &reservation{
		seg:     seg,
		etag:    etag,
		primary: output{dst: dst, ret: ret, stats: stats},
		flags:   flags,
	}
	q.reserved[etag] = res
	q.lock.Unlock()
	q.out <- res
}

func (o *output) write(p []byte) bool {
	if o.dst == nil {
		return true
	}
	_, err := o.dst.Write(p)
	if err != nil {
		o.ret <- err
		o.dst = nil
		o.ret = nil
		return true
	}
	return false
}

func (r *reservation) Write(p []byte) (int, error) {
	done := r.primary.write(p)
	for i := range r.aux {
		done = r.aux[i].write(p) && done
	}
	if done {
		return len(p), io.EOF
	}
	return len(p), nil
}

func (r *reservation) close(err error) {
	if r.primary.ret != nil {
		vm.HintEndSegment(r.primary.dst)
		r.primary.ret <- err
		r.primary.ret = nil
	}
	for i := range r.aux {
		if r.aux[i].ret != nil {
			vm.HintEndSegment(r.aux[i].dst)
			r.aux[i].ret <- err
			r.aux[i].ret = nil
		}
	}
}

func (r *reservation) hit() {
	r.primary.stats.hit()
	for i := range r.aux {
		r.aux[i].stats.hit()
	}
}

func (r *reservation) miss() {
	r.primary.stats.miss()
	for i := range r.aux {
		r.aux[i].stats.miss()
	}
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
