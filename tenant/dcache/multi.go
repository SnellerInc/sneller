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
	"context"
	"io"
	"sync/atomic"

	"github.com/SnellerInc/sneller/vm"
)

// MultiTable is a Table comprised of multiple Segments.
type MultiTable struct {
	Stats
	inner []*Table

	// NOTE: we don't actually look for
	// cancellation inside Segment.Decode, etc.
	// because of coalescing; we don't want a cancellation
	// to cause an unrelated query that happens to touch
	// the same data to be canceled.
	ctx   context.Context
	donec <-chan struct{}
	next  int32
}

// MultiTable constructs a MultiTable from a list of segments.
// The provided context will be used to exit WriteChunks early
// if the context is canceled between chunk fetches.
func (c *Cache) MultiTable(ctx context.Context, segs []Segment, flags Flag) *MultiTable {
	inner := make([]*Table, len(segs))
	for i := range segs {
		inner[i] = c.Table(segs[i], flags)
	}
	return &MultiTable{inner: inner, ctx: ctx, donec: ctx.Done()}
}

// acquire a reference to one of the input tables
func (m *MultiTable) get() *Table {
	n := atomic.AddInt32(&m.next, 1) - 1
	if int(n) >= len(m.inner) {
		return nil
	}
	// don't continue if we are canceled:
	if m.donec != nil {
		select {
		case <-m.donec:
			return nil
		default:
		}
	}
	t := m.inner[n]
	return t
}

func (m *MultiTable) write(w io.Writer) error {
	var ret chan error
	for {
		t := m.get()
		if t == nil {
			break
		}
		if ret == nil {
			ret = make(chan error, 1)
		}
		t.cache.queue.send(t.seg, w, t.flags, &m.Stats, ret)
		err := <-ret
		if err != nil {
			return err
		}
	}
	if ret != nil {
		close(ret) // force panic if there's a double-send
	}
	return nil
}

func (m *MultiTable) open(parallel int) int {
	// nothing really to do here, as we
	// open inner tables lazily, but let's
	// make sure that the previous close()
	// wasn't missed somehow...
	if m.next != 0 {
		panic("race on MultiTable open/close")
	}
	if parallel > len(m.inner) {
		parallel = len(m.inner)
	}
	return parallel
}

// WriteChunks implements vm.Table.WriteChunks
func (m *MultiTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	err := vm.SplitInput(dst, m.open(parallel), m.write)
	m.next = 0
	if err != nil {
		return err
	}
	return m.ctx.Err()
}
