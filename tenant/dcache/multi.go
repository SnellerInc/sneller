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
	"sync/atomic"

	"github.com/SnellerInc/sneller/vm"
)

// MultiTable is a Table comprised of multiple Segments.
type MultiTable struct {
	Stats
	inner []*Table
	next  int32
}

// MultiTable constructs a MultiTable from a list of segments.
func (c *Cache) MultiTable(segs []Segment, flags Flag) *MultiTable {
	inner := make([]*Table, len(segs))
	for i := range segs {
		inner[i] = c.Table(segs[i], flags)
	}
	return &MultiTable{inner: inner}
}

// acquire a reference to one of the input tables
//
// the table at m.inner[i] is guaranteed to
// be open once this call returns
func (m *MultiTable) get() *Table {
	n := atomic.AddInt32(&m.next, 1) - 1
	if int(n) >= len(m.inner) {
		return nil
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

// Chunks implements vm.Table.Chunks
func (m *MultiTable) Chunks() int {
	out := 0
	for i := range m.inner {
		out += m.inner[i].Chunks()
	}
	return out
}

// WriteChunks implements vm.Table.WriteChunks
func (m *MultiTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	// the strategy here is just to have the Table.write
	// method return early and proceed to the next table
	// if its input has already been exhausted; this keeps
	// all of the parallel goroutines fully-utilized for
	// as long as possible
	//
	// one drawback of opening every cache entry here
	// is that we end up mapping everything simultaneously;
	// this may not be ideal from a memory-pressure perspective...
	// on the flip side, we can rely on the table implementation
	// to simply avoid caching anything at all if we end up exhausting
	// the local cache by trying to open everything at once
	err := vm.SplitInput(dst, m.open(parallel), m.write)
	m.next = 0
	return err
}
