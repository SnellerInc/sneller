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

package vm

import (
	"io"
	"sync/atomic"
)

// Limit is a QuerySink that
// limits the number of rows written
// to the next QuerySink.
//
// See NewLimit
type Limit struct {
	remaining int64
	dst       QuerySink
}

type limiter struct {
	parent *Limit
	dst    rowConsumer
	done   bool
}

// Limit constructs a Limit that will
// write no more than 'n' rows to 'dst'.
func NewLimit(n int64, dst QuerySink) *Limit {
	return &Limit{
		dst:       dst,
		remaining: n,
	}
}

func (l *Limit) Open() (io.WriteCloser, error) {
	w, err := l.dst.Open()
	if err != nil {
		return nil, err
	}

	return splitter(&limiter{
		parent: l,
		dst:    asRowConsumer(w),
	}), nil
}

func (l *Limit) Close() error {
	return l.dst.Close()
}

func (l *limiter) Close() error {
	if !l.done {
		l.done = true
		return l.dst.Close()
	}
	return nil
}

func (l *limiter) symbolize(st *symtab) error {
	return l.dst.symbolize(st)
}

func (l *limiter) next() rowConsumer { return l.dst }

func (l *limiter) writeRows(rows []vmref) error {
	if l.done {
		return io.EOF
	}
	c := int64(len(rows))
	avail := atomic.AddInt64(&l.parent.remaining, -c)
	if avail < 0 {
		// adjust c so that we only
		// write the rows we are interested
		// in writing
		c += avail
		if c < 0 {
			// close early so that the next
			// sub-query can begin finalization
			// as early as possible
			l.done = true
			err := l.dst.Close()
			if err == nil {
				return io.EOF
			}
			return err
		}
	}
	return l.dst.writeRows(rows[:c])
}
