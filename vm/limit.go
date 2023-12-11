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

// NewLimit constructs a Limit that will
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

func (l *limiter) symbolize(st *symtab, aux *auxbindings) error {
	return l.dst.symbolize(st, aux)
}

func (l *limiter) next() rowConsumer { return l.dst }

func (l *limiter) writeRows(rows []vmref, rp *rowParams) error {
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
		if c <= 0 {
			// close early so that the next
			// sub-query can begin finalization
			// as early as possible
			l.done = true
			err := l.dst.Close()
			if err == nil {
				err = io.EOF
			}
			return err
		}
	}
	// limit aux rows as well
	for j := range rp.auxbound {
		rp.auxbound[j] = rp.auxbound[j][:c]
	}
	err := l.dst.writeRows(rows[:c], rp)
	if avail == 0 && err == nil {
		l.done = true
		err = l.dst.Close()
		if err == nil {
			err = io.EOF
		}
	}
	return err
}
