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

package ion

import (
	"encoding/binary"

	"github.com/SnellerInc/sneller/date"
)

type Ranges struct {
	paths []symstr // paths in insertion order
	m     map[symstr]dataRange
}

// AddTruncatedTime adds a truncated time value to the
// range tracker.
func (rs *Ranges) AddTruncatedTime(p Symbuf, t date.Time, trunc TimeTrunc) {
	tt := trunc.truncate(t)
	rs.AddTime(p, tt)
}

// AddTime adds a time value to the range tracker.
func (rs *Ranges) AddTime(p Symbuf, t date.Time) {
	if rs.m == nil {
		rs.m = make(map[symstr]dataRange)
	} else if r := rs.m[symstr(p)]; r != nil {
		switch r := r.(type) {
		case *timeRange:
			r.add(t)
		}
		return
	}
	k := symstr(p)
	r := newTimeRange(t)
	rs.paths = append(rs.paths, k)
	rs.m[k] = r
}

// commit is called after each object is added to
// commit any uncommitted range values.
func (rs *Ranges) commit() {
	for _, r := range rs.m {
		r.commit()
	}
}

// flush is called after every flush to indicate that
// the committed ranges have been written or otherwise
// consumed.
func (rs *Ranges) flush() {
	ps := rs.paths
	rs.paths = rs.paths[:0]
	for _, k := range ps {
		if rs.m[k].flush() {
			rs.paths = append(rs.paths, k)
		} else {
			delete(rs.m, k)
		}
	}
}

// reset the range tracker to its initial state.
func (rs *Ranges) reset() {
	rs.paths = rs.paths[:0]
	for k := range rs.m {
		delete(rs.m, k)
	}
}

// A dataRange holds an inclusive range of values a
// field can take within a chunk.
type dataRange interface {
	count() int
	// ranges returns the inclusive min and max
	// values within this range. The returned range
	// must reflect only values added before the
	// last call to commit.
	ranges() (min, max Datum, ok bool)
	// commit is called after every object is
	// committed and confirmed to be part of the
	// current chunk.
	commit()
	// flush is called after every flush to
	// indicate that the committed range has been
	// written or otherwise consumed.
	// Implementations should clear committed
	// values, keep uncommitted values, and return
	// whether uncommitted values are present.
	flush() (keep bool)
}

type timeRange struct {
	commits    int       // committed count
	min, max   date.Time // committed range
	hasRange   bool
	pending    date.Time // uncommitted value
	hasPending bool
}

func newTimeRange(t date.Time) *timeRange {
	return &timeRange{
		pending:    t,
		hasPending: true,
	}
}

func (r *timeRange) ranges() (min, max Datum, ok bool) {
	if r.hasRange {
		return Timestamp(r.min), Timestamp(r.max), true
	}
	return Datum{}, Datum{}, false
}

func (r *timeRange) commit() {
	if !r.hasPending {
		return
	}
	if !r.hasRange {
		r.min = r.pending
		r.max = r.pending
		r.hasRange = true
	} else if r.pending.Before(r.min) {
		r.min = r.pending
	} else if r.pending.After(r.max) {
		r.max = r.pending
	}
	r.commits++
	r.hasPending = false
}

func (r *timeRange) count() int { return r.commits }

func (r *timeRange) flush() bool {
	r.hasRange = false
	r.commits = 0
	return r.hasPending
}

func (r *timeRange) add(t date.Time) {
	r.pending = t
	r.hasPending = true
}

// Symbuf is an encoded list of symtab indices.
type Symbuf []byte

// Prepare the buffer to have n symbols pushed. This
// also clears the buffer.
func (b *Symbuf) Prepare(n int) {
	if cap(*b) < 4*n {
		*b = make(Symbuf, 0, 4*n)
	} else {
		*b = (*b)[:0]
	}
}

// Push adds a new symbol to the buffer. Prepare should
// be called first to ensure the capacity of the buffer
// is sufficient to accept all pushed symbols, or this
// method will panic.
func (b *Symbuf) Push(sym Symbol) {
	bb := (*b)[:len(*b)+4] // assume sufficient cap
	binary.LittleEndian.PutUint32(bb[len(*b):], uint32(sym))
	*b = bb
}

// symstr is an encoded list of symtab indices which
// can be used as a map key.
type symstr string

// transcode converts a symstr to an equivalent symstr
// using a different symbol table via a resymbolizer
func (s symstr) transcode(rs *resymbolizer) symstr {
	ret := make([]byte, len(s))
	for i := 0; i < len(s); i += 4 {
		n := binary.LittleEndian.Uint32([]byte(s[i:]))
		sym := rs.get(Symbol(n))
		binary.LittleEndian.PutUint32(ret[i:], uint32(sym))
	}
	return symstr(ret)
}

// resolve the path using the given symbol table.
func (s symstr) resolve(st *Symtab) []string {
	if len(s) == 0 {
		return nil
	}
	syms := make([]string, len(s)/4)
	for i := range syms {
		n := binary.LittleEndian.Uint32([]byte(s[4*i:]))
		syms[i] = st.Get(Symbol(n))
	}
	return syms
}
