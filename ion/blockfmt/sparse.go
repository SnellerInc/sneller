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

package blockfmt

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

type timeIndex struct {
	path   []string
	ranges TimeIndex
}

type SparseIndex struct {
	consts  ion.Struct
	indices []timeIndex
	blocks  int
}

// Const extracts the datum associated with
// the constant x from the sparse index,
// or returns (ion.Empty, false) if no such
// datum exists.
func (s *SparseIndex) Const(x string) (ion.Datum, bool) {
	f, ok := s.consts.FieldByName(x)
	if !ok {
		return ion.Empty, false
	}
	return f.Datum, true
}

func (t *timeIndex) slice(i, j int) timeIndex {
	return timeIndex{
		path:   t.path,
		ranges: t.ranges.trim(i, j),
	}
}

// Slice produces a sparse index for just the blocks
// in the half-open interval [i:j].
// Slice will panic if i is greater than j, i is less than zero,
// or j is greater than the number of blocks in the index.
func (s *SparseIndex) Slice(i, j int) SparseIndex {
	if i > j || i < 0 || j > s.Blocks() {
		panic("SparseIndex.Slice beyond blocks")
	}
	indices := make([]timeIndex, len(s.indices))
	for k := range indices {
		indices[k] = s.indices[k].slice(i, j)
	}
	return SparseIndex{
		consts:  s.consts,
		indices: indices,
		blocks:  j,
	}
}

// Trim produces a copy of s that only includes
// information up to block j. Trim will panic
// if j is greater than s.Blocks().
//
// Trim is equivalent to s.Slice(0, j)
func (s *SparseIndex) Trim(j int) SparseIndex {
	return s.Slice(0, j)
}

// Clone produces a deep copy of s.
func (s *SparseIndex) Clone() SparseIndex {
	indices := slices.Clone(s.indices)
	for i := range indices {
		indices[i].ranges = indices[i].ranges.Clone()
	}
	return SparseIndex{
		consts:  s.consts,
		indices: indices,
		blocks:  s.blocks,
	}
}

// emptyClone produces a copy of s with the same
// indices but no blocks.
func (s *SparseIndex) emptyClone() SparseIndex {
	out := SparseIndex{
		consts:  s.consts,
		indices: make([]timeIndex, len(s.indices)),
	}
	for i := range s.indices {
		out.indices[i].path = s.indices[i].path
	}
	return out
}

// Append tries to append next to s and returns
// true if the append operation was successful,
// or false otherwise. (Append will fail if the
// set of indices tracked in each SparseIndex is not the same.)
// The block positions in next are assumed to start
// at s.Blocks().
func (s *SparseIndex) Append(next *SparseIndex) bool {
	return s.AppendBlocks(next, 0, next.blocks)
}

// AppendBlocks is like Append, but only appends
// blocks from next from block i up to block j.
//
// This will panic if i > j or j > next.Blocks().
func (s *SparseIndex) AppendBlocks(next *SparseIndex, i, j int) bool {
	if i < 0 || j < 0 || i > j || j > next.blocks {
		panic("SparseIndex.AppendBlocks: index out of range")
	}
	if !s.consts.Equal(next.consts) {
		return false
	}
	eq := func(a, b timeIndex) bool {
		return slices.Equal(a.path, b.path)
	}
	if !slices.EqualFunc(s.indices, next.indices, eq) {
		return false
	}
	for i := range s.indices {
		s.indices[i].ranges.appendBlocks(&next.indices[i].ranges, i, j)
	}
	s.blocks += j - i
	return true
}

// Fields returns the number of individually
// indexed fields.
func (s *SparseIndex) Fields() int { return len(s.indices) }

// FieldNames returns the list of field names
// using '.' as a separator between the path components.
// NOTE: FieldNames does not escape the '.' character
// inside field names themselves, so the textual result
// of each field name may be ambiguous.
func (s *SparseIndex) FieldNames() []string {
	o := make([]string, 0, len(s.indices))
	for i := range s.indices {
		o = append(o, strings.Join(s.indices[i].path, "."))
	}
	return o
}

func (s *SparseIndex) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("blocks"))
	dst.WriteInt(int64(s.blocks))
	if !s.consts.IsEmpty() {
		dst.BeginField(st.Intern("consts"))
		s.consts.Encode(dst, st)
	}
	dst.BeginField(st.Intern("indices"))
	dst.BeginList(-1)
	for i := range s.indices {
		dst.BeginStruct(-1)
		dst.BeginField(st.Intern("path"))
		dst.BeginList(-1)
		l := s.indices[i].path
		for i := range l {
			dst.WriteSymbol(st.Intern(l[i]))
		}
		dst.EndList()
		dst.BeginField(st.Intern("ranges"))
		s.indices[i].ranges.Encode(dst, st)
		dst.EndStruct()
	}
	dst.EndList()
	dst.EndStruct()
}

func (d *TrailerDecoder) decodeSparse(s *SparseIndex, v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "blocks":
			n, err := f.Int()
			if err != nil {
				return err
			}
			s.blocks = int(n)
		case "consts":
			if !f.IsStruct() {
				return fmt.Errorf("expected consts to be a struct")
			}
			// XXX: we have to copy the bytes because
			// the resulting ion.Struct will alias the
			// slice and so we need to make a copy to
			// avoid data corruption
			s.consts, _ = f.Datum.Clone().Struct()
		case "indices":
			err := f.UnpackList(func(v ion.Datum) error {
				var val timeIndex
				err := v.UnpackStruct(func(f ion.Field) error {
					switch f.Label {
					case "path":
						var err error
						val.path, err = d.path(f.Datum)
						return err
					case "ranges":
						return d.decodeTimes(&val.ranges, f.Datum)
					}
					return nil
				})
				if err != nil {
					return err
				}
				s.indices = append(s.indices, val)
				return nil
			})
			return err
		}
		return nil
	})
	return err
}

// Get gets a TimeIndex associated with a path.
// The returned TimeIndex may be nil if no such
// index exists.
func (s *SparseIndex) Get(path []string) *TimeIndex {
	if idx := s.search(path); idx != nil {
		return &idx.ranges
	}
	return nil
}

func (s *SparseIndex) Push(rng []Range) {
	for i := range rng {
		tr, ok := rng[i].(*TimeRange)
		if !ok {
			continue
		}
		s.push(tr.path, tr.min, tr.max)
	}
	s.bump()
}

func (s *SparseIndex) MinMax(path []string) (min, max date.Time, ok bool) {
	tr := s.Get(path)
	if tr == nil {
		return
	}
	min, ok = tr.Min()
	max, _ = tr.Max() // always ok if min is ok
	return
}

func (s *SparseIndex) search(path []string) *timeIndex {
	j := sort.Search(len(s.indices), func(i int) bool {
		return pathcmp(s.indices[i].path, path) >= 0
	})
	if j < len(s.indices) && slices.Equal(path, s.indices[j].path) {
		return &s.indices[j]
	}
	return nil
}

func (s *SparseIndex) push(path []string, min, max date.Time) {
	j := sort.Search(len(s.indices), func(i int) bool {
		return pathcmp(s.indices[i].path, path) >= 0
	})
	if j < len(s.indices) && slices.Equal(path, s.indices[j].path) {
		s.indices[j].ranges.Push(min, max)
		return
	}
	// insertion-sort a new path entry
	s.indices = append(s.indices, timeIndex{})
	copy(s.indices[j+1:], s.indices[j:])
	s.indices[j].path = path
	s.indices[j].ranges = TimeIndex{}
	s.indices[j].ranges.Push(min, max)
}

func (s *SparseIndex) update(path []string, min, max date.Time) {
	j := sort.Search(len(s.indices), func(i int) bool {
		return pathcmp(s.indices[i].path, path) >= 0
	})
	if j < len(s.indices) && slices.Equal(path, s.indices[j].path) {
		s.indices[j].ranges.EditLatest(min, max)
		return
	}
	// insertion-sort a new path entry
	s.indices = append(s.indices, timeIndex{})
	copy(s.indices[j+1:], s.indices[j:])
	s.indices[j].path = path
	s.indices[j].ranges = TimeIndex{}
	s.indices[j].ranges.Push(min, max)
	s.indices[j].ranges.PushEmpty(s.blocks - 1)
}

// make sure every sub-range points to
// the same number of blocks
func (s *SparseIndex) bump() {
	s.blocks++
	for i := range s.indices {
		if b := s.indices[i].ranges.Blocks(); b < s.blocks {
			s.indices[i].ranges.PushEmpty(s.blocks - b)
		} else if b > s.blocks {
			println(b, ">", s.blocks)
			panic("bad block bookkeeping")
		}
	}
}

// update the most recent min/max values associated
// with a sparse index; it does not increase the number of blocks
func (s *SparseIndex) updateSummary(from *SparseIndex) {
	for i := range from.indices {
		if min, ok := from.indices[i].ranges.Min(); ok {
			max, _ := from.indices[i].ranges.Max()
			s.update(from.indices[i].path, min, max)
		}
	}
}

// push the min/max values associated with a sparse index
func (s *SparseIndex) pushSummary(from *SparseIndex) {
	for i := range from.indices {
		if min, ok := from.indices[i].ranges.Min(); ok {
			max, _ := from.indices[i].ranges.Max()
			s.push(from.indices[i].path, min, max)
		}
	}
	s.bump()
}

func (s *SparseIndex) Blocks() int { return s.blocks }
