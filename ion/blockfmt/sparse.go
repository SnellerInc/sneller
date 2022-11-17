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
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
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

func (t *timeIndex) trim(j int) timeIndex {
	return timeIndex{
		path:   t.path,
		ranges: t.ranges.trim(j),
	}
}

// Trim produces a copy of s that only includes
// information up to block j. Trim will panic
// if j is greater than s.Blocks().
func (s *SparseIndex) Trim(j int) SparseIndex {
	if j > s.Blocks() {
		panic("SparseIndex.Trim beyond blocks")
	}
	indices := make([]timeIndex, len(s.indices))
	for i := range indices {
		indices[i] = s.indices[i].trim(j)
	}
	return SparseIndex{
		indices: indices,
		blocks:  j,
	}
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
	if !s.consts.Empty() {
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

func (d *TrailerDecoder) decodeSparse(s *SparseIndex, body []byte) error {
	_, err := ion.UnpackStruct(d.Symbols, body, func(name string, field []byte) error {
		switch name {
		case "blocks":
			n, _, err := ion.ReadInt(field)
			if err != nil {
				return err
			}
			s.blocks = int(n)
		case "consts":
			// XXX: we have to copy the bytes because
			// the resulting ion.Struct will alias the
			// slice and so we need to make a copy to
			// avoid data corruption
			field = slices.Clone(field)
			d, _, err := ion.ReadDatum(d.Symbols, field)
			if err != nil {
				return err
			}
			consts, ok := d.Struct()
			if !ok {
				return fmt.Errorf("expected consts to be a struct")
			}
			s.consts = consts
		case "indices":
			_, err := ion.UnpackList(field, func(field []byte) error {
				var val timeIndex
				_, err := ion.UnpackStruct(d.Symbols, field, func(name string, field []byte) error {
					switch name {
					case "path":
						var err error
						val.path, err = d.path(field)
						return err
					case "ranges":
						return d.decodeTimes(&val.ranges, field)
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

func flatpath(p *expr.Path, dst []string) []string {
	flat := append(dst, p.First)
	for d := p.Rest; d != nil; d = d.Next() {
		dot, ok := d.(*expr.Dot)
		if !ok {
			return nil
		}
		flat = append(flat, dot.Field)
	}
	return flat
}

// GetPath works identically to Get, except for that
// it accepts an AST path expression instead of a list
// of path components.
func (s *SparseIndex) GetPath(p *expr.Path) *TimeIndex {
	flat := flatpath(p, nil)
	if flat == nil {
		return nil
	}
	return s.Get(flat)
}

func (s *SparseIndex) MinMax(p *expr.Path) (min, max date.Time, ok bool) {
	tr := s.GetPath(p)
	if tr == nil {
		return
	}
	min, ok = tr.Min()
	max, _ = tr.Max() // always ok if min is ok
	return
}

func (s *SparseIndex) search(path []string) *timeIndex {
	j := sort.Search(len(s.indices), func(i int) bool {
		return !pathless(s.indices[i].path, path)
	})
	if j < len(s.indices) && slices.Equal(path, s.indices[j].path) {
		return &s.indices[j]
	}
	return nil
}

func (s *SparseIndex) push(path []string, min, max date.Time) {
	j := sort.Search(len(s.indices), func(i int) bool {
		return !pathless(s.indices[i].path, path)
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
		return !pathless(s.indices[i].path, path)
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
