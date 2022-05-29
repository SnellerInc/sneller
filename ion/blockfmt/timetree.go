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
	"sort"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

type timespan struct {
	when   date.Time // value
	offset int       // offset associated with this value
}

// TimeIndex maintains a lossy mapping of time ranges
// to "blocks", where the time range -> block mapping is
// preserved precisely if the ranges are monotonic with
// respect to the block number. TimeIndex does not care
// about what constitutes a "block"; it merely maintains
// a linear mapping from timestamps to integers.
//
// TimeIndex can answer leftmost- and rightmost-bound
// queries for timestamp values with respect to the
// range of values inserted via Push.
//
// Because TimeIndex stores a monotonic list of time ranges
// and blocks, its serialized encoding is space-efficient,
// as the timestamps and block numbers can be delta-encoded.
//
// See TimeIndex.Push, TimeIndex.Start, and TimeIndex.End
type TimeIndex struct {
	// each value in min is a start offset
	// plus the minimum value in that span
	// (up to the next offset)
	min []timespan
	// each value in max is a max offset
	// plus the maximum value in that span
	max []timespan
}

// Reset removes all the values from t.
func (t *TimeIndex) Reset() {
	t.max = t.max[:0]
	t.min = t.min[:0]
}

func packList(dst *ion.Buffer, lst []timespan) {
	dst.BeginList(-1)
	timebase := int64(0)
	offbase := int64(0)
	for i := range lst {
		// both the timestamp and offset components
		// are delta-encoded relative to the previous value,
		// since we know in practice that these tend to be
		// small numbers, so the varint encoding should
		// compress them relatively well
		us := lst[i].when.UnixMicro()
		dst.WriteInt(us - timebase)
		timebase = us
		off := int64(lst[i].offset)
		dst.WriteInt(off - offbase)
		offbase = off
	}
	dst.EndList()
}

func (t *TimeIndex) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("max"))
	packList(dst, t.max)
	dst.BeginField(st.Intern("min"))
	packList(dst, t.min)
	dst.EndStruct()
}

func unpackSpans(buf []byte) ([]timespan, error) {
	var lst []timespan
	timebase := int64(0)
	offbase := int64(0)
	buf, _ = ion.Contents(buf)
	var v int64
	var err error
	for len(buf) > 0 {
		v, buf, err = ion.ReadInt(buf)
		if err != nil {
			return nil, err
		}
		when := date.UnixMicro(v + timebase)
		timebase += v
		v, buf, err = ion.ReadInt(buf)
		if err != nil {
			return nil, err
		}
		off := int(v + offbase)
		offbase += v
		lst = append(lst, timespan{when: when, offset: off})
	}
	return lst, nil
}

func (t *TimeIndex) Decode(st *ion.Symtab, buf []byte) error {
	*t = TimeIndex{}
	_, err := ion.UnpackStruct(st, buf, func(name string, field []byte) error {
		var err error
		switch name {
		case "max":
			t.max, err = unpackSpans(field)
		case "min":
			t.min, err = unpackSpans(field)
		}
		return err
	})
	return err
}

// Start produces the lowest offset (inclusive) at which
// the time 'when' could occur in the input block list.
func (t *TimeIndex) Start(when date.Time) int {
	if len(t.max) == 0 {
		return 0
	}
	// find the lowest max where when <= max
	j := sort.Search(len(t.max), func(i int) bool {
		return !when.After(t.max[i].when)
	})
	if j == 0 {
		// when is less than the lowest max
		return 0
	}
	return t.max[j-1].offset
}

// Contains returns true if the value 'when' could appear
// within this index, or false otherwise. Note that Contains
// is sensitive to holes in the index.
func (t *TimeIndex) Contains(when date.Time) bool {
	return t.Start(when) < t.End(when)
}

// End produces the highest offset (exclusive) at which
// the time 'when' could occur in the input block list.
// In other words, for a return value N, blocks [0, N)
// could contain the value "when".
func (t *TimeIndex) End(when date.Time) int {
	// find the lowest min where min > when,
	// then pick the left-hand-side of that interval
	j := sort.Search(len(t.min), func(i int) bool {
		return t.min[i].when.After(when)
	})
	// j == len(min) -> all blocks have values <= when
	if j == len(t.min) {
		return t.max[len(t.max)-1].offset
	}
	return t.min[j].offset
}

// Blocks returns the number of blocks in the index.
func (t *TimeIndex) Blocks() int {
	if len(t.max) == 0 {
		return 0
	}
	return t.max[len(t.max)-1].offset
}

func (t *TimeIndex) LeftIntervals() int {
	return len(t.min)
}

func (t *TimeIndex) RightIntervals() int {
	return len(t.max)
}

func (t *TimeIndex) pushMin(when date.Time) {
	pos := 0
	if len(t.max) > 0 {
		pos = t.max[len(t.max)-1].offset
	}
	j := len(t.min)
	// walk backwards and overwrite the first entry
	// where the minimum is larger than the new min
	for j > 0 && !when.After(t.min[j-1].when) {
		pos = t.min[j-1].offset
		j--
	}
	t.min = append(t.min[:j], timespan{
		when:   when,
		offset: pos,
	})
}

func (t *TimeIndex) pushMax(when date.Time) {
	// push right-boundary
	if len(t.max) == 0 {
		t.max = append(t.max, timespan{
			when:   when,
			offset: 1,
		})
		return
	}

	// if this entry is overlapping,
	// just increment the block counter
	last := t.max[len(t.max)-1]
	realmax := last.when
	if when.After(realmax) {
		realmax = when
	}

	// trim all trailing blocks that have an overlap:
	blocks := last.offset + 1
	endpos := len(t.max)
	for endpos > 0 && !when.After(t.max[endpos-1].when) {
		endpos--
	}
	// append the terminal block:
	t.max = append(t.max[:endpos], timespan{
		when:   realmax,
		offset: blocks,
	})
}

// Push pushes one new block to the index
// with the associated start and end times.
//
// If the time range specified in Push overlaps
// with block ranges that are already part of
// the index, those ranges will be coalesced into
// the union of the two ranges. In other words,
// overlapping ranges (or non-monotonic inserts
// more generally) will cause the precision of
// the TimeIndex mapping to relax until it can
// guarantee that it can maintain a monotonic
// time-to-block mapping. In the most degenerate case,
// the TimeIndex will simply map the minimum seen time
// to block 0 and maximum seen time to block N.
func (t *TimeIndex) Push(start, end date.Time) {
	if end.Before(start) {
		println(end.String(), "<", start.String())
		panic("TimeIndex.Push: end < start")
	}
	t.pushMin(start)
	t.pushMax(end)
}

func (t *TimeIndex) PushEmpty(num int) {
	if len(t.max) > 0 {
		t.max[len(t.max)-1].offset += num
	}
}

func (t *TimeIndex) EditLatest(min, max date.Time) {
	if len(t.max) == 0 {
		panic("EditLatest with zero entries")
	}
	// "pop" the last max block offset
	t.max[len(t.max)-1].offset--
	if len(t.max) > 1 && t.max[len(t.max)-1].offset == t.max[len(t.max)-2].offset {
		t.max = t.max[:len(t.max)-1]
	}
	// "pop" the last min block offset
	// if the max offset has been trimmed
	// back to this position
	if len(t.min) > 0 && t.min[len(t.min)-1].offset == t.max[len(t.max)-1].offset {
		t.min = t.min[:len(t.min)-1]
	}
	// ... and then push it again with latest(max, prevmax)
	t.Push(min, max)
}

func (t *TimeIndex) Min() (date.Time, bool) {
	if len(t.min) == 0 {
		return date.Time{}, false
	}
	return t.min[0].when, true
}

func (t *TimeIndex) Max() (date.Time, bool) {
	if len(t.max) == 0 {
		return date.Time{}, false
	}
	return t.max[len(t.max)-1].when, true
}
