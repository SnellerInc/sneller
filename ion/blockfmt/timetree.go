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
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
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

// String implements fmt.Stringer
func (t *TimeIndex) String() string {
	print := func(out *strings.Builder, span []timespan) {
		for i := range span {
			if i > 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(out, "%s @ %d", span[i].when.Time().Format(time.RFC3339Nano), span[i].offset)
		}
	}

	var out strings.Builder
	out.WriteString("max: [")
	print(&out, t.max)
	out.WriteString("] min: [")
	print(&out, t.min)
	out.WriteString("]")
	return out.String()
}

// Reset removes all the values from t.
func (t *TimeIndex) Reset() {
	t.max = t.max[:0]
	t.min = t.min[:0]
}

// Clone produces a deep copy of t.
func (t *TimeIndex) Clone() TimeIndex {
	return TimeIndex{
		min: slices.Clone(t.min),
		max: slices.Clone(t.max),
	}
}

func packList(dst *ion.Buffer, lst []timespan) {
	dst.BeginList(-1)
	st, dt := int64(0), int64(0)
	so, do := int64(0), int64(0)
	for i := range lst {
		// we encode only the difference between
		// the true value and the extrapolation
		// from the previous values; this means that
		// perfectly-spaced values are encoded as zeros,
		// which means they occupy only 1 ion byte
		w := lst[i].when.UnixMicro()
		u := w - st - dt // encoded value
		dt = w - st      // next extrapolated error
		st = w           // previous value
		dst.WriteInt(u)

		off := int64(lst[i].offset)
		o := off - so - do // encoded value
		do = off - so      // next extrapolated error
		so = off           // previous value
		dst.WriteInt(o)
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

func (d *TrailerDecoder) unpackSpans(dst *[]timespan, buf []byte) error {
	lst := d.spans[:0]
	st, dt := int64(0), int64(0)
	so, do := int64(0), int64(0)
	buf, _ = ion.Contents(buf)
	var v int64
	var err error
	for len(buf) > 0 {
		// see comment in packList() for the algorithm here:
		v, buf, err = ion.ReadInt(buf)
		if err != nil {
			return err
		}
		u := v + st + dt // real value
		dt += v          // error term to add to next result
		st = u           // previous result
		when := date.UnixMicro(u)
		v, buf, err = ion.ReadInt(buf)
		if err != nil {
			return err
		}
		off := v + so + do
		do += v
		so = off
		lst = append(lst, timespan{when: when, offset: int(off)})
	}
	d.spans = lst[len(lst):]
	*dst = lst[:len(lst):len(lst)]
	return nil
}

func (d *TrailerDecoder) decodeTimes(t *TimeIndex, buf []byte) error {
	_, err := ion.UnpackStruct(d.Symbols, buf, func(name string, field []byte) error {
		var err error
		switch name {
		case "max":
			err = d.unpackSpans(&t.max, field)
		case "min":
			err = d.unpackSpans(&t.min, field)
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

// Append concatenates t and next so that the
// ranges indexed by next occur immediately after
// the ranges indexed by t.
func (t *TimeIndex) Append(next *TimeIndex) {
	n := t.Blocks()
	for i := range next.min {
		t.pushMin(next.min[i].when, next.min[i].offset+n)
	}
	for i := range next.max {
		t.pushMax(next.max[i].when, next.max[i].offset+n)
	}
}

// Blocks returns the number of blocks in the index.
func (t *TimeIndex) Blocks() int {
	if len(t.max) == 0 {
		return 0
	}
	return t.max[len(t.max)-1].offset
}

// StartIntervals returns the number of distinct
// values that t.Start could return.
func (t *TimeIndex) StartIntervals() int {
	return len(t.min)
}

// EndIntervals returns the number of distinct
// values that t.End could return.
func (t *TimeIndex) EndIntervals() int {
	return len(t.max)
}

func (t *TimeIndex) pushMin(when date.Time, pos int) {
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

func (t *TimeIndex) pushMax(when date.Time, blocks int) {
	// push right-boundary
	if len(t.max) == 0 {
		t.max = append(t.max, timespan{
			when:   when,
			offset: blocks,
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
	b := t.Blocks()
	t.pushMin(start, b)
	t.pushMax(end, b+1)
}

// PushEmpty pushes num empty blocks to the index.
// The index should have more than zero entries
// already present (i.e. Push should have been
// called at least once).
func (t *TimeIndex) PushEmpty(num int) {
	if len(t.max) > 0 {
		t.max[len(t.max)-1].offset += num
	}
}

// EditLatest extends the range associated
// with the most recent call to Push.
// (EditLatest has no effect if (min, max)
// are no less/greater than the previous (min/max) pair.)
func (t *TimeIndex) EditLatest(min, max date.Time) {
	if len(t.max) == 0 {
		panic("EditLatest with zero entries")
	}
	// adjust max for the latest max
	l := &t.max[len(t.max)-1]
	if max.After(l.when) {
		l.when = max
	}
	// strip min intervals while min < latest(min)
	j := len(t.min)
	for j > 0 && min.Before(t.min[j-1].when) {
		j--
	}
	t.min = t.min[:j]
	// if we have a new global min, add it:
	if j == 0 {
		t.min = append(t.min, timespan{
			offset: 0,
			when:   min,
		})
	}
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

func (t *TimeIndex) trim(max int) TimeIndex {
	if max == 0 {
		panic("empty trim")
	}
	if len(t.max) == 0 {
		return TimeIndex{}
	}
	blocks := t.max[len(t.max)-1].offset
	if max > blocks {
		panic("TimeIndex.trim beyond max offset")
	}
	if max == blocks {
		return t.Clone()
	}

	// copy mins where min.offset < max
	j := len(t.min)
	for j > 0 && t.min[j-1].offset >= max {
		j--
	}
	var newmin []timespan
	if j == 0 {
		newmin = []timespan{{offset: 0, when: t.min[0].when}}
	} else {
		newmin = slices.Clone(t.min[:j])
	}

	// copy maxes where max.offset <= max
	j = len(t.max)
	for j > 0 && t.max[j-1].offset > max {
		j--
	}
	newmax := slices.Clone(t.max[:j])

	// ... and additionally include a final
	// right-hand-side boundary at max
	if j == 0 || newmax[j-1].offset < max {
		newmax = append(newmax, timespan{
			offset: max,
			when:   t.max[j].when,
		})
	}
	return TimeIndex{
		min: newmin,
		max: newmax,
	}
}
