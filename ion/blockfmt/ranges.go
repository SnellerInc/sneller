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
	"time"

	"github.com/SnellerInc/sneller/ion"
)

// Range describes the (closed) interval
// that the value of a particular
// path expression could occupy
type Range interface {
	Path() []string
	Min() ion.Datum
	Max() ion.Datum
}

func NewRange(path []string, min, max ion.Datum) Range {
	if min == nil || max == nil {
		panic("blockfmt.NewRange: min/max must not be nil")
	}
	if min, ok := min.(ion.Timestamp); ok {
		if max, ok := max.(ion.Timestamp); ok {
			return &TimeRange{
				path: path,
				min:  time.Time(min).UnixMicro(),
				diff: difftime(time.Time(min), time.Time(max)),
			}
		}
	}
	return &datumRange{
		path: path,
		min:  min,
		max:  max,
	}
}

type datumRange struct {
	path     []string
	min, max ion.Datum
}

func (r *datumRange) Path() []string { return r.path }
func (r *datumRange) Min() ion.Datum { return r.min }
func (r *datumRange) Max() ion.Datum { return r.max }

func difftime(min, max time.Time) int64 {
	return max.UnixMicro() - min.UnixMicro()
}

type TimeRange struct {
	path      []string
	min, diff int64 // microseconds
}

func (r *TimeRange) Path() []string     { return r.path }
func (r *TimeRange) Min() ion.Datum     { return ion.Timestamp(r.MinTime()) }
func (r *TimeRange) Max() ion.Datum     { return ion.Timestamp(r.MaxTime()) }
func (r *TimeRange) MinTime() time.Time { return time.UnixMicro(r.min).UTC() }
func (r *TimeRange) MaxTime() time.Time { return time.UnixMicro(r.min + r.diff).UTC() }

func (r *TimeRange) Union(t *TimeRange) {
	min, max := timeUnion(t.MinTime(), t.MaxTime(), r.MinTime(), r.MaxTime())
	r.min = min.UnixMicro()
	r.diff = difftime(min, max)
}

func timeUnion(min1, max1, min2, max2 time.Time) (min, max time.Time) {
	if min1.Before(min2) {
		min = min1
	} else {
		min = min2
	}
	if max1.After(max2) {
		max = max1
	} else {
		max = max2
	}
	return min, max
}

func pathless(a, b []string) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := range a[:n] {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return len(a) < len(b)
}

func pathequal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) > 0 && &a[0] == &b[0] {
		return true
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortByPath(lst []*TimeRange) {
	sort.Slice(lst, func(i, j int) bool {
		return pathless(lst[i].path, lst[j].path)
	})
}

// union two lists of time ranges
func union(a, b []*TimeRange) []*TimeRange {
	sortByPath(a)
	sortByPath(b)
	pos := 0
	max := len(a) - 1
	for i := range b {
		if pos > max {
			a = append(a, b[i:]...)
			break
		}
		bpath := b[i].path
		apath := a[pos].path
		// search for b <= a
		for pathless(apath, bpath) && pos < max {
			pos++
			apath = a[pos].path
		}
		if pathequal(apath, bpath) {
			a[pos].Union(b[i])
		} else {
			a = append(a, b[i])
		}
	}
	sortByPath(a) // make results deterministic
	return a
}

func (b *Blockdesc) merge(from *Blockdesc) {
	b.Chunks += from.Chunks
	b.Ranges = toRanges(
		union(
			toTimeRanges(b.Ranges),
			toTimeRanges(from.Ranges),
		))
}

func collectRanges(t *Trailer) [][]string {
	var out [][]string
	for i := range t.Blocks {
	rangeloop:
		for j := range t.Blocks[i].Ranges {
			p := t.Blocks[i].Ranges[j].Path()
			// FIXME: don't do polynomial-time comparison here :o
			for k := range out {
				if pathequal(out[k], p) {
					continue rangeloop
				}
			}
			out = append(out, p)
		}
	}
	return out
}
