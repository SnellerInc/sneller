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

package blockfmt

import (
	"strings"

	"slices"

	"github.com/SnellerInc/sneller/date"
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
	if min.IsTimestamp() && max.IsTimestamp() {
		min, _ := min.Timestamp()
		max, _ := max.Timestamp()
		return &TimeRange{
			path: path,
			min:  min,
			max:  max,
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

type TimeRange struct {
	path []string
	min  date.Time
	max  date.Time
}

func (r *TimeRange) Path() []string     { return r.path }
func (r *TimeRange) Min() ion.Datum     { return ion.Timestamp(r.min) }
func (r *TimeRange) Max() ion.Datum     { return ion.Timestamp(r.max) }
func (r *TimeRange) MinTime() date.Time { return r.min }
func (r *TimeRange) MaxTime() date.Time { return r.max }

func (r *TimeRange) Union(t *TimeRange) {
	r.min, r.max = timeUnion(t.min, t.max, r.min, r.max)
}

func timeUnion(min1, max1, min2, max2 date.Time) (min, max date.Time) {
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

func pathcmp(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := range a[:n] {
		if c := strings.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func sortByPath(lst []TimeRange) {
	slices.SortFunc(lst, func(left, right TimeRange) int {
		return pathcmp(left.path, right.path)
	})
}

// union unions the results from b into a
// and returns the mutated slice
// (the result is guaranteed not to alias b)
func union(a, b []TimeRange) []TimeRange {
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
		for pathcmp(apath, bpath) < 0 && pos < max {
			pos++
			apath = a[pos].path
		}
		if slices.Equal(apath, bpath) {
			a[pos].Union(&b[i])
		} else {
			a = append(a, b[i])
		}
	}
	sortByPath(a) // make results deterministic
	return a
}

func (b *blockpart) merge(from *blockpart) {
	b.chunks += from.chunks
	b.ranges = union(b.ranges, from.ranges)
}

func collectRanges(t *Trailer) [][]string {
	o := make([][]string, len(t.Sparse.indices))
	for i := range t.Sparse.indices {
		o[i] = t.Sparse.indices[i].path
	}
	return o
}
