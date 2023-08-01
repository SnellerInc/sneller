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

package ints

import "golang.org/x/exp/slices"

// Interval is a half-open interval [start, end)
// (start is always less than or equal to end)
type Interval struct {
	Start, End int
}

// Intervals represents a series of half-open
// intervals.
type Intervals []Interval

// Empty returns whether [in] is an empty
// interval.
func (in Interval) Empty() bool {
	return in.Start >= in.End
}

// Empty returns whether all the intervals in
// [in] are empty.
func (in Intervals) Empty() bool {
	for i := range in {
		if !in[i].Empty() {
			return false
		}
	}
	return true
}

// Len returns the length of the interval.
func (in Interval) Len() int {
	if in.End <= in.Start {
		return 0
	}
	return in.End - in.Start
}

// Len returns the length of the intervals.
func (in Intervals) Len() int {
	n := 0
	for i := range in {
		n += in[i].Len()
	}
	return n
}

// Clone returns a copy of [in].
func (in Intervals) Clone() Intervals {
	return slices.Clone(in)
}

// Compress compresses [in] so that all the
// contained intervals are ordered and
// non-overlapping.
func (in *Intervals) Compress() {
	// sort by start, then by end
	slices.SortFunc(*in, func(x, y Interval) int {
		if x.Start == y.Start {
			return x.End - y.End
		}
		return x.Start - y.Start
	})
	// remove duplicate ranges
	*in = slices.Compact(*in)

	// compress overlapping ranges
	oranges := (*in)[:0]
	for i := 0; i < len(*in); i++ {
		merged := 0
		// while the next-highest start range
		// starts below the current ranges' max,
		// collapse the ranges together
		for j := i + 1; j < len(*in); j++ {
			if (*in)[j].Start > (*in)[i].End {
				break
			}
			// extend intervals[i] as necessary
			if (*in)[j].End > (*in)[i].End {
				(*in)[i].End = (*in)[j].End
			}
			merged++
		}
		oranges = append(oranges, (*in)[i])
		i += merged
	}
	(*in) = oranges
}

// Overlaps returns whether [in] overlaps with
// the half-open interval [start, end).
//
// The behavior of Overlaps when start >= end is
// unspecified.
func (in Intervals) Overlaps(start, end int) bool {
	for i := range in {
		// ends before start: doesn't overlap
		if in[i].End <= start {
			continue
		}
		// starts after end: done
		if in[i].Start >= end {
			break
		}
		// we know in[i].End > start
		//      or in[i].Start < end
		return true
	}
	return false
}

// Visit visits distinct (non-overlapping)
// intervals within [in].
//
// If the intervals are empty, [fn] will be
// called once with (0, 0).
func (in Intervals) Visit(fn func(start, end int)) {
	any := false
	for i := range in {
		if !in[i].Empty() {
			any = true
			fn(in[i].Start, in[i].End)
		}
	}
	if !any {
		fn(0, 0)
	}
}

// Next removes and returns the first integer in
// the series. This returns (0, false) if [in]
// is empty.
func (in *Interval) Next() (int, bool) {
	if !in.Empty() {
		n := in.Start
		in.Start++
		return n, true
	}
	return 0, false
}

// Next removes and returns the first integer in
// the series. This returns (0, false) if [in]
// is empty.
func (in *Intervals) Next() (int, bool) {
	for len(*in) > 0 {
		n, ok := (*in)[0].Next()
		if ok {
			return n, true
		}
		*in = (*in)[1:]
	}
	return 0, false
}

// Each calls [fn] for each value in the
// interval.
func (in Interval) Each(fn func(int)) {
	for i := in.Start; i < in.End; i++ {
		fn(i)
	}
}

// Each calls [fn] for each value in the
// interval.
func (in Intervals) Each(fn func(n int)) {
	for i := range in {
		in[i].Each(fn)
	}
}

// EachErr calls [fn] for each value in the
// interval. If [fn] returns a non-nil error,
// this stops and returns the error.
func (in Interval) EachErr(fn func(int) error) error {
	for i := in.Start; i < in.End; i++ {
		err := fn(i)
		if err != nil {
			return err
		}
	}
	return nil
}

// EachErr calls [fn] for each value in the
// interval. If [fn] returns a non-nil error,
// this stops and returns the error.
func (in Intervals) EachErr(fn func(n int) error) error {
	for i := range in {
		err := in[i].EachErr(fn)
		if err != nil {
			return err
		}
	}
	return nil
}

// Intersect returns the intersection of [in]
// and [x]. If there is no overlap, the returned
// interval is empty.
func (in Interval) Intersect(x Interval) Interval {
	if in.End <= x.Start || in.Start >= x.End {
		return Interval{0, 0}
	}
	out := Interval{in.Start, in.End}
	if x.Start > out.Start {
		out.Start = x.Start
	}
	if x.End < out.End {
		out.End = x.End
	}
	return out
}

// Intersect returns the intersection of [in]
// and [x]. If there is no overlap, the returned
// interval is empty.
func (in Intervals) Intersect(x Intervals) Intervals {
	var out Intervals
	for i := range in {
		for j := range x {
			if isect := in[i].Intersect(x[j]); !isect.Empty() {
				out = append(out, isect)
			}
		}
	}
	out.Compress()
	return out
}
