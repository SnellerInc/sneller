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

package pir

// SizeClass is one of the output
// size classifications.
// See SizeZero, SizeOne, etc.
type SizeClass int

const (
	// SizeZero is the SizeClass
	// for queries that produce
	// zero rows of output.
	SizeZero SizeClass = iota
	// SizeOne is the SizeClass
	// for queries that produce
	// exactly one row of output.
	SizeOne
	// SizeExactSmall is the SizeClass
	// for queries that have an exact
	// cardinality (due to the presence
	// of LIMIT, etc.) and the cardinality
	// is known to be "small" for some
	// definition of "small" ...
	SizeExactSmall
	// SizeColumnCardinality is the SizeClass
	// for queries that have inexact cardinality
	// but produce a small-ish number of output
	// rows due to the presence of a grouping operation.
	// (Our optimistic assumption is that the cardinality
	// of most columns in most datasets is lower than LargeSize.)
	SizeColumnCardinality
	// SizeExactLarge is the SizeClass
	// for queries that have an exact
	// cardinality that is "large."
	SizeExactLarge
	// SizeUnknown is the SizeClass
	// for queries that have unknown output size.
	SizeUnknown
)

// LargeSize is the point at which
// an exact cardinality is considered
// to be "large" rather than "small"
const LargeSize = 10000

func (s SizeClass) String() string {
	switch s {
	case SizeZero:
		return "zero"
	case SizeOne:
		return "one"
	case SizeExactSmall:
		return "small"
	case SizeColumnCardinality:
		return "column-cardinality"
	case SizeExactLarge:
		return "large"
	default:
		return "unknown"
	}
}

// Exact returns whether the SizeClass
// has exact (known) cardinality.
func (s SizeClass) Exact() bool {
	return s <= SizeExactSmall || s == SizeExactLarge
}

// Small returns whether the SizeClass
// is considered "small."
//
// Whether or not a trace produces a "small"
// output set determines how certain query
// planning optimizations are performed.
func (s SizeClass) Small() bool {
	return s <= SizeColumnCardinality
}

// Class returns the "size class" of the
// result of executing the trace.
// See SizeClass for the definition
// of each result size class.
func (b *Trace) Class() SizeClass {
	cur := SizeUnknown
	for step := b.top; step != nil; step = step.parent() {
		// for each step, determine
		// if we've found a smaller output cardinality
		// than the current one
		next := SizeUnknown
		switch step := step.(type) {
		case *Limit:
			if step.Count >= LargeSize {
				next = SizeExactLarge
			} else if step.Count > 1 {
				next = SizeExactSmall
			} else if step.Count == 1 {
				next = SizeOne
			} else {
				return SizeZero
			}
		case NoOutput:
			return SizeZero
		case DummyOutput:
			next = SizeOne
		case *Aggregate:
			if step.GroupBy == nil {
				next = SizeOne
			} else {
				next = SizeColumnCardinality
			}
		case *Distinct:
			next = SizeColumnCardinality
		}
		if next < cur {
			cur = next
		}
	}
	return cur
}
