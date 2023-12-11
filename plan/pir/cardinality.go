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
		case *UnionMap:
			// we're UNION ALL-ing the child result;
			// let's assume we're multiplying the size
			// of the result by a small constant factor
			switch step.Child.Class() {
			case SizeZero:
				return SizeZero
			case SizeOne, SizeColumnCardinality, SizeExactSmall:
				return SizeColumnCardinality
			default:
				return SizeUnknown
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
