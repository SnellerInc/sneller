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

package regexp2

import "slices"

func min(r1, r2 rune) rune {
	if r1 < r2 {
		return r1
	}
	return r2
}

func max(r1, r2 rune) rune {
	if r1 < r2 {
		return r2
	}
	return r1
}

// overlapRange returns the overlap between the two provided ranges, returns true
// if there exists overlap; false otherwise
func overlapRange(range1, range2 symbolRangeT) ([]symbolRangeT, bool) {
	min1, max1 := range1.split()
	min2, max2 := range2.split()

	if (min1 <= max2) && (max1 >= min2) { // overlap
		result := make([]symbolRangeT, 0)

		r1 := min(min1, min2)
		r2 := max(min1, min2)
		r3 := min(max1, max2)
		r4 := max(max1, max2)

		if r1 <= (r2 - 1) {
			result = slices.Insert(result, 0, newSymbolRange(r1, r2-1))
		}
		if r2 <= r3 {
			result = slices.Insert(result, 0, newSymbolRange(r2, r3))
		}
		if (r3 + 1) <= r4 {
			result = slices.Insert(result, 0, newSymbolRange(r3+1, r4))
		}
		return result, true
	}
	return nil, false
}

type charGroupsRange struct {
	data setT[symbolRangeT]
}

func newCharGroupsRange() charGroupsRange {
	return charGroupsRange{newSet[symbolRangeT]()}
}

func (cg *charGroupsRange) add(newRange symbolRangeT) {
	if cg.data.empty() {
		cg.data.insert(newRange)
	} else if !cg.data.contains(newRange) {
		added := false
		for existingRange := range cg.data {
			if existingRange == newRange {
				added = true
				break
			} else {
				if overlap, present := overlapRange(newRange, existingRange); present {
					cg.data.erase(newRange)
					cg.data.erase(existingRange)
					for _, newRange2 := range overlap {
						cg.add(newRange2)
					}
					added = true
					break
				}
			}
		}
		if !added {
			cg.data.insert(newRange)
		}
	}
}

// refactor will refactor the provided symbol range
func (cg *charGroupsRange) refactor(symbolRange symbolRangeT) (*[]symbolRangeT, bool) {
	if cg.data.contains(symbolRange) {
		return nil, false
	}
	min1, max1 := symbolRange.split()

	result := make([]symbolRangeT, 0)

	for existingRange := range cg.data {
		min2, max2 := existingRange.split()
		if (min1 > max2) || (max1 < min2) { //no overlap
		} else {
			result = slices.Insert(result, 0, existingRange)
		}
	}
	if len(result) > 0 {
		return &result, true
	}
	return nil, false
}
