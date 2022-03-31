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

package db

import (
	"sort"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func timeSort(lst []blockfmt.Descriptor) {
	sort.Slice(lst, func(i, j int) bool {
		return lst[i].LastModified.Before(lst[j].LastModified)
	})
}

func (b *Builder) minMergeSize() int64 {
	if b.MinMergeSize > 0 {
		return int64(b.MinMergeSize)
	}
	return DefaultMinMerge
}

// decideMerge takes the list of existing objects
// in an index and decides which ones should be kept as-is
// and which ones should be merged into the current object being written
func (b *Builder) decideMerge(existing []blockfmt.Descriptor) (prepend, merge []blockfmt.Descriptor) {
	// if we haven't picked up sizes yet (?),
	// then don't do merging
	for i := range existing {
		if existing[i].Size == 0 {
			b.logf("skipping merge (missing output size for %s)", existing[i].Path)
			return existing, nil
		}
	}
	// super-simple heuristic for now:
	// group all of the objects below the minimum
	// merge size, and otherwise do nothing
	for i := range existing {
		if existing[i].Size < b.minMergeSize() {
			b.logf("merging %s into new output", existing[i].Path)
			merge = append(merge, existing[i])
		} else {
			prepend = append(prepend, existing[i])
		}
	}
	timeSort(prepend)
	timeSort(merge)
	return prepend, merge
}
