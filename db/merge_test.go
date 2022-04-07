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
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestMerge(t *testing.T) {
	const minMerge = 1024
	b := Builder{
		MinMergeSize: minMerge,
	}

	sizes := func(lst []blockfmt.Descriptor) []int64 {
		if len(lst) == 0 {
			return nil
		}
		out := make([]int64, len(lst))
		for i := range lst {
			out[i] = lst[i].Size
		}
		return out
	}

	cases := []struct {
		in, prepend, merge []int64
	}{
		{in: []int64{minMerge}, prepend: []int64{minMerge}},
		{in: []int64{minMerge - 1}, merge: []int64{minMerge - 1}},
		{in: []int64{minMerge, minMerge - 1}, prepend: []int64{minMerge}, merge: []int64{minMerge - 1}},
		{in: []int64{minMerge, minMerge - 1, minMerge - 2}, prepend: []int64{minMerge}, merge: []int64{minMerge - 1, minMerge - 2}},
		{in: []int64{minMerge * 2, minMerge, minMerge - 1, minMerge - 2}, prepend: []int64{minMerge * 2, minMerge}, merge: []int64{minMerge - 1, minMerge - 2}},
	}

	t0 := date.Now()
	for i := range cases {
		in := cases[i].in
		descs := make([]blockfmt.Descriptor, len(in))
		for i := range descs {
			descs[i].Size = in[i]
			// guarantee that time-sorted results
			// are also size-sorted results
			descs[i].LastModified = t0.Add(time.Second * time.Duration(i))
		}
		gotp, gotm := b.decideMerge(descs)
		if len(gotp)+len(gotm) != len(descs) {
			t.Errorf("case %d: got %d entries back; expected %d", i, len(gotp)+len(gotm), len(descs))
			continue
		}
		psizes := sizes(gotp)
		if !reflect.DeepEqual(psizes, cases[i].prepend) {
			t.Errorf("case %d: got prepend %v; expected %v", i, psizes, cases[i].prepend)
		}
		msizes := sizes(gotm)
		if !reflect.DeepEqual(msizes, cases[i].merge) {
			t.Errorf("case %d: got merge %v; expected %v", i, msizes, cases[i].merge)
		}
	}
}
