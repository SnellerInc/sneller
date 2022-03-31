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
	"reflect"
	"testing"
)

func TestCoalesce(t *testing.T) {
	cases := []struct {
		min     int
		in, out []int
	}{
		{
			min: 3,
			in:  []int{3, 3, 3},
			out: []int{3, 3, 3},
		},
		{
			min: 4,
			in:  []int{3, 3, 3},
			out: []int{9},
		},
		{
			min: 5,
			in:  []int{4, 3, 5},
			out: []int{7, 5},
		},
		{
			min: 9,
			in:  []int{3, 3, 3, 3, 3, 3, 3},
			out: []int{9, 12},
		},
		{
			min: 5,
			in:  []int{10, 3, 10, 3},
			out: []int{10, 16},
		},
		{
			min: 5,
			in:  []int{1, 2, 3, 4, 5},
			out: []int{6, 9},
		},
		{
			min: 5,
			in:  []int{3, 2, 3, 2, 3, 2},
			out: []int{5, 5, 5},
		},
	}
	for i := range cases {
		min := cases[i].min
		in := cases[i].in
		want := cases[i].out
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			blocks := make([]Blockdesc, len(in))
			for i := range in {
				blocks[i].Chunks = in[i]
			}
			got := coalesce(blocks, min)
			out := make([]int, len(got))
			for i := range out {
				out[i] = got[i].Chunks
			}
			if !reflect.DeepEqual(out, want) {
				t.Errorf("got : %v", out)
				t.Errorf("want: %v", want)
			}
		})
	}
}
