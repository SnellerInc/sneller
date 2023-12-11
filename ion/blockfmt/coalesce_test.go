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
			blocks := make([]blockpart, len(in))
			for i := range in {
				blocks[i].chunks = in[i]
			}
			got := coalesce(blocks, min)
			out := make([]int, len(got))
			for i := range out {
				out[i] = got[i].chunks
			}
			if !reflect.DeepEqual(out, want) {
				t.Errorf("got : %v", out)
				t.Errorf("want: %v", want)
			}
		})
	}
}
