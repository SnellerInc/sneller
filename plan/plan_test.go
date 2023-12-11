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

package plan

import (
	"slices"
	"testing"

	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func mksized(size int64) *Input {
	return &Input{
		Descs: []Descriptor{{
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{Size: size},
			},
		}},
	}
}

func TestDistribute(t *testing.T) {
	run := func(in []int64, val int, out []int) {
		t.Helper()
		parts := make([]tablePart, len(in))
		for i := range parts {
			parts[i].contents = mksized(in[i])
		}
		result := distribute(parts, val)
		if !slices.Equal(result, out) {
			t.Errorf("got %v want %v", result, out)
		}
	}

	run([]int64{0, 0, 0}, 3, []int{1, 1, 1})
	run([]int64{100, 200}, 3, []int{1, 2})
	run([]int64{100, 200}, 4, []int{1, 3})
	run([]int64{100, 200, 300}, 6, []int{1, 2, 3})
	run([]int64{100, 200, 300}, 12, []int{2, 4, 6})
	run([]int64{100, 200, 300}, 13, []int{2, 4, 7})
	run([]int64{102478}, 8, []int{8})
}

func TestNewSplit(t *testing.T) {
	testcases := []struct {
		name  string
		input string
	}{
		{
			name:  "issue 2561",
			input: `SELECT MISSING X, COUNT(*) FILTER (WHERE X)`,
		},
	}

	for i := range testcases {
		tc := &testcases[i]
		t.Run(tc.name, func(t *testing.T) {
			text := []byte(tc.input)
			q, err := partiql.Parse(text)
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewSplit(q, &splitEnv{})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
