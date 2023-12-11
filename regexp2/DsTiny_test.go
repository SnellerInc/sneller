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

package regexp2

import (
	"fmt"
	"testing"
)

func TestCreateCharGroupMap(t *testing.T) {

	type unitTest struct {
		input  [][]symbolRangeIDT
		output []tupleT
	}

	testCases := []unitTest{
		{
			input:  [][]symbolRangeIDT{{0}},
			output: []tupleT{{0, 1}},
		},
		{
			input:  [][]symbolRangeIDT{{0}, {1}},
			output: []tupleT{{0, 1}, {1, 2}},
		},
		{
			input:  [][]symbolRangeIDT{{0, 1}, {1}},
			output: []tupleT{{0, 1}, {1, 2}},
		},
		{
			input:  [][]symbolRangeIDT{{0, 1}},
			output: []tupleT{{0, 1}, {1, 1}},
		},
		{
			input:  [][]symbolRangeIDT{{0, 1, 2}, {2}},
			output: []tupleT{{0, 1}, {1, 1}, {2, 2}},
		},
		{
			input:  [][]symbolRangeIDT{{0}, {0, 1, 2, 3}},
			output: []tupleT{{0, 1}, {1, 2}, {2, 2}, {3, 2}},
		},
		{
			input:  [][]symbolRangeIDT{{0, 1}, {0, 1, 2}, {2, 3}},
			output: []tupleT{{0, 1}, {1, 1}, {2, 2}, {3, 3}},
		},
		{
			input:  [][]symbolRangeIDT{{1, 2, 3, 4}, {5}, {1, 2, 3, 5}, {1, 2, 3, 6}},
			output: []tupleT{{1, 1}, {2, 1}, {3, 1}, {4, 2}, {5, 3}, {6, 4}},
		},
	}

	run := func(ut *unitTest) {

		printTuple := func(obs, exp []tupleT) string {
			return fmt.Sprintf("\nobs %v\nexp %v", obs, exp)
		}

		obs := compressGroups(ut.input)
		exp := ut.output

		if len(obs) != len(exp) {
			t.Error(printTuple(obs, exp))
			return
		}

		for i, tupObs := range obs {
			tupExp := exp[i]
			if tupObs.groupID != tupExp.groupID {
				t.Error(printTuple(obs, exp))
				return
			}
			if tupObs.symbolRangeID != tupExp.symbolRangeID {
				t.Error(printTuple(obs, exp))
				return
			}
		}
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf(`case %d:`, i), func(t *testing.T) {
			run(&tc)
		})
	}
}
