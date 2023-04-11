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
