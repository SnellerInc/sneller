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

package rules

import (
	"fmt"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		text string
		want []Rule
	}{
		{
			text: ` // some comment text
(x y), "isOkay()" -> z
(x y:("z")) -> (bar baz)
`,
			want: []Rule{
				{
					From: []Value{
						List{
							{Name: "x"},
							{Name: "y"},
						},
						String("\"isOkay()\""),
					},
					To: Term{Name: "z"},
				},
				{
					From: []Value{
						List{
							{Name: "x"},
							{Name: "y", Value: List{{Value: String("\"z\"")}}},
						},
					},
					To: Term{
						Value: List{
							{Name: "bar"},
							{Name: "baz"},
						},
					},
				},
			},
		},
	}

	for i := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			r := strings.NewReader(tests[i].text)
			rules, err := Parse(r)
			if err != nil {
				t.Fatal(err)
			}
			if len(rules) != len(tests[i].want) {
				t.Errorf("got %d rules out; wanted %d", len(rules), len(tests[i].want))
			}
			for j := range rules {
				if j >= len(tests[i].want) {
					break
				}
				if !rules[j].Equal(&tests[i].want[j]) {
					t.Errorf("got  rule %s", rules[j].String())
					t.Errorf("want rule %s", tests[i].want[j].String())
				}
			}
		})
	}
}
