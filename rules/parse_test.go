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
			text: `(rx:"^x.*y$" foo 0) -> foo`,
			want: []Rule{
				{
					From: []Value{List{{Name: "rx", Value: String("^x.*y$")}, {Name: "foo"}, {Value: Int(0)}}},
					To:   Term{Name: "foo"},
				},
			},
		},
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
						String("isOkay()"),
					},
					To: Term{Name: "z"},
				},
				{
					From: []Value{
						List{
							{Name: "x"},
							{Name: "y", Value: List{{Value: String("z")}}},
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
