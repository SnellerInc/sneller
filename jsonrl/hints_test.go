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

package jsonrl

import (
	"fmt"
	"testing"
)

func TestEquivalence(t *testing.T) {
	cases := []struct {
		rulesObj string
		rulesArr string
	}{
		{
			rulesObj: `{
				"path.to.value.a": "int",
				"path.to.value.b": ["bool", "no_index"],
				"path.?"         : "number"
			}`,
			rulesArr: `[
				{ "path": "path.to.value.a", "hints": "int"                },
				{ "path": "path.to.value.b", "hints": ["bool", "no_index"] },
				{ "path": "path.?"         , "hints": "number"             }
			]`,
		},
		{
			rulesObj: `{
				"path.to.?": ["bool", "no_index"],
				"path.?"   : "number",
				"*"        : "ignore"
			}`,
			rulesArr: `[
				{ "path": "path.to.?", "extra": [0, "ignore me"], "hints": ["bool", "no_index"] },
				{ "path": "path.?"   , "extra": [0, "ignore me"], "hints": "number"             },
				{ "path": "*"        , "extra": [0, "ignore me"], "hints": "ignore"             }
			]`,
		},
	}

	for i := range cases {
		test := cases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			hintObj, err := ParseHint([]byte(test.rulesObj))
			if err != nil {
				t.Fatalf("invalid hints: %s", err)
			}

			hintArr, err := ParseHint([]byte(test.rulesArr))
			if err != nil {
				t.Fatalf("invalid hints: %s", err)
			}

			strObj := hintObj.String()
			strArr := hintArr.String()

			if strObj != strArr {
				t.Errorf("hints not equal")
				t.Errorf("obj:\n%v", strObj)
				t.Errorf("arr:\n%v", strArr)
			}
		})
	}
}
