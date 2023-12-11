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
