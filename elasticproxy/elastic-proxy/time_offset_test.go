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

package elastic_proxy

import (
	"encoding/json"
	"testing"
)

func TestTimeOffsets(t *testing.T) {
	data := []struct {
		json     string
		factor   int
		interval string
	}{
		{`"1m"`, 1, "m"},
		{`"+1m"`, 1, "m"},
		{`"-123ms"`, -123, "ms"},
		{`"-123nanos"`, -123, "ns"},
	}
	for _, test := range data {
		t.Run(test.json, func(t *testing.T) {
			var to timeOffset
			err := json.Unmarshal([]byte(test.json), &to)

			if err != nil {
				if test.interval != "" {
					t.Errorf("error parsing %q: %v", test.json, err)
				}
			} else {
				if test.interval == "" {
					t.Errorf("expected error while parsing %q", test.json)
				} else if to.Factor != test.factor || to.Interval != test.interval {
					t.Errorf("parsing %q yielded unexpected result", test.json)
				}
			}
		})
	}
}
