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

func TestCalendarIntervals(t *testing.T) {
	data := []struct {
		json     string
		interval string
	}{
		{`"1m"`, "m"},
		{`"minute"`, "m"},
		{`"23h"`, ""},
		{`"123ms"`, ""},
	}
	for _, test := range data {
		t.Run(test.json, func(t *testing.T) {
			var ci calendarInterval
			err := json.Unmarshal([]byte(test.json), &ci)

			if err != nil {
				if test.interval != "" {
					t.Errorf("error parsing %q: %v", test.json, err)
				}
			} else {
				if test.interval == "" {
					t.Errorf("expected error while parsing %q", test.json)
				} else if string(ci) != test.interval {
					t.Errorf("parsing %q yielded unexpected result", test.json)
				}
			}
		})
	}
}
