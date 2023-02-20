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
