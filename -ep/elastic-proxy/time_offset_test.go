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
