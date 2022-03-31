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

package blockfmt

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTimeUnion(t *testing.T) {
	now := time.Now().UTC()
	timerange := func(path []string, min, max int) *TimeRange {
		return &TimeRange{
			path: path,
			min:  now.Add(time.Duration(min) * time.Hour).UnixMicro(),
			diff: (time.Duration(max-min) * time.Hour).Microseconds(),
		}
	}

	cases := []struct {
		a, b []*TimeRange
		want []*TimeRange
	}{
		// nothing on either side:
		{
			[]*TimeRange{},
			[]*TimeRange{},
			[]*TimeRange{},
		},
		// just one side populated:
		{
			[]*TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
			[]*TimeRange{},
			[]*TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
		},
		// entirely disjoint:
		{
			[]*TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
			},
			[]*TimeRange{
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
			[]*TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
		},
		// partly overlapping:
		{
			[]*TimeRange{
				timerange([]string{"a", "b"}, -1, 1),
			},
			[]*TimeRange{
				timerange([]string{"a", "b"}, -2, 0),
				timerange([]string{"a", "b", "c"}, -2, -1),
			},
			[]*TimeRange{
				timerange([]string{"a", "b"}, -2, 1),
				timerange([]string{"a", "b", "c"}, -2, -1),
			},
		},
		// fully overlapping:
		{
			[]*TimeRange{
				// arranged out-of-order; rely on sorting:
				timerange([]string{"a", "b", "c"}, -2, -1),
				timerange([]string{"a", "b"}, -1, 0),
			},
			[]*TimeRange{
				timerange([]string{"a", "b"}, 0, 1),
				timerange([]string{"a", "b", "c"}, -1, 0),
			},
			[]*TimeRange{
				timerange([]string{"a", "b"}, -1, 1),
				timerange([]string{"a", "b", "c"}, -2, 0),
			},
		},
	}
	text := func(lst []*TimeRange) string {
		var out strings.Builder
		out.WriteByte('[')
		for i := range lst {
			if i != 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(&out, "{ %v, ", lst[i].path)
			fmt.Fprintf(&out, " %q, ", lst[i].MinTime().String())
			fmt.Fprintf(&out, " %q}", lst[i].MaxTime().String())
		}
		out.WriteByte(']')
		return out.String()
	}

	for i := range cases {
		acopy := make([]*TimeRange, len(cases[i].a))
		copy(acopy, cases[i].a)
		bcopy := make([]*TimeRange, len(cases[i].b))
		copy(bcopy, cases[i].b)

		out := union(cases[i].a, cases[i].b)
		if !reflect.DeepEqual(out, cases[i].want) {
			t.Errorf("case %d: got  %v", i, text(out))
			t.Errorf("case %d: want %v", i, text(cases[i].want))
		}
		// union should by symmetric
		out = union(bcopy, acopy)
		if !reflect.DeepEqual(out, cases[i].want) {
			t.Errorf("case %d reversed: got  %v", i, text(out))
			t.Errorf("case %d reversed: want %v", i, text(cases[i].want))
		}
	}
}
