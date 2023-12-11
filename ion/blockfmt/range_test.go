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

package blockfmt

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func TestTimeUnion(t *testing.T) {
	now := date.Now()
	timerange := func(path []string, min, max int) TimeRange {
		return TimeRange{
			path: path,
			min:  now.Add(time.Duration(min) * time.Hour),
			max:  now.Add(time.Duration(max) * time.Hour),
		}
	}

	cases := []struct {
		a, b []TimeRange
		want []TimeRange
	}{
		// nothing on either side:
		{
			[]TimeRange{},
			[]TimeRange{},
			[]TimeRange{},
		},
		// just one side populated:
		{
			[]TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
			[]TimeRange{},
			[]TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
		},
		// entirely disjoint:
		{
			[]TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
			},
			[]TimeRange{
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
			[]TimeRange{
				timerange([]string{"a", "b", "c"}, -1, 1),
				timerange([]string{"d", "e", "f"}, -2, -1),
			},
		},
		// partly overlapping:
		{
			[]TimeRange{
				timerange([]string{"a", "b"}, -1, 1),
			},
			[]TimeRange{
				timerange([]string{"a", "b"}, -2, 0),
				timerange([]string{"a", "b", "c"}, -2, -1),
			},
			[]TimeRange{
				timerange([]string{"a", "b"}, -2, 1),
				timerange([]string{"a", "b", "c"}, -2, -1),
			},
		},
		// fully overlapping:
		{
			[]TimeRange{
				// arranged out-of-order; rely on sorting:
				timerange([]string{"a", "b", "c"}, -2, -1),
				timerange([]string{"a", "b"}, -1, 0),
			},
			[]TimeRange{
				timerange([]string{"a", "b"}, 0, 1),
				timerange([]string{"a", "b", "c"}, -1, 0),
			},
			[]TimeRange{
				timerange([]string{"a", "b"}, -1, 1),
				timerange([]string{"a", "b", "c"}, -2, 0),
			},
		},
	}
	text := func(lst []TimeRange) string {
		var out strings.Builder
		out.WriteByte('[')
		for i := range lst {
			if i != 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(&out, "{ %v, ", lst[i].path)
			fmt.Fprintf(&out, " %q, ", lst[i].min.String())
			fmt.Fprintf(&out, " %q}", lst[i].max.String())
		}
		out.WriteByte(']')
		return out.String()
	}

	for i := range cases {
		acopy := make([]TimeRange, len(cases[i].a))
		copy(acopy, cases[i].a)
		bcopy := make([]TimeRange, len(cases[i].b))
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
