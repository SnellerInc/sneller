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

package plan

import (
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
)

func TestMultiIndex(t *testing.T) {
	base := date.Now()
	now := func(d int) date.Time {
		return base.Add(time.Duration(d) * time.Hour)
	}
	cases := []struct {
		in       multiIndex
		min, max date.Time
		ok       bool
	}{{
		// no indexes
		in: nil,
		ok: false,
	}, {
		// one index
		in: multiIndex{
			timeIndex{now(-1), now(1)},
		},
		min: now(-1),
		max: now(1),
		ok:  true,
	}, {
		// multi indexes
		in: multiIndex{
			timeIndex{now(-2), now(-1)},
			timeIndex{now(0), now(1)},
			timeIndex{now(1), now(2)},
		},
		min: now(-2),
		max: now(2),
		ok:  true,
	}}
	p := &expr.Path{First: "foo"}
	for i, c := range cases {
		min, max, ok := cases[i].in.TimeRange(p)
		if min != c.min || max != c.max || ok != c.ok {
			t.Errorf("test %d: mismatch", i)
			t.Error("\tgot:  ", min, max, ok)
			t.Error("\twant: ", c.min, c.max, c.ok)
		}
	}
}

type timeIndex struct {
	min, max date.Time
}

func (h timeIndex) TimeRange(*expr.Path) (min, max date.Time, ok bool) {
	return h.min, h.max, true
}
