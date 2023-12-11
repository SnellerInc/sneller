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

package plan

import (
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
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
	p := []string{"foo"}
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

func (h timeIndex) HasPartition(string) bool { return false }

func (h timeIndex) TimeRange(p []string) (min, max date.Time, ok bool) {
	return h.min, h.max, true
}
