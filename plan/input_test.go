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
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestInputFilter(t *testing.T) {
	var t0, t1, t2 blockfmt.Trailer

	day := func(d int) ion.Datum {
		t := date.Date(2021, 01, d, 0, 0, 0, 0)
		return ion.Timestamp(t)
	}
	mkrange := func(a, b int) blockfmt.Range {
		return blockfmt.NewRange([]string{"timestamp"}, day(a), day(b))
	}

	t0.Sparse.Push([]blockfmt.Range{mkrange(1, 2)})
	t1.Sparse.Push([]blockfmt.Range{mkrange(3, 4)})
	t2.Sparse.Push([]blockfmt.Range{mkrange(5, 6)})

	// exp matches t0 and t2 but not t1
	exp := parseExpr("timestamp <= `2021-01-02T00:00:00Z` OR timestamp >= `2021-01-05T00:00:00Z`")
	orig := &Input{
		Descs: []Descriptor{{
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{Path: "path/0"},
				Trailer:    t0,
			},
			Blocks: ints.Intervals{{0, 1}},
		}, {
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{Path: "path/1"},
				Trailer:    t1,
			},
			Blocks: ints.Intervals{{0, 1}},
		}, {
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{Path: "path/2"},
				Trailer:    t2,
			},
			Blocks: ints.Intervals{{0, 1}},
		}},
	}
	got := orig.Filter(exp)
	want := Input{
		Descs: []Descriptor{orig.Descs[0], orig.Descs[2]},
	}
	if !reflect.DeepEqual(got, &want) {
		t.Logf("got : %#v", got)
		t.Logf("want: %#v", want)
		t.Fatal("not equal")
	}
}
