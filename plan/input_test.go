// Copyright (C) 2023 Sneller, Inc.
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
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
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
		Descs: []blockfmt.Descriptor{
			{ObjectInfo: blockfmt.ObjectInfo{Path: "path/0"}, Trailer: t0},
			{ObjectInfo: blockfmt.ObjectInfo{Path: "path/1"}, Trailer: t1},
			{ObjectInfo: blockfmt.ObjectInfo{Path: "path/2"}, Trailer: t2},
		},
		Blocks: []blockfmt.Block{
			{Index: 0, Offset: 0},
			{Index: 1, Offset: 0},
			{Index: 2, Offset: 0},
		},
	}
	got := orig.Filter(exp)
	want := Input{
		Descs: []blockfmt.Descriptor{orig.Descs[0], orig.Descs[2]},
		Blocks: []blockfmt.Block{
			{Index: 0, Offset: 0},
			{Index: 1, Offset: 0},
		},
	}
	if !reflect.DeepEqual(got, &want) {
		t.Logf("got : %#v", got)
		t.Logf("want: %#v", want)
		t.Fatal("not equal")
	}
}
