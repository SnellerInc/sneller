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
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func testSparseRoundtrip(t *testing.T, si *SparseIndex) {
	var buf ion.Buffer
	var st ion.Symtab
	var cmp SparseIndex

	si.Encode(&buf, &st)
	err := cmp.Decode(&st, buf.Bytes())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if !reflect.DeepEqual(si, &cmp) {
		t.Helper()
		t.Fatal("result not equivalent")
	}
}

func TestSparseIndex(t *testing.T) {
	var si SparseIndex

	testSparseRoundtrip(t, &si)

	start := date.Now().Truncate(time.Microsecond)
	next := start.Add(time.Minute)

	fr := &futureRange2{}

	fr.SetMinMax([]string{"x"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.SetMinMax([]string{"x", "y"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.SetMinMax([]string{"a", "y"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.commit()

	if tr := fr.result.Get([]string{"x"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get(x) == nil")
	}
	if tr := fr.result.Get([]string{"x", "y"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get([x, y]) == nil")
	}
	if tr := fr.result.Get([]string{"a", "y"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get([a, y]) == nil")
	}
	testSparseRoundtrip(t, &si)

	// test that *not* adding a.y and also adding b.y
	// still leads to the correct result whereby both
	// are extended to the correct intervals
	start = start.Add(2 * time.Minute)
	next = start.Add(time.Minute)
	fr.SetMinMax([]string{"x"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.SetMinMax([]string{"x", "y"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.SetMinMax([]string{"b", "y"}, ion.Timestamp(start), ion.Timestamp(next))
	fr.commit()
	if tr := fr.result.Get([]string{"x"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get(x) == nil")
	}
	if tr := fr.result.Get([]string{"x", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([x, y]) == nil")
	}
	if tr := fr.result.Get([]string{"a", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([a, y]) == nil")
	}
	if tr := fr.result.Get([]string{"b", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([a, y]) == nil")
	}
	testSparseRoundtrip(t, &si)
}
