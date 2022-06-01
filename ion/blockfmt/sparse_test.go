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

func (s *SparseIndex) Decode(st *ion.Symtab, body []byte) error {
	var td TrailerDecoder
	td.Symbols = st
	return td.decodeSparse(s, body)
}

func TestSparseIndex(t *testing.T) {
	var si SparseIndex

	testSparseRoundtrip(t, &si)

	start := date.Now().Truncate(time.Microsecond)
	next := start.Add(time.Minute)

	si.push([]string{"x"}, start, next)
	si.push([]string{"x", "y"}, start, next)
	si.push([]string{"a", "y"}, start, next)
	si.bump()

	if tr := si.Get([]string{"x"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get(x) == nil")
	}
	if tr := si.Get([]string{"x", "y"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get([x, y]) == nil")
	}
	if tr := si.Get([]string{"a", "y"}); tr == nil || tr.Blocks() != 1 {
		t.Error("Get([a, y]) == nil")
	}
	testSparseRoundtrip(t, &si)

	// test that *not* adding a.y and also adding b.y
	// still leads to the correct result whereby both
	// are extended to the correct intervals
	start = start.Add(2 * time.Minute)
	next = start.Add(time.Minute)
	si.push([]string{"x"}, start, next)
	si.push([]string{"x", "y"}, start, next)
	si.push([]string{"b", "y"}, start, next)
	si.bump()
	if tr := si.Get([]string{"x"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get(x) == nil")
	}
	if tr := si.Get([]string{"x", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([x, y]) == nil")
	}
	if tr := si.Get([]string{"a", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([a, y]) == nil")
	}
	if tr := si.Get([]string{"b", "y"}); tr == nil || tr.Blocks() != 2 {
		t.Error("Get([a, y]) == nil")
	}
	testSparseRoundtrip(t, &si)
}
