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
	d, _, err := ion.ReadDatum(st, body)
	if err != nil {
		return err
	}
	return td.decodeSparse(s, d)
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

// test that changing the input bytes out after
// decoding doesn't cause data corruption
func TestSparseCorruption(t *testing.T) {
	var buf ion.Buffer
	var st ion.Symtab
	var got SparseIndex
	consts := ion.NewStruct(&st, []ion.Field{
		{Label: "foo", Datum: ion.String("bar")},
	})
	si := SparseIndex{consts: consts}
	si.Encode(&buf, &st)
	bytes := buf.Bytes()
	err := got.Decode(&st, bytes)
	if err != nil {
		t.Fatal(err)
	}
	// zero out bytes and make sure it didn't
	// corrupt consts
	for i := range bytes {
		bytes[i] = 0
	}
	if !ion.Equal(got.consts.Datum(), consts.Datum()) {
		t.Fatal("consts was corrupted")
	}
}
