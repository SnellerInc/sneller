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

package sorting

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestKtopAddAndCaptureAscendingOrder(t *testing.T) {
	limit := 5
	ktop := NewKtop(limit,
		[]Ordering{{NullsFirst, Ascending}})

	// 1. add records to struct
	records := []struct {
		id, num int64
		added   bool
	}{
		// records are added as they are going until the limit is reached
		{1, 100, true}, // ktop: [(1, 100)]
		{2, 1, true},   // ktop: [(1, 100), (2, 1)]
		{3, 50, true},  // ktop: [(1, 100), (2, 1), (3, 50)]
		{4, 101, true}, // ktop: [(1, 100), (2, 1), (3, 50), (4, 101)]
		{5, 5, true},   // ktop: [(1, 100), (2, 1), (3, 50), (4, 101), (5, 5)]
		// limit reached
		{6, 102, false}, // ktop: no changes (102 >= max=101)
		{7, 6, true},    // ktop: [(1, 100), (2, 1), (3, 50), (5, 5), (7, 6)]
		{8, 3, true},    // ktop: [(2, 1), (3, 50), (5, 5), (7, 6), (8, 3)]
		{9, 50, false},  // ktop: no changes (50 >= max=50)
		{10, 20, true},  // ktop: [(2, 1), (5, 5), (7, 6), (8, 3), (10, 20)]
	}

	var buf ion.Buffer
	var st ion.Symtab
	idSym := st.Intern("id")
	numSym := st.Intern("num")

	raw := make([]byte, 1024)
	fields := make([][2]uint32, 1)

	for i := range records {
		buf.Reset()
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(records[i].id)
		buf.BeginField(numSym)
		ofsNum := len(buf.Bytes()) - 1
		buf.WriteInt(records[i].num)
		sizeNum := len(buf.Bytes()) - 1 - ofsNum
		buf.EndStruct()

		// sort by num
		fields[0][0] = uint32(ofsNum)
		fields[0][1] = uint32(sizeNum)
		copy(raw, buf.Bytes())

		rec := IonRecord{
			Raw:         raw[:len(buf.Bytes())],
			FieldDelims: fields,
		}

		added := ktop.Add(&rec)
		if added != records[i].added {
			t.Logf("record %d: %+v", i, records[i])
			t.Logf("expected: %v", records[i].added)
			t.Logf("got: %v", added)
			t.Errorf("wrong result from Add")
		}
	}

	// 2. capture
	sorted := ktop.Capture()
	buf.Reset()
	buf.StartChunk(&st)
	for i := range sorted {
		// Note: we don't have boxed values in the test, thus Raw == Ion data
		buf.UnsafeAppend(sorted[i].Raw)
		if &sorted[i].Raw[0] == &raw[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
		if &sorted[i].FieldDelims[0] == &fields[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
	}

	expectedRows := []string{
		`{"id": 2, "num": 1}`,
		`{"id": 8, "num": 3}`,
		`{"id": 5, "num": 5}`,
		`{"id": 7, "num": 6}`,
		`{"id": 10, "num": 20}`,
	}

	testCompareRecords(t, buf.Bytes(), &st, expectedRows)
}

func TestKtopAddAndCaptureDescendingOrder(t *testing.T) {
	limit := 5
	ktop := NewKtop(limit, []Ordering{{NullsFirst, Descending}})

	// 1. add records to struct
	records := []struct {
		id, num int64
		added   bool
	}{
		// records are added as they are going until the limit is reached
		{1, 100, true}, // ktop: [(1, 100)]
		{2, 1, true},   // ktop: [(1, 100), (2, 1)]
		{3, 50, true},  // ktop: [(1, 100), (2, 1), (3, 50)]
		{4, 101, true}, // ktop: [(1, 100), (2, 1), (3, 50), (4, 101)]
		{5, 5, true},   // ktop: [(1, 100), (2, 1), (3, 50), (4, 101), (5, 5)]
		// limit reached
		{6, 102, true},  // ktop: [(1, 100), (3, 50), (4, 101), (5, 5), (6, 102)]
		{7, 6, true},    // ktop: [(1, 100), (3, 50), (4, 101), (6, 102), (7, 6)]
		{8, 3, false},   // ktop: no changes
		{9, 50, true},   // ktop: [(1, 100), (3, 50), (4, 101), (6, 102), (9, 50)]
		{10, 20, false}, // ktop: no changes
	}

	var buf ion.Buffer
	var st ion.Symtab
	idSym := st.Intern("id")
	numSym := st.Intern("num")

	raw := make([]byte, 1024)
	fields := make([][2]uint32, 1)

	for i := range records {
		buf.Reset()
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(records[i].id)
		buf.BeginField(numSym)
		ofsNum := len(buf.Bytes()) - 1
		buf.WriteInt(records[i].num)
		sizeNum := len(buf.Bytes()) - 1 - ofsNum
		buf.EndStruct()

		// sort by num
		fields[0][0] = uint32(ofsNum)
		fields[0][1] = uint32(sizeNum)
		copy(raw, buf.Bytes())

		rec := IonRecord{
			Raw:         raw[:len(buf.Bytes())],
			FieldDelims: fields,
		}

		added := ktop.Add(&rec)
		if added != records[i].added {
			t.Logf("record %d: %+v", i, records[i])
			t.Logf("expected: %v", records[i].added)
			t.Logf("got: %v", added)
			t.Errorf("wrong result from Add")
		}
	}

	// 2. capture
	sorted := ktop.Capture()
	buf.Reset()
	buf.StartChunk(&st)
	for i := range sorted {
		// Note: we don't have boxed values in the test, thus Raw == Ion data
		buf.UnsafeAppend(sorted[i].Raw)
		if &sorted[i].Raw[0] == &raw[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
		if &sorted[i].FieldDelims[0] == &fields[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
	}

	expectedRows := []string{
		`{"id": 6, "num": 102}`,
		`{"id": 4, "num": 101}`,
		`{"id": 1, "num": 100}`,
		`{"id": 9, "num": 50}`,
		`{"id": 3, "num": 50}`,
	}

	testCompareRecords(t, buf.Bytes(), &st, expectedRows)
}

func TestKtopMerge(t *testing.T) {
	limit := 3
	ktop1 := NewKtop(limit, []Ordering{{NullsFirst, Ascending}})
	ktop2 := NewKtop(limit, []Ordering{{NullsFirst, Ascending}})

	// 1. add records to struct
	records := []struct {
		id, num int64
		first   bool
	}{
		// ktop1: [1, 3, 5]
		{1, 1, true},
		{2, 3, true},
		{3, 5, true},
		{4, 7, true},
		{5, 9, true},

		// ktop2: [2, 4, 6]
		{10, 2, false},
		{11, 4, false},
		{12, 6, false},
		{13, 8, false},
		{14, 10, false},
	}

	var buf ion.Buffer
	var st ion.Symtab
	idSym := st.Intern("id")
	numSym := st.Intern("num")

	raw := make([]byte, 1024)
	fields := make([][2]uint32, 1)

	for i := range records {
		buf.Reset()
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(records[i].id)
		buf.BeginField(numSym)
		ofsNum := len(buf.Bytes()) - 1
		buf.WriteInt(records[i].num)
		sizeNum := len(buf.Bytes()) - 1 - ofsNum
		buf.EndStruct()

		// sort by num
		fields[0][0] = uint32(ofsNum)
		fields[0][1] = uint32(sizeNum)
		copy(raw, buf.Bytes())

		rec := IonRecord{
			Raw:         raw[:len(buf.Bytes())],
			FieldDelims: fields,
		}

		if records[i].first {
			ktop1.Add(&rec)
		} else {
			ktop2.Add(&rec)
		}
	}

	ktop1.Merge(ktop2)

	// 2. capture
	sorted := ktop1.Capture()
	buf.Reset()
	buf.StartChunk(&st)
	for i := range sorted {
		// Note: we don't have boxed values in the test, thus Raw == Ion data
		buf.UnsafeAppend(sorted[i].Raw)
		if &sorted[i].Raw[0] == &raw[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
		if &sorted[i].FieldDelims[0] == &fields[0] {
			t.Errorf("record %d was not copied, it referes the shared buffer", i)
		}
	}

	expectedRows := []string{
		`{"id": 1, "num": 1}`,
		`{"id": 10, "num": 2}`,
		`{"id": 2, "num": 3}`,
	}

	testCompareRecords(t, buf.Bytes(), &st, expectedRows)
}

func testCompareRecords(t *testing.T, bytes []byte, st *ion.Symtab, expectedRows []string) {
	for i, expected := range expectedRows {
		if len(bytes) == 0 {
			t.Fatalf("couldn't read row #%d: not enough data", i)
		}

		row, rest, err := ion.ReadDatum(st, bytes)
		if err != nil {
			t.Fatalf("couldn't read row #%d: %s", i, err)
		}
		bytes = rest

		want, err := ion.FromJSON(st, json.NewDecoder(strings.NewReader(expected)))
		if err != nil {
			t.Fatalf("string #%d %q is not JSON: %s", i, expected, err)
		}

		if !reflect.DeepEqual(row, want) {
			t.Errorf("row #%d", i)
			t.Errorf("got : %#v", row)
			t.Errorf("want: %#v", want)
		}
	}
}
