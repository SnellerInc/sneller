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
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestSingleColumnSortPlanning(t *testing.T) {
	testcases := []struct {
		spec     mtcSpec
		expected []string
	}{
		{
			spec:     mtcSpec{},
			expected: []string{},
		},
		// all possible types
		{
			spec: mtcSpec{
				nulls:      10,
				bool0:      5,
				bool1:      3,
				zeros:      8,
				negints:    12,
				posints:    9,
				floats:     30,
				timestamps: 2,
				strings:    40},
			expected: []string{
				"null [0,9]",
				"false [10,14]",
				"true [15,17]",
				"float [18,76]", // cast all numbers (8 + 12 + 9 + 30) to float
				"timestamp [77,78]",
				"string [79,118]",
			},
		},
		// only ints
		{
			spec: mtcSpec{
				zeros:   30,
				negints: 40,
				posints: 50},
			expected: []string{
				"negint [0,39]",
				"zero [40,69]",
				"posint [70,119]",
			},
		},
		// no numeric values
		{
			spec: mtcSpec{
				bool0:      10,
				bool1:      20,
				timestamps: 30,
				strings:    40},
			expected: []string{
				"false [0,9]",
				"true [10,29]",
				"timestamp [30,59]",
				"string [60,99]",
			},
		},
	}

	for i := range testcases {
		col := makeTestMixedTypeColumn(testcases[i].spec)
		limit := indicesRange{0, col.Len() - 1}
		plan := planSingleColumnSorting(&col, Ascending, NullsFirst, limit)

		got := []string{}
		for _, action := range plan {
			got = append(got, action.String())
		}

		if !reflect.DeepEqual(got, testcases[i].expected) {
			t.Logf("got %+v", got)
			t.Logf("expected %+v", testcases[i].expected)
			t.Errorf("wrong sequence of actions")
		}
	}
}

func TestSingleColumnSortPlanningDirectionAndNullsOrder(t *testing.T) {
	spec := mtcSpec{
		nulls:      10,
		bool0:      10,
		bool1:      10,
		zeros:      10,
		negints:    10,
		posints:    10,
		floats:     10,
		timestamps: 10,
		strings:    10}

	tc := []struct {
		direction  Direction
		nullsOrder NullsOrder
		expected   []string
	}{
		{
			direction:  Ascending,
			nullsOrder: NullsFirst,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
				"true [20,29]",
				"float [30,69]",
				"timestamp [70,79]",
				"string [80,89]",
			},
		},
		{
			direction:  Ascending,
			nullsOrder: NullsLast,
			expected: []string{
				"false [0,9]",
				"true [10,19]",
				"float [20,59]",
				"timestamp [60,69]",
				"string [70,79]",
				"null [80,89]",
			},
		},
		{
			direction:  Descending,
			nullsOrder: NullsFirst,
			expected: []string{
				"null [0,9]",
				"string [10,19]",
				"timestamp [20,29]",
				"float [30,69]",
				"true [70,79]",
				"false [80,89]",
			},
		},
		{
			direction:  Descending,
			nullsOrder: NullsLast,
			expected: []string{
				"string [0,9]",
				"timestamp [10,19]",
				"float [20,59]",
				"true [60,69]",
				"false [70,79]",
				"null [80,89]",
			},
		},
	}

	for i := range tc {
		col := makeTestMixedTypeColumn(spec)

		// when
		limit := indicesRange{0, col.Len() - 1}
		plan := planSingleColumnSorting(&col, tc[i].direction, tc[i].nullsOrder, limit)

		// then
		got := []string{}
		for _, action := range plan {
			got = append(got, action.String())
		}

		if !reflect.DeepEqual(got, tc[i].expected) {
			t.Logf("got %+v", got)
			t.Logf("expected %+v", tc[i].expected)
			t.Errorf("wrong sequence of actions, direction=%d, nullsOrder=%d",
				tc[i].direction, tc[i].nullsOrder)
		}
	}
}

func TestSingleColumnSortPlanningWithLimits(t *testing.T) {
	spec := mtcSpec{
		nulls:      10,
		bool0:      10,
		bool1:      10,
		zeros:      10,
		negints:    10,
		posints:    10,
		floats:     10,
		timestamps: 10,
		strings:    10,
	}
	col := makeTestMixedTypeColumn(spec)

	testcases := []struct {
		limit    int
		offset   int
		expected []string
	}{
		{
			limit: 90,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
				"true [20,29]",
				"float [30,69]",
				"timestamp [70,79]",
				"string [80,89]",
			},
		},
		{
			limit: 7,
			expected: []string{
				"null [0,9]",
			},
		},
		{
			limit: 19,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
			},
		},
		{
			limit: 27,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
				"true [20,29]",
			},
		},
		{
			limit: 50,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
				"true [20,29]",
				"float [30,69]",
			},
		},
		{
			limit: 71,
			expected: []string{
				"null [0,9]",
				"false [10,19]",
				"true [20,29]",
				"float [30,69]",
				"timestamp [70,79]",
			},
		},
		{
			offset: 25,
			limit:  50,
			expected: []string{
				"true [20,29]",
				"float [30,69]",
				"timestamp [70,79]",
			},
		},
	}

	for i := range testcases {
		limit := indicesRange{testcases[i].offset, testcases[i].offset + testcases[i].limit - 1}
		plan := planSingleColumnSorting(&col, Ascending, NullsFirst, limit)

		got := []string{}
		for _, action := range plan {
			got = append(got, action.String())
		}

		if !reflect.DeepEqual(got, testcases[i].expected) {
			t.Logf("got %+v", got)
			t.Logf("expected %+v", testcases[i].expected)
			t.Errorf("case #%d: wrong sequence of actions", i)
		}
	}
}

// simplifedAsyncConsumer performs only basic thread synchronisation,
// as the testSingleColumnSortNumericCasts sorts few items, so there
// would be at most one thread.
type simplifedAsyncConsumer struct {
	tp ThreadPool
}

func (a *simplifedAsyncConsumer) Notify(start, end int) { a.tp.Close(nil) }
func (a *simplifedAsyncConsumer) Start(pool ThreadPool) { a.tp = pool }

func TestSingleColumnSortNumericCastsNoAVX512Sorter(t *testing.T) {
	threads := 1
	rp := NewRuntimeParameters(threads, WithAVX512Sorter(false))

	testSingleColumnSortNumericCasts(t, &rp)
}

func TestSingleColumnSortNumericCastsAVX512Sorter(t *testing.T) {
	threads := 1
	rp := NewRuntimeParameters(threads, WithAVX512Sorter(true))

	testSingleColumnSortNumericCasts(t, &rp)
}

func testSingleColumnSortNumericCasts(t *testing.T, rp *RuntimeParameters) {
	// given
	var col MixedTypeColumn

	col.floatIndices = []uint64{1, 2, 3, 4}
	col.floatKeys = []float64{0.1, 0.2, 0.3, 0.4}
	col.negintIndices = []uint64{10, 11, 12, 13, 14}
	col.negintKeys = []uint64{1, 2, 3, 4, 5}
	col.zeroIndices = []uint64{20, 21, 22, 23, 24, 25}
	col.posintIndices = []uint64{30, 31, 32}
	col.posintKeys = []uint64{1, 2, 3}

	limit := indicesRange{0, col.Len() - 1}
	plan := planSingleColumnSorting(&col, Ascending, NullsFirst, limit)

	consumer := new(simplifedAsyncConsumer)
	pool := NewThreadPool(1)
	consumer.Start(pool)

	// when
	for _, action := range plan {
		err := action.invoke(consumer, pool, &col, rp)
		if err != nil {
			t.Errorf("Unexpected error %q returned", err)
		}
	}

	pool.Wait()

	// then
	if col.Len() != 18 {
		t.Errorf("wrong size of column %d", col.Len())
	}

	if len(col.zeroIndices) > 0 {
		t.Errorf("field must be empty")
	}

	if len(col.posintIndices) > 0 {
		t.Errorf("field must be empty")
	}

	if len(col.posintKeys) > 0 {
		t.Errorf("field must be empty")
	}

	if len(col.negintIndices) > 0 {
		t.Errorf("field must be empty")
	}

	if len(col.negintKeys) > 0 {
		t.Errorf("field must be empty")
	}

	expectedIndices := []uint64{
		14, 13, 12, 11, 10,
		20, 21, 22, 23, 24, 25, // these may be ordered differently
		1, 2, 3, 4,
		30, 31, 32,
	}

	// sorting is not stable, cannot assume any order for keys eq 0.0
	zeroIndices := make(map[uint64]bool)
	zeroIndices[20] = false
	zeroIndices[21] = false
	zeroIndices[22] = false
	zeroIndices[23] = false
	zeroIndices[24] = false
	zeroIndices[25] = false

	for i := 0; i < len(expectedIndices); i++ {
		expectedIdx := expectedIndices[i]
		idx := col.floatIndices[i]

		seen, ok := zeroIndices[idx]
		if ok {
			if seen {
				t.Logf("got %+v", col.floatIndices)
				t.Logf("expected %+v", expectedIndices)
				t.Fatalf("index of 0 value repeated")
			}
			zeroIndices[idx] = true
		} else if idx != expectedIdx {
			t.Logf("got %+v", col.floatIndices)
			t.Logf("expected %+v", expectedIndices)
			t.Fatalf("indices are wrongly sorted")
		}
	}

	for _, seen := range zeroIndices {
		if !seen {
			t.Logf("got %+v", col.floatIndices)
			t.Logf("expected %+v", expectedIndices)
			t.Fatal("not all indices for key=0.0 are present in the sorted collection")
		}
	}
}

type mtcSpec struct {
	nulls      int
	bool0      int
	bool1      int
	zeros      int
	negints    int
	posints    int
	floats     int
	strings    int
	timestamps int
}

func makeTestMixedTypeColumn(spec mtcSpec) (result MixedTypeColumn) {
	result.nullIndices = make([]uint64, spec.nulls)
	result.falseIndices = make([]uint64, spec.bool0)
	result.trueIndices = make([]uint64, spec.bool1)
	result.zeroIndices = make([]uint64, spec.zeros)
	result.negintIndices = make([]uint64, spec.negints)
	result.posintIndices = make([]uint64, spec.posints)
	result.floatIndices = make([]uint64, spec.floats)
	result.stringIndices = make([]uint64, spec.strings)
	result.timestampIndices = make([]uint64, spec.timestamps)

	return
}

// --------------------------------------------------

func TestSingleColumnSortSortingData(t *testing.T) {
	tc := []struct {
		direction  Direction
		nullsOrder NullsOrder
	}{
		{Ascending, NullsFirst},
		{Ascending, NullsLast},
		{Descending, NullsFirst},
		{Descending, NullsLast},
	}

	specAll := mtcSpec{
		nulls:      10,
		bool0:      20,
		bool1:      30,
		zeros:      40,
		negints:    100,
		posints:    100,
		floats:     100,
		strings:    50,
		timestamps: 30,
	}

	specInts := mtcSpec{
		zeros:   100,
		negints: 100,
		posints: 100,
	}

	specs := []mtcSpec{specAll, specInts}

	for i := range tc {
		for j := range specs {
			testSingleColumnSortSortingData(t, specs[j], tc[i].direction, tc[i].nullsOrder)
		}
	}
}

func testSingleColumnSortSortingData(t *testing.T, spec mtcSpec, direction Direction, nullsOrder NullsOrder) {
	// given
	records := makeTestMixedTypeIonRecords(spec)

	var col MixedTypeColumn
	for i := range records {
		err := col.Add(uint64(i), records[i])
		if err != nil {
			t.Fatalf("unexpected error while adding %s", err)
		}
	}

	rawrecords := make(map[uint32][][]byte)
	rawrecords[0] = records

	if col.Len() != len(records) {
		t.Fatalf("not all %d records were added: column size is %d", len(records), col.Len())
	}

	// when
	var dst bytes.Buffer
	var st ion.Symtab // Note: it's fine to have it empty here, we know we produce bare Ion values, not records
	writer, err := NewRowsWriter(&dst, &st, 32*1024)
	if err != nil {
		t.Fatal(err)
	}
	rp := NewRuntimeParameters(1)
	err = ByColumn(rawrecords, &col, direction, nullsOrder, nil, writer, &rp)
	if err != nil {
		t.Fatalf("unexpected error while sorting %s", err)
	}

	// then
	var prev []byte
	hasprev := false

	var disallowed int
	if direction == Ascending {
		disallowed = 1
	} else {
		disallowed = -1
	}

	iterToplevelIonObjects(dst.Bytes(), func(rec []byte) bool {
		if ion.TypeOf(rec) == ion.AnnotationType {
			return true
		}
		if ion.TypeOf(rec) != ion.StructType {
			t.Fatalf("expected a struct, got %s [% 02x]", ion.TypeOf(rec), rec)
		}

		rec, _ = ion.Contents(rec)

		if hasprev {
			rel := compareIonValues(prev, rec, direction, nullsOrder)
			if rel == disallowed {
				t.Logf("direction=%d, nullsOrder=%d, relation=%d", direction, nullsOrder, rel)
				t.Fatalf("values not sorted: prev=% 02x and record=% 02x", prev, rec)
				return false
			}
		}

		hasprev = true
		prev = rec

		return true
	})
}

func iterToplevelIonObjects(buf []byte, callback func([]byte) bool) error {
	for len(buf) > 0 {
		if ion.IsBVM(buf) {
			buf = buf[4:]
			continue
		}

		size := ion.SizeOf(buf)
		if size < 0 {
			return fmt.Errorf("Wrong Ion structure")
		}

		if !callback(buf[:size]) {
			return nil
		}
		buf = buf[size:]
	}

	return nil
}

func makeTestMixedTypeIonRecords(spec mtcSpec) [][]byte {
	rand.Seed(0) // make the results repeatable

	ionNull := []byte{0x0f}
	ionIntZero := []byte{0x20}
	ionFloatZero := []byte{0x40}
	ionBoolFalse := []byte{0x10}
	ionBoolTrue := []byte{0x11}

	var records [][]byte
	var buf ion.Buffer

	for i := 0; i < spec.nulls; i++ {
		records = append(records, ionNull)
	}

	for i := 0; i < spec.bool0; i++ {
		records = append(records, ionBoolFalse)
	}

	for i := 0; i < spec.bool1; i++ {
		records = append(records, ionBoolTrue)
	}

	for i := 0; i < spec.zeros; i++ {
		if rand.Float32() > 0.5 {
			records = append(records, ionFloatZero)
		} else {
			records = append(records, ionIntZero)
		}
	}

	add := func() {
		c := make([]byte, len(buf.Bytes()))
		copy(c, buf.Bytes())

		records = append(records, c)
	}

	for i := 0; i < spec.negints; i++ {
		buf.Reset()
		buf.WriteInt(-rand.Int63())

		add()
	}

	for i := 0; i < spec.posints; i++ {
		buf.Reset()
		buf.WriteUint(rand.Uint64())

		add()
	}

	for i := 0; i < spec.floats; i++ {
		buf.Reset()
		if rand.Float32() > 0.5 {
			buf.WriteFloat32(rand.Float32())
		} else {
			buf.WriteFloat64(rand.Float64())
		}

		add()
	}

	randString := func() string {
		var s string
		n := rand.Intn(32)
		for i := 0; i < n; i++ {
			c := rand.Intn('z' - 'a' + 1)
			s += string(rune(c + 'a'))
		}

		return s
	}

	for i := 0; i < spec.strings; i++ {
		buf.Reset()
		buf.WriteString(randString())

		add()
	}

	for i := 0; i < spec.timestamps; i++ {
		buf.Reset()
		year := 2010 + rand.Intn(30)
		month := 1 + rand.Intn(13)
		day := 1 + rand.Intn(30)
		hour := rand.Intn(60)
		min := rand.Intn(60)
		sec := 0
		nsec := 0
		buf.WriteTime(date.Date(year, month, day, hour, min, sec, nsec))

		add()
	}

	rand.Shuffle(len(records), func(i, j int) { records[i], records[j] = records[j], records[i] })

	return records
}
