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

package vm

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/sorting"
)

func TestSortSingleColumnAscendingNullsFirst(t *testing.T) {
	// Note: we don't include the ids of rows, as some keys repeat and
	// a sorting algorithm may order them randomly
	expected := []string{
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"false",
		"false",
		"true",
		"true",
		"-105",
		"-5",
		"-1.500000",
		"0",
		"0",
		"0",
		"0.500000",
		"15",
		"42",
		"200",
		"999999",
		"'aaaa!'",
		"'bat'",
		"'elephant'",
		"'kitten'",
		"'the Answer'",
	}

	testSingleColumnSorting(t, sorting.Ascending, sorting.NullsFirst, expected)
}

func TestSortSingleColumnAscendingNullsLast(t *testing.T) {
	// Note: we don't include the ids of rows, as some keys repeat and
	// a sorting algorithm may order them randomly
	expected := []string{
		"false",
		"false",
		"true",
		"true",
		"-105",
		"-5",
		"-1.500000",
		"0",
		"0",
		"0",
		"0.500000",
		"15",
		"42",
		"200",
		"999999",
		"'aaaa!'",
		"'bat'",
		"'elephant'",
		"'kitten'",
		"'the Answer'",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
	}

	testSingleColumnSorting(t, sorting.Ascending, sorting.NullsLast, expected)
}

func TestSortSingleColumnDescendingNulls_first(t *testing.T) {
	// Note: we don't include the ids of rows, as some keys repeat and
	// a sorting algorithm may order them randomly
	expected := []string{
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"'the Answer'",
		"'kitten'",
		"'elephant'",
		"'bat'",
		"'aaaa!'",
		"999999",
		"200",
		"42",
		"15",
		"0.500000",
		"0",
		"0",
		"0",
		"-1.500000",
		"-5",
		"-105",
		"true",
		"true",
		"false",
		"false",
	}

	testSingleColumnSorting(t, sorting.Descending, sorting.NullsFirst, expected)
}

func TestSortSingleColumnDescendingNullsLast(t *testing.T) {
	// Note: we don't include the ids of rows, as some keys repeat and
	// a sorting algorithm may order them randomly
	expected := []string{
		"'the Answer'",
		"'kitten'",
		"'elephant'",
		"'bat'",
		"'aaaa!'",
		"999999",
		"200",
		"42",
		"15",
		"0.500000",
		"0",
		"0",
		"0",
		"-1.500000",
		"-5",
		"-105",
		"true",
		"true",
		"false",
		"false",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
	}

	testSingleColumnSorting(t, sorting.Descending, sorting.NullsLast, expected)
}

func testSingleColumnSorting(t *testing.T, direction sorting.Direction, nullsOrder sorting.NullsOrder, expected []string) {
	// given
	input, err := singleColumnTestIon()
	if err != nil {
		t.Fatal(err)
	}

	orderBy := []SortColumn{
		SortColumn{Node: parsePath("key"), Direction: direction, Nulls: nullsOrder},
	}

	const parallelism = 1

	output := new(bytes.Buffer)
	sorter := NewOrder(output, orderBy, nil, parallelism)

	// when
	err = CopyRows(sorter, buftbl(input), 1)
	if err != nil {
		t.Fatal(err)
	}

	err = sorter.Close()
	if err != nil {
		t.Fatal(err)
	}

	// then
	compareIonWithExpectations(t, output.Bytes(), expected)
}

func singleColumnTestIon() (result []byte, err error) {
	var buf ion.Buffer
	var st ion.Symtab

	idSym := st.Intern("id")
	keySym := st.Intern("key")
	var id int64 = 0

	writeInt := func(x int) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteInt(int64(x))
		buf.EndStruct()
	}

	writeUint := func(x int) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteUint(uint64(x))
		buf.EndStruct()
	}

	writeFloat := func(x float64) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteFloat64(x)
		buf.EndStruct()
	}

	writeNull := func() {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteNull()
		buf.EndStruct()
	}

	writeBool := func(b bool) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteBool(b)
		buf.EndStruct()
	}

	writeString := func(s string) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteString(s)
		buf.EndStruct()
	}

	buf.StartChunk(&st)
	writeNull()
	writeBool(true)
	writeString("kitten")
	writeFloat(0.5)
	writeNull()
	writeBool(false)
	writeUint(0)
	writeUint(999999)
	writeUint(15)
	writeNull()
	writeBool(true)
	writeInt(-5)
	writeUint(200)
	writeNull()
	writeUint(0)
	writeBool(false)
	writeNull()
	writeInt(-105)
	writeString("elephant")
	writeUint(0)
	writeString("the Answer")
	writeUint(42)
	writeString("bat")
	writeNull()
	writeFloat(-1.5)
	writeString("aaaa!")

	return buf.Bytes(), nil
}

// --------------------------------------------------

func TestSortMultipleColumnsCase1(t *testing.T) {
	orderBy := []SortColumn{
		SortColumn{Node: parsePath("name1"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("coef"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("num"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("flag"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
	}

	// note: ORDER BY name, coef, num, flag
	expected := []string{
		"'Ann', 0.300000, false, 42",
		"'Ann', 1.400000, true, 761",
		"'John', 0.700000, true, 89",
		"'John', 0.800000, true, 42",
		"'John', 1.500000, true, 11",
		"'Kate', 0.000000, true, 11",
		"'Kate', 0.100000, true, 42",
		"'Kate', 0.800000, false, 11",
		"'Kate', 0.900000, false, 65",
		"'Mark', 0.200000, false, 42",
	}

	testMultiColumnSorting(t, orderBy, expected)
}

func TestSortMultipleColumnsCase2(t *testing.T) {
	orderBy := []SortColumn{
		SortColumn{Node: parsePath("coef"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("name1"), Direction: sorting.Descending, Nulls: sorting.NullsFirst},
	}

	// note: ORDER BY coef, name1
	expected := []string{
		"'Kate', 0.000000, true, 11",
		"'Kate', 0.100000, true, 42",
		"'Mark', 0.200000, false, 42",
		"'Ann', 0.300000, false, 42",
		"'John', 0.700000, true, 89",
		"'Kate', 0.800000, false, 11",
		"'John', 0.800000, true, 42",
		"'Kate', 0.900000, false, 65",
		"'Ann', 1.400000, true, 761",
		"'John', 1.500000, true, 11",
	}

	testMultiColumnSorting(t, orderBy, expected)
}

func TestSortMultipleColumnsCase3(t *testing.T) {
	orderBy := []SortColumn{
		SortColumn{Node: parsePath("num"), Direction: sorting.Descending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("flag"), Direction: sorting.Descending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("coef"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
	}

	// note: ORDER BY num DESC, flag DESC, coef
	expected := []string{
		"'Ann', 1.400000, true, 761",
		"'John', 0.700000, true, 89",
		"'Kate', 0.900000, false, 65",
		"'Kate', 0.100000, true, 42",
		"'John', 0.800000, true, 42",
		"'Mark', 0.200000, false, 42",
		"'Ann', 0.300000, false, 42",
		"'Kate', 0.000000, true, 11",
		"'John', 1.500000, true, 11",
		"'Kate', 0.800000, false, 11",
	}

	testMultiColumnSorting(t, orderBy, expected)
}

func TestSortWithMissingField(t *testing.T) {
	orderBy := []SortColumn{
		SortColumn{Node: parsePath("unknown"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("coef"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
		SortColumn{Node: parsePath("flag"), Direction: sorting.Ascending, Nulls: sorting.NullsFirst},
	}

	// ORDER BY unknown, coef
	expected := []string{
		"'Kate', 0.000000, true, 11",
		"'Kate', 0.100000, true, 42",
		"'Mark', 0.200000, false, 42",
		"'Ann', 0.300000, false, 42",
		"'John', 0.700000, true, 89",
		"'Kate', 0.800000, false, 11",
		"'John', 0.800000, true, 42",
		"'Kate', 0.900000, false, 65",
		"'Ann', 1.400000, true, 761",
		"'John', 1.500000, true, 11",
	}

	testMultiColumnSorting(t, orderBy, expected)
}

func testMultiColumnSorting(t *testing.T, orderBy []SortColumn, expected []string) {
	input, err := multiColumnTestIon()
	if err != nil {
		t.Fatal(err)
	}

	const parallelism = 1

	output := new(bytes.Buffer)
	sorter := NewOrder(output, orderBy, nil, parallelism)

	// when
	err = CopyRows(sorter, buftbl(input), 1)
	if err != nil {
		t.Fatal(err)
	}

	err = sorter.Close()
	if err != nil {
		t.Fatal(err)
	}

	// then
	compareIonWithExpectations(t, output.Bytes(), expected)
}

func multiColumnTestIon() (result []byte, err error) {
	var buf ion.Buffer
	var st ion.Symtab

	idSym := st.Intern("id")
	nameSym := st.Intern("name1") // XXX: there are problems when we use "name", which is a predefined symbol
	coefSym := st.Intern("coef")
	flagSym := st.Intern("flag")
	numSym := st.Intern("num")
	var id int64 = 0

	writeTuple := func(name string, coef float64, flag bool, num int) {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(nameSym)
		buf.WriteString(name)
		buf.BeginField(coefSym)
		buf.WriteFloat64(coef)
		buf.BeginField(flagSym)
		buf.WriteBool(flag)
		buf.BeginField(numSym)
		buf.WriteUint(uint64(num))
		buf.EndStruct()
	}

	buf.StartChunk(&st)
	writeTuple("Kate", 0.0, true, 11)
	writeTuple("John", 0.8, true, 42)
	writeTuple("Mark", 0.2, false, 42)
	writeTuple("Ann", 0.3, false, 42)
	writeTuple("Kate", 0.8, false, 11)
	writeTuple("John", 1.5, true, 11)
	writeTuple("Kate", 0.9, false, 65)
	writeTuple("John", 0.7, true, 89)
	writeTuple("Ann", 1.4, true, 761)
	writeTuple("Kate", 0.1, true, 42)

	return buf.Bytes(), nil
}

// --------------------------------------------------

func TestSortWithLimit(t *testing.T) {
	orderBy := []SortColumn{SortColumn{Node: parsePath("key"),
		Direction: sorting.Ascending,
		Nulls:     sorting.NullsFirst}}

	input, err := limitTestIon(100000)
	if err != nil {
		t.Fatal(err)
	}

	limit := sorting.Limit{Kind: sorting.LimitToRange, Offset: 2000, Limit: 15}

	const parallelism = 4

	output := new(bytes.Buffer)
	sorter := NewOrder(output, orderBy, &limit, parallelism)

	// when
	err = CopyRows(sorter, buftbl(input), parallelism)
	if err != nil {
		t.Fatal(err)
	}

	err = sorter.Close()
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"2000", "2001", "2002", "2003", "2004",
		"2005", "2006", "2007", "2008", "2009",
		"2010", "2011", "2012", "2013", "2014",
	}

	// then
	compareIonWithExpectations(t, output.Bytes(), expected)
}

func limitTestIon(rowsCount int) (result []byte, err error) {
	var buf ion.Buffer
	var st ion.Symtab

	ints := make([]int, rowsCount)
	for i := range ints {
		ints[i] = i
	}
	rand.Shuffle(rowsCount, func(i, j int) { ints[i], ints[j] = ints[j], ints[i] })

	idSym := st.Intern("id")
	keySym := st.Intern("key")
	var id int64 = 0

	buf.StartChunk(&st)
	for i := range ints {
		buf.BeginStruct(-1)
		buf.BeginField(idSym)
		buf.WriteInt(id)
		id += 1
		buf.BeginField(keySym)
		buf.WriteInt(int64(ints[i]))
		buf.EndStruct()
	}

	return buf.Bytes(), nil
}

// --------------------------------------------------

func parseIonRecords(bytes []byte) (result []string, err error) {
	// we assume id and key fields, where id is always int
	var st ion.Symtab

	bytes, err = st.Unmarshal(bytes)
	if err != nil {
		return
	}

	// do not include record id, just value(s)
	skipSymbol, _ := st.Symbolize("id")

	parseRecord := func(record []byte) ([]string, error) {
		var fields []string
		for len(record) > 0 {
			id, rest, err := ion.ReadLabel(record)
			if err != nil {
				return fields, err
			}

			var field string
			t := ion.TypeOf(rest)
			switch t {
			case ion.NullType:
				field = "null"
				rest = rest[ion.SizeOf(rest):]

			case ion.IntType:
				val, r, err := ion.ReadInt(rest)
				if err != nil {
					return fields, err
				}
				field = fmt.Sprintf("%d", val)
				rest = r

			case ion.UintType:
				val, r, err := ion.ReadUint(rest)
				if err != nil {
					return fields, err
				}
				field = fmt.Sprintf("%d", val)
				rest = r

			case ion.FloatType:
				val, r, err := ion.ReadFloat64(rest)
				if err != nil {
					return fields, err
				}
				field = fmt.Sprintf("%f", val)
				rest = r

			case ion.BoolType:
				val, r, err := ion.ReadBool(rest)
				if err != nil {
					return fields, err
				}
				field = fmt.Sprintf("%v", val)
				rest = r

			case ion.StringType:
				val, r, err := ion.ReadString(rest)
				if err != nil {
					return fields, err
				}
				field = fmt.Sprintf("'%s'", val)
				rest = r

			default:
				return fields, fmt.Errorf("Unsupported type %s", t)
			}

			record = rest
			if skipSymbol != id {
				fields = append(fields, field)
			}
		}

		return fields, nil
	}

	var record []byte
	for len(bytes) > 0 {
		if ion.TypeOf(bytes) != ion.StructType {
			return result, fmt.Errorf("Expected struct got %s", ion.TypeOf(bytes))
		}

		record, bytes = ion.Contents(bytes)
		fields, err := parseRecord(record)
		if err != nil {
			return nil, err
		}

		result = append(result, strings.Join(fields, ", "))
	}

	return
}

func compareIonWithExpectations(t *testing.T, output []byte, expected []string) {
	values, err := parseIonRecords(output)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(values, expected) {
		n := len(expected)
		if len(values) > n {
			n = len(values)
		}

		for i := 0; i < n; i++ {
			v := "<missing>"
			e := "<missing>"

			if i < len(values) {
				v = values[i]
			}

			if i < len(expected) {
				e = expected[i]
			}
			t.Logf("%-30s %-30s\n", v, e)
		}
		t.Helper()
		t.Error("Wrongly sorted")
	}
}

func parsePath(s string) *expr.Path {
	p, err := expr.ParsePath(s)
	if err != nil {
		panic(err)
	}

	return p
}
