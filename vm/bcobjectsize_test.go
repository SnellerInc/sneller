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
	"fmt"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestSizeUnsupportedIonValues(t *testing.T) {
	var ctx bctestContext
	defer ctx.free()

	inputV := ctx.vRegFromValues([]any{
		ion.Int(-42),
		ion.Uint(42),
		ion.Float(-5),
		ion.String("xyz123"),
		ion.Bool(true),
		ion.Bool(false),
		ion.Timestamp(date.Time{}),
		ion.Annotation(nil, "", ion.Null),
		ion.Blob([]byte{0x01, 0x02}),
		[]byte{0x00},
		[]byte{0x00},
		[]byte{0x00},
		[]byte{0x00},
		[]byte{0x00},
		[]byte{0x00},
		[]byte{0x00},
	}, nil)
	inputK := kRegData{mask: 0xFFFF}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opobjectsize, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Error(err)
		t.Fail()
	}

	verifyKRegOutput(t, &outputK, &kRegData{})
	verifyI64RegOutput(t, &outputS, &i64RegData{})
}

func TestSizeEmptyContainers(t *testing.T) {
	var ctx bctestContext
	defer ctx.free()

	// Ion length L == 0
	emptyList := []byte{0xb0}
	emptyStruct := []byte{0xd0}

	// Ion length L = 0xe -> uvint encoded length 0
	emptyList2 := []byte{0xbe, 0x80}
	emptyStruct2 := []byte{0xde, 0x80}

	inputV := ctx.vRegFromValues([]any{
		emptyList,
		emptyList,
		emptyStruct,
		emptyList2,
		emptyList2,
		emptyStruct2,
		emptyList,
		emptyList,
		emptyStruct2,
		emptyList2,
		emptyList2,
		emptyStruct2,
		emptyList,
		emptyList,
		emptyStruct,
		emptyList2,
	}, nil)
	inputK := kRegData{mask: 0xFFFF}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opobjectsize, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Error(err)
		t.Fail()
	}

	verifyKRegOutput(t, &outputK, &kRegData{mask: 0xFFFF})
	verifyI64RegOutput(t, &outputS, &i64RegData{})
}

func TestSizeList(t *testing.T) {
	var ctx bctestContext
	defer ctx.free()

	expectedOutputS := i64RegData{values: [16]int64{1, 10, 50, 7, 0, 12, 42, 89, 300, 111, 4, 0, 20, 30, 51, 230}}
	expectedOutputK := kRegData{mask: 0xFFFF}

	makeList := func(size int) ion.List {
		list := make([]ion.Datum, size)
		for i := range list {
			list[i] = ion.Int(int64(i + 1))
		}
		return ion.NewList(nil, list)
	}

	var inputData []any
	for i := range expectedOutputS.values {
		size := int(expectedOutputS.values[i])
		inputData = append(inputData, makeList(size).Datum())
	}

	inputV := ctx.vRegFromValues(inputData, nil)
	inputK := kRegData{mask: 0xFFFF}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opobjectsize, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Error(err)
		t.Fail()
	}

	verifyKRegOutput(t, &outputK, &expectedOutputK)
	verifyI64RegOutput(t, &outputS, &expectedOutputS)
}

func TestSizeListWithNulls(t *testing.T) {
	var ctx bctestContext
	defer ctx.free()

	expectedOutputS := i64RegData{values: [16]int64{1, 10, -1, 7, 0, 12, 42, 89, -1, 111, 4, 0, 20, 30, 51, -1}}
	expectedOutputK := kRegData{}

	makeList := func(size int) interface{} {
		if size < 0 {
			return []byte{0x0F}
		}
		list := make([]ion.Datum, size)
		for i := range list {
			list[i] = ion.Int(int64(i + 1))
		}
		return ion.NewList(nil, list).Datum()
	}

	var inputData []interface{}
	for i, size := range expectedOutputS.values {
		inputData = append(inputData, makeList(int(size)))
		if size >= 0 {
			expectedOutputK.mask |= uint16(1) << i
		} else {
			// missing lanes would have length zeroed
			expectedOutputS.values[i] = 0
		}
	}

	inputV := ctx.vRegFromValues(inputData, nil)
	inputK := kRegData{mask: 0xFFFF}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opobjectsize, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Error(err)
		t.Fail()
	}

	verifyKRegOutput(t, &outputK, &expectedOutputK)
	verifyI64RegOutput(t, &outputS, &expectedOutputS)
}

func TestSizeStruct(t *testing.T) {
	var ctx bctestContext
	var symtab ion.Symtab
	defer ctx.free()

	expectedOutputS := i64RegData{values: [16]int64{100, 1, 5, 487, 17, 0, 200, 80, 89, 44, 9, 31, 128, 127, 0, 8}}
	expectedOutputK := kRegData{mask: 0xFFFF}

	// create structure {"field1": 1, "field2": 2, ..., "fieldN": N}
	makeStruct := func(size int) ion.Struct {
		fields := make([]ion.Field, size)
		for i := range fields {
			fields[i].Label = fmt.Sprintf("field%d", i+1)
			fields[i].Datum = ion.Int(int64(i + 1))
		}
		return ion.NewStruct(&symtab, fields)
	}

	var inputData []interface{}
	for _, size := range expectedOutputS.values {
		inputData = append(inputData, makeStruct(int(size)).Datum())
	}

	inputV := ctx.vRegFromValues(inputData, &symtab)
	inputK := kRegData{mask: 0xFFFF}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opobjectsize, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Error(err)
		t.Fail()
	}

	verifyKRegOutput(t, &outputK, &expectedOutputK)
	verifyI64RegOutput(t, &outputS, &expectedOutputS)
}
