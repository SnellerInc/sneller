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
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/tests"

	"golang.org/x/exp/slices"
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

func TestSizeNullContainers(t *testing.T) {
	var ctx bctestContext
	defer ctx.free()

	nullList := []byte{0xbf}
	nullSexp := []byte{0xcf}
	nullStruct := []byte{0xdf}

	inputV := ctx.vRegFromValues([]any{
		nullList,
		nullSexp,
		nullStruct,
		nullList,
		nullSexp,
		nullStruct,
		nullList,
		nullSexp,
		nullStruct,
		nullList,
		nullSexp,
		nullStruct,
		nullList,
		nullSexp,
		nullStruct,
		nullList,
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
	emptySexp := []byte{0xc0}
	emptyStruct := []byte{0xd0}

	// Ion length L = 0xe -> uvint encoded length 0
	emptyList2 := []byte{0xbe, 0x80}
	emptySexp2 := []byte{0xce, 0x80}
	emptyStruct2 := []byte{0xde, 0x80}

	inputV := ctx.vRegFromValues([]any{
		emptyList,
		emptySexp,
		emptyStruct,
		emptyList2,
		emptySexp2,
		emptyStruct2,
		emptyList,
		emptySexp,
		emptyStruct2,
		emptyList2,
		emptySexp2,
		emptyStruct2,
		emptyList,
		emptySexp,
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
			return []byte{0xbf}
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
			fields[i].Value = ion.Int(int64(i + 1))
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

// --------------------------------------------------

// go:noescape
func bcobjectsize_test_uvint_length(valid uint16, data []byte, offsets *[16]uint32, mask *uint16, length *[16]uint32)

func TestSizeUVIntLengthForValidValues(t *testing.T) {
	// given
	data := []byte{ // 0-3 byte uvbyte
		0x00, 0x00, 0x00, 0xff, // too big (masked out)
		0xf0, 0x00, 0x00, 0xff, // 1
		0x00, 0xf0, 0x00, 0xff, // 2
		0xf0, 0xf0, 0x00, 0xff, // 1
		0x00, 0x00, 0xf0, 0xff, // 3
		0xf0, 0x00, 0xf0, 0xff, // 1
		0x00, 0xf0, 0xf0, 0xff, // 2
		0xf0, 0xf0, 0xf0, 0xff, // 1
		0x00, 0x00, 0x00, 0xff, // too big (masked out)
		0xf0, 0xff, 0xff, 0xff, // 1
		0x00, 0xf0, 0xff, 0xff, // 2
		0xf0, 0xf0, 0xff, 0xff, // 1
		0x00, 0x00, 0xf0, 0xff, // 3
		0xf0, 0x00, 0xf0, 0xff, // 1
		0x00, 0xf0, 0xf0, 0xff, // 2
		0xf0, 0xf0, 0xf0, 0xff, // 1
	}

	var offsets [16]uint32
	for i := 0; i < 16; i++ {
		offsets[i] = uint32(i * 4)
	}

	valid := uint16(0xfefe)
	var lengths [16]uint32
	var mask uint16

	expectedLengths := [16]uint32{
		0, 1, 2, 1, 3, 1, 2, 1,
		0, 1, 2, 1, 3, 1, 2, 1,
	}

	gm, err := tests.GuardMemory(data)
	if err != nil {
		t.Error(err)
	}
	defer gm.Free()
	data = gm.Data

	// when
	bcobjectsize_test_uvint_length(valid, data, &offsets, &mask, &lengths)

	// then
	if mask != 0 {
		t.Errorf("valid mask has to be 0, it is %016b (0x%02x)", mask, mask)
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeUVIntLengthForInvalidValues(t *testing.T) {
	// given
	data := []byte{
		// lack of end-marker within 32 bits
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		// end-marker at 3rd byte
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x80,
	}

	var offsets [16]uint32
	for i := 0; i < 16; i++ {
		offsets[i] = uint32(i * 4)
	}

	valid := uint16(0xffff)
	var lengths [16]uint32
	var mask uint16

	var expectedLengths [16]uint32
	for i := 0; i < 16; i++ {
		expectedLengths[i] = uint32(0xffffffff)
	}

	// when
	bcobjectsize_test_uvint_length(valid, data, &offsets, &mask, &lengths)

	// then
	if mask != 0xffff {
		t.Errorf("valid mask has to be 0xffff, it is %016b (0x%02x)", mask, mask)
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

// --------------------------------------------------

// go:noescape
func bcobjectsize_test_object_header_size(valid uint16, data []byte, offsets *[16]uint32, mask *uint16, headerLength, objectLength *[16]uint32)

type testSizeObjects struct {
	ion    []byte
	header uint32
	object uint32
	fail   bool
}

func testSizeParseIonHeader(t *testing.T, objects []testSizeObjects) {
	for len(objects) > 16 {
		head := objects[:16]
		testSizeParseIonHeaderAuxiliary(t, head)

		objects = objects[16:]
	}

	if len(objects) > 0 {
		testSizeParseIonHeaderAuxiliary(t, objects)
	}
}

func testSizeParseIonHeaderAuxiliary(t *testing.T, objects []testSizeObjects) {
	if len(objects) > 16 {
		panic("not all object will be tested")
	}

	var data []byte
	var valid uint16
	var offsets [16]uint32
	var expectedHeaderLength [16]uint32
	var expectedObjectLength [16]uint32

	var fail bool
	for i := range objects {
		fail = fail || objects[i].fail
	}

	for i := range objects {
		offsets[i] = uint32(len(data))
		data = append(data, objects[i].ion...)

		expectedHeaderLength[i] = objects[i].header
		expectedObjectLength[i] = objects[i].object
		valid |= uint16(1 << i)
	}

	if fail {
		for i := range expectedHeaderLength {
			expectedHeaderLength[i] = 0xffffffff
			expectedObjectLength[i] = 0xffffffff
		}
	}

	// when
	var mask uint16
	var headerLength [16]uint32
	var objectLength [16]uint32

	data = slices.Grow(data, 4)

	gm, err := tests.GuardMemory(data)
	if err != nil {
		t.Error(err)
	}
	defer gm.Free()
	data = gm.Data

	bcobjectsize_test_object_header_size(valid, data, &offsets, &mask, &headerLength, &objectLength)

	// then
	if !reflect.DeepEqual(headerLength, expectedHeaderLength) {
		t.Logf("expected = %d", expectedHeaderLength)
		t.Logf("got      = %d", headerLength)
		t.Errorf("wrong header lengths")
	}

	if !reflect.DeepEqual(objectLength, expectedObjectLength) {
		t.Logf("expected = %d", expectedObjectLength)
		t.Logf("got      = %d", objectLength)
		t.Errorf("wrong object lengths")
	}
}

func TestSizeParseIonHeaderValidObjects(t *testing.T) {
	objects := []testSizeObjects{
		{
			ion:    []byte{0x00}, // null
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x0f}, // null (null)
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x05, 0x00, 0x00, 0x00, 0x00, 0x00}, // nop (5 bytes)
			header: 1,
			object: 5,
		},
		{
			ion:    []byte{0x10}, // bool(false)
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x11}, // bool(true)
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x20}, // zero
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x27, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // positive num
			header: 1,
			object: 7,
		},
		{
			ion:    []byte{0x31, 0xff}, // negative num
			header: 1,
			object: 1,
		},
		{
			ion:    []byte{0x48, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // f64
			header: 1,
			object: 8,
		},
		{
			ion:    []byte{0x8f}, // null string
			header: 1,
			object: 0,
		},
		{
			ion:    []byte{0x9e, (0x80 + 42), 0x00, 0x00}, // list with 42-byte body (without actual body, 2 bytes as padding)
			header: 2,
			object: 42,
		},
		{
			ion:    []byte{0x9e, (0x80 + 42), 0xff, 0x00}, // list with 42-byte body
			header: 2,
			object: 42,
		},
		{
			ion:    []byte{0x9e, (0x80 + 42), 0x00, 0xff}, // list with 42-byte body
			header: 2,
			object: 42,
		},
		{
			ion:    []byte{0x9e, (0x80 + 42), 0xff, 0xff}, // list with 42-byte body
			header: 2,
			object: 42,
		},
		{
			ion:    []byte{0xae, 0x0b, 0xf0, 0x00}, // blob (without actual body, 0x00 is a padding)
			header: 3,
			object: 1520,
		},
		{
			ion:    []byte{0xae, 0x0b, 0xf0, 0xff}, // blob (without actual body, 0xff is a padding)
			header: 3,
			object: 1520,
		},
		{
			ion:    []byte{0xbe, 0x8e, 0x21, 0x01, 0x21, 0x02, 0x21, 0x03, 0x21, 0x04, 0x21, 0x05, 0x21, 0x06, 0x21, 0x07}, // list [1, 2, 3, 4, 5, 6, 7]
			header: 2,
			object: 14,
		},
		{
			ion:    []byte{0xce, 0x7f, 0x7f, 0xff}, // sexp (without actual body)
			header: 4,
			object: 0x1fffff,
		},
	}

	testSizeParseIonHeader(t, objects)
}

func TestSizeParseIonHeaderValidObjectsWithOneByteLength(t *testing.T) {
	objects := make([]testSizeObjects, 16)
	for i := range objects {
		objects[i].ion = []byte{0xde, byte(0x80 + i + 1)}
		objects[i].header = 2
		objects[i].object = uint32(i + 1)
	}

	testSizeParseIonHeader(t, objects)
}

func TestSizeParseIonHeaderTooBigObject(t *testing.T) {
	objects := []testSizeObjects{
		{
			ion:  []byte{0x8e, 0x00, 0x00, 0x00, 0x00}, // uvint has more than 3 bytes
			fail: true,
		},
	}

	testSizeParseIonHeader(t, objects)
}
