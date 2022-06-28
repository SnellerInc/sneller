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

	"github.com/SnellerInc/sneller/ion"
)

func TestSizeUnsupportedIonValues(t *testing.T) {
	expectedLengths := [16]uint64{}

	var values []interface{}
	values = append(values, ion.Int(-42))
	values = append(values, ion.Uint(42))
	values = append(values, ion.Float(-5))
	values = append(values, ion.String("xyz123"))
	values = append(values, ion.Bool(true))
	values = append(values, ion.Bool(false))
	values = append(values, ion.Timestamp{})
	values = append(values, &ion.Annotation{})
	values = append(values, ion.Blob{0x01, 0x02})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})
	values = append(values, []byte{0x00})

	var ctx bctestContext
	ctx.current = 0xffff
	defer ctx.Free()
	ctx.setInputIonFields(values, nil)

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()

	if ctx.current != 0x00 {
		t.Errorf("Valid mask has to be zero, it is %02x", ctx.current)
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeNullContainers(t *testing.T) {
	expectedLengths := [16]uint64{}

	nullList := []byte{0xbf}
	nullSexp := []byte{0xcf}
	nullStruct := []byte{0xdf}

	var values []interface{}
	values = append(values, nullList)
	values = append(values, nullSexp)
	values = append(values, nullStruct)
	values = append(values, nullList)
	values = append(values, nullSexp)
	values = append(values, nullStruct)
	values = append(values, nullList)
	values = append(values, nullSexp)
	values = append(values, nullStruct)
	values = append(values, nullList)
	values = append(values, nullSexp)
	values = append(values, nullStruct)
	values = append(values, nullList)
	values = append(values, nullSexp)
	values = append(values, nullStruct)
	values = append(values, nullList)

	var ctx bctestContext
	defer ctx.Free()
	ctx.current = 0xffff
	ctx.setInputIonFields(values, nil)

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()

	if ctx.current != 0x00 {
		t.Errorf("Valid mask has to be zero, it is %02x", ctx.current)
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeEmptyContainers(t *testing.T) {
	expectedLengths := [16]uint64{}

	// Ion length L == 0
	emptyList := []byte{0xb0}
	emptySexp := []byte{0xc0}
	emptyStruct := []byte{0xd0}

	// Ion length L = 0xe -> uvint encoded length 0
	emptyList2 := []byte{0xbe, 0x80}
	emptySexp2 := []byte{0xce, 0x80}
	emptyStruct2 := []byte{0xde, 0x80}

	var values []interface{}
	values = append(values, emptyList)
	values = append(values, emptySexp)
	values = append(values, emptyStruct)
	values = append(values, emptyList2)
	values = append(values, emptySexp2)
	values = append(values, emptyStruct2)
	values = append(values, emptyList)
	values = append(values, emptySexp)
	values = append(values, emptyStruct2)
	values = append(values, emptyList2)
	values = append(values, emptySexp2)
	values = append(values, emptyStruct2)
	values = append(values, emptyList)
	values = append(values, emptySexp)
	values = append(values, emptyStruct)
	values = append(values, emptyList2)

	var ctx bctestContext
	ctx.current = 0xffff
	ctx.setInputIonFields(values, nil)

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()
	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeList(t *testing.T) {
	expectedLengths := [16]uint64{1, 10, 50, 7, 0, 12, 42, 89, 300, 111, 4, 0, 20, 30, 51, 230}

	makeList := func(size int) *ion.List {
		list := make([]ion.Datum, size)
		for i := range list {
			list[i] = ion.Int(i + 1)
		}
		return ion.NewList(nil, list)
	}

	var values []interface{}
	for i := range expectedLengths {
		size := int(expectedLengths[i])
		values = append(values, makeList(size))
	}

	var ctx bctestContext
	defer ctx.Free()
	ctx.setInputIonFields(values, nil)
	ctx.current = 0xffff

	valid := ctx.current

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()

	if ctx.current != valid {
		t.Logf("expected %02x, got %02x", valid, ctx.current)
		t.Errorf("invalid mask")
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeListWithNulls(t *testing.T) {
	makeList := func(size int) interface{} {
		if size < 0 {
			return []byte{0xbf}
		}
		list := make([]ion.Datum, size)
		for i := range list {
			list[i] = ion.Int(i + 1)
		}
		return ion.NewList(nil, list)
	}

	var expectedLengths [16]uint64
	var expectedMask uint16
	var values []interface{}
	{
		lengths := [16]int{1, 10, -1, 7, 0, 12, 42, 89, -1, 111, 4, 0, 20, 30, 51, -1}
		for i, size := range lengths {
			values = append(values, makeList(size))
			if size >= 0 {
				expectedLengths[i] = uint64(size)
				expectedMask |= uint16(1 << i)
			} else {
				expectedLengths[i] = 0
			}
		}
	}

	var ctx bctestContext
	defer ctx.Free()
	ctx.setInputIonFields(values, nil)
	ctx.current = 0xffff

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()

	if ctx.current != expectedMask {
		t.Logf("expected = %016b", expectedMask)
		t.Logf("got      = %016b", ctx.current)
		t.Errorf("invalid mask")
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
}

func TestSizeStruct(t *testing.T) {
	// given
	expectedLengths := [16]uint64{
		100, 1, 5, 487,
		17, 0, 200, 80,
		89, 44, 9, 31,
		128, 127, 0, 8,
	}

	if len(expectedLengths) != 16 {
		panic("please define exactly 16 testcases, as they have to fit in an AVX512 register")
	}

	var symtab ion.Symtab

	// create structure {"field1": 1, "field2": 2, ..., "fieldN": N}
	makeStruct := func(size int) *ion.Struct {
		fields := make([]ion.Field, size)
		for i := range fields {
			fields[i].Label = fmt.Sprintf("field%d", i+1)
			fields[i].Value = ion.Int(i + 1)
		}
		return ion.NewStruct(&symtab, fields)
	}

	var values []interface{}
	for i := range expectedLengths {
		size := int(expectedLengths[i])
		values = append(values, makeStruct(size))
	}

	var ctx bctestContext
	defer ctx.Free()
	ctx.setInputIonFields(values, &symtab)
	ctx.current = 0xffff

	err := ctx.Execute(opobjectsize)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	lengths := ctx.getScalarUint64()

	if ctx.current != 0xffff {
		t.Errorf("Valid mask has to be 0xffff, it is %02x", ctx.current)
	}

	if !reflect.DeepEqual(lengths, expectedLengths) {
		t.Logf("expected = %d", expectedLengths)
		t.Logf("got      = %d", lengths)
		t.Errorf("wrong lengths")
	}
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
