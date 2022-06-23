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
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestRowsWriter(t *testing.T) {
	// given
	var symtab ion.Symtab
	symtab.Intern("test1")
	symtab.Intern("test2")

	const chunkAlignment = 64

	dst := new(bytes.Buffer)

	writer, err := NewRowsWriter(dst, &symtab, chunkAlignment)
	if err != nil {
		t.Errorf("Unexpected error %q", err)
		return
	}

	rows := []IonRecord{
		IonRecord{Raw: []byte{0x83, 'c', 'a', 't'}},
		IonRecord{Raw: []byte{0x84, 'd', 'e', 'e', 'r'}},
		IonRecord{Raw: []byte{0x82, '4', '2'}},
		IonRecord{Raw: []byte{0x89, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}},
	}

	// when
	for i := 0; i < 30; i++ {
		n := i%len(rows) + 1
		err = writer.WriteRows(rows[:n])
		if err != nil {
			t.Errorf("Unexpected error reported %q", err)
			return
		}
	}

	err = writer.Close()
	if err != nil {
		t.Errorf("Unexpected error %q", err)
		return
	}

	// then
	encoded := dst.Bytes()

	ionObjects := []string{
		"BVM 4", "annotation 20", "struct 5", "struct 5", "struct 6", "struct 5", "struct 6", "struct 4", "struct 5",
		"BVM 4", "annotation 20", "struct 6", "struct 4", "struct 11", "struct 5", "struct 5", "struct 6",
		"BVM 4", "annotation 20", "struct 5", "struct 6", "struct 4", "struct 5", "struct 6", "struct 4",
		"BVM 4", "annotation 20", "struct 11", "struct 5", "struct 5", "struct 6", "struct 5", "struct 6",
		"BVM 4", "annotation 20", "struct 4", "struct 5", "struct 6", "struct 4", "struct 11", "struct 5", "struct 5",
		"BVM 4", "annotation 20", "struct 6", "struct 5", "struct 6", "struct 4", "struct 5", "struct 6", "struct 4",
		"BVM 4", "annotation 20", "struct 11", "struct 5", "struct 5", "struct 6", "struct 5", "struct 6",
		"BVM 4", "annotation 20", "struct 4", "struct 5", "struct 6", "struct 4", "struct 11", "struct 5", "struct 5",
		"BVM 4", "annotation 20", "struct 6", "struct 5", "struct 6", "struct 4", "struct 5", "struct 6", "struct 4",
		"BVM 4", "annotation 20", "struct 11", "struct 5", "struct 5", "struct 6", "struct 5", "struct 6",
		"BVM 4", "annotation 20", "struct 4", "struct 5", "struct 6", "struct 4", "struct 11", "struct 5", "struct 5",
		"BVM 4", "annotation 20", "struct 6",
	}

	testExpectations(t, encoded, ionObjects)
}

func TestRowsWriterLargePaddingCase(t *testing.T) {
	// given
	var symtab ion.Symtab
	symtab.Intern("test1")
	symtab.Intern("test2")
	symtab.Intern("test3")

	const chunkAlignment = 10 * 1024

	dst := new(bytes.Buffer)

	writer, err := NewRowsWriter(dst, &symtab, chunkAlignment)
	if err != nil {
		t.Errorf("Unexpected error %q", err)
		return
	}

	rows := []IonRecord{
		IonRecord{Raw: ionObject(ion.StructType, 2*1024)}, // chunk #1 with approx 8kB padding
		IonRecord{Raw: ionObject(ion.StructType, 9*1024)}, // chunk #2 with no padding
	}

	// when
	err = writer.WriteRows(rows)
	if err != nil {
		t.Errorf("Error while writing rows: %s", err)
		return
	}

	err = writer.Close()
	if err != nil {
		t.Errorf("Unexpected error %q", err)
		return
	}

	// then
	encoded := dst.Bytes()

	ionObjects := []string{
		"BVM 4", "annotation 27", "struct 2054",
		"BVM 4", "annotation 27", "struct 9222",
	}

	testExpectations(t, encoded, ionObjects)
}

func testExpectations(t *testing.T, encoded []byte, ionObjects []string) {
	{
		result, err := toplevelIonObjects(encoded)
		if err != nil {
			t.Errorf("Error while decoding Ion: %s", err)
			return
		}

		if !reflect.DeepEqual(result, ionObjects) {
			t.Logf("result: %v\n", result)
			t.Logf("expected: %v\n", ionObjects)
			t.Error("Wrongly encoded")
		}
	}
}

func TestRowsWriterRowCannotFitIntoAChunk(t *testing.T) {
	// given
	var symtab ion.Symtab
	symtab.Intern("test1")
	symtab.Intern("test2")
	symtab.Intern("test3")

	const chunkAlignment = 100

	dst := new(bytes.Buffer)

	writer, err := NewRowsWriter(dst, &symtab, chunkAlignment)
	if err != nil {
		t.Errorf("Unexpected error %q", err)
		return
	}

	rows := []IonRecord{
		IonRecord{Raw: make([]byte, 100)},
	}

	// when
	err = writer.WriteRows(rows)

	// then
	if err == nil {
		t.Errorf("Expected error to be reported")
		return
	}

	expected := "record cannot fit in a chunk - size of record: 102, size of symtab: 31, chunk alignment: 100"
	if err.Error() != expected {
		t.Errorf("Expected error %q got %q", expected, err)
	}
}

func TestRowsWriterChunkAlignemntTooSmall(t *testing.T) {
	// given
	var symtab ion.Symtab
	symtab.Intern("test1")
	symtab.Intern("test2")
	symtab.Intern("test3")

	const chunkAlignment = 16

	dst := new(bytes.Buffer)

	// when
	_, err := NewRowsWriter(dst, &symtab, chunkAlignment)

	// then
	if err == nil {
		t.Errorf("Expected error to be reported")
		return
	}

	expected := "chunk size 16 too small, marshalled symbtab has 31"
	if err.Error() != expected {
		t.Errorf("Expected error %q got %q", expected, err)
	}
}

func toplevelIonObjects(buf []byte) (result []string, err error) {
	for len(buf) > 0 {
		if ion.IsBVM(buf) {
			result = append(result, "BVM 4")
			buf = buf[4:]
			continue
		}

		size := ion.SizeOf(buf)
		if size < 0 {
			return nil, fmt.Errorf("Wrong Ion structure")
		}

		result = append(result, fmt.Sprintf("%s %d", ion.TypeOf(buf), size))
		buf = buf[size:]
	}

	return result, nil
}

func ionObject(t ion.Type, size uint) []byte {
	alloc := size + uint(ion.UVarintSize(size)) + 1
	buf := make([]byte, alloc)
	ion.UnsafeWriteTag(buf, t, size)

	return buf
}
