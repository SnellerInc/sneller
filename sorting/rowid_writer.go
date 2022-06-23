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
	"fmt"
)

// rowIDWriter implements SortedDataWriter
//
// rowIDWriter uses the data collected during the single column sorting
// loading phase, and writes Ion records based on their IDs.
type rowIDWriter struct {
	rowsWriter *RowsWriter
	column     *MixedTypeColumn
	records    [][][]byte
	indices    indicesReferences
}

func newRowIDWriter(records map[uint32][][]byte, column *MixedTypeColumn, plan []sortAction, rowsWriter *RowsWriter) (*rowIDWriter, error) {
	writer := &rowIDWriter{
		column:     column,
		rowsWriter: rowsWriter,
	}

	indices, err := buildIndicesLookup(column, plan)
	if err != nil {
		return nil, err
	}

	writer.indices = *indices

	writer.records = make([][][]byte, len(records))

	// make a plain array of arrays from a map of arrays
	// the output has to be orderd by chunkID
	for chunkID := 0; chunkID < len(records); chunkID++ {
		arr, ok := records[uint32(chunkID)]
		if !ok {
			// incomplete sorting? screwed synchronization?
			return nil, fmt.Errorf("unable to find records from chunk #%d", chunkID)
		}

		writer.records[chunkID] = arr
	}

	return writer, nil
}

// Write implements SortedDataWriter.Write
func (w *rowIDWriter) Write(start, end int) error {
	// Note: The [start:end] range always refer to a single subarray from
	//       MixedTypeColumn object. Thus it's safe & sufficient to use
	//       only `start` index to get the associated subarray.
	indices := w.indices.find(start)
	if indices == nil {
		return fmt.Errorf("unable to locate data for subrange [%d:%d]", start, end)
	}

	startIndex := indices.span.start
	arr := *indices.arr
	writer := w.rowsWriter
	for i := start - startIndex; i <= end-startIndex; i++ {
		id := ID(arr[i])
		err := writer.WriteRecord(w.records[id.chunk()][id.row()])
		if err != nil {
			return err
		}
	}

	return nil
}

// Close implements io.Close
func (w *rowIDWriter) Close() error {
	return w.rowsWriter.Close()
}

type indicesReference struct {
	arr  *[]uint64    // pointer to a member of MixedTypeColumn
	span indicesRange // what subrange of the entire range is covered by this array
}

type indicesReferences []indicesReference

func (arr indicesReferences) find(index int) *indicesReference {
	for i := range arr {
		if arr[i].span.contains(index) {
			return &arr[i]
		}
	}

	return nil
}

func buildIndicesLookup(col *MixedTypeColumn, plan []sortAction) (*indicesReferences, error) {
	var ret indicesReferences
	for i := range plan {
		var r indicesReference

		r.arr = col.indicesByName(plan[i].name)
		if r.arr == nil {
			return nil, fmt.Errorf("unknown indices range %q", plan[i].name)
		}

		r.span.start = plan[i].start
		r.span.end = r.span.start + plan[i].length - 1

		ret = append(ret, r)
	}

	return &ret, nil
}
