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
	"sort"
)

type multipleColumnsData struct {
	records    []IonRecord
	directions []Direction
	nullsOrder []NullsOrder
	limit      indicesRange
	rp         *RuntimeParameters
}

type sortedDataWriter struct {
	data       multipleColumnsData
	rowsWriter *RowsWriter
}

// ByColumns performs sorting of Ion records.
func ByColumns(records []IonRecord, directions []Direction, nullsOrder []NullsOrder, limit *Limit, rowsWriter *RowsWriter, rp *RuntimeParameters) (err error) {
	data := multipleColumnsData{
		records:    records,
		directions: directions,
		nullsOrder: nullsOrder,
		rp:         rp,
	}

	if limit != nil {
		data.limit = limit.FinalRange(data.Len())
	} else {
		data.limit.start = 0
		data.limit.end = data.Len()
	}

	// 1. setup
	writer := sortedDataWriter{data: data, rowsWriter: rowsWriter}
	threadPool := NewThreadPool(rp.Threads)
	consumer := NewAsyncConsumer(&writer, 0, data.Len()-1, limit)
	consumer.Start(threadPool)

	// 2. sort
	if rp.UseStdlib {
		sort.Sort(&data)
		consumer.Notify(0, data.Len()-1)
	} else {
		multiColumnSort(&data, threadPool, consumer)
	}

	// 3. wait until sorting is done
	return threadPool.Wait()
}

// --------------------------------------------------

func (m *multipleColumnsData) tuple(index int) (result ionTuple) {
	result.rawFields = make([][]byte, m.ColumnsCount())

	record := &m.records[index]
	for columnID := 0; columnID < m.ColumnsCount(); columnID++ {
		result.rawFields[columnID] = record.UnsafeField(columnID)
	}

	return
}

func (m *multipleColumnsData) lessUnsafe(a, b int) bool {
	record1 := &m.records[a]
	record2 := &m.records[b]
	for i := 0; i < m.ColumnsCount(); i++ {
		raw1 := record1.UnsafeField(i)
		raw2 := record2.UnsafeField(i)
		direction := m.directions[i]
		nullsOrder := m.nullsOrder[i]

		relation := compareIonValues(raw1, raw2, direction, nullsOrder)
		if relation != 0 {
			return (relation < 0) == (direction == Ascending)
		}
	}

	return false
}

func (m *multipleColumnsData) lessTupleIndexUnsafe(t ionTuple, index int) bool {

	record := &m.records[index]
	for i := 0; i < m.ColumnsCount(); i++ {
		raw1 := t.rawFields[i]
		raw2 := record.UnsafeField(i)
		direction := m.directions[i]
		nullsOrder := m.nullsOrder[i]

		relation := compareIonValues(raw1, raw2, direction, nullsOrder)
		if relation != 0 {
			return (relation < 0) == (direction == Ascending)
		}
	}

	return false
}

func (m *multipleColumnsData) lessIndexTupleUnsafe(index int, t ionTuple) bool {

	record := &m.records[index]
	for i := 0; i < m.ColumnsCount(); i++ {
		raw1 := record.UnsafeField(i)
		raw2 := t.rawFields[i]
		direction := m.directions[i]
		nullsOrder := m.nullsOrder[i]

		relation := compareIonValues(raw1, raw2, direction, nullsOrder)
		if relation != 0 {
			return (relation < 0) == (direction == Ascending)
		}
	}

	return false
}

func (m *multipleColumnsData) ColumnsCount() int {
	return len(m.directions)
}

// sort.Sort interface
func (m *multipleColumnsData) Len() int {
	return len(m.records)
}

// sort.Sort interface
func (m *multipleColumnsData) Less(i, j int) bool {
	return m.lessUnsafe(i, j)
}

// sort.Sort interface
func (m *multipleColumnsData) Swap(i, j int) {
	m.records[i], m.records[j] = m.records[j], m.records[i]
}

// --------------------------------------------------

// SortedDataWriter.Write
func (m *sortedDataWriter) Write(start, end int) error {
	return m.rowsWriter.WriteRows(m.data.records[start : end+1])
}
