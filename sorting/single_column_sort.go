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

/*
Specialisation for sorting by single column

A brief overview of design.

1. The keys we're sorting by are decoded from the input once during loading and cached.
2. The decoded keys are split by their Ion type (type MixedTypeColumn).
3. Each key has the associated row ID (fields `xyzIndices` of MixedTypeColumn).
4. A row ID is a unique 64-bit number: 32-bit chunk ID and 32-bit row ID
   within chunk's data.
5. Rows collected from each chunk are stored in separate arrays (field
   `rawrecords` of Sorted). In the end we have a map: chunk ID -> records.
6. Sorting is done by routines specialised for each type. Keys and row IDs
   are sorted simultaneously.
7. When writing data back, we use row ID to lookup proper subarray and then
   concrete bytes.
*/

import (
	"fmt"
	"sort"
	"sync"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// ByColumn sorts collection of records by values from a single column.
func ByColumn(rawrecords map[uint32][][]byte, column *MixedTypeColumn, direction Direction, nullsOrder NullsOrder, limit *Limit, rowsWriter *RowsWriter, rp *RuntimeParameters) error {
	// 1. setup
	var lim indicesRange
	if limit != nil {
		lim = limit.FinalRange(column.Len())
	} else {
		lim.start = 0
		lim.end = column.Len() - 1
	}

	plan := planSingleColumnSorting(column, direction, nullsOrder, lim)

	writer, err := newRowIDWriter(rawrecords, column, plan, rowsWriter)
	if err != nil {
		return err
	}

	threadPool := NewThreadPool(rp.Threads)
	consumer := NewAsyncConsumer(writer, 0, column.Len()-1, limit)
	consumer.Start(threadPool)

	// 2. sort
	scsExecute(column, plan, threadPool, consumer, rp)

	// 3. wait until sorting is done
	return threadPool.Wait()
}

// MixedTypeColumn contains copy of data from Ion with the values
// split into distinct types.
//
// Indices are bare pointers cast to uint64. Note that some
// classes of values do not have keys.
//
// Note: We don't distinguish float32 and float64, float32
//       values are convert into float64 during building the structure.
type MixedTypeColumn struct {
	nullIndices          []uint64 // null or missing
	falseIndices         []uint64 // bool == false
	trueIndices          []uint64 // bool == true
	zeroIndices          []uint64 // zero values (both integer (0x20) and float (0x40))
	negintKeys           []uint64 // only negative integers
	negintIndices        []uint64
	posintKeys           []uint64 // only positive integers
	posintIndices        []uint64
	floatKeys            []float64
	floatIndices         []uint64
	stringKeys           []string
	stringIndices        []uint64
	fastTimestampKeys    []uint64 // timestamp represented as 8-byte values
	fastTimestampIndices []uint64
	timestampKeys        []date.Time // all other timestamps
	timestampIndices     []uint64
	// When add/remove a type, please update `subarrayTypes` const below.
}

// Add adds a record associated with the key (given as an Ion value)
func (m *MixedTypeColumn) Add(recordID uint64, buf []byte) error {
	T, L := ion.DecodeTLV(buf[0])

	switch T {
	case ion.NullType:
		if L == ionNullIndicator {
			m.nullIndices = append(m.nullIndices, recordID)
		}

	case ion.BoolType:
		if L == ionBoolFalse {
			m.falseIndices = append(m.falseIndices, recordID)
		} else if L == ionBoolTrue {
			m.trueIndices = append(m.trueIndices, recordID)
		} else {
			return fmt.Errorf("malformed Ion: % 02x", buf)
		}

	case ion.UintType:
		if L != 0 {
			m.posintIndices = append(m.posintIndices, recordID)
			m.posintKeys = append(m.posintKeys, ionParseIntMagnitude(buf))
		} else {
			m.zeroIndices = append(m.zeroIndices, recordID)
		}

	case ion.IntType:
		m.negintIndices = append(m.negintIndices, recordID)
		m.negintKeys = append(m.negintKeys, ionParseIntMagnitude(buf))

	case ion.FloatType:
		if L == ionFloat32 {
			m.floatIndices = append(m.floatIndices, recordID)
			m.floatKeys = append(m.floatKeys, float64(ionParseFloat32(buf)))
		} else if L == ionFloat64 {
			m.floatIndices = append(m.floatIndices, recordID)
			m.floatKeys = append(m.floatKeys, ionParseFloat64(buf))
		} else if L == ionFloatPositiveZero {
			m.zeroIndices = append(m.zeroIndices, recordID)
		}

	case ion.StringType:
		m.stringIndices = append(m.stringIndices, recordID)
		m.stringKeys = append(m.stringKeys, ionParseString(buf))

	case ion.TimestampType:
		ts, ok := ionParseSimplifiedTimestamp(buf)
		if ok {
			m.fastTimestampIndices = append(m.fastTimestampIndices, recordID)
			m.fastTimestampKeys = append(m.fastTimestampKeys, ts)
		} else {
			m.timestampIndices = append(m.timestampIndices, recordID)
			m.timestampKeys = append(m.timestampKeys, ionParseTimestamp(buf))
		}

	default:
		return fmt.Errorf("unsupported Ion type %s", T)
	}

	return nil
}

// AddMissing adds a record for which there not associated key
//
// Sorting makes no distinction between NULL and MISSING values,
// thus such records go to the collection of nulls.
func (m *MixedTypeColumn) AddMissing(recordID uint64) {
	m.nullIndices = append(m.nullIndices, recordID)
}

// Append appends all data from another column
func (m *MixedTypeColumn) Append(o *MixedTypeColumn) {
	m.nullIndices = append(m.nullIndices, o.nullIndices...)
	m.falseIndices = append(m.falseIndices, o.falseIndices...)
	m.trueIndices = append(m.trueIndices, o.trueIndices...)
	m.zeroIndices = append(m.zeroIndices, o.zeroIndices...)
	m.negintKeys = append(m.negintKeys, o.negintKeys...)
	m.negintIndices = append(m.negintIndices, o.negintIndices...)
	m.posintKeys = append(m.posintKeys, o.posintKeys...)
	m.posintIndices = append(m.posintIndices, o.posintIndices...)
	m.floatKeys = append(m.floatKeys, o.floatKeys...)
	m.floatIndices = append(m.floatIndices, o.floatIndices...)
	m.stringKeys = append(m.stringKeys, o.stringKeys...)
	m.stringIndices = append(m.stringIndices, o.stringIndices...)
	m.fastTimestampKeys = append(m.fastTimestampKeys, o.fastTimestampKeys...)
	m.fastTimestampIndices = append(m.fastTimestampIndices, o.fastTimestampIndices...)
	m.timestampKeys = append(m.timestampKeys, o.timestampKeys...)
	m.timestampIndices = append(m.timestampIndices, o.timestampIndices...)
}

// Len returns total number of rows.
func (m *MixedTypeColumn) Len() int {
	return len(m.nullIndices) + len(m.falseIndices) + len(m.trueIndices) +
		len(m.zeroIndices) + len(m.negintIndices) + len(m.posintIndices) +
		len(m.floatIndices) + len(m.stringIndices) + len(m.timestampIndices) +
		len(m.fastTimestampKeys)
}

func (m *MixedTypeColumn) indicesByName(name string) *[]uint64 {
	switch name {
	case "null":
		return &m.nullIndices
	case "false":
		return &m.falseIndices
	case "true":
		return &m.trueIndices
	case "zero":
		return &m.zeroIndices
	case "negint":
		return &m.negintIndices
	case "posint":
		return &m.posintIndices
	case "float":
		return &m.floatIndices
	case "string":
		return &m.stringIndices
	case "timestamp":
		if len(m.timestampIndices) > 0 {
			return &m.timestampIndices
		} else {
			return &m.fastTimestampIndices
		}
	}

	return nil
}

type actionProcedure func(SortedDataConsumer, ThreadPool, *MixedTypeColumn, int, int, *RuntimeParameters) error

// sortAction represents procedure that has to be done on the indices and possibly on the keys.
//
// Number `start` points the offset in the whole column.
type sortAction struct {
	exec   actionProcedure
	start  int
	length int
	name   string
}

func (s *sortAction) invoke(c SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, rp *RuntimeParameters) error {
	return s.exec(c, pool, col, 0, s.length-1, rp)
}

func (s *sortAction) String() string {
	return fmt.Sprintf("%s [%d,%d]", s.name, s.start, s.start+s.length-1)
}

// planSingleColumnSorting build a sequence of operations that have to be executed
// in order to sort a column of different types.
func planSingleColumnSorting(col *MixedTypeColumn, direction Direction, nullsOrder NullsOrder, limit indicesRange) []sortAction {
	const subarrayTypes = 9 // count of MixedTypeColumn.XXXIndices
	seq := make([]sortAction, 0, subarrayTypes)

	actionAlreadySorted := func(c SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
		c.Notify(start, end)
		return nil
	}

	add := func(proc actionProcedure, length int, name string) {
		if length == 0 {
			return
		}

		seq = append(seq, sortAction{
			exec:   proc,
			start:  -1,
			length: length,
			name:   name,
		})
	}

	// 1. first sort by known values: nulls, false, true
	nullsAtStart := (nullsOrder == NullsFirst) == (direction == Ascending)
	if nullsAtStart {
		add(actionAlreadySorted, len(col.nullIndices), "null")
	}

	add(actionAlreadySorted, len(col.falseIndices), "false")
	add(actionAlreadySorted, len(col.trueIndices), "true")

	// 2. sort numeric values
	switch chooseNumericSorting(col) {
	case sortNoNumericValues:
		break

	case sortIntegers:
		actionSortNegIntegers := func(consumer SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
			// Note: we're sorting absolute values of negative numbers,
			//       this is why the ordering is reversed
			if rp.UseAVX512Sorter {
				return quicksortAVX512Uint64(
					col.negintKeys,
					col.negintIndices,
					pool,
					direction.reversed(),
					consumer,
					rp)
			} else {
				if direction == Ascending {
					sort.Sort(&sortUint64Desc{
						col.negintKeys,
						col.negintIndices,
					})
				} else {
					sort.Sort(&sortUint64Asc{
						col.negintKeys,
						col.negintIndices,
					})
				}
				consumer.Notify(start, end)
			}

			return nil
		}

		actionSortPosIntegers := func(consumer SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
			if rp.UseAVX512Sorter {
				return quicksortAVX512Uint64(
					col.posintKeys,
					col.posintIndices,
					pool,
					direction,
					consumer,
					rp)
			} else {
				if direction == Ascending {
					sort.Sort(&sortUint64Asc{
						col.posintKeys,
						col.posintIndices,
					})
				} else {
					sort.Sort(&sortUint64Desc{
						col.posintKeys,
						col.posintIndices,
					})
				}
				consumer.Notify(start, end)
			}

			return nil
		}

		add(actionSortNegIntegers, len(col.negintIndices), "negint")
		add(actionAlreadySorted, len(col.zeroIndices), "zero")
		add(actionSortPosIntegers, len(col.posintIndices), "posint")

	case sortFloats:
		actionSortFloats := func(consumer SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
			var cast [3]struct {
				indices []uint64
				keys    []float64
			}

			var wg sync.WaitGroup

			if len(col.negintIndices) > 0 {
				wg.Add(1)
				go func() {
					cast[0].indices, cast[0].keys = col.castNegintToFloat64()
					wg.Done()
				}()
			}

			if len(col.zeroIndices) > 0 {
				wg.Add(1)
				go func() {
					cast[1].indices, cast[1].keys = col.castZerosToFloat64()
					wg.Done()
				}()
			}

			if len(col.posintIndices) > 0 {
				wg.Add(1)
				go func() {
					cast[2].indices, cast[2].keys = col.castPosintToFloat64()
					wg.Done()
				}()
			}

			wg.Wait()

			for i := 0; i < 3; i++ {
				col.floatIndices = append(col.floatIndices, cast[i].indices...)
				col.floatKeys = append(col.floatKeys, cast[i].keys...)
			}

			col.zeroIndices = nil
			col.posintIndices = nil
			col.posintKeys = nil
			col.negintIndices = nil
			col.negintKeys = nil

			if rp.UseAVX512Sorter {
				return quicksortAVX512Float64(
					col.floatKeys,
					col.floatIndices,
					pool,
					direction,
					consumer,
					rp)
			} else {
				if direction == Ascending {
					sort.Sort(&sortFloat64Asc{
						col.floatKeys,
						col.floatIndices,
					})
				} else {
					sort.Sort(&sortFloat64Desc{
						col.floatKeys,
						col.floatIndices,
					})
				}
				consumer.Notify(start, end)
			}

			return nil
		}

		length := len(col.zeroIndices) + len(col.negintIndices) +
			len(col.posintIndices) + len(col.floatIndices)

		add(actionSortFloats, length, "float")
	}

	// 3. sort timestamps
	actionSortTimestamps := func(consumer SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
		if len(col.timestampKeys) > 0 {
			col.promoteSimplifiedTimestamps()
			if direction == Ascending {
				sort.Sort(&sortDateTimeAsc{
					col.timestampKeys,
					col.timestampIndices,
				})
			} else {
				sort.Sort(&sortDateTimeDesc{
					col.timestampKeys,
					col.timestampIndices,
				})
			}
			consumer.Notify(start, end)

		} else {
			if rp.UseAVX512Sorter {
				return quicksortAVX512Uint64(
					col.fastTimestampKeys,
					col.fastTimestampIndices,
					pool,
					direction,
					consumer,
					rp)
			} else {
				if direction == Ascending {
					sort.Sort(&sortUint64Asc{
						col.fastTimestampKeys,
						col.fastTimestampIndices,
					})
				} else {
					sort.Sort(&sortUint64Desc{
						col.fastTimestampKeys,
						col.fastTimestampIndices,
					})
				}
				consumer.Notify(start, end)
			}
		}

		return nil
	}

	add(actionSortTimestamps, len(col.timestampIndices)+len(col.fastTimestampIndices), "timestamp")

	// 4. sort strings
	actionSortStrings := func(c SortedDataConsumer, pool ThreadPool, col *MixedTypeColumn, start int, end int, rp *RuntimeParameters) error {
		if direction == Ascending {
			sort.Sort(&sortStringAsc{
				col.stringKeys,
				col.stringIndices,
			})
		} else {
			sort.Sort(&sortStringDesc{
				col.stringKeys,
				col.stringIndices,
			})
		}
		c.Notify(start, end)
		return nil
	}

	add(actionSortStrings, len(col.stringIndices), "string")

	if !nullsAtStart {
		add(actionAlreadySorted, len(col.nullIndices), "null")
	}
	// FIXME: blob/clob/array/struct types are not supported

	if len(seq) == 0 {
		return seq
	}

	// Reverse order of subarrays for descending sorting
	if direction == Descending {
		i := 0
		j := len(seq) - 1
		for i < j {
			seq[i], seq[j] = seq[j], seq[i]
			i += 1
			j -= 1
		}
	}

	// Calculate global offsets of subarrays and apply limit
	start := 0

	filtered := make([]sortAction, 0, subarrayTypes)
	for i := range seq {
		r := indicesRange{start: start, end: start + seq[i].length - 1}

		if !r.disjoint(limit) {
			seq[i].start = start
			filtered = append(filtered, seq[i])
		}

		start += seq[i].length
	}

	return filtered
}

func (m *MixedTypeColumn) castZerosToFloat64() (indices []uint64, keys []float64) {
	indices = m.zeroIndices
	keys = make([]float64, len(m.zeroIndices))
	return
}

func (m *MixedTypeColumn) castNegintToFloat64() (indices []uint64, keys []float64) {
	indices = m.negintIndices
	keys = make([]float64, len(m.negintKeys))
	for i := 0; i < len(m.negintKeys); i++ {
		keys[i] = -float64(m.negintKeys[i])
	}
	return
}

func (m *MixedTypeColumn) castPosintToFloat64() (indices []uint64, keys []float64) {
	indices = m.posintIndices
	keys = make([]float64, len(m.posintKeys))
	for i := 0; i < len(m.posintKeys); i++ {
		keys[i] = float64(m.posintKeys[i])
	}
	return
}

func (m *MixedTypeColumn) promoteSimplifiedTimestamps() {
	if len(m.fastTimestampKeys) == 0 {
		return
	}

	keys := make([]date.Time, len(m.fastTimestampKeys))
	for i := range m.fastTimestampKeys {
		keys[i] = simplifiedTimestampToTime(m.fastTimestampKeys[i])
	}

	m.timestampKeys = append(m.timestampKeys, keys...)
	m.timestampIndices = append(m.timestampIndices, m.fastTimestampIndices...)

	m.fastTimestampKeys = nil
	m.fastTimestampIndices = nil
}

type numericSorting int

const (
	sortNoNumericValues numericSorting = iota
	sortIntegers
	sortFloats
)

// Find out what kind of numeric conversion is neeed to properly sort
// numeric types. All low-level Ion numeric types (int, uint, float32,
// float32) are treated the same.
func chooseNumericSorting(col *MixedTypeColumn) numericSorting {

	if len(col.floatIndices) > 0 {
		return sortFloats
	}

	if len(col.posintIndices) > 0 || len(col.negintIndices) > 0 || len(col.zeroIndices) > 0 {
		return sortIntegers
	}

	return sortNoNumericValues
}
