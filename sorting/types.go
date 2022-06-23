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

// Direction encodes a sorting direction of column (SQL: ASC/DESC)
type Direction int

const (
	Ascending  Direction = 1  // Sort ascending
	Descending Direction = -1 // Sort descending
)

func (d Direction) reversed() Direction { return -d }

// NullsOrder encodes order of null values (SQL: NULL FIRST/NULLS LAST)
type NullsOrder int

const (
	NullsFirst NullsOrder = iota // Null values goes first
	NullsLast                    // Null values goes last
)

// ID is a row ID used internally by single column sorting.
//
// It's a combination of input chunk ID and row ID within a chunk.
type ID uint64

func (i ID) chunk() int {
	return int(i >> 32)
}

func (i ID) row() int {
	return int(i & 0xffffffff)
}

// IonRecord stores raw bytes of a single Ion structure alongside field locations.
//
// Fields are limited only to these used during sorting.
type IonRecord struct {
	// positions inside `Raw`
	FieldDelims [][2]uint32

	// raw holds bytes for boxed values and original Ion data (in this order)
	Raw []byte

	// the size of boxed values space in `Raw`
	Boxed uint32

	// SymtabID is the ID of the symbol table
	// associated with this record (used by external code)
	SymtabID int
}

func setSlice[T any](v *[]T, other []T) {
	if cap(*v) >= len(other) {
		*v = (*v)[:len(other)]
	} else {
		*v = make([]T, len(other))
	}
	copy((*v), other)
}

func (r *IonRecord) set(other *IonRecord) {
	setSlice(&r.FieldDelims, other.FieldDelims)
	setSlice(&r.Raw, other.Raw)
	r.Boxed = other.Boxed
	r.SymtabID = other.SymtabID
}

// Bytes returns Ion bytes
func (r *IonRecord) Bytes() []byte {
	return r.Raw[int(r.Boxed):]
}

// snapshot makes copy of all record memory.
//
// We assume that newly added records use shared memory (see ../../sort.go),
// thus to keep the records across calls we have to make a snapshot.
func (r *IonRecord) snapshot() {
	raw := make([]byte, len(r.Raw))
	copy(raw, r.Raw)
	fields := make([][2]uint32, len(r.FieldDelims))
	copy(fields, r.FieldDelims)

	r.Raw = raw
	r.FieldDelims = fields
}

// missingIndicator is an Ion value returned when a field is missing.
//
// It is a null value, as ordering of NULL and MISSING is exactly the same.
var missingIndicator = []byte{0x0f}

// UnsafeField retrieves Ion data associated with the i-th column.
//
// Asserts that the index is correct, is in 0..len(Fields)/2
func (r *IonRecord) UnsafeField(i int) []byte {
	offset := r.FieldDelims[i][0]
	length := r.FieldDelims[i][1]
	if length > 0 {
		return r.Raw[offset : offset+length]
	}

	return missingIndicator
}

// RuntimeParameters carries various setting that are used during sorting.
type RuntimeParameters struct {
	// Parallelism level
	Threads int

	ChunkAlignment int

	// Force using single threaded algorithm from Go standard library.
	UseStdlib bool

	// When sorting by single colum use a specalised sorting routine.
	UseSingleColumnSorter bool

	// Use AVX512-assisted sorting
	UseAVX512Sorter bool

	// Parameter for the parallel quicksort: the minimum size
	// of subarray which is processed in parallel; shorter
	// subarrays are sorted on a single thread.
	QuicksortSplitThreshold int

	// Use a specialised k-top procedure when query's limit is not
	// larger then the threshold.
	KtopLimitThreshold int
}

// RuntimeOption is a function that sets given option of RuntimeParameters instance.
type RuntimeOption func(*RuntimeParameters)

// NewRuntimeParameters creates default runtime parameters.
func NewRuntimeParameters(threads int, opts ...RuntimeOption) (rp RuntimeParameters) {
	rp.Threads = threads
	rp.ChunkAlignment = 1024 * 1024 // FIXME: obtain it from the core?
	rp.UseStdlib = false
	rp.UseSingleColumnSorter = true
	rp.UseAVX512Sorter = true
	rp.QuicksortSplitThreshold = 5 * 1024
	rp.KtopLimitThreshold = 1000 // FIXME: it's a wild guess

	for i := range opts {
		opts[i](&rp)
	}

	return
}

// WithAVX512Sorter is an option for NewRuntimeParameters.
func WithAVX512Sorter(enable bool) RuntimeOption {
	return func(rp *RuntimeParameters) {
		rp.UseAVX512Sorter = enable
	}
}
