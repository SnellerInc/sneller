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
	"github.com/SnellerInc/sneller/heap"
)

// Ktop stores the given number of k smallest/largest records.
type Ktop struct {
	// indirect is heap ordering of records;
	// we use integer indirection here so that
	// re-ordering the heap doesn't require swapping
	// the complete IonRecord structures
	indirect []int
	// storage for records, up to limit
	records []IonRecord

	orders []Ordering
	limit  int
}

// NewKtop constructs a new Ktop object.
func NewKtop(limit int, order []Ordering) *Ktop {
	return &Ktop{
		orders: order,
		limit:  limit,
	}
}

// Add tries to add a new record to collection.
//
// Returns true if the record was added.
func (k *Ktop) Add(rec *IonRecord) bool {
	return k.add(rec, true)
}

// Greatest returns the "largest" element
// in the heap, where "largest" is defined
// as the element furthest from the beginning
// of the sort order.
func (k *Ktop) Greatest() *IonRecord {
	if len(k.indirect) == 0 {
		return nil
	}
	return &k.records[k.indirect[0]]
}

// Full returns true if there are as many
// entries in the heap as LIMIT, otherwise false.
func (k *Ktop) Full() bool {
	return len(k.indirect) == k.limit
}

func (k *Ktop) add(rec *IonRecord, copyrec bool) bool {
	if len(k.records) < k.limit {
		if copyrec {
			rec.snapshot()
		}
		n := len(k.records)
		k.records = append(k.records, *rec)
		heap.PushSlice(&k.indirect, n, k.greater)
		return true
	}

	// new record less than max - overwite the max record
	// and then fix the heap to preserve the ordering
	if k.recGreater(&k.records[k.indirect[0]], rec) {
		k.records[k.indirect[0]].set(rec)
		heap.FixSlice(k.indirect, 0, k.greater)
		return true
	}

	return false
}

// Merge adds all records from another Ktop object.
func (k *Ktop) Merge(o *Ktop) {
	for i := range o.records {
		k.add(&o.records[i], false)
	}
}

// Records returns the current captured
// list of records. The order of the returned
// results is undefined.
func (k *Ktop) Records() []IonRecord {
	return k.records
}

// Capture returns sorted collection of records and cleans the collection.
func (k *Ktop) Capture() (result []IonRecord) {
	result = make([]IonRecord, len(k.indirect))
	i := len(k.indirect) - 1
	for len(k.indirect) > 0 {
		record := heap.PopSlice(&k.indirect, k.greater)
		result[i] = k.records[record]
		i -= 1
	}
	return result
}

func (k *Ktop) greater(leftnum, rightnum int) bool {
	return k.recGreater(&k.records[leftnum], &k.records[rightnum])
}

func (k *Ktop) recGreater(record1, record2 *IonRecord) bool {
	for i := range k.orders {
		raw1 := record1.UnsafeField(i)
		raw2 := record2.UnsafeField(i)
		cmp := k.orders[i].Compare(raw1, raw2)
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
		// cmp == 0 -> continue
	}
	return false
}
