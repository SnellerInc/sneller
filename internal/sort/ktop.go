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

package sort

import (
	"container/heap"
)

// Ktop stores the given number of k smallest/largest records.
type Ktop struct {
	records []IonRecord
	orders  []Ordering
	limit   int
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

func (k *Ktop) add(rec *IonRecord, copyrec bool) bool {
	if len(k.records) < k.limit {
		if copyrec {
			rec.snapshot()
		}
		heap.Push(k, *rec)
		return true
	}

	// new record less than max - add it to the heap and discard the current max
	if k.less(rec, &k.records[0]) {
		if copyrec {
			rec.snapshot()
		}
		k.records[0] = *rec
		heap.Fix(k, 0)
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
	result = make([]IonRecord, k.Len())
	i := k.Len() - 1
	for len(k.records) > 0 {
		record := heap.Pop(k).(IonRecord)
		result[i] = record
		i -= 1
	}

	return result
}

// Len implements sort.Interface
func (k *Ktop) Len() int { return len(k.records) }

func (k *Ktop) less(record1, record2 *IonRecord) bool {
	for i := range k.orders {
		raw1 := record1.UnsafeField(i)
		raw2 := record2.UnsafeField(i)
		cmp := k.orders[i].Compare(raw1, raw2)
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
		// cmp == 0 -> continue
	}
	return false
}

// Less implements sort.Interface
func (k *Ktop) Less(i, j int) bool {
	return k.less(&k.records[j], &k.records[i]) // note swapped indices
}

// Swap implements sort.Interface
func (k *Ktop) Swap(i, j int) {
	k.records[i], k.records[j] = k.records[j], k.records[i]
}

// Push implements heap.Interface
func (k *Ktop) Push(x interface{}) {
	k.records = append(k.records, x.(IonRecord))
}

// Pop implements heap.Interface
func (k *Ktop) Pop() interface{} {
	old := k.records
	n := len(old)
	x := old[n-1]
	k.records = old[0 : n-1]
	return x
}
