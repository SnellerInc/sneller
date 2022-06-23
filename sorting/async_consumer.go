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
	"container/heap"
)

// indicesRange is a closed interval of indices (both start and end are inclusive).
//
// There's an assumption that a valid range holds start <= end.
type indicesRange struct {
	start, end int
}

// disjoint checks if ranges don't have any common indices.
func (r *indicesRange) disjoint(r2 indicesRange) bool {
	return r.end < r2.start || r.start > r2.end
}

// contains checks if range contains the given index.
func (r *indicesRange) contains(x int) bool {
	return x >= r.start && x <= r.end
}

type AsyncConsumer struct {
	writer    SortedDataWriter
	pool      ThreadPool
	all       indicesRange     // what is the range of indices to sort
	limit     indicesRange     // subrange of `all` to be written out (limit==all if no limits are applied)
	remaining indicesRange     // tail of `all` that left to sort
	queue     sortedRangeQueue // already sorted subranges that are not ready to be written out

	ready chan indicesRange
}

func NewAsyncConsumer(writer SortedDataWriter, start, end int, limit *Limit) SortedDataConsumer {
	consumer := AsyncConsumer{
		writer:    writer,
		queue:     sortedRangeQueue{},
		all:       indicesRange{start, end},
		remaining: indicesRange{start, end},
		ready:     make(chan indicesRange)}

	if limit == nil {
		consumer.limit = consumer.all
	} else {
		rowsCount := end - start + 1
		l := limit.FinalRange(rowsCount)
		consumer.limit = indicesRange{start: start + l.start, end: start + l.end}
	}

	heap.Init(&consumer.queue)

	return &consumer
}

// Notify informs the consumer that range [start:end] of input rows is sorted.
//
// We are assuming that all incoming ranges are disjoint and finally
// their sum equals to a.all.
func (a *AsyncConsumer) Notify(start, end int) {
	a.ready <- indicesRange{start, end}
}

// Start implements SortedDataConsumer
func (a *AsyncConsumer) Start(pool ThreadPool) {
	a.pool = pool

	go func() {

		writeAllReadyChunks := func() error {
			for len(a.queue) > 0 {
				if (a.queue)[0].start != a.remaining.start {
					break
				}

				// range r covers the head of remaining range, it can be saved
				r := heap.Pop(&a.queue).(indicesRange)
				if !r.disjoint(a.limit) {
					start := maxInt(r.start, a.limit.start)
					end := minInt(r.end, a.limit.end)

					err := a.writer.Write(start, end)
					if err != nil {
						return err
					}
				}

				// advance the pointer
				a.remaining.start = r.end + 1
			}

			return nil
		}

		var err error

		for {
			// get the next sorted subrange
			r := <-a.ready
			heap.Push(&a.queue, r)

			err = writeAllReadyChunks()
			if err != nil {
				break
			}

			// if the last range has just been flushed, we're done
			if a.remaining.start >= a.remaining.end {
				break
			}
		}

		a.pool.Close(err)
	}()
}

// sortedRangeQueue keeps sort ranges ordered by the start index
type sortedRangeQueue []indicesRange

// Len implements sort.Interface
func (r sortedRangeQueue) Len() int { return len(r) }

// Less implements sort.Interface
func (r sortedRangeQueue) Less(i, j int) bool { return r[i].start < r[j].start }

// Swap implements sort.Interface
func (r sortedRangeQueue) Swap(i, j int) {
	r[i].start, r[j].start = r[j].start, r[i].start
	r[i].end, r[j].end = r[j].end, r[i].end
}

// Push implements heap.Interface
func (r *sortedRangeQueue) Push(x interface{}) {
	*r = append(*r, x.(indicesRange))
}

// Pop implements heap.Interface
func (r *sortedRangeQueue) Pop() interface{} {
	old := *r
	n := len(old)
	x := old[n-1]
	*r = old[0 : n-1]
	return x
}
