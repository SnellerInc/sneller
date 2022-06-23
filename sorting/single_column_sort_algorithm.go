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

import "github.com/SnellerInc/sneller/date"

func scsExecute(column *MixedTypeColumn, plan []sortAction, threadPool ThreadPool, mainConsumer SortedDataConsumer, rp *RuntimeParameters) {
	for i := range plan {
		sf := func(start, end int, args interface{}, tp ThreadPool) {
			action := args.(sortAction)
			consumer := proxyConsumer{globalOffset: action.start,
				parent: mainConsumer}
			action.invoke(consumer, tp, column, rp)
		}

		// Note: we use -1, -1 as the action already keeps the range it working on.
		threadPool.Enqueue(-1, -1, sf, plan[i])
	}
}

// --------------------------------------------------

// proxyConsumer implements SortedDataConsumer interface.
//
// Its responsibility is translation of notifications from
// sorted subrange into notifications related to the whole
// column, managed by the parent consumer.
//
// (It may seem overkill, as the current actions notify
// for the whole subarray they sort. However, in the future
// we'll have multi-threaded sorting.)
type proxyConsumer struct {
	// Start of range being sorted
	//
	// There's no need to store length of range, because sorting
	// procedures will never cross the boundary of their subranges.
	globalOffset int

	parent SortedDataConsumer
}

func (p proxyConsumer) Notify(start, end int) {
	p.parent.Notify(start+p.globalOffset, end+p.globalOffset)
}

func (p proxyConsumer) Start(ThreadPool) {
	// not needed, the parent already manages the pool
}

// --------------------------------------------------

//// interfaces for golang sort package

type sortUint64Asc struct {
	keys    []uint64
	indices []uint64
}

func (s *sortUint64Asc) Len() int {
	return len(s.keys)
}

func (s *sortUint64Asc) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}

func (s *sortUint64Asc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortFloat64Asc struct {
	keys    []float64
	indices []uint64
}

func (s *sortFloat64Asc) Len() int {
	return len(s.keys)
}

func (s *sortFloat64Asc) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}

func (s *sortFloat64Asc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortStringAsc struct {
	keys    []string
	indices []uint64
}

func (s *sortStringAsc) Len() int {
	return len(s.keys)
}

func (s *sortStringAsc) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}

func (s *sortStringAsc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortDateTimeAsc struct {
	keys    []date.Time
	indices []uint64
}

func (s *sortDateTimeAsc) Len() int {
	return len(s.keys)
}

func (s *sortDateTimeAsc) Less(i, j int) bool {
	return s.keys[i].Before(s.keys[j])
}

func (s *sortDateTimeAsc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortUint64Desc struct {
	keys    []uint64
	indices []uint64
}

func (s *sortUint64Desc) Len() int {
	return len(s.keys)
}

func (s *sortUint64Desc) Less(i, j int) bool {
	return s.keys[i] > s.keys[j]
}

func (s *sortUint64Desc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortFloat64Desc struct {
	keys    []float64
	indices []uint64
}

func (s *sortFloat64Desc) Len() int {
	return len(s.keys)
}

func (s *sortFloat64Desc) Less(i, j int) bool {
	return s.keys[i] > s.keys[j]
}

func (s *sortFloat64Desc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortStringDesc struct {
	keys    []string
	indices []uint64
}

func (s *sortStringDesc) Len() int {
	return len(s.keys)
}

func (s *sortStringDesc) Less(i, j int) bool {
	return s.keys[i] > s.keys[j]
}

func (s *sortStringDesc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

// --------------------------------------------------

type sortDateTimeDesc struct {
	keys    []date.Time
	indices []uint64
}

func (s *sortDateTimeDesc) Len() int {
	return len(s.keys)
}

func (s *sortDateTimeDesc) Less(i, j int) bool {
	return s.keys[j].Before(s.keys[i])
}

func (s *sortDateTimeDesc) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}
