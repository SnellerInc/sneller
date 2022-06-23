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

// Design:
//
// A ThreadPool runs a SortingFunction that sorts given range of items (passed as [start:end]).
// The SortingFunction is supposed either to enqueue sorting subranges on the ThreadPool,
// or notify SortedDataConsumer that the range is sorted.
//
// It's SortedDataConsumer responsibility to Close the ThreadPool. Neither SortingFunction
// nor ThreadPools know the condition when the whole sorting is done (or not).

// SortingFunction sorts a range of indices given as the two first argument.
// Any additional arguments are implementation-defined and carried by the
// interface{} argument. A sorting function may, if needed, spawn new tasks
// on a thread pool.
type SortingFunction func(int, int, interface{}, ThreadPool)

// SortedDataConsumer coordinates process of sorting (which is likely multi-threaded).
type SortedDataConsumer interface {
	// Notify signal that a subrange [start:end] is already sorted.
	Notify(start, end int)

	// Start consuming sorted data which is sorted on a thread pool.
	// Once all data is sorted, a consumer is supposed to close the pool.
	Start(pool ThreadPool)
}

// SortedDataWriter is a row-aware writer that writes input rows
// which is used by SortedDataWriter to perform actual writing of
// sorted subranges of the input.
type SortedDataWriter interface {
	// Write writes range of sorted rows
	Write(start, end int) error
}

type ThreadPool interface {
	Enqueue(start, end int, fun SortingFunction, args interface{})
	Close(error)
	Wait() error
}
