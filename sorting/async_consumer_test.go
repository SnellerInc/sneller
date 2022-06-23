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
	"reflect"
	"testing"
)

func TestAsyncConsumerCase1(t *testing.T) {

	notifications := []indicesRange{
		indicesRange{start: 50, end: 60},
		indicesRange{start: 0, end: 20},
		indicesRange{start: 40, end: 49},
		indicesRange{start: 21, end: 39},
		indicesRange{start: 61, end: 100}}

	testAsyncConsumer(t, notifications)
}

func TestAsyncConsumerCase2(t *testing.T) {

	notifications := []indicesRange{
		indicesRange{start: 21, end: 39},
		indicesRange{start: 61, end: 100},
		indicesRange{start: 50, end: 60},
		indicesRange{start: 40, end: 49},
		indicesRange{start: 0, end: 20}}

	testAsyncConsumer(t, notifications)
}

func TestAsyncConsumerCase3(t *testing.T) {

	notifications := []indicesRange{
		indicesRange{start: 0, end: 100}}

	testAsyncConsumer(t, notifications)
}

func executeNotifications(notifications []indicesRange, limit *Limit) *FakeSortedDataWriter {
	var writer FakeSortedDataWriter

	consumer := NewAsyncConsumer(&writer, 0, 100, limit)
	threads := 1
	threadPool := NewThreadPool(threads)

	consumer.Start(threadPool)

	go func() {
		for _, r := range notifications {
			consumer.Notify(r.start, r.end)
		}
	}()

	threadPool.Wait()

	return &writer
}

func testAsyncConsumer(t *testing.T, notifications []indicesRange) {

	writer := executeNotifications(notifications, nil)

	if !isRange(writer.items, 0, 100) {
		t.Errorf("Expected range from 0 to 100, got %v", writer.items)
	}

	{
		expectedCalls := len(notifications)
		if writer.writeCalls != expectedCalls {
			t.Errorf("writer.Write() got called %d time, expected count is %d",
				writer.writeCalls, expectedCalls)
		}
	}
}

// --------------------------------------------------

func TestAsyncConsumerLimitHandling(t *testing.T) {
	R := func(start, end int) indicesRange {
		return indicesRange{start: start, end: end}
	}

	all := make([]int, 101)
	for i := range all {
		all[i] = i
	}

	testcases := []struct {
		limit         Limit
		items         []int
		notifications []indicesRange
	}{
		{
			limit:         Limit{Kind: LimitToHeadRows, Offset: 0, Limit: 5},
			items:         []int{0, 1, 2, 3, 4},
			notifications: []indicesRange{R(0, 100)},
		},
		{
			limit:         Limit{Kind: LimitToHeadRows, Offset: 0, Limit: 5},
			items:         []int{0, 1, 2, 3, 4},
			notifications: []indicesRange{R(0, 0), R(1, 1), R(2, 2), R(3, 3), R(4, 4), R(5, 100)},
		},
		{
			limit:         Limit{Kind: LimitToHeadRows, Offset: 0, Limit: 150},
			items:         all,
			notifications: []indicesRange{R(0, 0), R(1, 1), R(2, 2), R(3, 3), R(4, 4), R(5, 100)},
		},
		{
			limit:         Limit{Kind: LimitToHeadRows, Offset: 0, Limit: 5},
			items:         []int{0, 1, 2, 3, 4},
			notifications: []indicesRange{R(0, 3), R(4, 20), R(21, 100)},
		},
		{
			limit:         Limit{Kind: LimitToTopRows, Offset: 0, Limit: 6},
			items:         []int{95, 96, 97, 98, 99, 100},
			notifications: []indicesRange{R(0, 100)},
		},
		{
			limit:         Limit{Kind: LimitToTopRows, Offset: 0, Limit: 200},
			items:         all,
			notifications: []indicesRange{R(0, 100)},
		},
		{
			limit:         Limit{Kind: LimitToRange, Offset: 40, Limit: 4},
			items:         []int{40, 41, 42, 43},
			notifications: []indicesRange{R(0, 100)},
		},
		{
			limit:         Limit{Kind: LimitToRange, Offset: 90, Limit: 40},
			items:         []int{90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100},
			notifications: []indicesRange{R(0, 100)},
		},
	}

	for i := range testcases {
		tc := &testcases[i]
		writer := executeNotifications(tc.notifications, &tc.limit)
		if !reflect.DeepEqual(writer.items, tc.items) {
			t.Errorf("expected %v, got %v\n", tc.items, writer.items)
		}
	}
}

// --------------------------------------------------

func TestAsyncConsumerPassesErrorFromWrite(t *testing.T) {
	// given
	var writer FakeSortedDataWriter
	writer.writeErr = fmt.Errorf("Write error")

	testPassingErrorsToThreadPool(t, writer, writer.writeErr)
}

func testPassingErrorsToThreadPool(t *testing.T, writer FakeSortedDataWriter, expected error) {
	// given
	consumer := NewAsyncConsumer(&writer, 0, 100, nil)
	threads := 1
	threadPool := NewThreadPool(threads)

	// when
	consumer.Start(threadPool)

	go consumer.Notify(0, 100)

	err := threadPool.Wait()

	// then
	if err == nil {
		t.Errorf("Expected error to be returned")
		return
	}

	if err.Error() != expected.Error() {
		t.Errorf("Expected error %v, got %v", expected, err)
	}
}

// --------------------------------------------------

type FakeSortedDataWriter struct {
	items      []int
	writeCalls int
	writeErr   error
}

func (w *FakeSortedDataWriter) Write(start, end int) error {
	for i := start; i <= end; i++ {
		w.items = append(w.items, i)
	}
	w.writeCalls += 1
	return w.writeErr
}

// --------------------------------------------------

func isRange(items []int, start, end int) bool {
	j := 0
	for i := start; i <= end; i++ {
		if items[j] != i {
			return false
		}
		j++
	}

	return true
}
