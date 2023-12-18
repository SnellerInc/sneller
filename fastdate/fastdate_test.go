// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package fastdate

import "testing"

func testDateTimeRecomposition(t *testing.T, unixtime int64) {
	dt, time := dateTimeFromTimestamp(Timestamp(unixtime))
	unixtime2 := int64(unixTimeFromDateTime(dt, time))

	if unixtime != unixtime2 {
		t.Errorf("Failed to recompose %d | decomposed year(%d) month(%d) day(%d) unixtime(%d)", unixtime, dt.Year(), dt.Month(), dt.Day(), unixtime2)
	}
}

func TestFastDate(t *testing.T) {
	testDateTimeRecomposition(t, -1000000000000000)
	testDateTimeRecomposition(t, -100000000000000)
	testDateTimeRecomposition(t, -10000000000000)
	testDateTimeRecomposition(t, -1000000000000)
	testDateTimeRecomposition(t, -100000000000)
	testDateTimeRecomposition(t, -10000000000)
	testDateTimeRecomposition(t, -1000000000)
	testDateTimeRecomposition(t, -100000000)
	testDateTimeRecomposition(t, -10000000)
	testDateTimeRecomposition(t, -1000000)
	testDateTimeRecomposition(t, -100000)
	testDateTimeRecomposition(t, -10000)
	testDateTimeRecomposition(t, -1000)
	testDateTimeRecomposition(t, -100)
	testDateTimeRecomposition(t, -10)
	testDateTimeRecomposition(t, -1)
	testDateTimeRecomposition(t, 0)
	testDateTimeRecomposition(t, 1)
	testDateTimeRecomposition(t, 10)
	testDateTimeRecomposition(t, 100)
	testDateTimeRecomposition(t, 1000)
	testDateTimeRecomposition(t, 10000)
	testDateTimeRecomposition(t, 100000)
	testDateTimeRecomposition(t, 1000000)
	testDateTimeRecomposition(t, 10000000)
	testDateTimeRecomposition(t, 100000000)
	testDateTimeRecomposition(t, 1000000000)
	testDateTimeRecomposition(t, 10000000000)
	testDateTimeRecomposition(t, 100000000000)
	testDateTimeRecomposition(t, 1000000000000)
	testDateTimeRecomposition(t, 10000000000000)
	testDateTimeRecomposition(t, 100000000000000)
	testDateTimeRecomposition(t, 1000000000000000)
	testDateTimeRecomposition(t, 10000000000000000)
	testDateTimeRecomposition(t, 100000000000000000)
}
