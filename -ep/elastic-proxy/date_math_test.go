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

package elastic_proxy

import (
	"testing"
	"time"
)

func TestDateMath(t *testing.T) {
	loc, _ := time.LoadLocation("Europe/Amsterdam")
	now, _ := time.ParseInLocation(time.RFC3339Nano, "2022-05-12T14:51:34.123456+02:00", loc)
	testItems := []struct {
		text    string
		rfcTime string
	}{
		{"now", "2022-05-12T14:51:34.123456+02:00"},
		{"now/s", "2022-05-12T14:51:34+02:00"},
		{"now/m", "2022-05-12T14:51:00+02:00"},
		{"now/h", "2022-05-12T14:00:00+02:00"},
		{"now/H", "2022-05-12T14:00:00+02:00"},
		{"now/d", "2022-05-12T00:00:00+02:00"},
		{"now/w", "2022-05-08T00:00:00+02:00"},
		{"now/M", "2022-05-01T00:00:00+02:00"},
		{"now/y", "2022-01-01T00:00:00+01:00"},
		{"now+10s", "2022-05-12T14:51:44.123456+02:00"},
		{"now+10m", "2022-05-12T15:01:34.123456+02:00"},
		{"now+10h", "2022-05-13T00:51:34.123456+02:00"},
		{"now+10H", "2022-05-13T00:51:34.123456+02:00"},
		{"now+10d", "2022-05-22T14:51:34.123456+02:00"},
		{"now+10w", "2022-07-21T14:51:34.123456+02:00"},
		{"now+10M", "2023-03-12T14:51:34.123456+01:00"},
		{"now+10y", "2032-05-12T14:51:34.123456+02:00"},
		{"now-10s", "2022-05-12T14:51:24.123456+02:00"},
		{"now-10m", "2022-05-12T14:41:34.123456+02:00"},
		{"now/d+10m", "2022-05-12T00:10:00+02:00"},
		{"now-1d/d+10m", "2022-05-11T00:10:00+02:00"},
		{"now+1y-2M+3d-4w+5h-6m+7s", "2023-02-15T19:45:41.123456+01:00"},
		{"now+1y-2M+3d-4w+5h-6m+7s/s", "2023-02-15T19:45:41+01:00"},
		{"2022.01.03||+1M", "2022-02-03T00:00:00Z"},
		{"2022.01.03 12:34||+1M", "2022-02-03T12:34:00Z"},
		{"2022.01.03 12:34:56||+1M", "2022-02-03T12:34:56Z"},
		{"2022.01.03 12:34:56.123||+1M", "2022-02-03T12:34:56.123Z"},
		{"2022.01.03 12:34:56.123456||+1M", "2022-02-03T12:34:56.123456Z"},
	}
	for _, item := range testItems {
		t.Run(item.text, func(t *testing.T) {
			result, err := ParseDateMath(item.text, now)
			if err != nil {
				if item.rfcTime != err.Error() {
					t.Fatalf("error: %v (expected: %v)", err.Error(), item.rfcTime)
				}
			} else {
				rfcTime := result.Format(time.RFC3339Nano)
				if rfcTime != item.rfcTime {
					t.Fatalf("got: %v, expected: %v", rfcTime, item.rfcTime)
				}
			}
		})
	}
}
