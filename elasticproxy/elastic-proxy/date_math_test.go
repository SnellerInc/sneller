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
