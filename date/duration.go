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

package date

import (
	"fmt"
	"strings"
)

// A Duration represents a calendar duration
// with year, month, and day components. This is
// intended to be used to represent expiration
// times on a granularity of days. Use
// time.Duration to represent durations smaller
// than one day.
type Duration struct {
	Year, Month, Day int
}

// ParseDuration parses a duration string.
func ParseDuration(s string) (Duration, bool) {
	y, m, d, ok := parseDuration([]byte(s))
	if !ok || y == 0 && m == 0 && d == 0 {
		return Duration{}, false
	}
	return Duration{y, m, d}, true
}

// Add adds d to t.
func (d Duration) Add(t Time) Time {
	year := t.Year() + d.Year
	month := t.Month() + d.Month
	day := t.Day() + d.Day
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()
	ns := t.Nanosecond()
	return Date(year, month, day, hour, min, sec, ns)
}

// Sub subtracts d from t.
func (d Duration) Sub(t Time) Time {
	year := t.Year() - d.Year
	month := t.Month() - d.Month
	day := t.Day() - d.Day
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()
	ns := t.Nanosecond()
	return Date(year, month, day, hour, min, sec, ns)
}

// Zero returns whether d is equal to the zero
// value of a Duration.
func (d Duration) Zero() bool {
	return d == Duration{}
}

// String implements io.Stringer
func (d Duration) String() string {
	var sb strings.Builder
	if d.Year != 0 {
		fmt.Fprintf(&sb, "%dy", d.Year)
	}
	if d.Month != 0 {
		fmt.Fprintf(&sb, "%dm", d.Month)
	}
	if d.Day != 0 || d.Year == 0 && d.Month == 0 {
		fmt.Fprintf(&sb, "%dd", d.Day)
	}
	return sb.String()
}

// MarshalText implements encoding.TextMarshaler
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler
func (d *Duration) UnmarshalText(b []byte) error {
	dn, ok := ParseDuration(string(b))
	if !ok {
		return fmt.Errorf("date: failed to parse duration %q", b)
	}
	*d = dn
	return nil
}
