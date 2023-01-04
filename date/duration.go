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
