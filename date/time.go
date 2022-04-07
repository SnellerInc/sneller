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
	"errors"
	"fmt"
	"time"
)

// A Time represents a date and time with a nanosecond
// component. This representation allows for faster
// extraction of date components than time.Time, at the
// cost of slower conversion to Unix times.
//
// This representation cannot store years below 0 or
// above 16,383. Years falling outside that range will
// be truncated to fit within that range.
type Time struct {
	ts uint64
	ns uint32
}

// Parse parses a date string from data
// and returns the associated time and true,
// or the zero time value and false if the buffer
// did not contain a recognzied date format.
//
// Parse attempts to recognize strings
// that (approximately) match RFC3339 timestamps
// with optional nanosecond precision and timezone/offset
// components. Parse will automatically ignore leading
// and trailing whitespace as long as the middle characters
// of data are unambiguously a timestamp.
func Parse(data []byte) (Time, bool) {
	year, month, day, hour, min, sec, ns, ok := parse(data)
	if !ok {
		return Time{}, false
	}
	return Date(year, month, day, hour, min, sec, ns), true
}

// Date constructs a Time from components. Values of
// month, day, hour, min, sec, and ns outside their
// usual ranges will be normalized. Values for year
// outside of the range [0, 16383] will be truncated to
// fit within that range.
func Date(year, month, day, hour, min, sec, ns int) Time {
	sec, ns = norm(sec, ns, 1e9)
	min, sec = norm(min, sec, 60)
	hour, min = norm(hour, min, 60)
	day, hour = norm(day, hour, 24)
	year, month, day = normdate(year, month, day)
	return date(year, month, day, hour, min, sec, ns)
}

func date(year, month, day, hour, min, sec, ns int) Time {
	if year < 0 {
		year = 0
	} else if year > (1<<14)-1 {
		year = (1 << 14) - 1
	}
	ts := (uint64(year) & 0xffff << 40) |
		(uint64(month-1) & 0xff << 32) |
		(uint64(day-1) & 0xff << 24) |
		(uint64(hour) & 0xff << 16) |
		(uint64(min) & 0xff << 8) |
		(uint64(sec) & 0xff)
	return Time{ts: ts, ns: uint32(ns)}
}

// FromTime returns a Time equivalent to t.
func FromTime(t time.Time) Time {
	t = t.UTC()
	year, month, day := t.Year(), int(t.Month()), t.Day()
	hour, min, sec := t.Hour(), t.Minute(), t.Second()
	ns := t.Nanosecond()
	return date(year, month, day, hour, min, sec, ns)
}

// Now returns the current time.
func Now() Time {
	return FromTime(time.Now())
}

// UnixMicro returns a Time from the given Unix time in
// seconds and nanoseconds.
func Unix(sec, ns int64) Time {
	return FromTime(time.Unix(sec, ns))
}

// UnixMicro returns a Time from the given Unix time in
// microseconds.
func UnixMicro(us int64) Time {
	return FromTime(time.UnixMicro(us))
}

// Time returns t as a time.Time.
func (t Time) Time() time.Time {
	year, month, day := t.Year(), time.Month(t.Month()), t.Day()
	hour, min, sec := t.Hour(), t.Minute(), t.Second()
	return time.Date(year, month, day, hour, min, sec, int(t.ns), time.UTC)
}

// Year returns the year component of t.
func (t Time) Year() int {
	return int(t.ts & 0xffff0000000000 >> 40)
}

// Month returns the month component of t.
func (t Time) Month() int {
	return int(t.ts&0xff00000000>>32) + 1
}

// Day returns the day component of t.
func (t Time) Day() int {
	return int(t.ts&0xff000000>>24) + 1
}

// Hour returns the hour component of t.
func (t Time) Hour() int {
	return int(t.ts & 0xff0000 >> 16)
}

// Minute returns the minute component of t.
func (t Time) Minute() int {
	return int(t.ts & 0xff00 >> 8)
}

// Second returns the second component of t.
func (t Time) Second() int {
	return int(t.ts & 0xff)
}

// Nanosecond returns the nanosecond component of t.
func (t Time) Nanosecond() int {
	return int(t.ns)
}

// Unix returns t as the number of seconds since the
// Unix epoch.
func (t Time) Unix() int64 {
	return t.Time().Unix()
}

// Unix returns t as the number of microseconds since
// the Unix epoch.
func (t Time) UnixMicro() int64 {
	return t.Time().UnixMicro()
}

// Unix returns t as the number of nanoseconds since
// the Unix epoch.
func (t Time) UnixNano() int64 {
	return t.Time().UnixNano()
}

// Equal returns whether t == t2.
func (t Time) Equal(t2 Time) bool {
	return t == t2
}

// Before returns whether t is before t2.
func (t Time) Before(t2 Time) bool {
	return t.ts < t2.ts || (t.ts == t2.ts && t.ns < t2.ns)
}

// Before returns whether t is after t2.
func (t Time) After(t2 Time) bool {
	return t.ts > t2.ts || (t.ts == t2.ts && t.ns > t2.ns)
}

// IsZero returns whether t is the zero value,
// corresponding to January 1st of year zero.
func (t Time) IsZero() bool {
	return t == Time{}
}

// AppendRFC3339 appends t formatted as an RFC3339
// compliant string to b.
func (t Time) AppendRFC3339(b []byte) []byte {
	return t.Time().AppendFormat(b, time.RFC3339)
}

// AppendRFC3339Nano is like AppendRFC3339 but includes
// nanoseconds.
func (t Time) AppendRFC3339Nano(b []byte) []byte {
	return t.Time().AppendFormat(b, time.RFC3339Nano)
}

// Add adds d to t.
func (t Time) Add(d time.Duration) Time {
	return FromTime(t.Time().Add(d))
}

// Round rounds t to the nearest multiple of d.
func (t Time) Round(d time.Duration) Time {
	return FromTime(t.Time().Round(d))
}

// Truncate rounds t down to a multiple of d.
func (t Time) Truncate(d time.Duration) Time {
	return FromTime(t.Time().Truncate(d))
}

// String implements io.Stringer. The returned string
// is meant to be used for debugging purposes.
func (t Time) String() string {
	y, mo, d := t.Year(), t.Month(), t.Day()
	h, mi, s := t.Hour(), t.Minute(), t.Second()
	ns := t.Nanosecond()
	if ns == 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d +0000 UTC", y, mo, d, h, mi, s)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%d +0000 UTC", y, mo, d, h, mi, s, ns)
}

// MarshalJSON implements json.Marshaler.
func (t Time) MarshalJSON() ([]byte, error) {
	return t.AppendRFC3339Nano(nil), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Time) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var ok bool
	*t, ok = Parse(b[1 : len(b)-1])
	if !ok {
		return errors.New("failed to parse JSON")
	}
	return nil
}

var monthdays = [12]int{
	31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
}

func daysin(y, m int) int {
	d := monthdays[m-1]
	if m == 2 && isleap(y) {
		d++
	}
	return d
}

func normdate(y, m, d int) (year, month, day int) {
	y, m = norm(y, m-1, 12)
	m++
	md := daysin(y, m)
	if d >= 1 && d <= md {
		return y, m, d
	}
	for d < 1 {
		if m--; m < 1 {
			y, m = y-1, 12
		}
		md = daysin(y, m)
		d += md
	}
	for ; d > md; md = daysin(y, m) {
		d -= md
		if m++; m > 12 {
			y, m = y+1, 1
		}
	}
	return y, m, d
}
