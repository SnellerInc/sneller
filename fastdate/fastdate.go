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

// DateTime composition and decomposition is based on the following article:
//
//   https://howardhinnant.github.io/date_algorithms.html

const daysPer400YearCycle = 146097
const millisecondsPerSecond = 1000
const microsecondsPerSecond = 1000000
const microsecondsPerMinute = 60 * microsecondsPerSecond
const microsecondsPerHour = 60 * microsecondsPerMinute
const microsecondsPerDay = 24 * microsecondsPerHour // 86400000000

const unixDaysToYear0Delta = 719468

var extractDoyPredicate = [12]uint32{
	59,  // March
	90,  // April
	120, // May
	151, // June
	181, // July
	212, // August
	243, // September
	273, // October
	304, // November
	334, // December
	0,   // January
	31,  // February
}

var truncQuarterPredicate = [12]byte{
	10, // March     -> January (previous year of the internal format)
	1,  // April     -> April
	1,  // May       -> April
	1,  // June      -> April
	4,  // July      -> July
	4,  // August    -> July
	4,  // September -> July
	7,  // October   -> October
	7,  // November  -> October
	7,  // December  -> October
	10, // January   -> January
	10, // February  -> January
}

type Timestamp int64

type DecomposedDate struct {
	year  int32
	month uint16 // from 0 to 11 (starting from zero)
	day   uint16 // from 0 to 30 (starting from zero)
}

func isLeapYear(y int32) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

func floorDivInt32(x, y int32) int32 {
	if x < 0 {
		x = x - y + 1
	}
	return x / y
}

func floorDivInt64(x, y int64) int64 {
	if x < 0 {
		x = x - y + 1
	}
	return x / y
}

func dateFromUnixDays(days int64) DecomposedDate {
	days += unixDaysToYear0Delta

	era := floorDivInt64(days, daysPer400YearCycle)
	doe := uint32(days - era*daysPer400YearCycle)
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365

	y := int32(yoe) + int32(era)*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	m := (5*doy + 2) / 153
	d := doy - (153*m+2)/5

	return DecomposedDate{
		year:  y,
		month: uint16(m),
		day:   uint16(d),
	}
}

func unixDaysFromDate(dd DecomposedDate) int64 {
	y := dd.year
	m := uint32(dd.month)
	d := uint32(dd.day)

	era := floorDivInt32(y, 400)
	yoe := uint32(y - era*400)             // [0..399]
	doy := (153*(m)+2)/5 + d               // [0..365]
	doe := yoe*365 + yoe/4 - yoe/100 + doy // [0..146096]

	return int64(era)*daysPer400YearCycle + int64(doe) - unixDaysToYear0Delta
}

func extractNumDaysAndTimeFromUnixTime(ts Timestamp) (int64, uint64) {
	days := floorDivInt64(int64(ts), microsecondsPerDay)
	return days, uint64(int64(ts) - days*microsecondsPerDay)
}

func dateTimeFromTimestamp(ts Timestamp) (DecomposedDate, uint64) {
	days, time := extractNumDaysAndTimeFromUnixTime(ts)
	return dateFromUnixDays(days), time
}

func unixTimeFromDateTime(dd DecomposedDate, time uint64) Timestamp {
	days := unixDaysFromDate(dd)
	return Timestamp(days*microsecondsPerDay + int64(time))
}

func (dd DecomposedDate) Year() int32 {
	y := dd.year
	if dd.month >= 10 {
		y++
	}
	return y
}

func (dd DecomposedDate) Month() uint32 {
	m := uint32(dd.month) + 3
	if m > 12 {
		m -= 12
	}
	return m
}

func (dd DecomposedDate) Quarter() uint32 {
	// Maps a month starting from March into a quarter starting from 0.
	predicate := (uint64(1) << 0) |
		(uint64(2) << 4) |
		(uint64(2) << 8) |
		(uint64(2) << 12) |
		(uint64(3) << 16) |
		(uint64(3) << 20) |
		(uint64(3) << 24) |
		(uint64(4) << 28) |
		(uint64(4) << 32) |
		(uint64(4) << 36) |
		(uint64(1) << 40) |
		(uint64(1) << 44)
	return uint32(predicate>>(dd.month<<2)) & 0xF
}

func (dd DecomposedDate) Day() uint32 {
	return uint32(dd.day) + 1
}

func (ts Timestamp) AddMicrosecond(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val), true
}

func (ts Timestamp) AddMillisecond(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val*1000), true
}

func (ts Timestamp) AddSecond(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val*1000000), true
}

func (ts Timestamp) AddMinute(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val*microsecondsPerMinute), true
}

func (ts Timestamp) AddHour(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val*microsecondsPerHour), true
}

func (ts Timestamp) AddDay(val int64) (Timestamp, bool) {
	return Timestamp(int64(ts) + val*microsecondsPerDay), true
}

func (ts Timestamp) AddMonth(val int64) (Timestamp, bool) {
	dd, time := dateTimeFromTimestamp(ts)

	m := int64(dd.month) + val

	yDiff := floorDivInt64(m, 12)
	y := int64(dd.year) + yDiff

	dd.month = uint16(m - yDiff*12)
	dd.year = int32(y)

	return Timestamp(unixTimeFromDateTime(dd, time)), true
}

func (ts Timestamp) AddQuarter(val int64) (Timestamp, bool) {
	return ts.AddMonth(val * 3)
}

func (ts Timestamp) AddYear(val int64) (Timestamp, bool) {
	dd, time := dateTimeFromTimestamp(ts)
	y := int64(dd.year) + val
	dd.year = int32(y)
	return unixTimeFromDateTime(dd, time), true
}

func (ts Timestamp) DateBin(origin Timestamp, stride int64) (Timestamp, bool) {
	delta := int64(ts) - int64(origin)
	truncated := floorDivInt64(delta, int64(stride)) * int64(stride)
	return Timestamp(truncated + int64(origin)), true
}

func (ts Timestamp) DateDiffMicrosecond(origin Timestamp) (Timestamp, bool) {
	return Timestamp(int64(origin) - int64(ts)), true
}

func (ts Timestamp) DateDiffParam(origin Timestamp, param uint64) (int64, bool) {
	a := int64(origin) >> 3
	b := int64(ts) >> 3

	delta := a - b
	diff := delta / int64(param>>3)

	return diff, true
}

func (ts Timestamp) DateDiffMonth(other Timestamp) int64 {
	inverted := ts > other
	if inverted {
		ts, other = other, ts
	}

	// a is the lesser timestamp and b is the greater one.
	aDate, aTime := dateTimeFromTimestamp(ts)

	// Greater timestamp's value decremented by hours/minutes/... from the lesser timestamp.
	other -= Timestamp(aTime)
	bDate, _ := dateTimeFromTimestamp(other)

	// Calculate `year * 12 + months` for each timestamp.
	am := int64(aDate.year)*12 + int64(aDate.month)
	bm := int64(bDate.year)*12 + int64(bDate.month)

	// Rough months difference - 1.
	m := (bm - am) - 1

	// Increment one month if the lesser timestamp's day of month <= greater timestamp's day of month.
	if aDate.day <= bDate.day {
		m += 1
	}

	// Final months difference - always positive at this point.
	if m < 0 {
		m = 0
	}

	if inverted {
		m = -m
	}

	return m
}

func (ts Timestamp) ExtractMicrosecond() uint32 {
	result := int32(ts % microsecondsPerMinute)
	if result < 0 {
		result += microsecondsPerMinute
	}
	return uint32(result)
}

func (ts Timestamp) ExtractMillisecond() uint32 {
	return ts.ExtractMicrosecond() / millisecondsPerSecond
}

func (ts Timestamp) ExtractSecond() uint32 {
	return ts.ExtractMicrosecond() / microsecondsPerSecond
}

func (ts Timestamp) ExtractMinute() uint32 {
	result := int64(ts % microsecondsPerHour)
	if result < 0 {
		result += microsecondsPerHour
	}
	return uint32(uint64(result) / (microsecondsPerMinute))
}

func (ts Timestamp) ExtractHour() uint32 {
	result := int64(ts % microsecondsPerDay)
	if result < 0 {
		result += microsecondsPerDay
	}
	return uint32(uint64(result) / (microsecondsPerHour))
}

func (ts Timestamp) ExtractDay() uint32 {
	dd, _ := dateTimeFromTimestamp(ts)
	return dd.Day()
}

func (ts Timestamp) ExtractDOW() uint32 {
	dow := int32(floorDivInt64(int64(ts), microsecondsPerDay)+4) % 7
	if dow < 0 {
		dow += 7
	}
	return uint32(dow)
}

func (ts Timestamp) ExtractDOY() uint32 {
	dd, _ := dateTimeFromTimestamp(ts)
	doy := extractDoyPredicate[dd.month] + dd.Day()
	if dd.month < 10 && isLeapYear(dd.Year()) {
		doy++
	}
	return doy
}

func (ts Timestamp) ExtractMonth() uint32 {
	dd, _ := dateTimeFromTimestamp(ts)
	return dd.Month()
}

func (ts Timestamp) ExtractQuarter() uint32 {
	dd, _ := dateTimeFromTimestamp(ts)
	return dd.Quarter()
}

func (ts Timestamp) ExtractYear() int32 {
	dd, _ := dateTimeFromTimestamp(ts)
	return dd.Year()
}

func (ts Timestamp) ToUnixEpoch() int64 {
	return int64(floorDivInt64(int64(ts), microsecondsPerSecond))
}

func (ts Timestamp) TruncMillisecond() Timestamp {
	return Timestamp(floorDivInt64(int64(ts), 1000) * 1000)
}

func (ts Timestamp) TruncSecond() Timestamp {
	return Timestamp(floorDivInt64(int64(ts), microsecondsPerSecond) * microsecondsPerSecond)
}

func (ts Timestamp) TruncMinute() Timestamp {
	return Timestamp(floorDivInt64(int64(ts), microsecondsPerMinute) * microsecondsPerMinute)
}

func (ts Timestamp) TruncHour() Timestamp {
	return Timestamp(floorDivInt64(int64(ts), microsecondsPerHour) * microsecondsPerHour)
}

func (ts Timestamp) TruncDay() Timestamp {
	return Timestamp(floorDivInt64(int64(ts), microsecondsPerDay) * microsecondsPerDay)
}

func (ts Timestamp) TruncDOW(dow uint32) Timestamp {
	days, _ := extractNumDaysAndTimeFromUnixTime(ts)
	off := days + 4 - int64(dow)
	adj := floorDivInt64(off, 7) * 7
	return Timestamp((adj - 4 + int64(dow)) * microsecondsPerDay)
}

func (ts Timestamp) TruncMonth() Timestamp {
	dd, _ := dateTimeFromTimestamp(ts)
	dd.day = 0
	return unixTimeFromDateTime(dd, 0)
}

func (ts Timestamp) TruncQuarter() Timestamp {
	dd, _ := dateTimeFromTimestamp(ts)
	if dd.month == 0 {
		dd.year--
	}
	dd.month = uint16(truncQuarterPredicate[dd.month])
	dd.day = 0
	return unixTimeFromDateTime(dd, 0)
}

func (ts Timestamp) TruncYear() Timestamp {
	dd, _ := dateTimeFromTimestamp(ts)
	if dd.month < 10 {
		dd.year--
	}
	dd.month = 10
	dd.day = 0
	return unixTimeFromDateTime(dd, 0)
}
