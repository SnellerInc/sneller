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
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalidUnit = errors.New("invalid unit")
)

//go:generate ragel -L -Z -G2 date_math_lexer.rl
//go:generate gofmt -w date_math_lexer.go

func adjust(t time.Time, n int, unit string) time.Time {
	if t.IsZero() {
		return t
	}
	switch unit {
	case "y":
		return t.AddDate(n, 0, 0)
	case "M":
		return t.AddDate(0, n, 0)
	case "w":
		return t.AddDate(0, 0, 7*n)
	case "d":
		return t.AddDate(0, 0, n)
	case "h", "H":
		return t.Add(time.Duration(n) * time.Hour)
	case "m":
		return t.Add(time.Duration(n) * time.Minute)
	case "s":
		return t.Add(time.Duration(n) * time.Second)
	}
	panic("unexpected unit (error in ragel definition)")
}

func round(t time.Time, unit string) time.Time {
	if t.IsZero() {
		return t
	}
	switch unit {
	case "y":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "M":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "w":
		// We'll always round to the nearest sunday
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		return t.Add(time.Duration(-24*int(t.Weekday())) * time.Hour)
	case "d":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "h", "H":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "m":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case "s":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	}
	panic("unexpected unit (error in ragel definition)")
}

func parseDate(text string) time.Time {
	text = strings.ReplaceAll(text, ".", "-")
	t, err := time.Parse("2006-1-2", text)
	if err != nil {
		panic("invalid date parsing")
	}
	return t
}

func addTime(t time.Time, text string) time.Time {
	if t.IsZero() {
		return t
	}
	parseDigit := func(i int) (int, int, int) {
		if i < 0 {
			return 0, 0, -1
		}
		num := 0
		l := 0
		for i < len(text) {
			ch := text[i]
			i++
			if ch < '0' || ch > '9' {
				break
			}
			num = (num * 10) + int(ch-'0')
			l++
		}
		for i < len(text) {
			ch := text[i]
			if ch >= '0' && ch <= '9' {
				return num, l, i
			}
			i++
		}
		return num, l, -1
	}

	hour, _, next := parseDigit(0)
	minute, _, next := parseDigit(next)
	seconds, _, next := parseDigit(next)

	// Nano is more difficult, because we actually get a fraction
	nano, l, _ := parseDigit(next)
	for l < 9 {
		nano = nano * 10
		l++
	}
	return t.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(seconds)*time.Second + time.Duration(nano)*time.Nanosecond)
}
