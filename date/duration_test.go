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
	"encoding"
	"encoding/json"
	"testing"
)

var (
	_ encoding.TextMarshaler   = Duration{}
	_ encoding.TextUnmarshaler = &Duration{}
)

func TestParseDuration(t *testing.T) {
	caseno := 0
	run := func(in string, y, m, d int, wantok bool) {
		t.Helper()
		defer func() { caseno++ }()
		want := Duration{y, m, d}
		got, ok := ParseDuration(in)
		if want != got || wantok != ok {
			t.Errorf("case %d: want (%q, %v), got (%q, %v)", caseno, want, wantok, got, ok)
		}
	}
	// good
	run("1y", 1, 0, 0, true)
	run("12m", 0, 12, 0, true)
	run("1y6m", 1, 6, 0, true)
	run("30d", 0, 0, 30, true)
	run("0y0m1d", 0, 0, 1, true)
	run("12345d", 0, 0, 12345, true)
	run("999y9999m99999d", 999, 9999, 99999, true)
	// bad
	run("", 0, 0, 0, false)
	run("a", 0, 0, 0, false)
	run("1a", 0, 0, 0, false)
	run("foo", 0, 0, 0, false)
	run("1yfoo", 0, 0, 0, false)
	run("100dbar", 0, 0, 0, false)
	run("1d ", 0, 0, 0, false)
	run("9999y", 0, 0, 0, false)
	run("99999m", 0, 0, 0, false)
	run("100000d", 0, 0, 0, false)
	run("0d", 0, 0, 0, false)
	run("0y0m0d", 0, 0, 0, false)
}

func TestDurationAdd(t *testing.T) {
	caseno := 0
	run := func(in string, tm, want Time) {
		t.Helper()
		defer func() { caseno++ }()
		d, ok := ParseDuration(in)
		if !ok {
			t.Errorf("case %d: bad duration %q", caseno, in)
			return
		}
		got := d.Add(tm)
		if want != got {
			t.Errorf("case %d: want %v, got %v", caseno, want, got)
		}
	}
	ymdt := Date
	ymd := func(y, m, d int) Time {
		return ymdt(y, m, d, 0, 0, 0, 0)
	}
	run("1y", ymd(2022, 12, 12), ymd(2023, 12, 12))
	run("1m", ymd(2022, 12, 12), ymd(2023, 1, 12))
	run("1d", ymd(2022, 12, 12), ymd(2022, 12, 13))
	run("13d", ymd(2022, 12, 12), ymd(2022, 12, 25))
	run("1y1m99d", ymd(2022, 12, 12), ymd(2024, 4, 20))
	run("13d", ymd(2022, 12, 12), ymd(2022, 12, 25))
	run("100y", ymd(2022, 12, 12), ymd(2122, 12, 12))
	run("1d", ymdt(2022, 12, 12, 1, 2, 3, 456), ymdt(2022, 12, 13, 1, 2, 3, 456))
	run("1y", ymdt(2022, 12, 12, 1, 2, 3, 456), ymdt(2023, 12, 12, 1, 2, 3, 456))
}

func TestDurationJSON(t *testing.T) {
	want, _ := ParseDuration("1y6m15d")
	var cfg struct {
		Expiry Duration `json:"expiry"`
	}
	in := `{"expiry":"1y6m15d"}`
	err := json.Unmarshal([]byte(in), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want != cfg.Expiry {
		t.Fatal(want, "!=", cfg.Expiry)
	}
	got, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if in != string(got) {
		t.Fatal(in, "!=", string(got))
	}
}
