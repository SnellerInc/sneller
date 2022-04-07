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
	"math/rand"
	"testing"
	"time"
)

func TestRFC3339(t *testing.T) {
	in := []string{
		"2019-10-12T07:20:50.52Z",
		"2019-10-12T07:20:50.52334-05:00",
		"1992-01-23T12:24:32.999999999+07:00",
		"2022-01-01T00:20:00+01:30",
		"2022-12-31T23:59:59-00:30",
	}
	for i := range in {
		out, ok := Parse([]byte(in[i]))
		if !ok {
			t.Errorf("couldn't parse %q", in[i])
			continue
		}
		want, err := time.Parse(time.RFC3339Nano, in[i])
		if err != nil {
			t.Fatal(err)
		}
		for _, err := range check(out, want) {
			t.Errorf("%s: got %s; wanted %s", err, out, want)
		}
		testFormat(t, out, want)
		// move the date around forwards and backwards
		// and check that the parsed time is okay
		// (this won't end up adjusting the timezone)
		for j := 0; j < 1000; j++ {
			ref := want.Add(time.Duration(int64(rand.Uint64())))
			buf := ref.Format(time.RFC3339Nano)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := Parse([]byte(buf))
			if !ok {
				t.Fatalf("couldn't parse %s", buf)
			}
			for _, err := range check(got, ref) {
				t.Errorf("iter %d: %s: got %s from %s; wanted %s", j, err, got, buf, ref.UTC())
			}
			testFormat(t, got, ref)
		}
	}
}

func testFormat(t *testing.T, got Time, want time.Time) {
	t.Helper()
	want = want.UTC()
	s1 := got.String()
	s2 := want.String()
	if s1 != s2 {
		t.Errorf("String: %s != %s", s1, s2)
	}
	b1 := got.AppendRFC3339Nano(nil)
	b2 := want.AppendFormat(nil, time.RFC3339Nano)
	if string(b1) != string(b2) {
		t.Errorf("AppendRFC3339Nano: %s != %s", b1, b2)
	}
	j1, err1 := got.MarshalJSON()
	j2, err2 := want.MarshalJSON()
	if err2 != nil {
		if err1 != nil {
			t.Error("MarshalJSON: expected error")
		}
		return
	}
	if string(j1) != string(j2) {
		t.Errorf("MarshalJSON: %s != %s", j1, j2)
	}
	var got2 Time
	if err := got2.UnmarshalJSON(j1); err != nil {
		t.Errorf("UnmarshalJSON: %v", err)
		return
	}
	if !got2.Equal(got) {
		t.Errorf("UnmarshalJSON: %s != %s", got, got2)
	}
}

// test strings that are not standards-conforming
// but nonetheless are unambiguously time strings
func TestNonConforming(t *testing.T) {
	in := []struct {
		in, normal string
	}{
		// leading + trailing spaces; no offset:
		{" 2019-10-12T07:20:50.52  ", "2019-10-12T07:20:50.52Z"},
		{"2019-10-12T07:20:50.52", "2019-10-12T07:20:50.52Z"},
		{"2022-01-13T21:47:34", "2022-01-13T21:47:34Z"},
		// T -> ' '
		{" 2019-10-12 07:20:50.52334-05:00", "2019-10-12T07:20:50.52334-05:00"},
	}
	for i := range in {
		buf := []byte(in[i].in)
		out, ok := Parse(buf)
		if !ok {
			t.Errorf("couldn't parse %q", in[i].in)
		}
		want, err := time.Parse(time.RFC3339Nano, in[i].normal)
		if err != nil {
			t.Fatalf("invalid reference string %q: %s", in[i].normal, err)
		}
		for _, err := range check(out, want) {
			t.Errorf("%s: %s != %s", err, out, want)
		}
	}
}

func TestNormalization(t *testing.T) {
	rng := func(min, max int) int {
		return min + rand.Intn(max-min)
	}
	for i := 0; i < 100000; i++ {
		y, mo, d := rng(1000, 3000), rng(-100, 100), rng(-500, 500)
		h, mi, s := rng(-100, 100), rng(-1000, 1000), rng(-1000, 1000)
		ns := rng(-1e15, 1e15)
		got := Date(y, mo, d, h, mi, s, ns)
		want := time.Date(y, time.Month(mo), d, h, mi, s, ns, time.UTC)
		for _, err := range check(got, want) {
			t.Errorf("case %d: %s: %s != %s", i, err, got, want)
			t.Error("input:", y, mo, d, h, mi, s, ns)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	str := "2019-10-12T07:20:50.52Z"
	b.Run("std", func(b *testing.B) {
		b.SetBytes(int64(len(str)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := time.Parse(time.RFC3339Nano, str)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fast", func(b *testing.B) {
		buf := []byte(str)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, ok := Parse(buf)
			if !ok {
				b.Fatal("parsing failed")
			}
		}
	})
}

func BenchmarkAppend3339Nano(b *testing.B) {
	var buf []byte
	t := Date(2021, 4, 7, 12, 0, 0, 123456789)
	for i := 0; i < b.N; i++ {
		buf = t.AppendRFC3339Nano(buf[:0])
	}
}

func BenchmarkString(b *testing.B) {
	t := Date(2021, 4, 7, 12, 0, 0, 123456789)
	for i := 0; i < b.N; i++ {
		t.String()
	}
}

func check(got Time, want time.Time) (e []string) {
	if !got.Time().Equal(want) {
		e = append(e, "as times")
	}
	if !got.Equal(FromTime(want)) {
		e = append(e, "as dates")
	}
	want = want.UTC()
	y1, mo1, d1 := got.Year(), got.Month(), got.Day()
	y2, mo2, d2 := want.Year(), want.Month(), want.Day()
	if y1 != y2 || mo1 != int(mo2) || d1 != d2 {
		e = append(e, "date parts")
	}
	h1, mi1, s1, ns1 := got.Hour(), got.Minute(), got.Second(), got.Nanosecond()
	h2, mi2, s2, ns2 := want.Hour(), want.Minute(), want.Second(), want.Nanosecond()
	if h1 != h2 || mi1 != mi2 || s1 != s2 || ns1 != ns2 {
		e = append(e, "time parts")
	}
	return e
}
