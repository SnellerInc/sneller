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
		if !out.Equal(want) {
			t.Errorf("got %s; wanted %s", out, want)
		}
		// move the date around forwards and backwards
		// and check that the parsed time is okay
		// (this won't end up adjusting the timezone)
		for j := 0; j < 1000; j++ {
			ref := want.Add(time.Duration(int64(rand.Uint64())))
			buf := ref.Format(time.RFC3339Nano)
			if err != nil {
				t.Fatal(err)
			}
			out, ok := Parse([]byte(buf))
			if !ok {
				t.Fatalf("couldn't parse %s", buf)
			}
			if !out.Equal(ref) {
				t.Fatalf("iter %d got %s from %s", j, out.Format(time.RFC3339Nano), buf)
			}
		}
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
		if !out.Equal(want) {
			t.Errorf("%s != %s", out, want)
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
