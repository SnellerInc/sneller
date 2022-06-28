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

package ion

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	"golang.org/x/exp/slices"
)

func testdata(t *testing.T, name string) []byte {
	buf, err := ioutil.ReadFile(filepath.Join("../testdata/", name))
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return buf
}

func TestParkingSymtab(t *testing.T) {
	buf := testdata(t, "parking.10n")
	// strings in the symbol table,
	// in the order in which they appear
	want := []string{
		"Ticket",
		"IssueData",
		"IssueTime",
		"MeterId",
		"MarkedTime",
		"RPState",
		"PlateExpiry",
		"VIN",
		"Make",
		"BodyStyle",
		"Color",
		"Location",
		"Route",
		"Agency",
		"ViolationCode",
		"ViolationDescr",
		"Fine",
		"Latitude",
		"Longitude",
	}

	var s Symtab
	rest, err := s.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("consumed %d bytes", len(buf)-len(rest))
	got := s.interned
	if len(got) != len(want) {
		t.Errorf("want %s", want)
		t.Fatalf("got %s", got)
	}
	for i := range got {
		if want[i] != got[i] {
			t.Errorf("want[%d]=%q, got[%d]=%q", i, want[i], i, got[i])
		}
	}

	s.clear()
	buf = testdata(t, "quintuple.ion")
	rest, err = s.Unmarshal(buf)
	t.Logf("consumed %d bytes", len(buf)-len(rest))
	if err != nil {
		t.Logf("start: %x", buf[:16])
		t.Fatal(err)
	}
}

// test incremental symbol table marshalling + unmarshalling
func TestSymtabMarshalPart(t *testing.T) {
	syms := []string{
		"Ticket",
		"IssueData",
		"IssueTime",
		"MeterId",
		"MarkedTime",
		"RPState",
		"PlateExpiry",
		"VIN",
		"Make",
		"BodyStyle",
		"Color",
		"Location",
		"Route",
		"Agency",
		"ViolationCode",
		"ViolationDescr",
		"Fine",
		"Latitude",
		"Longitude",
	}
	var dst Buffer
	for i := 0; i < 100; i++ {
		dst.Reset()
		rand.Shuffle(len(syms), func(i, j int) {
			syms[i], syms[j] = syms[j], syms[i]
		})
		var st Symtab
		r := rand.Intn(len(syms))
		for r == 0 {
			r = rand.Intn(len(syms))
		}
		for _, sym := range syms[:r] {
			st.Intern(sym)
		}
		st.Marshal(&dst, true)
		max := st.MaxID()
		for _, sym := range syms[r:] {
			st.Intern(sym)
		}
		st.MarshalPart(&dst, Symbol(max))
		var out Symtab
		rest, err := out.Unmarshal(dst.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if len(rest) > 0 {
			rest, err = out.Unmarshal(rest)
			if err != nil {
				t.Fatal(err)
			}
			if len(rest) > 0 {
				t.Fatalf("%d bytes left over?", len(rest))
			}
		}
		if !out.Equal(&st) {
			t.Logf("in  MaxID %d", st.MaxID())
			for i := 10; i < st.MaxID(); i++ {
				t.Logf("in  %d = %s", i, st.Get(Symbol(i)))
			}
			t.Logf("out MaxID %d", out.MaxID())
			for i := 10; i < out.MaxID(); i++ {
				t.Logf("out %d = %s", i, out.Get(Symbol(i)))
			}
			t.Fatalf("case %d: (slice @%d) not equal", i, r)
		}
	}
}

func TestSymtabMarshal(t *testing.T) {
	buf := testdata(t, "parking.10n")
	var s Symtab
	rest, err := s.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	pre := buf[:len(buf)-len(rest)]
	var b Buffer
	s.Marshal(&b, true)
	got := b.Bytes()
	if !bytes.Equal(got, pre) {
		t.Errorf("len(got)=%d, len(in)=%d", len(got), len(pre))
		t.Errorf("input data: %x", pre)
		t.Errorf("output    : %x", got)
	}
	_, err = s.Unmarshal(got)
	if err != nil {
		t.Error(err)
	}

	// see if we get the same result
	// by manually populating a table
	var s2 Symtab
	var b2 Buffer
	for i := 0; i < s.MaxID(); i++ {
		if got := s2.Intern(s.Get(Symbol(i))); got != Symbol(i) {
			t.Fatalf("intern %d -> %d", i, got)
		}
		s2.Intern(s.Get(Symbol(i)))
	}
	s2.Marshal(&b2, true)
	got2 := b2.Bytes()
	if !bytes.Equal(got, got2) {
		for i := 0; i < s.MaxID(); i++ {
			if s2.Get(Symbol(i)) != s.Get(Symbol(i)) {
				t.Errorf("symbol %d: %q versus %q", i, s2.Get(Symbol(i)), s.Get(Symbol(i)))
			}
		}
		t.Errorf("after manually populating symbols:")
		t.Errorf("wanted data: %x", got)
		t.Errorf("got data   : %x", got2)
	}
}

func TestSymtabAlias(t *testing.T) {
	want := []string{"foo", "bar", "baz"}
	var st Symtab
	st.Intern("foo")
	st.Intern("bar")
	st.Intern("baz")
	got := st.alias()
	var st2 Symtab
	st2.Intern("foo")
	st2.Intern("quux")
	st2.CloneInto(&st)
	if !slices.Equal(got, want) {
		t.Errorf("want %q, got %q", want, got)
	}
	st.Reset()
	if !slices.Equal(got, want) {
		t.Errorf("want %q, got %q", want, got)
	}
}
