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
	"testing"
)

func TestBag(t *testing.T) {
	items := []Datum{
		Null,
		String("foo"),
		Int(-1),
		Uint(1000),
		Bool(true),
		Bool(false),
		NewStruct(nil,
			[]Field{
				{"foo", String("foo"), 0},
				{"bar", Null, 0},
				{"inner", NewList(nil, []Datum{
					Int(-1), Uint(0), Uint(1),
				}).Datum(), 0},
				{"name", String("should-come-first"), 0},
			},
		).Datum(),
	}

	var bag Bag
	for i := range items {
		bag.AddDatum(items[i])
	}
	if bag.Len() != len(items) {
		t.Fatalf("bag.Len=%d; expected %d", bag.Len(), len(items))
	}
	i := 0
	bag.Each(func(d Datum) bool {
		if !d.Equal(items[i]) {
			t.Errorf("item %d is %v", i, d)
		}
		i++
		return true
	})

	// transcode to a second symbol table
	var st Symtab
	for _, x := range []string{"baz", "bar", "foo", "quux"} {
		st.Intern(x)
	}
	var buf Buffer
	var bag2 Bag
	bag.Encode(&buf, &st)
	bag2.Add(&st, buf.Bytes())
	if !bag.Equals(&bag2) {
		t.Fatal("!bag.Equal(bag2)")
	}

	bag.Append(&bag2)
	if bag.Len() != len(items)*2 {
		t.Fatalf("bag.Len=%d, want %d", bag.Len(), len(items)*2)
	}
	i = 0
	n := 0
	bag.Each(func(d Datum) bool {
		if !d.Equal(items[i]) {
			t.Errorf("item %d is %v", i, d)
		}
		i++
		n++
		if i == len(items) {
			i = 0
		}
		return true
	})
	if n != bag.Len() {
		t.Fatalf("Each iterated %d times, but bag.Len()=%d", n, bag.Len())
	}
}
