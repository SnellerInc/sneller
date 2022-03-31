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

package blob

import (
	"reflect"
	"testing"
)

func TestBitmap(t *testing.T) {
	b := MakeBitmap(12)
	steps := []struct {
		set   int
		unset int
		want  Bitmap
	}{
		{want: Bitmap{0b0000_0001, 0b0000}, set: 0},
		{want: Bitmap{0b0001_0001, 0b0000}, set: 4},
		{want: Bitmap{0b0001_0001, 0b1000}, set: 11},
		{want: Bitmap{0b0001_0001, 0b1001}, set: 8},
		{want: Bitmap{0b0001_0001, 0b1000}, unset: 8},
		{want: Bitmap{0b0001_0001, 0b1000}, unset: 3},
		{want: Bitmap{0b0001_1001, 0b1000}, set: 3},
	}
	for i, s := range steps {
		if s.unset > 0 {
			b.Unset(s.unset)
			if b.Get(s.unset) {
				t.Errorf("%d set after unsetting", s.unset)
			}
		} else {
			b.Set(s.set)
			if !b.Get(s.set) {
				t.Errorf("%d unset after setting", s.set)
			}
		}
		if !reflect.DeepEqual(s.want, b) {
			t.Errorf("step %d: got %08b, want %08b", i, s.want, b)
		}
	}

	// Test that out of range gets return false and
	// don't panic.
	if b.Get(-1) {
		t.Error("get(-1) should have returned false")
	}
	if b.Get(99) {
		t.Error("get(99) should have returned false")
	}
}
