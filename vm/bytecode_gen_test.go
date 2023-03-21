// Copyright (C) 2023 Sneller, Inc.
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

package vm

import (
	"reflect"
	"testing"
)

func TestGeneratedBytecodeSpec(t *testing.T) {
	for i := range opinfo {
		want := &opinfo[i]
		got := &generated[i]

		if want.text != got.text {
			t.Logf("got : %v", got.text)
			t.Logf("want: %v", want.text)
			t.Errorf("wrong text")
		}

		if want.scratch != got.scratch {
			t.Logf("got : %v", got.scratch)
			t.Logf("want: %v", want.scratch)
			t.Errorf("%s: wrong scratch size", got.text)
		}

		if !reflect.DeepEqual(want.in, got.in) {
			t.Logf("got : %v", got.in)
			t.Logf("want: %v", want.in)
			t.Errorf("%s: wrong input args", got.text)
		}

		if !reflect.DeepEqual(want.out, got.out) {
			t.Logf("got : %v", got.out)
			t.Logf("want: %v", want.out)
			t.Errorf("%s: wrong output args", got.text)
		}

		if !reflect.DeepEqual(want.va, got.va) {
			t.Logf("got : %v", got.va)
			t.Logf("want: %v", want.va)
			t.Errorf("%s: wrong va args", got.text)
		}
	}
}
