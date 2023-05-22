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

package tests

import (
	"strings"
	"testing"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func TestReadTestcase(t *testing.T) {
	// given
	input := `
## Key1: value1
section 1(a)

section 1(b)
--- second section

section 2(a)


section 2(b)

section 2(c)
---

section 3(a)
#section 13(a)
#
#section 323(a)
##KEY2      : value2


section 3(b)


`

	reader := strings.NewReader(input)

	// when
	spec, err := ReadSpec(reader)

	// then
	if err != nil {
		t.Errorf("unexpected error %s", err)
	}

	{
		got := len(spec.Sections)
		want := 3
		if got != want {
			t.Logf("got: %d", got)
			t.Logf("want: %d", want)
			t.Errorf("wrong number of sections")
		}
	}

	{
		got := len(spec.Tags)
		want := 2
		if got != want {
			t.Logf("got: %d", got)
			t.Logf("want: %d", want)
			t.Errorf("wrong number of keys")
		}
	}

	slicesEqual(t, spec.Sections[0], []string{"section 1(a)", "section 1(b)"})
	slicesEqual(t, spec.Sections[1], []string{"section 2(a)", "section 2(b)", "section 2(c)"})
	slicesEqual(t, spec.Sections[2], []string{"section 3(a)", "section 3(b)"})

	{
		got := spec.Tags
		want := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}
		if !maps.Equal(got, want) {
			t.Logf("got: %s", got)
			t.Logf("want: %s", want)
			t.Errorf("wrong key-value map")
		}
	}
}

func slicesEqual(t *testing.T, got, want []string) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Logf("got: %s", got)
		t.Logf("want: %s", want)
		t.Errorf("wrong section")
	}
}
