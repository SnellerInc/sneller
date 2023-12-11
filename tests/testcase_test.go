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

package tests

import (
	"strings"
	"testing"

	"slices"

	"golang.org/x/exp/maps"
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
