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

package aws

import (
	"strings"
	"testing"
)

func TestScan(t *testing.T) {
	var foo, bar, baz, quux string
	basespec := []scanspec{
		{prefix: "foo", dst: &foo},
		{prefix: "bar", dst: &bar},
		{prefix: "baz", dst: &baz},
		{prefix: "quux", dst: &quux},
	}
	text := strings.Join([]string{
		"[default]",
		"foo=foo_result",
		"ignore this line",
		"bar = bar_result",
		"baz= baz_result",
		"quux  =quux_result",
		"ignoreme=",
		"=invalid line",
		"x=y=z",
		"[section2]",
		"foo=section2_result",
		"bar=section2_bar_result",
	}, "\n")
	spec := make([]scanspec, len(basespec))
	copy(spec, basespec)
	err := scan(strings.NewReader(text), "default", spec)
	if err != nil {
		t.Fatal(err)
	}
	if foo != "foo_result" {
		t.Errorf("foo = %q", foo)
	}
	if bar != "bar_result" {
		t.Errorf("bar = %q", bar)
	}
	if baz != "baz_result" {
		t.Errorf("baz = %q", baz)
	}
	if quux != "quux_result" {
		t.Errorf("quux = %q", quux)
	}
	copy(spec, basespec)
	err = scan(strings.NewReader(text), "section2", spec)
	if err != nil {
		t.Fatal(err)
	}
	if foo != "section2_result" {
		t.Errorf("foo = %q", foo)
	}
	if bar != "section2_bar_result" {
		t.Errorf("bar = %q", bar)
	}
}
