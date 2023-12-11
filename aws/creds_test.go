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
