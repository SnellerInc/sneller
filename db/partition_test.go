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

package db

import (
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestCollector(t *testing.T) {
	good := func(conf []Partition, input, glob, name string, cons []ion.Field) {
		t.Helper()
		var c collector
		err := c.init(conf)
		if err != nil {
			t.Error("init:", err)
			return
		}
		part, err := c.part(glob, input)
		if err != nil {
			t.Error("part:", err)
			return
		}
		if part.name != name {
			t.Errorf("wanted name %q, got %q", name, part.name)
		}
		if !reflect.DeepEqual(cons, part.cons) {
			t.Errorf("result mismatch: %v != %v", cons, part.cons)
		}
	}
	bad := func(parts []Partition, errstr string) {
		t.Helper()
		var c collector
		err := c.init(parts)
		if err == nil {
			t.Errorf("expected error")
		} else if err.Error() != errstr {
			t.Errorf("error does not match:")
			t.Errorf("  want: %s", errstr)
			t.Errorf("  got:  %s", err.Error())
		}
	}
	// test some good partitions
	good([]Partition{
		{Field: "x"},
		{Field: "y"},
	},
		"/foo/bar", "/{x}/{y}", "foo/bar",
		[]ion.Field{
			{Label: "x", Datum: ion.String("foo")},
			{Label: "y", Datum: ion.String("bar")},
		},
	)
	good([]Partition{
		{Field: "path", Value: "$x/$y"},
	},
		"/foo/bar", "/{x}/{y}", "foo/bar",
		[]ion.Field{
			{Label: "path", Datum: ion.String("foo/bar")},
		},
	)
	good([]Partition{
		{Field: "bar", Value: "foo-$x-baz"},
	},
		"/foo/bar/baz", "/foo/{x}/baz", "foo-bar-baz",
		[]ion.Field{
			{Label: "bar", Datum: ion.String("foo-bar-baz")},
		},
	)
	good([]Partition{
		{Field: "n", Type: "int"},
	},
		"/foo/123/bar", "/foo/{n}/bar", "123",
		[]ion.Field{
			{Label: "n", Datum: ion.Int(123)},
		},
	)
	good([]Partition{
		{Field: "date", Type: "date", Value: "$yyyy-$mm-$dd"},
	},
		"/foo/2022/10/26/file.json",
		"/foo/{yyyy}/{mm}/{dd}/*.json",
		"2022-10-26",
		[]ion.Field{
			{Label: "date", Datum: ion.Timestamp(date.Date(2022, 10, 26, 0, 0, 0, 0))},
		},
	)
	good([]Partition{{
		Field: "time", Type: "timestamp", Value: "$yyyy-$mm-$dd $hh:00:00"},
	},
		"/foo/2022/10/26/03/file.json",
		"/foo/{yyyy}/{mm}/{dd}/{hh}/*.json",
		"2022-10-26 03:00:00",
		[]ion.Field{
			{Label: "time", Datum: ion.Timestamp(date.Date(2022, 10, 26, 3, 0, 0, 0))},
		},
	)
	// test some bad partitions
	bad([]Partition{
		{Field: ""},
	}, `empty partition name`)
	bad([]Partition{
		{Field: "foo"},
		{Field: "bar"},
		{Field: "baz"},
		{Field: "bar"},
	}, `duplicate partition name "bar"`)
	bad([]Partition{
		{Field: "!@#$"},
	}, `cannot use field name "!@#$" as value template`)
}

func TestCheckSegment(t *testing.T) {
	run := func(s string, want bool) {
		t.Helper()
		got := checkSegment([]byte(s))
		if want != got {
			t.Errorf("want %v, got %v", want, got)
		}
	}
	run("a", true)
	run("a/b", true)
	run("foo", true)
	run("foo/bar", true)
	run("foo/bar/baz", true)
	run("f.oo/bar", true)
	run("foo/b..ar", true)

	run("", false)
	run("/", false)
	run(".", false)
	run("..", false)
	run("foo/bar/", false)
	run("foo//bar", false)
	run("/foo/bar", false)
	run("foo/.", false)
	run("foo/..", false)
	run("foo/./bar", false)
	run("foo/../bar", false)
	run("./foo", false)
	run("../foo", false)
}
