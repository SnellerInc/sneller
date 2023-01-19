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
