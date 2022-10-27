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
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestMakePartitions(t *testing.T) {
	good := func(part Partition, in blockfmt.Input, want ion.Datum) {
		t.Helper()
		tmpls, err := makePartitions([]Partition{part})
		if err != nil {
			t.Error("make:", err)
			return
		}
		if part.Field != tmpls[0].Field {
			t.Errorf("field name mismatch: %q != %q", part.Field, tmpls[0].Field)
		}
		got, err := tmpls[0].Eval(&in)
		if err != nil {
			t.Error("eval:", err)
			return
		}
		if !ion.Equal(want, got) {
			t.Errorf("result mismatch: %v != %v", want, got)
		}
	}
	bad := func(parts []Partition, errstr string) {
		t.Helper()
		_, err := makePartitions(parts)
		if err == nil {
			t.Errorf("expected error")
		} else if err.Error() != errstr {
			t.Errorf("error does not match:")
			t.Errorf("  want: %s", errstr)
			t.Errorf("  got:  %s", err.Error())
		}
	}
	// test some good partitions
	good(Partition{Field: "x"},
		blockfmt.Input{Path: "/foo/bar", Glob: "/{x}/{y}"},
		ion.String("foo"))
	good(Partition{Field: "bar", Value: "foo-$x-baz"},
		blockfmt.Input{Path: "/foo/bar/baz", Glob: "/foo/{x}/baz"},
		ion.String("foo-bar-baz"))
	good(Partition{Field: "n", Type: "int"},
		blockfmt.Input{Path: "/foo/123/bar", Glob: "/foo/{n}/bar"},
		ion.Int(123))
	good(Partition{Field: "date", Type: "date", Value: "$yyyy-$mm-$dd"},
		blockfmt.Input{Path: "/foo/2022/10/26/file.json", Glob: "/foo/{yyyy}/{mm}/{dd}/*.json"},
		ion.Timestamp(date.Date(2022, 10, 26, 0, 0, 0, 0)))
	good(Partition{Field: "time", Type: "timestamp", Value: "$yyyy-$mm-$dd $hh:00:00"},
		blockfmt.Input{Path: "/foo/2022/10/26/03/file.json", Glob: "/foo/{yyyy}/{mm}/{dd}/{hh}/*.json"},
		ion.Timestamp(date.Date(2022, 10, 26, 3, 0, 0, 0)))
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
	bad([]Partition{
		{Field: "foo", Type: "badtype"},
	}, `invalid type "badtype"`)
}
