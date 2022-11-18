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

package expr

import (
	"fmt"
	"testing"
)

func TestQuoteUnquote(t *testing.T) {
	tcs := []struct {
		in, out string
	}{
		{"foo", "'foo'"},
		{"", "''"},
		{"a \t\n\r\v\f\a\b", `'a \t\n\r\v\f\a\b'`},
		{"b '/\\ c", `'b \'\/\\ c'`},
		{"żółw", `'\u017c\u00f3\u0142w'`},
		{"'xyz'", "'\\'xyz\\''"},
	}

	for i := range tcs {
		tc := &tcs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			quoted := Quote(tc.in)
			unqoted, err := Unquote(quoted)
			if err != nil {
				t.Fatalf("unexpected error %s", err)
			}

			if quoted != tc.out {
				t.Logf("got  = %s", quoted)
				t.Logf("want = %s", tc.out)
				t.Errorf("wrong quote")
			}

			if unqoted != tc.in {
				t.Logf("got  = %s", unqoted)
				t.Logf("want = %s", tc.in)
				t.Errorf("wrong unquote")
			}
		})
	}
}

func TestUnquoteValid(t *testing.T) {
	tcs := []struct {
		in  string
		out string
	}{
		{
			in:  "'żółw'",
			out: `żółw`,
		},
		{
			in:  `'\u005F'`,
			out: "_",
		},
	}

	for i := range tcs {
		tc := &tcs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			unquoted, err := Unquote(tc.in)
			if err != nil {
				t.Fatalf("unexpected error %s", err)
			}

			if unquoted != tc.out {
				t.Logf("got  = %s", unquoted)
				t.Logf("want = %s", tc.out)
				t.Errorf("wrong result")
			}
		})
	}
}

func TestUnquoteErrors(t *testing.T) {
	tcs := []struct {
		in  string
		err string
	}{
		{
			in:  "test'",
			err: `expr.Unquote: string does not start with "'"`,
		},
		{
			in:  "'test",
			err: `expr.Unquote: string does not end with "'"`,
		},
		{
			in:  "a",
			err: `expr.Unquote: string "a" too short`,
		},
		{
			in:  "",
			err: `expr.Unquote: string "" too short`,
		},
		{
			in:  "'test\\'",
			err: `expr.Unescape: cannot unescape trailing \`,
		},
		{
			in:  "'test\\z'",
			err: `expr.Unescape: unexpected backslash escape of 'z' (0x7a)`,
		},
		{
			in:  "'\\uab'",
			err: `expr.Unescape: invalid \u escape sequence`,
		},
		{
			in:  "'\\u00Hf'",
			err: `expr.Unescape: invalid hex digit "H"`,
		},
		{
			in:  "'\\ud8ff'", // reserved value
			err: `expr.Unescape: rune Ud8ff is invalid`,
		},
		{
			in:  "'\x80'", // a sole UTF-8 continuation byte
			err: `expr.Unescape: invalid rune 0x80`,
		},
	}

	for i := range tcs {
		tc := &tcs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err := Unquote(tc.in)
			if err == nil {
				t.Fatal("expected error")
			}

			got := err.Error()
			if got != tc.err {
				t.Logf("got  = %s", got)
				t.Logf("want = %s", tc.err)
				t.Errorf("wrong error message")
			}
		})
	}
}
