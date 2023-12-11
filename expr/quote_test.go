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
