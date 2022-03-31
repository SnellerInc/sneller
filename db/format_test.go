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

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestFormat(t *testing.T) {
	cases := []struct {
		explicit, name, want string
		fallback             func(name string) blockfmt.RowFormat
	}{
		{
			explicit: "json",
			name:     "foo.log",
			want:     "json",
		},
		{
			name: "foo.bar.json",
			want: "json",
		},
		{
			explicit: "json.gz",
			name:     "foo.log",
			want:     "json.gz",
		},
		{
			name: "foo.json.zst",
			want: "json.zst",
		},
		{
			name: "foo.bar.baz",
			fallback: func(_ string) blockfmt.RowFormat {
				return blockfmt.SuffixToFormat[".json.zst"]()
			},
			want: "json.zst",
		},
		{
			name:     "x.gz",
			explicit: "json.gz",
			fallback: func(_ string) blockfmt.RowFormat {
				panic("should not be called")
				return nil
			},
			want: "json.gz",
		},
		{
			name: "foo.bar",
			want: "",
		},
	}

	for i := range cases {
		b := Builder{
			Fallback: cases[i].fallback,
		}
		f := b.Format(cases[i].explicit, cases[i].name)
		if f == nil {
			if cases[i].want != "" {
				t.Errorf("couldn't get format for %q %q", cases[i].explicit, cases[i].name)
			}
			continue
		}
		if f.Name() != cases[i].want {
			t.Errorf("Format(%q, %q) = %q, wanted %q", cases[i].explicit, cases[i].name, f.Name(), cases[i].want)
		}
	}
}
