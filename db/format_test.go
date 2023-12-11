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
				return blockfmt.MustSuffixToFormat(".json.zst")
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
		c := Config{
			Fallback: cases[i].fallback,
		}
		f, err := c.Format(cases[i].explicit, cases[i].name, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
