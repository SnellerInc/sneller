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

package jsonrl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestFlatten(t *testing.T) {
	text := `

 [ {"x": 2, "y": 3}, {"a": null, "b": "123456"} ]
 [ {"x": 3, "y": 4}, {"b": null, "a": "xyzabc"} ]
 [ ]

`

	src := strings.NewReader(text)
	var buf bytes.Buffer
	out := ion.Chunker{
		W:     &buf,
		Align: 1024,
	}
	err := Convert(src, &out, nil)
	if err != nil {
		t.Fatal(err)
	}
	out.Flush()
	if n := count(t, buf.Bytes()); n != 4 {
		t.Errorf("got %d items?", n)
	}

	src = strings.NewReader(strings.TrimSpace(text))
	buf.Reset()
	out = ion.Chunker{W: &buf, Align: 1024}
	err = Convert(src, &out, nil)
	if err != nil {
		t.Fatal(err)
	}
	out.Flush()
	if n := count(t, buf.Bytes()); n != 4 {
		t.Errorf("got %d items?", n)
	}
}

func TestBadLists(t *testing.T) {
	text := []string{
		`[{"x": 1}{"y": 1}]`,
		`[{"x": 1}, {"y": 1}`,
		`[{"x": 1}, {"y": 1}, ]`,
		`{"x": 1}{"y": 1}]`,
		`[[{"x": 1, "y": 2}]`,
	}
	for _, str := range text {
		src := strings.NewReader(str)
		var buf bytes.Buffer
		out := ion.Chunker{
			W:     &buf,
			Align: 1024,
		}
		err := Convert(src, &out, nil)
		if err == nil {
			t.Fatalf("no error on %q", str)
		}
	}
}
