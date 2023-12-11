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
	err := Convert(src, &out, nil, nil)
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
	err = Convert(src, &out, nil, nil)
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
		err := Convert(src, &out, nil, nil)
		if err == nil {
			t.Fatalf("no error on %q", str)
		}
	}
}
