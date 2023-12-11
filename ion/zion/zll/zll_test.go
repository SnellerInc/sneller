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

package zll

import (
	"io"
	"os"
	"testing"
)

func TestShapecount(t *testing.T) {
	paths := []string{
		"testdata/a.bin",
		"testdata/b.bin",
	}
	for _, path := range paths {
		buf := readfile(t, path)
		t.Run(path, func(t *testing.T) {
			got := shapecount(buf)
			want := shapecountref(buf)
			if got != want {
				t.Logf("got:  %d", got)
				t.Logf("want: %d", want)
				t.Errorf("wrong output")
			}
		})
	}
}

func shapecountref(shape []byte) int {
	count := 0
	for len(shape) > 0 {
		fc := (shape[0] & 0x1f)
		skip := (fc + 3) / 2
		if fc < 16 {
			count++
		}
		shape = shape[skip:]
	}

	return count
}

func BenchmarkShapecount(b *testing.B) {
	paths := []string{
		"testdata/a.bin",
		"testdata/b.bin",
	}
	for _, path := range paths {
		buf := readfile(b, path)
		b.Run(path, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = shapecount(buf)
			}
		})
	}
}

func readfile(t testing.TB, path string) []byte {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	res, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	return res
}
