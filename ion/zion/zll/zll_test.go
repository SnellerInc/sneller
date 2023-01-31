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
