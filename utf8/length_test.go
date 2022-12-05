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

package utf8

import (
	"fmt"
	"testing"
	"unicode/utf8"
)

func TestValidStringLength(t *testing.T) {
	testcases := [][]byte{
		[]byte(""),
		[]byte("A"),
		[]byte("01"),
		[]byte("012"),
		[]byte("0123"),
		[]byte("01234"),
		[]byte("012345"),
		[]byte("0123456"),
		[]byte("01234567"),
		[]byte("012345678"),
		[]byte("0123456789"),
		[]byte("all ascii"),
		[]byte("wąż"),
		[]byte("żółw"),
	}

	for i := range testcases {
		str := testcases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			want := utf8.RuneCount(str)
			got := ValidStringLength(str)
			if want != got {
				t.Logf("want = %d", want)
				t.Logf("got  = %d", got)
				t.Errorf("wrong result for %q", str)
			}
		})
	}
}

func BenchmarkValidStringLength(b *testing.B) {
	str := []byte("quite long string with the Polish word 'żółw' - a turtle")
	for i := 0; i < b.N; i++ {
		ValidStringLength(str)
	}
}

func BenchmarkRuneCount(b *testing.B) {
	str := []byte("quite long string with the Polish word 'żółw' - a turtle")
	for i := 0; i < b.N; i++ {
		utf8.RuneCount(str)
	}
}
