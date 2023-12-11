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
