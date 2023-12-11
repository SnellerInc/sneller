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

package vm

import (
	"math/bits"
	"testing"
)

func TestAggregateSumf(t *testing.T) {
	wiki := []float64{1.0, 1e100, 1.0, -1e100}
	input := make([]float64, len(wiki))

	for comb := uint32(0); comb <= 256; comb++ {
		// given
		a := (comb >> (0 * 2)) & 0x3
		b := (comb >> (1 * 2)) & 0x3
		c := (comb >> (2 * 2)) & 0x3
		d := (comb >> (3 * 2)) & 0x3

		x := uint8((1 << a) | (1 << b) | (1 << c) | (1 << d))
		if bits.OnesCount8(x) != 4 {
			continue
		}

		input[0] = wiki[a]
		input[1] = wiki[b]
		input[2] = wiki[c]
		input[3] = wiki[d]

		// when
		sum := 0.0
		correction := 0.0
		for _, x := range wiki {
			sum, correction = neumaierSummation(sum, x, correction)
		}

		result := sum + correction

		// then
		want := 2.0
		if result != want {
			t.Logf("got : %.f", result)
			t.Logf("want: %.f", want)
			t.Errorf("wrong sum of %+v", input)
		}
	}
}
