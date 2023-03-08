// Copyright (C) 2023 Sneller, Inc.
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
