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

package vm

import (
	"math"
	"testing"
)

func TestPow2Int(t *testing.T) {
	t.Run("Pow2Int", func(t *testing.T) {
		for i := -1022; i <= 1023; i++ {
			got, err := pow2Int(i)
			if err != nil {
				t.Errorf("unexpcted error: %s", err)
				return
			}
			want := math.Pow(2.0, float64(i))

			if got != want {
				t.Logf("got:  %f (%016x)", got, math.Float64bits(got))
				t.Logf("want: %f (%016x)", want, math.Float64bits(want))
				t.Errorf("wrong result")
			}
		}
	})
}

func BenchmarkPowStdlib(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for exp := -1022; exp <= 1023; exp++ {
			_ = math.Pow(2.0, float64(exp))
		}
	}
}

func BenchmarkPow2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for exp := -1022; exp <= 1023; exp++ {
			_, _ = pow2Int(exp)
		}
	}
}
