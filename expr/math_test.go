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

package expr

import (
	"math/big"
	"testing"
)

func TestModulusRational(t *testing.T) {
	testcases := []struct {
		name string
		x    *big.Rat
		y    *big.Rat
		want *big.Rat
	}{
		{
			name: "5 % 3 = 2",
			x:    big.NewRat(5, 1),
			y:    big.NewRat(3, 1),
			want: big.NewRat(2, 1),
		},
		{
			name: "5 % -3 = 2",
			x:    big.NewRat(5, 1),
			y:    big.NewRat(-3, 1),
			want: big.NewRat(2, 1),
		},
		{
			name: "-5 % 3 = 2",
			x:    big.NewRat(5, 1),
			y:    big.NewRat(-3, 1),
			want: big.NewRat(2, 1),
		},
		{
			name: "-5 % -3 = 2",
			x:    big.NewRat(5, 1),
			y:    big.NewRat(-3, 1),
			want: big.NewRat(2, 1),
		},
		{
			name: "10 % 5 = 0",
			x:    big.NewRat(10, 1),
			y:    big.NewRat(5, 1),
			want: big.NewRat(0, 1),
		},
		{
			name: "5.7 % 2.2 = 1.3",
			x:    big.NewRat(57, 10),
			y:    big.NewRat(22, 10),
			want: big.NewRat(13, 10),
		},
		{
			name: "0.7 % 12.5 = 0.7",
			x:    big.NewRat(7, 10),
			y:    big.NewRat(125, 10),
			want: big.NewRat(7, 10),
		},
		{
			name: "4.5 % 1.3 = 0.6",
			x:    big.NewRat(45, 10),
			y:    big.NewRat(13, 10),
			want: big.NewRat(6, 10),
		},
	}

	for i := range testcases {
		tc := testcases[i]
		t.Run(tc.name, func(t *testing.T) {
			got := modulusRational(tc.x, tc.y)
			if tc.want.Cmp(got) != 0 {
				t.Logf("got : %s", got)
				t.Logf("want: %s", tc.want)
				t.Errorf("wrong result")
			}
		})
	}
}
