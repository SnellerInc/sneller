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
