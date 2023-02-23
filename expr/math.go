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
)

// modulusRational calculates modulus of two numbers.
//
// Assumption: y is already validated that it's not zero
func modulusRational(x, y *big.Rat) *big.Rat {
	if x.IsInt() && y.IsInt() {
		xi := x.Num()
		yi := y.Num()
		if xi.IsInt64() && yi.IsInt64() {
			x := xi.Int64()
			y := yi.Int64()
			return big.NewRat(x%y, 1)
		}

		// If x or y cannot be represented as an int64,
		// then fallback to the generic algorithm
	}

	// x mod y = x - floor(x/y) * y
	intpart := new(big.Rat)
	{
		// tmp = x / y
		tmp := new(big.Rat)
		tmp.Quo(x, y)

		// intpart = floor(y/x)
		i := new(big.Int)
		r := new(big.Int)
		i.QuoRem(tmp.Num(), tmp.Denom(), r)
		intpart.SetInt(i)
	}

	// t0 := floor(x/y) * y
	t0 := new(big.Rat)
	t0.Mul(y, intpart)

	// t1 := x - t0
	t1 := new(big.Rat)
	t1.Sub(x, t0)

	return t1
}
