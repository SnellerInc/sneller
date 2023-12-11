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
