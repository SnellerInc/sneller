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

// Implementation of HyperLogLog, based on "HyperLogLog: the analysis
// of a near-optimalcardinality estimation algorithm"
// http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

package vm

import (
	"fmt"
	"math"
)

// aggApproxCountDistinctInit initializes aggregation buffer
func aggApproxCountDistinctInit(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// aggApproxCountDistinctUpdateBuckets merges src with dst buffer
func aggApproxCountDistinctUpdateBuckets(n int, dst, src []byte) {
	if len(src) != len(dst) {
		panic(fmt.Sprintf("attempt to update incompatible buckets (src len is %d, dst len is %d)",
			len(src), len(dst)))
	}
	if len(src) < n {
		panic(fmt.Sprintf("requires at least %d bytes, got buffer %d", n, len(src)))
	}

	for i := 0; i < n; i++ {
		if src[i] > dst[i] {
			dst[i] = src[i]
		}
	}
}

// aggApproxCountDistinctHLL calculates approximate cardinality
// based on bytes in b.
//
// It uses HyperLogLog formula, i.e. factor * m * m / sum_i^m {2^{-b_i}}
// where m is the number of buckets.
func aggApproxCountDistinctHLL(b []byte) uint64 {
	return uint64(estimate(b))
}

func estimate(b []byte) float64 {
	e := rawestimate(b)
	m := float64(len(b))

	if e < 5*m/2 {
		// small range correction
		v := zerocount(b)
		if v != 0 {
			e = m * math.Log(m/float64(v))
		}

		return e
	}

	const pow = float64(1 << 32) // 2^32
	if e > pow/30 {
		// large range correction
		return -pow * math.Log(1-e/pow)
	}

	// no correction
	return e
}

func rawestimate(b []byte) float64 {
	h := 0.0
	for i := range b {
		h += pow2IntUnsafe(-int(b[i]))
	}

	m := len(b)

	return alpha(m) * float64(m) * float64(m) / h
}

func zerocount(b []byte) int {
	n := 0
	for i := range b {
		if b[i] == 0 {
			n += 1
		}
	}

	return n
}

func alpha(m int) float64 {
	switch m {
	case 16:
		return 0.673

	case 32:
		return 0.697

	case 64:
		return 0.709
	}

	return 0.7213 / (1.0 + 1.079/float64(m))
}

// pow2Int calculates Pow(2.0, float64(exp)).
//
// Returns error if the exponent is out of range for float64 type.
func pow2Int(exp int) (float64, error) {
	const f64minexp = -1022
	const f64maxexp = 1023
	if exp < f64minexp || exp > f64maxexp {
		return 0.0, fmt.Errorf("out of range")
	}

	return pow2IntUnsafe(exp), nil
}

// pow2IntUnsafe calculates Pow(2.0, float64(exp)).
//
// It assumes the exponent is in the valid range for float64 type.
func pow2IntUnsafe(exp int) float64 {
	const f64bias = 1023
	const f64exppos = 52

	bin := uint64(exp+f64bias) << f64exppos

	return math.Float64frombits(bin)
}
