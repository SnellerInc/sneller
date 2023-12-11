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

// Implementation of HyperLogLog, based on "HyperLogLog: the analysis
// of a near-optimalcardinality estimation algorithm"
// http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

package vm

import (
	"fmt"
	"math"
)

// aggApproxCountDistinctInit initializes an aggregation buffer
func aggApproxCountDistinctInit(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// aggApproxCountDistinctUpdateBuckets merges src with dst buffer
func aggApproxCountDistinctUpdateBuckets(n int, dst, src []byte) {
	// Note: callers may pass src & dst bigger than actual n bytes.
	//       We must only assure that the buffers are large enough,
	//       and update exactly n bytes
	if len(src) < n {
		panic(fmt.Sprintf("requires at least %d src bytes, got buffer %d", n, len(src)))
	}
	if len(dst) < n {
		panic(fmt.Sprintf("requires at least %d dst bytes, got buffer %d", n, len(dst)))
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
		h += 1.0 / float64(uint64(1)<<b[i])
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
