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

// This file contains all supporting functions for handling
// Kahan-Babushka-Neumaier summation algorithm[1] used in
// aggregates.
//
// [1] https://en.wikipedia.org/wiki/Kahan_summation_algorithm

import (
	"encoding/binary"
	"math"
)

// Memory layout: 16 x (float64: compensation, float64: sum, uint64: count)
const aggregateOpSumFDataSize = 16 * (8 + 8 + 8)

func neumaierSummationInit(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// neumaierSummation provides basic step of Kahan-Babushka-Neumaier algorithm.
//
// It takes the already calculated `sum` and compensation `c`,
// and adds a new value `x`. It returns the new sum and compensation.
func neumaierSummation(sum, x, c float64) (newsum float64, newc float64) {
	t := sum + x
	if math.Abs(sum) >= math.Abs(x) {
		c += (sum - t) + x
	} else {
		c += (x - t) + sum
	}

	newsum = t
	newc = c
	return
}

// neumaierSummationMerge merges two states of summation algorithm.
// A state consists 16 independent sums and compensations.
func neumaierSummationMerge(dst, src []byte) {
	k := 16
	n := k * 8

	dstCorr := dst[:n]
	dstSum := dst[n : 2*n]
	dstCount := dst[2*n : 3*n]

	srcCorr := src[:n]
	srcSum := src[n : 2*n]
	srcCount := src[2*n : 3*n]

	for i := 0; i < k; i++ {
		c := getfloat64(dstCorr, i)
		sum := getfloat64(dstSum, i)
		xi := getfloat64(srcSum, i)
		ci := getfloat64(srcCorr, i)

		sum, c = neumaierSummation(sum, xi, c)
		sum, c = neumaierSummation(sum, ci, c)

		setfloat64(dstCorr, i, c)
		setfloat64(dstSum, i, sum)

		count1 := getuint64(srcCount, i)
		count2 := getuint64(dstCount, i)
		setuint64(dstCount, i, count1+count2)
	}
}

// neumaierSummationFinalize folds 16 partial summation results into a single scalar value.
func neumaierSummationFinalize(data []byte) {
	k := 16
	n := k * 8

	srcCorr := data[:n]
	srcSum := data[n : 2*n]
	srcCount := data[2*n : 3*n]

	sum := 0.0
	c := 0.0
	count := uint64(0)

	for i := 0; i < k; i++ {
		// calculate the final sum: merge all KBN states
		//
		// note that the correction from i-th state is treated as an input value
		ci := getfloat64(srcCorr, i)
		xi := getfloat64(srcSum, i)

		sum, c = neumaierSummation(sum, xi, c)
		sum, c = neumaierSummation(sum, ci, c)

		// update the count
		count += getuint64(srcCount, i)
	}

	// apply the final correction
	sum += c

	setuint64(data, 1, count)
	setfloat64(data, 0, sum)
}

func getuint64(b []byte, idx int) uint64 {
	view := b[idx*8 : idx*8+8]
	return binary.LittleEndian.Uint64(view)
}

func setuint64(b []byte, idx int, val uint64) {
	view := b[idx*8 : idx*8+8]
	binary.LittleEndian.PutUint64(view, val)
}

func getfloat64(b []byte, idx int) float64 {
	return math.Float64frombits(getuint64(b, idx))
}

func setfloat64(b []byte, idx int, val float64) {
	setuint64(b, idx, math.Float64bits(val))
}
