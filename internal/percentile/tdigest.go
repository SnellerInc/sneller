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

// Package percentile provides a pure go implementation of tDigest aggregation and
// the computation of percentiles
package percentile

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type weightT = float32
type meanT = float32

// centroidT average position of all points in a shape
type centroidT struct {
	Mean   meanT
	Weight weightT
}

// CentroidsT is sorted by the mean of the centroidT, ascending.
type CentroidsT []centroidT

func (c CentroidsT) Len() int {
	return len(c)
}

func (c CentroidsT) sort() {
	sort.Slice(c[:], func(i, j int) bool {
		return c[i].Mean < c[j].Mean
	})
}

type TDigest struct {
	Data        CentroidsT
	TotalWeight weightT // total TotalWeight of all centroids
	Min, Max    weightT
}

func NewTDigest(data []meanT, compression int) *TDigest {

	// create an empty TDigest struct
	t := TDigest{
		Data:        make(CentroidsT, 0),
		TotalWeight: 0,
		Min:         math.MaxFloat32,
		Max:         -math.MaxFloat32,
	}

	newCentroids := make([]centroidT, len(data))
	for i, d := range data {
		newCentroids[i] = centroidT{Mean: d, Weight: 1}
	}
	t.addCentroids(newCentroids, compression)
	return &t
}

func mySin(x float64) float64 {
	// use internally 32-bits sin, but return 64-bits results
	return float64(float32(math.Sin(x)))
}
func myASin(x float64) float64 {
	// use internally 32-bits sin, but return 64-bits results
	return float64(float32(math.Asin(x)))
}
func myPi() float64 {
	// use internally 32-bits sin, but return 64-bits results
	return float64(float32(math.Pi))
}

var onePi = myPi()
var halfPi = myPi() / 2

// centroidsCompress compresses the provided 48 centroids
func centroidsCompress(wIn [3 * 16]weightT, mIn [3 * 16]meanT, inLen int, totalW float32, compression int) (wOut [2 * 16]weightT, mOut [2 * 16]meanT, outLen int, weightLimits, weightSums [48]weightT) {

	// only trigger compression when total number of centroids exceeds the compression level
	if inLen <= compression {
		for i := 0; i < 2*16; i++ {
			wOut[i] = wIn[i]
			mOut[i] = mIn[i]
		}
		return wOut, mOut, inLen, weightLimits, weightSums
	}

	// pre-compute weightLimits with 16 lanes in parallel
	{
		weigthCalc := func(totalW, wSum weightT, compression int) weightT {
			x1 := wSum / totalW
			x2 := 2.0 * x1
			x3 := x2 - 1.0

			if (x3 > 1.0) || (x3 < -1) { // test to make sure ranges are constrained
				panic("invalid asin input")
			}

			x4 := myASin(float64(x3))
			x5 := x4 + halfPi
			x6 := float64(compression) * x5
			x7 := x6 / onePi
			x8 := x7 + 1

			y1 := math.Min(x8, float64(compression))
			y2 := y1 * onePi
			y3 := y2 / float64(compression)
			y4 := y3 - halfPi

			if (y4 > halfPi) || (y4 < -halfPi) { // test to make sure ranges are constrained
				panic("invalid sin input")
			}
			y5 := mySin(y4)
			y6 := y5 + 1
			y7 := weightT(y6 / 2)
			y8 := y7 * totalW
			return y8
		}

		weightSums[0] = 0

		wSum2 := weightT(0)
		for i := 1; i < inLen; i++ {
			wSum2 += wIn[i-1]
			weightSums[i] = wSum2
		}
		for i := 0; i < inLen; i++ {
			weightLimits[i] = weigthCalc(totalW, weightSums[i], compression)
		}
	}

	// compress the centroids
	weightLimit := weightLimits[0]

	wSum := wIn[0]
	wOut[0] = wIn[0]
	mOut[0] = mIn[0]
	n := 1

	for i := 1; i < inLen; i++ {
		wi := wIn[i]
		mi := mIn[i]
		wSum += wi

		if wSum <= weightLimit { // weightLimit is unaltered
			// merge the new centroid with the last centroid in data
			y0 := wOut[n-1]
			x0 := mOut[n-1]

			y1 := y0 + wi
			x1 := mi - x0
			x2 := wi * x1
			x3 := x2 / y1
			x4 := x0 + x3

			wOut[n-1] = y1
			mOut[n-1] = x4
		} else {
			weightLimit = weightLimits[i]
			//log.Printf("6b48bf81: i %v: weightLimit %v", i, weightLimit)

			// add the new centroid as a separate centroid in data
			wOut[n] = wi
			mOut[n] = mi
			n++
		}
	}
	outLen = n
	return
}

func (t *TDigest) addCentroids(newCentroids CentroidsT, compression int) {
	// Append all Data centroids to the newCentroids list and sort
	if t.Data.Len() > 2*16 {
		panic(fmt.Sprintf("addCentroids data is too big %v", t.Data.Len()))
	}
	if newCentroids.Len() > 16 {
		panic(fmt.Sprintf("addCentroids: newCentroids is too big %v", newCentroids.Len()))
	}

	newCentroids = append(newCentroids, t.Data...)
	newCentroids.sort() // main bottleneck of the algorithm; newCentroids should not exceed 3*16

	var wIn [3 * 16]weightT
	var mIn [3 * 16]meanT

	inLen := newCentroids.Len()
	for i := 0; i < inLen; i++ {
		wIn[i] = newCentroids[i].Weight
		mIn[i] = newCentroids[i].Mean
	}

	totalW := meanT(0.0)
	for i := 0; i < inLen; i++ {
		totalW += wIn[i]
	}
	t.TotalWeight = totalW

	wOut, mOut, outLen, _, _ := centroidsCompress(wIn, mIn, inLen, totalW, compression)

	t.Data = make([]centroidT, outLen)
	for i := 0; i < outLen; i++ {
		t.Data[i].Mean = mOut[i]
		t.Data[i].Weight = wOut[i]
	}

	// update the new Min/Max
	tmp := t.Data[0].Mean
	if t.Min > tmp {
		t.Min = tmp
	}
	tmp = t.Data[t.Data.Len()-1].Mean

	if t.Max < tmp {
		t.Max = tmp
	}
}

// Merge of two TDigest structures is nothing more than add the centroids
// of one tDigest to the centroids of the other tDigest
func (t *TDigest) Merge(t2 *TDigest, compression int) {
	if t.Min > t2.Min {
		t.Min = t2.Min
	}
	if t.Max < t2.Max {
		t.Max = t2.Max
	}

	if t2.Data.Len() <= 16 {
		t.addCentroids(t2.Data, compression)
		return
	}

	remainder := t2.Data
	for remainder.Len() > 16 {
		t.addCentroids(remainder[:16], compression)
		remainder = remainder[16:]
	}
	t.addCentroids(remainder, compression)
}

// Percentile returns the (approximate) Percentile of
// the distribution. Accepted values for q are between 0.0 and 1.0.
// Returns NaN if count is zero or bad inputs.
func (t *TDigest) Percentile(p float32) float32 {
	return t.Percentiles([]float32{p})[0]
}

func (t *TDigest) Percentiles(p []float32) []float32 {

	weightedAverage := func(mean1 meanT, weight1 weightT, mean2 meanT, weight2 weightT) weightT {
		weightedAverageSorted := func(m1, w1, m2, w2 weightT) weightT {
			x := float64((m1*w1 + m2*w2) / (w1 + w2))
			return weightT(math.Max(float64(m1), math.Min(x, float64(m2))))
		}
		if mean1 <= mean2 {
			return weightedAverageSorted(mean1, weight1, mean2, weight2)
		}
		return weightedAverageSorted(mean2, weight2, mean1, weight1)
	}

	cumulative := make([]weightT, t.Data.Len()+1)
	{
		sumWeight := weightT(0.0)
		for i, centroid := range t.Data {
			currW := centroid.Weight
			cumulative[i] = sumWeight + currW/2
			sumWeight += currW
		}
		cumulative[t.Data.Len()] = sumWeight
	}

	result := make([]float32, len(p))
	for i, q := range p {
		dataLen := t.Data.Len()
		switch {
		case q < 0 || q > 1 || dataLen == 0:
			result[i] = float32(math.NaN())
		case dataLen == 1:
			result[i] = t.Data[0].Mean
		case q == 0:
			result[i] = t.Min
		case q == 1:
			result[i] = t.Max
		default:
			index := q * t.TotalWeight
			if index <= t.Data[0].Weight/2.0 {
				result[i] = t.Min + (((2 * index) / t.Data[0].Weight) * (t.Data[0].Mean - t.Min))
			} else {
				lower := sort.Search(len(cumulative), func(i int) bool {
					return cumulative[i] >= index
				})
				if lower+1 < len(cumulative) {
					z1 := index - cumulative[lower-1]
					z2 := cumulative[lower] - index
					result[i] = weightedAverage(t.Data[lower-1].Mean, z2, t.Data[lower].Mean, z1)
				} else {
					lastWeight := t.Data[dataLen-1].Weight / 2
					w1 := index - (t.TotalWeight - lastWeight)
					w2 := lastWeight - w1
					result[i] = weightedAverage(t.Data[dataLen-1].Mean, w1, t.Max, w2)
				}
			}
		}
	}
	return result
}

func (t *TDigest) String() string {
	if t.Data.Len() == 0 {
		return "EMPTY"
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("TotalWeight %v; max %v; min %v; nCentroids %v\n", t.TotalWeight, t.Max, t.Min, t.Data.Len()))
	for i := 0; i < t.Data.Len(); i++ {
		sb.WriteString(fmt.Sprintf("(mean %v; weight %v)\n", t.Data[i].Mean, t.Data[i].Weight))
	}
	return sb.String()
}
