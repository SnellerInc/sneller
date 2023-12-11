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

package percentile

import (
	"fmt"
	"slices"
	"sort"
	"testing"
)

// Sortnet16f32 sort network of 16 float32 elements (f32)
//
//go:noescape
func Sortnet16f32(data []float32)

//go:noescape
func Sortnet48w32x16f32f32(data []float32)

type pair struct {
	x, y int
}

var sortPair6Prefix4x2 = [][]pair{
	{{0, 4}, {3, 5}},
	{{2, 4}, {1, 3}},
	{{3, 4}, {1, 2}},
}

// networks for 13/14/15 elements all have 9 layers thus not faster than 16 elements
// no considering within 128-bit lane swaps opportunities
var sortPair16 = [][]pair{
	{{0, 5}, {1, 4}, {2, 12}, {3, 13}, {6, 7}, {8, 9}, {10, 15}, {11, 14}},
	{{0, 2}, {1, 10}, {3, 6}, {4, 7}, {5, 14}, {8, 11}, {9, 12}, {13, 15}},
	{{0, 8}, {1, 3}, {2, 11}, {4, 13}, {5, 9}, {6, 10}, {7, 15}, {12, 14}},
	{{0, 1}, {2, 4}, {3, 8}, {5, 6}, {7, 12}, {9, 10}, {11, 13}, {14, 15}},
	{{1, 3}, {2, 5}, {4, 8}, {6, 9}, {7, 11}, {10, 13}, {12, 14}},
	{{1, 2}, {3, 5}, {4, 11}, {6, 8}, {7, 9}, {10, 12}, {13, 14}},
	{{2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}, {12, 13}},
	{{4, 6}, {5, 7}, {8, 10}, {9, 11}},
	{{3, 4}, {5, 6}, {7, 8}, {9, 10}, {11, 12}},
}

// Number32 is either int32 or float32
type Number32 interface {
	int32 | float32
}

type dataIdx[K Number32] struct {
	dataA, dataB K
}

// sortNetworkRef is the reference implementation to test sorting network data
func sortNetworkRef[K Number32](data []K, sortPairs [][]pair) {
	for _, p := range sortPairs {
		for _, swap := range p {
			if data[swap.x] > data[swap.y] {
				tmp := data[swap.y]
				data[swap.y] = data[swap.x]
				data[swap.x] = tmp
			}
		}
	}
}

func sortWithAux[K Number32](dataA, dataB []K) (resA, resB []K) {
	dataLen := len(dataA)

	resA = make([]K, dataLen)
	resB = make([]K, dataLen)

	dataNew := make([]dataIdx[K], dataLen)
	for i := 0; i < dataLen; i++ {
		dataNew[i].dataA = dataA[i]
		dataNew[i].dataB = dataB[i]
	}
	sort.Slice(dataNew, func(i, j int) bool {
		if dataNew[i].dataA == dataNew[j].dataA {
			return dataNew[i].dataB == dataNew[j].dataB
		}
		return dataNew[i].dataA < dataNew[j].dataA
	})

	for i := 0; i < dataLen; i++ {
		resA[i] = dataNew[i].dataA
		resB[i] = dataNew[i].dataB
	}
	return
}

func equalWithAux[K Number32](obsDataA, expDataA, obsDataB, expDataB []K) error {
	dataLen := len(obsDataA)
	if len(expDataA) != dataLen || len(obsDataB) != dataLen || len(expDataB) != dataLen {
		return fmt.Errorf("unequal length of data (%v, %v) and idx (%v, %v)", dataLen, len(expDataA), len(obsDataB), len(expDataB))
	}
	for i := 0; i < dataLen; i++ {
		if obsDataA[i] != expDataA[i] {
			return fmt.Errorf("obsDataA[%v] = %v while expDataA[%v] = %v", i, obsDataA[i], i, expDataA[i])
		}
	}
	doDataBcheck := false
	for i := 0; i < dataLen; i++ {
		if obsDataB[i] != expDataB[i] {
			doDataBcheck = true
			break
		}
	}
	if doDataBcheck {
		dataX1 := make([]dataIdx[K], dataLen)
		dataX2 := make([]dataIdx[K], dataLen)
		for i := 0; i < dataLen; i++ {
			dataX1[i].dataA = obsDataA[i]
			dataX1[i].dataB = obsDataB[i]
			dataX2[i].dataA = expDataA[i]
			dataX2[i].dataB = expDataB[i]
		}
		sort.Slice(dataX1, func(i, j int) bool {
			if dataX1[i].dataA == dataX1[j].dataA {
				return dataX1[i].dataB < dataX1[j].dataB
			}
			return dataX1[i].dataA < dataX1[j].dataA
		})
		sort.Slice(dataX2, func(i, j int) bool {
			if dataX2[i].dataA == dataX2[j].dataA {
				return dataX2[i].dataB < dataX2[j].dataB
			}
			return dataX2[i].dataA < dataX2[j].dataA
		})
		for i := 0; i < dataLen; i++ {
			if dataX1[i].dataA != dataX2[i].dataA || dataX1[i].dataB != dataX2[i].dataB {
				return fmt.Errorf("obsDataA[%v] = %v while expDataA[%v] = %v", i, dataX1[i], i, dataX2[i])
			}
		}
	}
	return nil
}

func toString(d []float32) string {
	msg := "[]float32{"
	for i := 0; i < len(d); i++ {
		if d[i] < 10 {
			msg += " "
		}
		msg += fmt.Sprintf("%v", d[i])
		if i < len(d)-1 {
			msg += ","
		}
	}
	return msg + "}"
}

// Tests for sorting 16 floats
func calcNet16f32(orgData []float32) error {

	// 2. calculate expected values
	expData := slices.Clone(orgData)
	slices.Sort(expData)

	// 3. check whether the sorting network is correct
	{
		x := slices.Clone(orgData)
		sortNetworkRef(x, sortPair16)
		if !slices.Equal(x, expData) {
			return fmt.Errorf("sorting network is not correct! %v, %v", orgData, expData)
		}
	}

	// 4. call the assembly
	obsData := slices.Clone(orgData)
	Sortnet16f32(obsData)

	// 5. check if correct
	if !slices.Equal(obsData, expData) {
		msg := fmt.Sprintf("\norg data = %v\n", toString(orgData))
		msg += fmt.Sprintf("obs data = %v\n", toString(obsData))
		msg += fmt.Sprintf("exp data = %v\n", toString(expData))
		return fmt.Errorf("%v", msg)
	}
	return nil
}

func TestNet16f32(t *testing.T) {
	type unitTest struct {
		data [16]float32
	}
	unitTests := []unitTest{
		{[16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}},
		{[16]float32{0, 0, 0, 0, 0, 0, 0, 82, -29, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, ut := range unitTests {
		if err := calcNet16f32(ut.data[:]); err != nil {
			t.Error(err)
		}
	}
}

func FuzzNet16f32(f *testing.F) {
	f.Fuzz(func(t *testing.T, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 float32) {
		data := []float32{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		if err := calcNet16f32(data); err != nil {
			t.Error(err)
		}
	})
}

// Tests for sorting 3*16 floats (first 2*16 and last 16 elements already sorted)
func calcNet48w32x16f32f32(orgDataA, orgDataB []float32) error {

	if !slices.IsSorted(orgDataA[:2*16]) || !slices.IsSorted(orgDataA[2*16:]) {
		return fmt.Errorf("data was not sorted to begin with")
	}

	// 2. calculate expected values
	expDataA, expDataB := sortWithAux(orgDataA, orgDataB)

	// 4. transform such that it can be fed to ASM, and call the assembly
	obsData := make([]float32, 6*16)
	for i := 0; i < len(orgDataA); i++ {
		obsData[i+0*16] = orgDataA[i]
		obsData[i+3*16] = orgDataB[i]
	}
	Sortnet48w32x16f32f32(obsData)

	// 5. check if correct
	obsDataA := obsData[(0 * 16):(3 * 16)]
	obsDataB := obsData[(3 * 16):(6 * 16)]

	if err := equalWithAux(obsDataA, expDataA, obsDataB, expDataB); err != nil {
		msg := fmt.Sprintf("\norg dataA = %v\n", toString(orgDataA))
		msg += fmt.Sprintf("exp dataA = %v\n", toString(expDataA))
		msg += fmt.Sprintf("obs dataA = %v\n", toString(obsDataA))
		msg += fmt.Sprintf("org dataB = %v\n", toString(orgDataB))
		msg += fmt.Sprintf("exp dataB = %v\n", toString(expDataB))
		msg += fmt.Sprintf("obs dataB = %v\n", toString(obsDataB))
		return fmt.Errorf("%v", msg)
	}
	return nil
}

func TestNet48w32x16f32f32(t *testing.T) {
	type unitTest struct {
		dataA0 [2 * 16]float32
		dataA1 [1 * 16]float32
	}
	unitTests := []unitTest{
		{
			[2 * 16]float32{
				0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1,
				2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3},
			[1 * 16]float32{
				4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5},
		},
		{
			[2 * 16]float32{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
				16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			[1 * 16]float32{
				32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47},
		},
	}
	for _, ut := range unitTests {
		dataA := append(ut.dataA0[:], ut.dataA1[:]...)
		dataB := make([]float32, 3*16)
		for i := 0; i < 3*16; i++ {
			//dataB[i] = float32(i)
			dataB[i] = dataA[i]
		}
		if err := calcNet48w32x16f32f32(dataA, dataB); err != nil {
			t.Error(err)
		}
	}
}

func FuzzNet48w32x16f32f32(f *testing.F) {
	f.Fuzz(func(t *testing.T,
		d0a, d1a, d2a, d3a, d4a, d5a, d6a, d7a, d8a, d9a, d10a, d11a, d12a, d13a, d14a, d15a,
		d0b, d1b, d2b, d3b, d4b, d5b, d6b, d7b, d8b, d9b, d10b, d11b, d12b, d13b, d14b, d15b,
		d0c, d1c, d2c, d3c, d4c, d5c, d6c, d7c, d8c, d9c, d10c, d11c, d12c, d13c, d14c, d15c float32) {

		// 1. assemble data
		dataA1 := []float32{
			d0a, d1a, d2a, d3a, d4a, d5a, d6a, d7a, d8a, d9a, d10a, d11a, d12a, d13a, d14a, d15a,
			d0b, d1b, d2b, d3b, d4b, d5b, d6b, d7b, d8b, d9b, d10b, d11b, d12b, d13b, d14b, d15b}
		slices.Sort(dataA1)

		dataA2 := []float32{d0c, d1c, d2c, d3c, d4c, d5c, d6c, d7c, d8c, d9c, d10c, d11c, d12c, d13c, d14c, d15c}
		slices.Sort(dataA2)

		dataA := append(dataA1, dataA2...)

		dataB := make([]float32, 3*16)
		for i := 0; i < len(dataB); i++ {
			dataB[i] = float32(i) + 100 // just add some content into the auxiliary data
		}
		if err := calcNet48w32x16f32f32(dataA, dataB); err != nil {
			t.Error(err)
		}
	})
}

// Tests for sorting 6 (first 4 and last 2 elements already sorted)
func FuzzNet6Prefix4x2(f *testing.F) {
	f.Fuzz(func(t *testing.T, d0, d1, d2, d3, d4, d5 int32) {
		dataA := []int32{d0, d1, d2, d3}
		dataB := []int32{d4, d5}
		slices.Sort(dataA)
		slices.Sort(dataB)
		data := append(dataA, dataB...)

		data2 := slices.Clone(data)
		sortNetworkRef(data, sortPair6Prefix4x2)
		slices.Sort(data2)

		if !slices.Equal(data, data2) {
			t.Log(data)
			t.Log(data2)
			t.Fail()
		}
	})
}

// benchmarks
func BenchmarkNet16f32(b *testing.B) {
	// since the sorting network is branch free, the actual content of the
	// data has no effect on the performance, just take 1..16 as data
	data := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	sum := float32(0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Sortnet16f32(data)
		sum += data[0]
	}
	b.StopTimer()
	b.Log(sum)
}

func BenchmarkSortNetwork16f32(b *testing.B) {
	// since the sorting network is branch free, the actual content of the
	// data has no effect on the performance, just take 1..16 as data
	data := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	sum := float32(0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Sortnet16f32(data)
		sum += data[0]
	}
	b.StopTimer()
	b.Log(sum)
}

func BenchmarkSort16f32(b *testing.B) {
	data := []float32{1, 2, 3, 24, 15, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	sum := float32(0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		slices.Sort(data)
		sum += data[0]
	}
	b.StopTimer()
	b.Log(sum)
}
