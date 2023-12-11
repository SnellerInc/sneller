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
	"log"
	"math"
	"strings"
	"testing"
)

func concat2(x0, x1 [16]float32) (result [2 * 16]float32) {
	for i := 0; i < 16; i++ {
		result[i] = x0[i]
		result[i+16] = x1[i]
	}
	return result
}

func concat3(x0, x1, x2 [16]float32) (result [3 * 16]float32) {
	for i := 0; i < 16; i++ {
		result[i] = x0[i]
		result[i+16] = x1[i]
		result[i+32] = x2[i]
	}
	return result
}

func printData(
	obsLen int, obsMean, obsWeight [2 * 16]float32,
	expLen int, expMean, expWeight [2 * 16]float32) string {

	colorRed := "\033[31m"
	colorReset := "\033[0m"

	result := strings.Builder{}
	result.WriteString(fmt.Sprintf("expected number of centroids %v; observed %v\n", expLen, obsLen))
	result.WriteString("weight, obsMean\n")
	for i := 0; i < 32; i++ {
		if i < expLen || i < obsLen {
			expStr := ""
			if i < expLen {
				expStr = fmt.Sprintf("%4.9g, %4.9g", expWeight[i], expMean[i])
			}
			obsStr := ""
			if i < obsLen {
				obsStr = fmt.Sprintf("%4.9g, %4.9g", obsWeight[i], obsMean[i])
			}
			equal := expStr == obsStr
			if !equal {
				result.WriteString(colorRed)
			}
			result.WriteString(fmt.Sprintf("%v:", i))
			if i < expLen {
				result.WriteString("exp: " + expStr)
			}
			if i < obsLen {
				result.WriteString("; obs: " + obsStr)
			}
			result.WriteString("\n")
			if !equal {
				result.WriteString(colorReset)
			}
		}
	}
	return result.String()
}

func equalData(
	obsLen int, obsMean, obsWeight [2 * 16]float32,
	expLen int, expMean, expWeight [2 * 16]float32) bool {
	if obsLen != expLen {
		return false
	}
	for i := 0; i < obsLen; i++ {
		if (obsMean[i] != expMean[i]) || (obsWeight[i] != expWeight[i]) {
			return false
		}
	}
	return true
}

// Tests for sorting 16 floats
func calcCentroidsCompress(
	inLen int, mean0, mean1, mean2, weight0, weight1, weight2 [16]float32,
	expLen int, expMean0, expMean1, expWeight0, expWeight1 [16]float32,
	print bool) error {

	mean := concat3(mean0, mean1, mean2)
	weight := concat3(weight0, weight1, weight2)
	expMean := concat2(expMean0, expMean1)
	expWeight := concat2(expWeight0, expWeight1)

	totalW := float32(0)
	for i := 0; i < 16; i++ {
		totalW += weight0[i] + weight1[i] + weight2[i]
	}

	{
		// 2. calculate expected values
		obsWeight, obsMean, obsLen, _, _ := centroidsCompress(weight, mean, inLen, totalW, 16)

		if print {
			log.Print(printData(obsLen, obsWeight, obsMean, expLen, expWeight, expMean))
		}
		// 3. check if reference is equal
		if !equalData(obsLen, obsWeight, obsMean, expLen, expWeight, expMean) {
			return fmt.Errorf(printData(obsLen, obsWeight, obsMean, expLen, expWeight, expMean))
		}
	}
	return nil
}

func TestCentroidsCompress(t *testing.T) {
	type unitTest struct {
		print bool

		lenIn                           int
		meanIn0, meanIn1, meanIn2       [16]float32
		weightIn0, weightIn1, weightIn2 [16]float32

		lenOut                 int
		meanOut0, meanOut1     [16]float32
		weightOut0, weightOut1 [16]float32
	}

	inf := math.Float32frombits(0x7F800000)

	unitTests := []unitTest{
		{ // NOTE register is only partly filled, empty centroids should have weight equal to zero
			print:     false,
			lenIn:     16 + 3, // example equal to "go test -v -run=TestQueries/0031"
			meanIn0:   [16]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			meanIn1:   [16]float32{17, 18, 19, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			meanIn2:   [16]float32{inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			weightIn0: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightIn1: [16]float32{1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			weightIn2: [16]float32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

			lenOut:     19,
			meanOut0:   [16]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			meanOut1:   [16]float32{17, 18, 19, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			weightOut0: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightOut1: [16]float32{1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			print:     false,
			lenIn:     3 * 16,
			meanIn0:   [16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			meanIn1:   [16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			meanIn2:   [16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			weightIn0: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightIn1: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightIn2: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},

			lenOut:     18,
			meanOut0:   [16]float32{15, 14, 12.5, 10.5, 8, 5, 1.5, 13.5, 9.5, 5.5, 1.5, 13.5, 10, 7, 4.5, 2.5},
			meanOut1:   [16]float32{1, 0, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			weightOut0: [16]float32{1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4, 3, 3, 2, 2},
			weightOut1: [16]float32{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{ // NOTE register is only partly filled, empty centroids should have weight equal to zero
			print:     false,
			lenIn:     2*16 + 1,
			meanIn0:   [16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			meanIn1:   [16]float32{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			meanIn2:   [16]float32{15, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			weightIn0: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightIn1: [16]float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			weightIn2: [16]float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

			lenOut:     17,
			meanOut0:   [16]float32{15, 14, 13, 11.5, 9.5, 7.5, 5, 2, 9.66666698, 12, 9, 6.5, 4.5, 2.5, 1, 0},
			meanOut1:   [16]float32{15, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf, inf},
			weightOut0: [16]float32{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 2, 2, 2, 1, 1},
			weightOut1: [16]float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, ut := range unitTests {
		if err := calcCentroidsCompress(
			ut.lenIn, ut.meanIn0, ut.meanIn1, ut.meanIn2, ut.weightIn0, ut.weightIn1, ut.weightIn2,
			ut.lenOut, ut.meanOut0, ut.meanOut1, ut.weightOut0, ut.weightOut1, ut.print); err != nil {
			t.Error(err)
		}
	}
}
