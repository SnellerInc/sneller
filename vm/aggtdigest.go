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

package vm

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"github.com/SnellerInc/sneller/internal/percentile"
)

// tDigestDataSize is the total number of bytes used by the tDigest Aggregation.
// 1st element (float32): total sum of weights;
// 2nd element (float32): maximum value (from the aggregated values);
// 3rd element (float32): minimum value (from the aggregated values);
// 4th element (uint32): number of centroids (range 0, 32).
// These four elements make 16 bytes, the remaining bytes contain upto 32 centroids;
// a centroid contains a 'mean' and a 'weight'. First 32 weights (float32) are stored,
// followed by 32 means (float32), if the positions are used (because there are not
// that many centroids), mean of +inf and weight of 1 is used. The remaining bytes are
// used as scratch by the aggregator code.
const tDigestDataSize = 16 + 13*64

// tDigestDS data-structure for tDigest
type tDigestDS []byte

func (t tDigestDS) putLen(len int) {
	binary.LittleEndian.PutUint32(t[12:], uint32(len))
}
func (t tDigestDS) getLen() int {
	return int(binary.LittleEndian.Uint32(t[12:]))
}
func (t tDigestDS) putWeightSum(sum float32) {
	binary.LittleEndian.PutUint32(t[0:], math.Float32bits(sum))
}
func (t tDigestDS) getWeightSum() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(t[0:]))
}
func (t tDigestDS) putMeanMax(sum float32) {
	binary.LittleEndian.PutUint32(t[4:], math.Float32bits(sum))
}
func (t tDigestDS) getMeanMax() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(t[4:]))
}
func (t tDigestDS) putMeanMin(sum float32) {
	binary.LittleEndian.PutUint32(t[8:], math.Float32bits(sum))
}
func (t tDigestDS) getMeanMin() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(t[8:]))
}
func (t tDigestDS) putWeight(weight float32, idx int) {
	offset := 16 + (0 * 64) + (idx * 4)
	binary.LittleEndian.PutUint32(t[offset:], math.Float32bits(weight))
}
func (t tDigestDS) getWeight(idx int) float32 {
	offset := 16 + (0 * 64) + (idx * 4)
	return math.Float32frombits(binary.LittleEndian.Uint32(t[offset:]))
}
func (t tDigestDS) putMean(weight float32, idx int) {
	offset := 16 + (2 * 64) + (idx * 4)
	binary.LittleEndian.PutUint32(t[offset:], math.Float32bits(weight))
}
func (t tDigestDS) getMean(idx int) float32 {
	offset := 16 + (2 * 64) + (idx * 4)
	return math.Float32frombits(binary.LittleEndian.Uint32(t[offset:]))
}

func (t tDigestDS) clear() {
	for i := 0; i < tDigestDataSize; i++ {
		t[i] = 0
	}
}

//lint:ignore U1000 kept for testing purposes
func (t tDigestDS) debugDump() string {
	weightSum := t.getWeightSum()
	meanMax := t.getMeanMax()
	meanMin := t.getMeanMin()
	lenIn := t.getLen()
	if lenIn > 32 {
		log.Printf("lenIn %v is too large", lenIn)
		lenIn = 32
	}
	result := fmt.Sprintf("weightSum %v; meanMax %v; meanMin %v; lenIn %v\n", weightSum, meanMax, meanMin, lenIn)
	for i := 0; i < lenIn; i++ {
		result += fmt.Sprintf("mean %v; weight %v\n", t.getMean(i), t.getWeight(i))
	}
	return result
}

// tDigestInit initializes an aggregation buffer
func tDigestInit(data []byte) {
	for i := range data {
		data[i] = 0
	}
}

func createTDigest(data tDigestDS) (*percentile.TDigest, error) {
	lenIn := data.getLen()
	if lenIn > 32 {
		return nil, fmt.Errorf("while creating tDigest from data: number of centroids %v is too large (max 32)", lenIn)
	}
	t := percentile.TDigest{
		Data:        make(percentile.CentroidsT, lenIn),
		TotalWeight: data.getWeightSum(),
		Max:         data.getMeanMax(),
		Min:         data.getMeanMin(),
	}
	for i := 0; i < lenIn; i++ {
		t.Data[i].Weight = data.getWeight(i)
		t.Data[i].Mean = data.getMean(i)
	}
	return &t, nil
}

// createDs fills data with the provided tDigest content
func createDs(tDigest *percentile.TDigest, data tDigestDS) {
	nCentroids := tDigest.Data.Len()
	data.putLen(nCentroids)
	if nCentroids > 0 {
		data.putWeightSum(tDigest.TotalWeight)
		data.putMeanMax(tDigest.Max)
		data.putMeanMin(tDigest.Min)
		for i := 0; i < nCentroids; i++ {
			data.putWeight(tDigest.Data[i].Weight, i)
			data.putMean(tDigest.Data[i].Mean, i)
		}
	}
}

// tDigestMerge merges src with dst buffer
func tDigestMerge(dst, src tDigestDS) error {
	lenSrc := src.getLen()
	if lenSrc > 0 {
		lenDst := dst.getLen()
		if lenDst == 0 {
			// source contains processed data while dst does not: only copy from src to dst
			copy(dst[:tDigestDataSize], src[:tDigestDataSize])
		} else {
			// both source and dst contain processed data: merge both results into dst
			t1, err1 := createTDigest(src)
			if err1 != nil {
				return err1
			}
			t2, err2 := createTDigest(dst)
			if err2 != nil {
				return err2
			}
			t1.Merge(t2, 16)
			createDs(t1, dst)
		}
		src.clear()
	}
	// else: source is empty: do nothing
	return nil
}

// calcPercentiles calculates approximate percentiles using the tDigest data
func calcPercentiles(data tDigestDS, p []float32) ([]float32, error) {
	t, err := createTDigest(data)
	if err != nil {
		return nil, err
	}
	return t.Percentiles(p), nil
}
