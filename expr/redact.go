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
	"encoding/base32"
	"encoding/binary"
	"math"

	"github.com/dchest/siphash"
)

const (
	k0, k1 = 0, 1
)

func redactBuf(buf []byte) uint64 {
	return siphash.Hash(k0, k1, buf)
}

func redactInt(i int64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i))
	res := redactBuf(buf[:])
	return int64(res)
}

func redactFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// don't redact these; couldn't possibly be secret...
		return f
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
	res := redactBuf(buf[:])
	// map onto the range [0, 1)
	return float64(int64(res)) / float64(1<<63)
}

func redactString(s string) string {
	res := redactBuf([]byte(s))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], res)
	return base32.StdEncoding.EncodeToString(buf[:])
}
