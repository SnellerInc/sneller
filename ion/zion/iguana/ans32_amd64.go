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

package iguana

import (
	"golang.org/x/exp/slices"
	"golang.org/x/sys/cpu"
)

// In case of buffer exhaustion grow the buffer capacity by 3/2=1.5 times
const (
	ansBufferGrowthNumerator   = 3
	ansBufferGrowthDenominator = 2
)

func init() {
	if growthFactor := float64(ansBufferGrowthNumerator) / float64(ansBufferGrowthDenominator); growthFactor <= 1.0 {
		panic("The growth factor must be strictly greater than 1")
	}

	if cpu.X86.HasAVX512 {
		ansCompress = ansCompressAVX512Generic
		ansDecompress = ansDecompressAVX512Generic
		ansDecodeTable = ansDecodeTableAVX512Generic
	}

	if cpu.X86.HasAVX512VBMI {
		ansDecompress = ansDecompressAVX512VBMI
	}
}

type ansCoreFlags uint32

const (
	ansCoreFlagExpandForward ansCoreFlags = 1 << iota
	ansCoreFlagExpandReverse
)

func ansCompressAVX512Generic(enc *ANSEncoder) {
	for {
		if r := ansCompressCoreAVX512Generic(enc); r == 0 {
			return
		} else {
			// At least one of the buffers has insufficient capacity
			if r&ansCoreFlagExpandForward != 0 {
				enc.bufFwd = slices.Grow(enc.bufFwd, (len(enc.bufFwd)*ansBufferGrowthNumerator)/ansBufferGrowthDenominator)
			}
			if r&ansCoreFlagExpandReverse != 0 {
				enc.bufRev = slices.Grow(enc.bufRev, (len(enc.bufRev)*ansBufferGrowthNumerator)/ansBufferGrowthDenominator)
			}
		}
	}
}

//go:noescape
func ansDecompressAVX512VBMI(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode)

//go:noescape
func ansDecompressAVX512Generic(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode)

//go:noescape
func ansDecodeTableAVX512Generic(tab *ANSDenseTable, src []byte) ([]byte, errorCode)

//go:noescape
func ansCompressCoreAVX512Generic(enc *ANSEncoder) ansCoreFlags
