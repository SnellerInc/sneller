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

	ansCompress = ansCompressAVX512Generic
	ansDecompress = ansDecompressAVX512Generic
	ansDecodeTable = ansDecodeTableAVX512Generic

	if cpu.X86.HasAVX512VBMI {
		ansDecompress = ansDecompressAVX512VBMI
	}
}

type ansCoreFlags uint32

const (
	ansCoreFlagExpandForward ansCoreFlags = 1 << iota
	ansCoreFlagExpandReverse
)

func ansCompressAVX512Generic(enc *ansParallelEncoder) {
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
//go:nosplit
func ansDecompressAVX512VBMI(dst []byte, dstLen int, src []byte, tab *AnsDenseTable) ([]byte, errorCode)

//go:noescape
//go:nosplit
func ansDecompressAVX512Generic(dst []byte, dstLen int, src []byte, tab *AnsDenseTable) ([]byte, errorCode)

//go:noescape
//go:nosplit
func ansDecodeTableAVX512Generic(tab *AnsDenseTable, src []byte) ([]byte, errorCode)

//go:noescape
//go:nosplit
func ansCompressCoreAVX512Generic(enc *ansParallelEncoder) ansCoreFlags
