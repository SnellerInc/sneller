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

// Package iguana implements a Lizard-derived compression/decompression pipeline
package iguana

// EncodingMode specifies what structural compression should be applied to the compressor's input.
type EncodingMode byte

const (
	EncodingRaw    EncodingMode = iota   // No structural compression is applied
	EncodingIguana              = 1 << 0 // Iguana structural compression is applied
)

// EntropyMode specifies what entropy compression should be applied to the results of the selected structural compressor.
type EntropyMode byte

const (
	EntropyNone      EntropyMode = iota // No entropy compression is applied
	EntropyANS32                        // Vectorized, 32-way interleaved 8-bit rANS entropy compression should be applied
	EntropyANS1                         // Scalar, one-way 8-bit rANS entropy compression should be applied
	EntropyANSNibble                    // Scalar, one-way 4-bit rANS entropy compression should be applied
)

const (
	DefaultEntropyRejectionThreshold = 1.0
	entropyInitialBufferSize         = 256 * 1024
)

type EncodingRequest struct {
	Src                       []byte
	EncMode                   EncodingMode
	EntMode                   EntropyMode
	EntropyRejectionThreshold float32
	EnableSecondaryResolver   bool
}

// header byte
const ( //lint:ignore U1000 symbolic names for flags currently handled through bitops
	streamCompressedTokens      byte = 1 << stridTokens
	streamCompressedOffset16         = 1 << stridOffset16
	streamCompressedOffset24         = 1 << stridOffset24
	streamCompressedVarLitLen        = 1 << stridVarLitLen
	streamCompressedVarMatchLen      = 1 << stridVarMatchLen
	streamCompressedLiterals         = 1 << stridLiterals
)

const (
	matchLenBits     = 4
	literalLenBits   = 3
	mmLongOffsets    = 16
	initLastOffset   = 0
	maxShortLitLen   = 7
	maxShortMatchLen = 15
	lastLongOffset   = 31
)
