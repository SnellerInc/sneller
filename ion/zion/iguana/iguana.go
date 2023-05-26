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

type EncodingMode byte

const (
	EncodingRaw       EncodingMode = iota
	EncodingIguana                 = 1 << 0
	EncodingANS                    = 1 << 1
	EncodingIguanaANS              = EncodingIguana | EncodingANS
)

const (
	DefaultANSThreshold = 1.0
)

type EncodingRequest struct {
	Src                   []byte
	Mode                  EncodingMode
	ANSRejectionThreshold float32
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
