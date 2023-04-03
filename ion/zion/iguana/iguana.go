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
