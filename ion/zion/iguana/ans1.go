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
	"encoding/binary"
	"slices"
)

// This experimental arithmetic compression/decompression functionality is based on
// the work of Fabian Giesen, available here: https://github.com/rygorous/ryg_rans
// and kindly placed in the Public Domain per the CC0 licence:
// https://github.com/rygorous/ryg_rans/blob/master/LICENSE
//
// For theoretical background, please refer to Jaroslaw Duda's seminal paper on rANS:
// https://arxiv.org/pdf/1311.2540.pdf

type ANS1Encoder struct {
	state   uint32
	buf     []byte
	src     []byte
	stats   *ANSStatistics
	statbuf ANSStatistics
}

func (e *ANS1Encoder) init(src []byte, stats *ANSStatistics) {
	e.src = src
	e.buf = slices.Grow(e.buf[:0], entropyInitialBufferSize)
	e.state = ansWordL
	e.stats = stats
}

func (e *ANS1Encoder) put(v byte) {
	q := e.stats.table[v]
	freq := q & ansStatisticsFrequencyMask
	start := (q >> ansStatisticsFrequencyBits) & ansStatisticsCumulativeFrequencyMask
	// renormalize
	x := e.state
	if x >= ((ansWordL>>ansWordMBits)<<ansWordLBits)*freq {
		e.buf = binary.LittleEndian.AppendUint16(e.buf, uint16(x))
		x >>= ansWordLBits
	}
	// x = C(s,x)
	e.state = ((x / freq) << ansWordMBits) + (x % freq) + start
}

func (e *ANS1Encoder) flush() {
	e.buf = binary.LittleEndian.AppendUint32(e.buf, e.state)
}

func (e *ANS1Encoder) Encode(src []byte) ([]byte, error) {
	stats := &e.statbuf
	stats.observe(src)
	dst, err := e.EncodeExplicit(src, stats)
	if err != nil {
		return dst, err
	}
	return stats.Encode(dst), nil
}

func (e *ANS1Encoder) EncodeExplicit(src []byte, stats *ANSStatistics) ([]byte, error) {
	// Initialize the rANS encoder
	e.init(src, stats)
	ans1Compress(e)
	lenBuf := len(e.buf)
	buf := slices.Grow(e.buf, lenBuf+ansDenseTableMaxLength)
	return buf, nil
}

func ans1CompressReference(enc *ANS1Encoder) {
	srcLen := len(enc.src)
	for i := srcLen - 1; i >= 0; i-- {
		enc.put(enc.src[i])
	}
	enc.flush()
	enc.src = enc.src[:0]
}

func ANS1Decode(src []byte, dstLen int) ([]byte, error) {
	r, ec := ans1Decode(src, dstLen)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ANS1DecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, error) {
	r, ec := ans1DecodeExplicit(src, tab, dstLen, dst)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ans1Decode(src []byte, dstLen int) ([]byte, errorCode) {
	dst := make([]byte, 0, dstLen)
	var tab ANSDenseTable
	data, ec := ansDecodeTable(&tab, src)
	if ec != ecOK {
		return nil, ec
	}
	return ans1DecodeExplicit(data, &tab, dstLen, dst)
}

func ans1DecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, errorCode) {
	r, ec := ans1Decompress(dst, dstLen, src, tab)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ans1DecompressReference(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) {
	lenSrc := len(src)
	if lenSrc < 4 {
		return nil, ecWrongSourceSize
	}
	cursorSrc := lenSrc - 4
	state := binary.LittleEndian.Uint32(src[cursorSrc:])
	cursorDst := 0

	for {
		x := state
		slot := x & (ansWordM - 1)
		t := tab[slot]
		freq := uint32(t & (ansWordM - 1))
		bias := uint32((t >> ansWordMBits) & (ansWordM - 1))
		// s, x = D(x)
		state = freq*(x>>ansWordMBits) + bias
		s := byte(t >> 24)
		if cursorDst < dstLen {
			dst = append(dst, s)
			cursorDst++
		} else {
			break
		}

		// Normalize state
		if x := state; x < ansWordL {
			v := binary.LittleEndian.Uint16(src[cursorSrc-2:])
			cursorSrc -= 2
			state = (x << ansWordLBits) | uint32(v)
		}
	}
	return dst, ecOK
}

func init() {
	if ansWordMBits > 12 {
		panic("the value of ansWordMBits must not exceed 12")
	}
}

var ans1Compress func(enc *ANS1Encoder) = ans1CompressReference
var ans1Decompress func(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) = ans1DecompressReference
