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

	"golang.org/x/exp/slices"
)

// This experimental arithmetic compression/decompression functionality is based on
// the work of Fabian Giesen, available here: https://github.com/rygorous/ryg_rans
// and kindly placed in the Public Domain per the CC0 licence:
// https://github.com/rygorous/ryg_rans/blob/master/LICENSE
//
// For theoretical background, please refer to Jaroslaw Duda's seminal paper on rANS:
// https://arxiv.org/pdf/1311.2540.pdf

type ANS32Encoder struct {
	state   [32]uint32
	bufFwd  []byte
	bufRev  []byte
	src     []byte
	stats   *ANSStatistics
	statbuf ANSStatistics
}

func (e *ANS32Encoder) init(src []byte, stats *ANSStatistics) {
	e.src = src
	e.bufFwd = slices.Grow(e.bufFwd[:0], entropyInitialBufferSize)
	e.bufRev = slices.Grow(e.bufRev[:0], entropyInitialBufferSize)
	for i := range e.state {
		e.state[i] = ansWordL
	}
	e.stats = stats
}

func (e *ANS32Encoder) put(chunk []byte) {
	avail := len(chunk)
	// the forward half
	for lane := 15; lane >= 0; lane-- {
		if lane < avail {
			q := e.stats.table[chunk[lane]]
			freq := q & ansStatisticsFrequencyMask
			start := (q >> ansStatisticsFrequencyBits) & ansStatisticsCumulativeFrequencyMask
			// renormalize
			x := e.state[lane]
			if x >= ((ansWordL>>ansWordMBits)<<ansWordLBits)*freq {
				e.bufFwd = binary.BigEndian.AppendUint16(e.bufFwd, uint16(x))
				x >>= ansWordLBits
			}
			// x = C(s,x)
			e.state[lane] = ((x / freq) << ansWordMBits) + (x % freq) + start
		}
	}
	// the reverse half
	for lane := 31; lane >= 16; lane-- {
		if lane < avail {
			q := e.stats.table[chunk[lane]]
			freq := q & ansStatisticsFrequencyMask
			start := (q >> ansStatisticsFrequencyBits) & ansStatisticsCumulativeFrequencyMask
			// renormalize
			x := e.state[lane]
			if x >= ((ansWordL>>ansWordMBits)<<ansWordLBits)*freq {
				e.bufRev = binary.LittleEndian.AppendUint16(e.bufRev, uint16(x))
				x >>= ansWordLBits
			}
			// x = C(s,x)
			e.state[lane] = ((x / freq) << ansWordMBits) + (x % freq) + start
		}
	}
}

func (e *ANS32Encoder) flush() {
	for lane := 15; lane >= 0; lane-- {
		e.bufFwd = binary.BigEndian.AppendUint32(e.bufFwd, e.state[lane])
	}
	for lane := 16; lane < 32; lane++ {
		e.bufRev = binary.LittleEndian.AppendUint32(e.bufRev, e.state[lane])
	}
}

func (e *ANS32Encoder) Encode(src []byte) ([]byte, error) {
	stats := &e.statbuf
	stats.observe(src)
	dst, err := e.EncodeExplicit(src, stats)
	if err != nil {
		return dst, err
	}
	return stats.Encode(dst), nil
}

func (e *ANS32Encoder) EncodeExplicit(src []byte, stats *ANSStatistics) ([]byte, error) {
	// Initialize the rANS encoder
	e.init(src, stats)
	ans32Compress(e)
	lenFwd := len(e.bufFwd)
	lenRev := len(e.bufRev)
	buf := slices.Grow(e.bufFwd, lenFwd+lenRev+ansDenseTableMaxLength)
	// In-place inversion of bufFwd
	for i, j := 0, lenFwd-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	buf = append(buf, e.bufRev...)
	return buf, nil
}

func ans32CompressReference(enc *ANS32Encoder) {
	srcLen := len(enc.src)
	srcLast := srcLen % 32
	k := srcLen - srcLast

	// Process the last chunk first
	enc.put(enc.src[k : k+srcLast])

	// Process the remaining chunks
	for k -= 32; k >= 0; k -= 32 {
		enc.put(enc.src[k : k+32])
	}
	enc.flush()
	enc.src = enc.src[:0]
}

func ANS32Decode(src []byte, dstLen int) ([]byte, error) {
	r, ec := ans32Decode(src, dstLen)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ANS32DecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, error) {
	r, ec := ans32DecodeExplicit(src, tab, dstLen, dst)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ans32Decode(src []byte, dstLen int) ([]byte, errorCode) {
	dst := make([]byte, 0, dstLen)
	var tab ANSDenseTable
	data, ec := ansDecodeTable(&tab, src)
	if ec != ecOK {
		return nil, ec
	}
	return ans32DecodeExplicit(data, &tab, dstLen, dst)
}

func ans32DecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, errorCode) {
	r, ec := ans32Decompress(dst, dstLen, src, tab)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ans32DecompressReference(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) {
	var state [32]uint32
	cursorFwd := 64
	cursorRev := len(src) - 64

	for lane := 0; lane < 16; lane++ {
		state[lane] = binary.LittleEndian.Uint32(src[lane*4:])
		state[lane+16] = binary.LittleEndian.Uint32(src[lane*4+cursorRev:])
	}

	cursorDst := 0

Outer:
	for {
		for lane := 0; lane < 32; lane++ {
			x := state[lane]
			slot := x & (ansWordM - 1)
			t := tab[slot]
			freq := uint32(t & (ansWordM - 1))
			bias := uint32((t >> ansWordMBits) & (ansWordM - 1))
			// s, x = D(x)
			state[lane] = freq*(x>>ansWordMBits) + bias
			s := byte(t >> 24)
			if cursorDst < dstLen {
				dst = append(dst, s)
				cursorDst++
			} else {
				break Outer
			}
		}
		// Normalize the forward part
		for lane := 0; lane < 16; lane++ {
			if x := state[lane]; x < ansWordL {
				v := binary.LittleEndian.Uint16(src[cursorFwd:])
				cursorFwd += 2
				state[lane] = (x << ansWordLBits) | uint32(v)
			}
		}
		// Normalize the reverse part
		for lane := 16; lane < 32; lane++ {
			if x := state[lane]; x < ansWordL {
				v := binary.LittleEndian.Uint16(src[cursorRev-2:])
				cursorRev -= 2
				state[lane] = (x << ansWordLBits) | uint32(v)
			}
		}
	}
	return dst, ecOK
}

func init() {
	if ansWordMBits > 12 {
		panic("the value of ansWordMBits must not exceed 12")
	}
}

var ans32Compress func(enc *ANS32Encoder) = ans32CompressReference
var ans32Decompress func(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) = ans32DecompressReference
