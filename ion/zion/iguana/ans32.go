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

	"github.com/SnellerInc/sneller/ints"
	"golang.org/x/exp/slices"
)

// This experimental arithmetic compression/decompression functionality is based on
// the work of Fabian Giesen, available here: https://github.com/rygorous/ryg_rans
// and kindly placed in the Public Domain per the CC0 licence:
// https://github.com/rygorous/ryg_rans/blob/master/LICENSE
//
// For theoretical background, please refer to Jaroslaw Duda's seminal paper on rANS:
// https://arxiv.org/pdf/1311.2540.pdf

const (
	ansWordLBits = 16
	ansWordL     = uint32(1) << ansWordLBits
	ansWordMBits = 12
	ansWordM     = uint32(1) << ansWordMBits
)

const (
	ansStatisticsFrequencyBits           = ansWordMBits
	ansStatisticsFrequencyMask           = (1 << ansStatisticsFrequencyBits) - 1
	ansStatisticsCumulativeFrequencyBits = ansWordMBits
	ansStatisticsCumulativeFrequencyMask = (1 << ansStatisticsCumulativeFrequencyBits) - 1
)

const ansInitialBufferSize = 256 * 1024

type ansRawStatistics struct {
	freqs    [256]uint32
	cumFreqs [256 + 1]uint32
}

type ANSStatistics struct {
	table      [256]uint32
	ctrl, data ansBitStream // re-used for serialization
}

func (s *ansRawStatistics) normalizeFreqs() {
	s.calcCumFreqs()
	targetTotal := uint32(ansWordM)
	curTotal := s.cumFreqs[256] // TODO: prefix sum

	// resample distribution based on cumulative freqs
	for i := 1; i <= 256; i++ {
		s.cumFreqs[i] = uint32((uint64(targetTotal) * uint64(s.cumFreqs[i])) / uint64(curTotal))
	}

	// if we nuked any non-0 frequency symbol to 0, we need to steal
	// the range to make the frequency nonzero from elsewhere.
	//
	// this is not at all optimal, i'm just doing the first thing that comes to mind.
	for i := 0; i < 256; i++ {
		if (s.freqs[i] != 0) && (s.cumFreqs[i+1] == s.cumFreqs[i]) {
			// symbol i was set to zero freq

			// find best symbol to steal frequency from (try to steal from low-freq ones)
			bestFreq := ^uint32(0)
			bestSteal := -1
			for j := 0; j < 256; j++ {
				freq := s.cumFreqs[j+1] - s.cumFreqs[j]
				if (freq > 1) && (freq < bestFreq) {
					bestFreq = freq
					bestSteal = j
				}
			}

			// and steal from it!
			if bestSteal < i {
				for j := bestSteal + 1; j <= i; j++ {
					s.cumFreqs[j]--
				}
			} else {
				for j := i + 1; j <= bestSteal; j++ {
					s.cumFreqs[j]++
				}
			}
		}
	}

	// calculate updated freqs and make sure we didn't screw anything up
	for i := 0; i < 256; i++ {
		// calc updated freq
		s.freqs[i] = s.cumFreqs[i+1] - s.cumFreqs[i]
	}
}

func (s *ansRawStatistics) calcCumFreqs() {
	// TODO: another prefix sum
	for i := 0; i < 256; i++ {
		s.cumFreqs[i+1] = s.cumFreqs[i] + s.freqs[i]
	}
}

// Genuine Sneller code starts here

func (s *ANSStatistics) set(raw *ansRawStatistics) {
	for i := 0; i < 256; i++ {
		s.table[i] = (raw.cumFreqs[i] << ansStatisticsCumulativeFrequencyBits) | raw.freqs[i]
	}
}

const (
	ansCtrlBlockSize        = 96
	ansNibbleBlockMaxLength = 384 // 256 3-nibble groups
	ansDenseTableMaxLength  = ansCtrlBlockSize + ansNibbleBlockMaxLength
)

type ANSDenseTable [ansWordM]uint32

type ANSEncoder struct {
	state   [32]uint32
	bufFwd  []byte
	bufRev  []byte
	src     []byte
	stats   *ANSStatistics
	statbuf ANSStatistics
}

func (e *ANSEncoder) init(src []byte, stats *ANSStatistics) {
	e.src = src
	e.bufFwd = slices.Grow(e.bufFwd[:0], ansInitialBufferSize)
	e.bufRev = slices.Grow(e.bufRev[:0], ansInitialBufferSize)
	for i := range e.state {
		e.state[i] = ansWordL
	}
	e.stats = stats
}

func (e *ANSEncoder) put(chunk []byte) {
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

func (e *ANSEncoder) flush() {
	for lane := 15; lane >= 0; lane-- {
		e.bufFwd = binary.BigEndian.AppendUint32(e.bufFwd, e.state[lane])
	}
	for lane := 16; lane < 32; lane++ {
		e.bufRev = binary.LittleEndian.AppendUint32(e.bufRev, e.state[lane])
	}
}

func (e *ANSEncoder) Encode(src []byte) ([]byte, error) {
	stats := &e.statbuf
	stats.observe(src)
	dst, err := e.EncodeExplicit(src, stats)
	if err != nil {
		return dst, err
	}
	return stats.Encode(dst), nil
}

func (e *ANSEncoder) EncodeExplicit(src []byte, stats *ANSStatistics) ([]byte, error) {
	// Initialize the rANS encoder
	e.init(src, stats)
	ansCompress(e)
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

func ansCompressReference(enc *ANSEncoder) {
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

func ANSDecode(src []byte, dstLen int) ([]byte, error) {
	r, ec := ansDecode(src, dstLen)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ANSDecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, error) {
	r, ec := ansDecodeExplicit(src, tab, dstLen, dst)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ansDecode(src []byte, dstLen int) ([]byte, errorCode) {
	dst := make([]byte, 0, dstLen)
	var tab ANSDenseTable
	data, ec := tab.decode(src)
	if ec != ecOK {
		return nil, ec
	}
	return ansDecodeExplicit(data, &tab, dstLen, dst)
}

func ansDecodeExplicit(src []byte, tab *ANSDenseTable, dstLen int, dst []byte) ([]byte, errorCode) {
	r, ec := ansDecompress(dst, dstLen, src, tab)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ansDecompressReference(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) {
	var state [32]uint32
	cursorFwd := 64
	cursorRev := len(src) - 64

	for lane := 0; lane < 16; lane++ {
		state[lane] = (uint32(src[lane*4+3]) << 24) | (uint32(src[lane*4+2]) << 16) | (uint32(src[lane*4+1]) << 8) | uint32(src[lane*4])
		state[lane+16] = (uint32(src[lane*4+3+cursorRev]) << 24) | (uint32(src[lane*4+2+cursorRev]) << 16) | (uint32(src[lane*4+1+cursorRev]) << 8) | uint32(src[lane*4+cursorRev])
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
				v := uint16(src[cursorFwd]) | (uint16(src[cursorFwd+1]) << 8)
				cursorFwd += 2
				state[lane] = (x << ansWordLBits) | uint32(v)
			}
		}
		// Normalize the reverse part
		for lane := 16; lane < 32; lane++ {
			if x := state[lane]; x < ansWordL {
				v := uint16(src[cursorRev-2]) | (uint16(src[cursorRev-1]) << 8)
				cursorRev -= 2
				state[lane] = (x << ansWordLBits) | uint32(v)
			}
		}
	}
	return dst, ecOK
}

// Encode appends the serialized representation of
// the statistics s to the buffer dst and returns
// the extended buffer.
func (s *ANSStatistics) Encode(dst []byte) []byte {
	s.ctrl.reset()
	s.data.reset()

	// 000 => 0
	// 001 => 1
	// 010 => 2
	// 011 => 3
	// 100 => 4
	// 101 => one nibble f - 5
	// 110 => two nibbles f - 21
	// 111 => three nibbles f - 277

	for i := 0; i < 256; i++ {
		f := s.table[i] & ansStatisticsFrequencyMask
		if f < 5 {
			s.ctrl.add(f, 3)
		} else if f < 21 {
			s.ctrl.add(0b101, 3)
			s.data.add(f-5, 4)
		} else if f < 277 {
			s.ctrl.add(0b110, 3)
			s.data.add(f-21, 8)
		} else {
			s.ctrl.add(0b111, 3)
			s.data.add(f-277, 12)
		}
	}

	s.ctrl.flush()
	s.data.flush()

	lenCtrl := len(s.ctrl.buf)
	lenData := len(s.data.buf)

	base := len(dst)
	res := slices.Grow(dst, lenData+lenCtrl)
	res = res[:base+lenData+lenCtrl]

	for i := range s.data.buf {
		res[base+lenData-i-1] = s.data.buf[i]
	}
	copy(res[base+lenData:], s.ctrl.buf)
	return res
}

// NewANSStatistics computes an ANS frequency table
// on the buffer src.
func NewANSStatistics(src []byte) *ANSStatistics {
	stats := &ANSStatistics{}
	stats.observe(src)
	return stats
}

func (s *ANSStatistics) observe(src []byte) {
	stats := &ansRawStatistics{}
	srcLen := len(src)

	if srcLen == 0 {
		// Edge case #1: empty input. Arbitrarily assign probability 1/2 to the last two symbols
		stats.freqs[254] = ansWordM / 2
		stats.freqs[255] = ansWordM / 2
		stats.cumFreqs[255] = ansWordM / 2
		stats.cumFreqs[256] = ansWordM
		s.set(stats)
		return
	}

	nonZeroFreqIdx := ansHistogram(&stats.freqs, src)
	if stats.freqs[nonZeroFreqIdx] == uint32(srcLen) {
		// Edge case #2: repetition of a single character.
		//
		// The ANS normalized cumulative frequencies by definition must sum up to a power of 2 (=ansWordM)
		// to allow for fast division/modulo operations implemented with shift/and. The problem is that
		// the sum is exactly a power of 2: it requires N+1 bits to encode the leading one and the N zeros
		// that follow. This can be fixed by introducing an artificial symbol to the alphabet (a byte with
		// the value of 256) and assiging it the lowest possible normalized probability of occurrence (=1/ansWordM).
		// It will then be re-scaled to the value of 1, making the preceeding cumulative frequencies sum up
		// precisely to ansWordM-1, which can be encoded using N bits. Since the symbol is not encodable
		// in 8 bits, it cannot occur in the input data block, and so will never be mistakenly decoded as well.
		// This tricks saves 2 bits that can be reused for other purposes with a negligible impact on the
		// compression ratio. On top of that, it solves the repeated single character degenerate input case,
		// as no symbol can have the probability of ocurrence equal to 1 -- it will be (ansWordM-1)/ansWordM
		// in the worst case.

		stats.freqs[nonZeroFreqIdx] = ansWordM - 1
		for i := nonZeroFreqIdx + 1; i < 257; i++ {
			stats.cumFreqs[i] = ansWordM - 1
		}
		s.set(stats)
		return
	}

	stats.normalizeFreqs()
	s.set(stats)
}

func ansHistogram(freqs *[256]uint32, src []byte) int {
	// 4-way histogram calculation to compensate for the store-to-load forwarding issues observed here:
	// https://fastcompression.blogspot.com/2014/09/counting-bytes-fast-little-trick-from.html
	var histograms [4][256]uint32
	n := uint(len(src))
	e := ints.AlignDown(n, 4)
	for i := uint(0); i < e; i += 4 {
		histograms[0][src[i+0]]++
		histograms[1][src[i+1]]++
		histograms[2][src[i+2]]++
		histograms[3][src[i+3]]++
	}
	// Process the remainder
	for i := e; i < n; i++ {
		histograms[0][src[i]]++
	}
	// Add up all the ways
	for i := 0; i < 256; i++ {
		freqs[i] = histograms[0][i] + histograms[1][i] + histograms[2][i] + histograms[3][i]
	}

	// Find the index of some non-zero freq
	for i := 0; i < 256; i++ {
		if freqs[i] != 0 {
			return i
		}
	}
	return -1 // Unreachable, len(src) > 0 must be ensured by the caller
}

type ansBitStream struct {
	acc uint64
	cnt int
	buf []byte
}

func (s *ansBitStream) reset() {
	s.acc = 0
	s.cnt = 0
	s.buf = s.buf[:0]
}

func (s *ansBitStream) add(v uint32, k uint32) {
	m := ^(^uint32(0) << k)
	s.acc |= uint64(v&m) << s.cnt
	s.cnt += int(k)
	for s.cnt >= 8 {
		s.buf = append(s.buf, byte(s.acc))
		s.acc >>= 8
		s.cnt -= 8
	}
}

func (s *ansBitStream) flush() {
	for s.cnt > 0 {
		s.buf = append(s.buf, byte(s.acc))
		s.acc >>= 8
		s.cnt -= 8
	}
}

func ansFetchNibble(src []byte, idx int) (uint32, int, errorCode) {
	if idx < 0 {
		return 0, idx, ecOutOfInputData
	}
	x := src[idx>>1]
	if (idx & 1) == 1 {
		return uint32(x & 0x0f), idx - 1, ecOK
	} else {
		return uint32(x >> 4), idx - 1, ecOK
	}
}

// Decode deserializes the probability distribution table into *tab and returns
// the prefix that precedes the serialized data.
func (tab *ANSDenseTable) Decode(src []byte) ([]byte, error) {
	r, ec := ansDecodeTable(tab, src)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func (tab *ANSDenseTable) decode(src []byte) ([]byte, errorCode) {
	r, ec := ansDecodeTable(tab, src)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ansDecodeTableReference(tab *ANSDenseTable, src []byte) ([]byte, errorCode) {
	// The code part is encoded as 256 3-bit values, making it 96 bytes in total.
	// Decoding it in groups of 24 bits is convenient: 8 words at a time.
	srcLen := len(src)
	if srcLen < ansCtrlBlockSize {
		return nil, ecWrongSourceSize
	}
	ctrl := src[srcLen-ansCtrlBlockSize:]
	var freqs [256]uint32
	nibidx := (srcLen-ansCtrlBlockSize-1)*2 + 1
	k := 0
	for i := 0; i < ansCtrlBlockSize; i += 3 {
		x := uint32(ctrl[i]) | uint32(ctrl[i+1])<<8 | uint32(ctrl[i+2])<<16
		// Eight 3-bit control words fit within a single 24-bit chunk
		for j := 0; j != 8; j++ {
			v := x & 0x07
			x >>= 3
			var ec errorCode
			switch v {
			case 0b111:
				// Three nibbles f - 277
				var x0, x1, x2 uint32
				x0, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				x1, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				x2, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				freqs[k] = (x0 | (x1 << 4) | (x2 << 8)) + 277
			case 0b110:
				// Two nibbles f - 21
				var x0, x1 uint32
				x0, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				x1, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				freqs[k] = (x0 | (x1 << 4)) + 21
			case 0b101:
				// One nibble f - 5
				var x0 uint32
				x0, nibidx, ec = ansFetchNibble(src, nibidx)
				if ec != ecOK {
					return nil, ec
				}
				freqs[k] = x0 + 5
			default:
				// Explicit encoding of a short value
				freqs[k] = v
			}
			k++
		}
	}

	// The normalized frequencies have been recovered. Fill the dense table accordingly.
	start := uint32(0)
	for sym, freq := range freqs {
		for i := uint32(0); i < freq; i++ {
			slot := start + i
			tab[slot] = (uint32(sym) << 24) | (i << ansWordMBits) | freq
		}
		start += freq
	}
	return src[:(nibidx+1)>>1], ecOK
}

func init() {
	if ansWordMBits > 12 {
		panic("the value of ansWordMBits must not exceed 12")
	}
}

var ansCompress func(enc *ANSEncoder) = ansCompressReference
var ansDecompress func(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode) = ansDecompressReference
var ansDecodeTable func(tab *ANSDenseTable, src []byte) ([]byte, errorCode) = ansDecodeTableReference
