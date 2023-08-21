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

	"github.com/SnellerInc/sneller/ints"
)

// For theoretical background, please refer to Jaroslaw Duda's seminal paper on rANS:
// https://arxiv.org/pdf/1311.2540.pdf

const (
	ansNibbleWordLBits = 16
	ansNibbleWordL     = uint32(1) << ansNibbleWordLBits
	ansNibbleWordMBits = 12
	ansNibbleWordM     = uint32(1) << ansNibbleWordMBits
)

const (
	ansNibbleStatisticsFrequencyBits           = ansNibbleWordMBits
	ansNibbleStatisticsFrequencyMask           = (1 << ansNibbleStatisticsFrequencyBits) - 1
	ansNibbleStatisticsCumulativeFrequencyBits = ansNibbleWordMBits
	ansNibbleStatisticsCumulativeFrequencyMask = (1 << ansNibbleStatisticsCumulativeFrequencyBits) - 1
)

type ansNibbleRawStatistics struct {
	freqs    [16]uint32
	cumFreqs [16 + 1]uint32
}

type ANSNibbleStatistics struct {
	table      [16]uint32
	ctrl, data ansNibbleBitStream // re-used for serialization
}

func (s *ansNibbleRawStatistics) normalizeFreqs() {
	s.calcCumFreqs()
	targetTotal := uint32(ansNibbleWordM)
	curTotal := s.cumFreqs[16] // TODO: prefix sum

	// resample distribution based on cumulative freqs
	for i := 1; i <= 16; i++ {
		s.cumFreqs[i] = uint32((uint64(targetTotal) * uint64(s.cumFreqs[i])) / uint64(curTotal))
	}

	// if we nuked any non-0 frequency symbol to 0, we need to steal
	// the range to make the frequency nonzero from elsewhere.
	//
	// this is not at all optimal, i'm just doing the first thing that comes to mind.
	for i := 0; i < 16; i++ {
		if (s.freqs[i] != 0) && (s.cumFreqs[i+1] == s.cumFreqs[i]) {
			// symbol i was set to zero freq

			// find best symbol to steal frequency from (try to steal from low-freq ones)
			bestFreq := ^uint32(0)
			bestSteal := -1
			for j := 0; j < 16; j++ {
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
	for i := 0; i < 16; i++ {
		// calc updated freq
		s.freqs[i] = s.cumFreqs[i+1] - s.cumFreqs[i]
	}
}

func (s *ansNibbleRawStatistics) calcCumFreqs() {
	// TODO: another prefix sum
	for i := 0; i < 16; i++ {
		s.cumFreqs[i+1] = s.cumFreqs[i] + s.freqs[i]
	}
}

func (s *ANSNibbleStatistics) set(raw *ansNibbleRawStatistics) {
	for i := 0; i < 16; i++ {
		s.table[i] = (raw.cumFreqs[i] << ansNibbleStatisticsCumulativeFrequencyBits) | raw.freqs[i]
	}
}

const (
	ansNibbleCtrlBlockSize        = 6
	ansNibbleNibbleBlockMaxLength = 24 // 16 3-nibble groups
	ansNibbleDenseTableMaxLength  = ansNibbleCtrlBlockSize + ansNibbleNibbleBlockMaxLength
)

type ANSNibbleDenseTable [ansNibbleWordM]uint32

type ANSNibbleEncoder struct {
	state   uint32
	bufFwd  []byte
	src     []byte
	stats   *ANSNibbleStatistics
	statbuf ANSNibbleStatistics
}

func (e *ANSNibbleEncoder) init(src []byte, stats *ANSNibbleStatistics) {
	e.src = src
	e.bufFwd = slices.Grow(e.bufFwd[:0], entropyInitialBufferSize)
	e.state = ansNibbleWordL
	e.stats = stats
}

func (e *ANSNibbleEncoder) putNibble(v byte) {
	q := e.stats.table[v]
	freq := q & ansNibbleStatisticsFrequencyMask
	start := (q >> ansNibbleStatisticsFrequencyBits) & ansNibbleStatisticsCumulativeFrequencyMask
	// renormalize
	x := e.state
	if x >= ((ansNibbleWordL>>ansNibbleWordMBits)<<ansNibbleWordLBits)*freq {
		e.bufFwd = binary.LittleEndian.AppendUint16(e.bufFwd, uint16(x))
		x >>= ansNibbleWordLBits
	}
	// x = C(s,x)
	e.state = ((x / freq) << ansNibbleWordMBits) + (x % freq) + start
}

func (e *ANSNibbleEncoder) flush() {
	e.bufFwd = binary.LittleEndian.AppendUint32(e.bufFwd, e.state)
}

func (e *ANSNibbleEncoder) Encode(src []byte) ([]byte, error) {
	stats := &e.statbuf
	stats.observe(src)
	dst, err := e.EncodeExplicit(src, stats)
	if err != nil {
		return dst, err
	}
	return stats.Encode(dst), nil
}

func (e *ANSNibbleEncoder) EncodeExplicit(src []byte, stats *ANSNibbleStatistics) ([]byte, error) {
	// Initialize the rANS encoder
	e.init(src, stats)
	ansNibbleCompress(e)
	return e.bufFwd, nil
}

func ansNibbleCompress(enc *ANSNibbleEncoder) {
	for i := len(enc.src); i > 0; i-- {
		v := enc.src[i-1]
		enc.putNibble(v >> 4)
		enc.putNibble(v & 0x0f)
	}
	enc.flush()
	enc.src = enc.src[:0]
}

func ANSNibbleDecode(src []byte, dstLen int) ([]byte, error) {
	r, ec := ansNibbleDecode(src, dstLen)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ANSNibbleDecodeExplicit(src []byte, tab *ANSNibbleDenseTable, dstLen int, dst []byte) ([]byte, error) {
	r, ec := ansNibbleDecodeExplicit(src, tab, dstLen, dst)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func ansNibbleDecode(src []byte, dstLen int) ([]byte, errorCode) {
	dst := make([]byte, 0, dstLen)
	var tab ANSNibbleDenseTable
	data, ec := tab.decode(src)
	if ec != ecOK {
		return nil, ec
	}
	return ansNibbleDecodeExplicit(data, &tab, dstLen, dst)
}

func ansNibbleDecodeExplicit(src []byte, tab *ANSNibbleDenseTable, dstLen int, dst []byte) ([]byte, errorCode) {
	r, ec := ansNibbleDecompress(dst, dstLen, src, tab)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ansNibbleDecompress(dst []byte, dstLen int, src []byte, tab *ANSNibbleDenseTable) ([]byte, errorCode) {
	srcLen := len(src)
	if srcLen < 4 {
		return nil, ecWrongSourceSize
	}

	state := binary.LittleEndian.Uint32(src[srcLen-4:])
	cursorSrc := srcLen - 6
	cursorDst := 0

	for {
		var loNib, hiNib byte

		{
			x := state
			slot := x & (ansNibbleWordM - 1)
			t := tab[slot]
			freq := uint32(t & (ansNibbleWordM - 1))
			bias := uint32((t >> ansNibbleWordMBits) & (ansNibbleWordM - 1))
			// s, x = D(x)
			state = freq*(x>>ansNibbleWordMBits) + bias
			loNib = byte(t >> 24)

			// Normalize
			if x := state; x < ansNibbleWordL {
				v := binary.LittleEndian.Uint16(src[cursorSrc:])
				cursorSrc -= 2
				state = (x << ansNibbleWordLBits) | uint32(v)
			}
		}

		{
			x := state
			slot := x & (ansNibbleWordM - 1)
			t := tab[slot]
			freq := uint32(t & (ansNibbleWordM - 1))
			bias := uint32((t >> ansNibbleWordMBits) & (ansNibbleWordM - 1))
			// s, x = D(x)
			state = freq*(x>>ansNibbleWordMBits) + bias
			hiNib = byte(t >> 24)

			// Normalize
			if x := state; x < ansNibbleWordL {
				v := binary.LittleEndian.Uint16(src[cursorSrc:])
				cursorSrc -= 2
				state = (x << ansNibbleWordLBits) | uint32(v)
			}
		}

		nib := (hiNib << 4) | loNib
		dst = append(dst, nib)
		cursorDst++

		if cursorDst >= dstLen {
			break
		}
	}
	return dst, ecOK
}

// Encode appends the serialized representation of
// the statistics s to the buffer dst and returns
// the extended buffer.
func (s *ANSNibbleStatistics) Encode(dst []byte) []byte {
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

	for i := 0; i < 16; i++ {
		f := s.table[i] & ansNibbleStatisticsFrequencyMask
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

// NewANSNibbleStatistics computes an ANS frequency table
// on the buffer src.
func NewANSNibbleStatistics(src []byte) *ANSNibbleStatistics {
	stats := &ANSNibbleStatistics{}
	stats.observe(src)
	return stats
}

func (s *ANSNibbleStatistics) observe(src []byte) {
	stats := &ansNibbleRawStatistics{}
	srcLen := len(src)

	if srcLen == 0 {
		// Edge case #1: empty input. Arbitrarily assign probability 1/2 to the last two symbols
		stats.freqs[14] = ansNibbleWordM / 2
		stats.freqs[15] = ansNibbleWordM / 2
		stats.cumFreqs[15] = ansNibbleWordM / 2
		stats.cumFreqs[16] = ansNibbleWordM
		s.set(stats)
		return
	}

	nonZeroFreqIdx := ansNibbleHistogram(&stats.freqs, src)
	if stats.freqs[nonZeroFreqIdx] == uint32(2*srcLen) { // 2* due to working with nibbles
		// Edge case #2: repetition of a single character.
		//
		// The ANS normalized cumulative frequencies by definition must sum up to a power of 2 (=ansNibbleWordM)
		// to allow for fast division/modulo operations implemented with shift/and. The problem is that
		// the sum is exactly a power of 2: it requires N+1 bits to encode the leading one and the N zeros
		// that follow. This can be fixed by introducing an artificial symbol to the alphabet (a nibble with
		// the value of 16) and assiging it the lowest possible normalized probability of occurrence (=1/ansNibbleWordM).
		// It will then be re-scaled to the value of 1, making the preceeding cumulative frequencies sum up
		// precisely to ansNibbleWordM-1, which can be encoded using N bits. Since the symbol is not encodable
		// in 8 bits, it cannot occur in the input data block, and so will never be mistakenly decoded as well.
		// This tricks saves 2 bits that can be reused for other purposes with a negligible impact on the
		// compression ratio. On top of that, it solves the repeated single character degenerate input case,
		// as no symbol can have the probability of ocurrence equal to 1 -- it will be (ansNibbleWordM-1)/ansNibbleWordM
		// in the worst case.

		stats.freqs[nonZeroFreqIdx] = ansNibbleWordM - 1
		for i := nonZeroFreqIdx + 1; i < 17; i++ {
			stats.cumFreqs[i] = ansNibbleWordM - 1
		}
		s.set(stats)
		return
	}

	stats.normalizeFreqs()
	s.set(stats)
}

func ansNibbleHistogram(freqs *[16]uint32, src []byte) int {
	// 4-way histogram calculation to compensate for the store-to-load forwarding issues observed here:
	// https://fastcompression.blogspot.com/2014/09/counting-bytes-fast-little-trick-from.html
	var histograms [4][16]uint32
	n := uint(len(src))
	e := ints.AlignDown(n, 4)
	for i := uint(0); i < e; i += 4 {
		histograms[0][src[i+0]&0x0f]++
		histograms[1][src[i+0]>>4]++
		histograms[2][src[i+1]&0x0f]++
		histograms[3][src[i+1]>>4]++
		histograms[0][src[i+2]&0x0f]++
		histograms[1][src[i+2]>>4]++
		histograms[2][src[i+3]&0x0f]++
		histograms[3][src[i+3]>>4]++
	}
	// Process the remainder
	for i := e; i < n; i++ {
		histograms[0][src[i]&0x0f]++
		histograms[0][src[i]>>4]++
	}
	// Add up all the ways
	for i := 0; i < 16; i++ {
		freqs[i] = histograms[0][i] + histograms[1][i] + histograms[2][i] + histograms[3][i]
	}

	// Find the index of some non-zero freq
	for i := 0; i < 16; i++ {
		if freqs[i] != 0 {
			return i
		}
	}
	return -1 // Unreachable, len(src) > 0 must be ensured by the caller
}

type ansNibbleBitStream struct {
	acc uint64
	cnt int
	buf []byte
}

func (s *ansNibbleBitStream) reset() {
	s.acc = 0
	s.cnt = 0
	s.buf = s.buf[:0]
}

func (s *ansNibbleBitStream) add(v uint32, k uint32) {
	m := ^(^uint32(0) << k)
	s.acc |= uint64(v&m) << s.cnt
	s.cnt += int(k)
	for s.cnt >= 8 {
		s.buf = append(s.buf, byte(s.acc))
		s.acc >>= 8
		s.cnt -= 8
	}
}

func (s *ansNibbleBitStream) flush() {
	for s.cnt > 0 {
		s.buf = append(s.buf, byte(s.acc))
		s.acc >>= 8
		s.cnt -= 8
	}
}

// Decode deserializes the probability distribution table into *tab and returns
// the prefix that precedes the serialized data.
func (tab *ANSNibbleDenseTable) Decode(src []byte) ([]byte, error) {
	r, ec := ansNibbleDecodeTable(tab, src)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

func (tab *ANSNibbleDenseTable) decode(src []byte) ([]byte, errorCode) {
	r, ec := ansNibbleDecodeTable(tab, src)
	if ec != ecOK {
		return nil, ec
	}
	return r, ecOK
}

func ansNibbleDecodeTable(tab *ANSNibbleDenseTable, src []byte) ([]byte, errorCode) {
	// The code part is encoded as 16 3-bit values, making it 6 bytes in total.
	// Decoding it in groups of 24 bits is convenient: 8 words at a time.
	srcLen := len(src)
	if srcLen < ansNibbleCtrlBlockSize {
		return nil, ecWrongSourceSize
	}
	ctrl := src[srcLen-ansNibbleCtrlBlockSize:]

	var freqs [16]uint32
	nibidx := (srcLen-ansNibbleCtrlBlockSize-1)*2 + 1
	k := 0
	for i := 0; i < ansNibbleCtrlBlockSize; i += 3 {
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
			tab[slot] = (uint32(sym) << 24) | (i << ansNibbleWordMBits) | freq
		}
		start += freq
	}

	return src[:(nibidx+1)>>1], ecOK
}

func init() {
	if ansNibbleWordMBits > 12 {
		panic("the value of ansNibbleWordMBits must not exceed 12")
	}
}
