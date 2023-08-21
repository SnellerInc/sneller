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
	"slices"

	"github.com/SnellerInc/sneller/ints"
)

const (
	ansOptimizeStatistics = false
)

const (
	ansStatisticsFrequencyBits           = ansWordMBits
	ansStatisticsFrequencyMask           = (1 << ansStatisticsFrequencyBits) - 1
	ansStatisticsCumulativeFrequencyBits = ansWordMBits
	ansStatisticsCumulativeFrequencyMask = (1 << ansStatisticsCumulativeFrequencyBits) - 1
)

type ansRawStatistics struct {
	freqs          [256]uint32
	cumFreqs       [256 + 1]uint32
	partialContent []ansStatisticsEntry
}

type ANSStatistics struct {
	table          [256]uint32
	ctrl, data     ansBitStream // re-used for serialization
	partialContent []ansStatisticsEntry
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
	s.partialContent = raw.partialContent
}

func (s *ansRawStatistics) optimize() {
	histo := [256]ansStatisticsEntry{}
	for i, f := range s.freqs {
		histo[i] = ansStatisticsEntry{freq: uint16(f), idx: uint8(i)}
	}
	slices.SortFunc(histo[:], func(a, b ansStatisticsEntry) int { return int(b.freq) - int(a.freq) })

	cum := uint32(0)
	nEntries := 0
	for i, v := range histo {
		cum += uint32(v.freq)
		if cum >= (ansWordM*80)/100 {
			nEntries = i + 1
			break
		}
	}

	if nEntries > 6 { // Too many entries to benefit from compression
		return
	}

	rest := uint32(len(histo) - nEntries)
	total := ansWordM - rest

	// Re-scale to the cumulative value of ansWordM
	scaledTotal := rest
	for i := 0; i < nEntries; i++ {
		f := (uint32(histo[i].freq) * total) / cum
		scaledTotal += f
		histo[i].freq = uint16(f)
	}

	for i := 0; scaledTotal < ansWordM; scaledTotal++ {
		histo[i].freq++
		i = (i + 1) % nEntries
	}

	for i := range s.freqs {
		s.freqs[i] = 1 // Minimal encodable non-zero probability
	}

	for i := 0; i < nEntries; i++ {
		s.freqs[histo[i].idx] = uint32(histo[i].freq)
	}

	s.partialContent = histo[:nEntries]
	s.calcCumFreqs()
}

const (
	ansCtrlBlockSize        = 96
	ansNibbleBlockMaxLength = 384 // 256 3-nibble groups
	ansDenseTableMaxLength  = ansCtrlBlockSize + ansNibbleBlockMaxLength
)

type ANSDenseTable [ansWordM]uint32

// Encode appends the serialized representation of
// the statistics s to the buffer dst and returns
// the extended buffer.

type ansStatisticsEntry struct {
	freq uint16
	idx  uint8
}

func (s *ANSStatistics) encodeVarNibble(v uint32) {
	for {
		k := v & 0b0111
		v >>= 3
		if v == 0 {
			s.data.add(k|0b1000, 4)
			break
		} else {
			s.data.add(k, 4)
		}
	}
}

func ansFetchVarNibble(src []byte, nibidx int) (uint32, int, errorCode) {
	var r uint32
	for i := 0; ; i++ {
		var x uint32
		var ec errorCode
		x, nibidx, ec = ansFetchNibble(src, nibidx)
		if ec != ecOK {
			return 0, 0, ec
		}

		r |= ((x & 0b0111) << (i * 3))
		if (x & 0b1000) != 0 {
			return r, nibidx, ecOK
		}
	}
}

func (s *ANSStatistics) Encode(dst []byte) []byte {
	compressionLevel := len(s.partialContent)
	if compressionLevel != 0 {
		dst = s.EncodePartial(dst)
	} else {
		dst = s.EncodeFull(dst)
	}
	return append(dst, byte(compressionLevel))
}

// EncodeFull appends the full serialized representation of
// the statistics s to the buffer dst and returns the extended buffer.
func (s *ANSStatistics) EncodeFull(dst []byte) []byte {
	s.ctrl.reset()
	s.data.reset()

	for i := 0; i < 256; i++ {
		f := s.table[i] & ansStatisticsFrequencyMask

		// 000 => 0
		// 001 => 1
		// 010 => 2
		// 011 => 3
		// 100 => 4
		// 101 => one nibble f - 5
		// 110 => two nibbles f - 21
		// 111 => three nibbles f - 277

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

// EncodePartial appends a partial serialized representation of
// the statistics s to the buffer dst and returns the extended buffer.
func (s *ANSStatistics) EncodePartial(dst []byte) []byte {
	s.data.reset()
	for _, v := range s.partialContent {
		s.encodeVarNibble(uint32(v.freq))
		s.encodeVarNibble(uint32(v.idx))
	}
	s.data.flush()
	for i := len(s.data.buf) - 1; i >= 0; i-- {
		dst = append(dst, s.data.buf[i])
	}
	return dst
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
	if ansOptimizeStatistics {
		stats.optimize()
	}
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

func ansDecodeTable(tab *ANSDenseTable, src []byte) ([]byte, errorCode) {
	lenSrc := len(src)
	if lenSrc < 1 {
		return nil, ecOutOfInputData
	}

	compressionLevel := src[lenSrc-1]
	src = src[:lenSrc-1]

	if compressionLevel == 0x00 {
		return ansDecodeFullTable(tab, src)
	} else {
		return ansDecodePartialTable(tab, src, int(compressionLevel))
	}
}

func ansDecodePartialTable(tab *ANSDenseTable, src []byte, nEntries int) ([]byte, errorCode) {
	var freqs [256]uint32
	for i := range freqs {
		freqs[i] = 1
	}

	srcLen := len(src)
	nibidx := (srcLen-1)*2 + 1

	for i := 0; i < nEntries; i++ {
		var ec errorCode
		var f, idx uint32

		f, nibidx, ec = ansFetchVarNibble(src, nibidx)
		if ec != ecOK {
			return nil, ec
		}

		idx, nibidx, ec = ansFetchVarNibble(src, nibidx)
		if ec != ecOK {
			return nil, ec
		}
		freqs[idx] = f
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

func ansDecodeFullTableReference(tab *ANSDenseTable, src []byte) ([]byte, errorCode) {
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

// Decode deserializes the probability distribution table into *tab and returns
// the prefix that precedes the serialized data.
func (tab *ANSDenseTable) Decode(src []byte) ([]byte, error) {
	r, ec := ansDecodeTable(tab, src)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return r, nil
}

var ansDecodeFullTable func(tab *ANSDenseTable, src []byte) ([]byte, errorCode) = ansDecodeFullTableReference
