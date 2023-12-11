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

package zll

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/SnellerInc/sneller/ion"
	"golang.org/x/exp/slices"
	"golang.org/x/sys/cpu"
)

//go:noescape
func decodeNumericVecAVX512(dst []byte, src []byte, itemCount uint) (int, int)

//go:noescape
func decodeNumericVecAVX512VBMI(dst []byte, src []byte, itemCount uint) (int, int)

// encodeVarUInt encodes a varuint value and appends it to the destination
// buffer `dst`. This function is only used to encode smaller values that
// represent for example a length of an array as the size of the ION we
// encode is limited.
func encodeVarUInt(dst []byte, value uint) []byte {
	if value < (1 << 7) {
		return append(dst,
			byte(value)|0x80)
	}

	if value < (1 << 14) {
		return append(dst,
			byte((value>>7)&0x7F),
			byte(value&0xFF)|0x80)
	}

	if value < (1 << 21) {
		return append(dst,
			byte((value>>14)&0x7F),
			byte((value>>7)&0x7F),
			byte(value&0xFF)|0x80)
	}

	if value < (1 << 28) {
		return append(dst,
			byte((value>>21)&0x7F),
			byte((value>>14)&0x7F),
			byte((value>>7)&0x7F),
			byte(value&0xFF)|0x80)
	}

	panic("Number too large in encodeVarUInt")
}

func decodeVarUInt(src []byte) ([]byte, uint) {
	b := src[0]
	value := uint(b & 0x7F)

	i := int(1)
	for b < 128 {
		b = src[i]
		i++
		value = (value << 7) | uint(b&0x7F)
	}

	return src[i:], value
}

type encodingID uint8

// encoding identifiers
//
// NOTE: the `encodingIntX` was carefully assigned so it represents
// also a valid ION header (1 << 5) forms 0x20, and a sign bit at
// (1 << 4) would form either 0x20 or 0x30 for negative ints.
const (
	encodingInt0 encodingID = iota // encodes zeros - no payload, can be used to encode N zeros
	encodingIntX                   // encodes ints beyond int8 range - doesn't form a cluster
	encodingInt8                   // encodes a cluster of int8 values (but not zeros)
	encodingCF12                   // encodes a cluster of CF12 floats (12-bits per float)
	encodingCF16                   // encodes a cluster of CF16 floats (16-bits per float)
	encodingCF24                   // encodes a cluster of CF24 floats (24-bits per float)
	encodingCF32                   // encodes a cluster of CF32 floats (32-bits per float)
	encodingFP64                   // encodes a cluster of FP64 floats (stored as little endian)
)

// This can be tweaked to tune the compressor to trade more
// space for larger clusters, which are faster to decompress.
var clusterSizeHint = [...]int{
	encodingCF12: 4,
	encodingCF16: 6,
	encodingCF24: 8,
	encodingCF32: 8,
	encodingFP64: 1,
}

// Returns whether the `encoding` must be used to encode the value.
//
// In general floating point encodings can always use a more wasteful one, but integer encodings cannot.
func isIntegerEncoding(encoding encodingID) bool {
	return encoding <= encodingInt8
}

// CF12 encoding - floating point values encoded in 12-bits.
const (
	cf12ExponentBits = 1
	cf12MantissaBits = 12 - cf12ExponentBits
	cf12MantissaMin  = -(1 << (cf12MantissaBits - 1))
	cf12MantissaMax  = -cf12MantissaMin - 1
)

var cf12Exponents = [1 << cf12ExponentBits]float64{
	1e3,
	1e4,
}

func decode2xCF12(src []byte) (f1, f2 float64) {
	_ = src[:3]
	u := uint32(src[0]) | (uint32(src[1]) << 8) | (uint32(src[2]) << 16)

	u1 := u & 0xFFF
	u2 := u >> 12

	mant1 := int32(u1 & ((1 << cf12MantissaBits) - 1))
	mant2 := int32(u2 & ((1 << cf12MantissaBits) - 1))

	mant1 = (mant1 << (32 - cf12MantissaBits)) >> (32 - cf12MantissaBits)
	mant2 = (mant2 << (32 - cf12MantissaBits)) >> (32 - cf12MantissaBits)

	exp1 := int32(u1 >> cf12MantissaBits)
	exp2 := int32(u2 >> cf12MantissaBits)

	f1 = float64(mant1) / cf12Exponents[exp1]
	f2 = float64(mant2) / cf12Exponents[exp2]

	return
}

// CF16 encoding - floating point values encoded in 16-bits.
const (
	cf16ExponentBits = 1
	cf16MantissaBits = 16 - cf16ExponentBits
	cf16MantissaMin  = -(1 << (cf16MantissaBits - 1))
	cf16MantissaMax  = -cf16MantissaMin - 1
)

var cf16Exponents = [1 << cf16ExponentBits]float64{
	1e4,
	1e5,
}

func decode2xCF16(src []byte) (f1, f2 float64) {
	_ = src[:4]
	u1 := uint32(src[0]) | (uint32(src[1]) << 8)
	u2 := uint32(src[2]) | (uint32(src[3]) << 8)

	mant1 := int32(u1 & ((1 << cf16MantissaBits) - 1))
	mant2 := int32(u2 & ((1 << cf16MantissaBits) - 1))

	mant1 = (mant1 << (32 - cf16MantissaBits)) >> (32 - cf16MantissaBits)
	mant2 = (mant2 << (32 - cf16MantissaBits)) >> (32 - cf16MantissaBits)

	exp1 := int32(u1 >> cf16MantissaBits)
	exp2 := int32(u2 >> cf16MantissaBits)

	f1 = float64(mant1) / cf16Exponents[exp1]
	f2 = float64(mant2) / cf16Exponents[exp2]

	return
}

// CF24 encoding:
const (
	cf24ExponentBits = 3
	cf24MantissaBits = 24 - cf24ExponentBits
	cf24MantissaMin  = -(1 << (cf24MantissaBits - 1))
	cf24MantissaMax  = -cf24MantissaMin - 1
)

// note: for correct rounding we need to use
// 1/1^exponent rather than 1^-exponent
var cf24Exponents = [1 << cf24ExponentBits]float64{
	1e5,
	1e6,
	1e7,
	1e8,
	1e9,
	1e10,
	1e11,
	1e13,
}

func decodeCF24(src []byte) float64 {
	_ = src[:3]
	u := uint32(src[0]) | (uint32(src[1]) << 8) | (uint32(src[2]) << 16)

	// read mantissa (sign-extended) and exponent
	mant := int32(u & ((1 << cf24MantissaBits) - 1))
	mant = (mant << (32 - cf24MantissaBits)) >> (32 - cf24MantissaBits)
	exp := int32(u >> cf24MantissaBits)

	return float64(mant) / cf24Exponents[exp]
}

// CF32 encoding:
const (
	cf32ExponentBits = 1
	cf32MantissaBits = 32 - cf32ExponentBits
	cf32MantissaMin  = -(1 << (cf32MantissaBits - 1))
	cf32MantissaMax  = -cf32MantissaMin - 1
)

// note: for correct rounding we need to use
// 1/1^exponent rather than 1^-exponent
var cf32Exponents = [1 << cf32ExponentBits]float64{
	1e9,
	1e13,
}

func decodeCF32(src []byte) float64 {
	_ = src[:4]
	u := uint32(src[0]) | (uint32(src[1]) << 8) | (uint32(src[2]) << 16) | (uint32(src[3]) << 24)

	// read mantissa (sign-extended) and exponent
	mant := int32(u & ((1 << cf32MantissaBits) - 1))
	mant = (mant << (32 - cf32MantissaBits)) >> (32 - cf32MantissaBits)
	exp := int32(u >> cf32MantissaBits)

	return float64(mant) / cf32Exponents[exp]
}

func write24bits(dst []byte, bits uint64) []byte {
	return append(dst, byte(bits), byte(bits>>8), byte(bits>>16))
}

func write32bits(dst []byte, bits uint64) []byte {
	return binary.LittleEndian.AppendUint32(dst, uint32(bits))
}

func encodeCF12(f float64) (uint64, bool) {
	for exp := range cf12Exponents {
		scaled := f * cf12Exponents[exp]
		mant := int32(math.RoundToEven(scaled))
		if mant > cf12MantissaMax || mant < cf12MantissaMin {
			continue
		}
		if ret := float64(mant) / cf12Exponents[exp]; ret != f {
			continue
		}

		return (uint64(exp) << cf12MantissaBits) | (uint64(mant) & ((1 << cf12MantissaBits) - 1)), true
	}

	return 0, false
}

func encodeCF16(f float64) (uint64, bool) {
	for exp := range cf16Exponents {
		scaled := f * cf16Exponents[exp]
		mant := int32(math.RoundToEven(scaled))
		if mant > cf16MantissaMax || mant < cf16MantissaMin {
			continue
		}
		if ret := float64(mant) / cf16Exponents[exp]; ret != f {
			continue
		}

		return (uint64(exp) << cf16MantissaBits) | (uint64(mant) & ((1 << cf16MantissaBits) - 1)), true
	}

	return 0, false
}

func encodeCF24(f float64) (uint64, bool) {
	for exp := range cf24Exponents {
		scaled := f * cf24Exponents[exp]
		mant := int32(math.RoundToEven(scaled))
		if mant > cf24MantissaMax || mant < cf24MantissaMin {
			continue
		}
		if ret := float64(mant) / cf24Exponents[exp]; ret != f {
			continue
		}

		return (uint64(exp) << cf24MantissaBits) | (uint64(mant) & ((1 << cf24MantissaBits) - 1)), true
	}

	return 0, false
}

func encodeCF32(f float64) (uint64, bool) {
	for exp := range cf32Exponents {
		scaled := f * cf32Exponents[exp]
		mant := int32(math.RoundToEven(scaled))
		if mant > cf32MantissaMax || mant < cf32MantissaMin {
			continue
		}
		if ret := float64(mant) / cf32Exponents[exp]; ret != f {
			continue
		}

		return (uint64(exp) << cf32MantissaBits) | (uint64(mant) & ((1 << cf32MantissaBits) - 1)), true
	}

	return 0, false
}

func encodeFloatValue(f float64, startEncoding encodingID) (value uint64, encoding encodingID) {
	// don't use floating point encoding if the value can be encoded as integer.
	// The reason for applying such restriction is to avoid having to serialize
	// as integers on the encoder side.
	i := int64(f)
	if float64(i) == f {
		if i == 0 {
			return 0, encodingInt0
		}

		if i >= -128 && i <= 127 {
			return uint64(i & 0xFF), encodingInt8
		}

		return uint64(i), encodingIntX
	}

	if startEncoding <= encodingCF12 {
		payload, ok := encodeCF12(f)
		if ok {
			return payload, encodingCF12
		}
	}

	if startEncoding <= encodingCF16 {
		payload, ok := encodeCF16(f)
		if ok {
			return payload, encodingCF16
		}
	}

	if startEncoding <= encodingCF24 {
		payload, ok := encodeCF24(f)
		if ok {
			return payload, encodingCF24
		}
	}

	if startEncoding <= encodingCF32 {
		payload, ok := encodeCF32(f)
		if ok {
			return payload, encodingCF32
		}
	}

	return math.Float64bits(f), encodingFP64
}

func makeClusterHeader(encoding encodingID, size int) byte {
	return byte(encoding<<5) | byte(size-1)
}

func countFloatsToMerge(encodings []encodingID, maxLookAhead int, clusterEncoding encodingID) int {
	count := len(encodings)

	// let's limit the look ahead a bit
	if count > maxLookAhead {
		count = maxLookAhead
	}

	ret := 0

	// the idea is to grow the cluster to the point of the same encoding, and to terminate
	// when we see either integer or encoding that would need more bytes to encode the float
	for i := 0; i < count; i++ {
		encoding := encodings[i]
		if encoding > clusterEncoding || isIntegerEncoding(encoding) {
			break
		}

		if encoding == clusterEncoding {
			ret = i
		}
	}

	return ret
}

func compressFloatValues(src []float64, dst []byte, listSize int) ([]byte, bool) {
	count := len(src)

	dst = encodeVarUInt(dst, uint(count))
	dst = encodeVarUInt(dst, uint(listSize))

	encodingArray := make([]encodingID, count)
	for i := 0; i < count; i++ {
		_, encodingArray[i] = encodeFloatValue(src[i], encodingInt0)
	}

	// To not complicate code, we mark the index of the last header so we can potentially
	// patch it in case that we want to encode a cluster that has the same encoding. This
	// is only required to add an additional cluster of CF12 of CF16 floats, because the
	// maximum cluster size that we produce here is 32 values - but since CF12 and CF16
	// values are stored in pairs, we can theoretically form clusters having more than 32
	// elements.
	//
	// So, the code that adds a new cluster should either set `lastHeaderIndex` to the
	// index of the header in case it's CF12 or CF16, or set it to -1 to indicate that
	// there is either no past header or it's not usable for this purpose.
	lastHeaderIndex := -1
	lastHeaderEncoding := encodingInt0

	i := 0
	for i < count {
		encoding := encodingArray[i]
		clusterSize := 1
		maxClusterSize := count - i

		if maxClusterSize > 32 {
			maxClusterSize = 32
		}

		switch encoding {
		case encodingInt0:
			for clusterSize = 1; clusterSize < maxClusterSize; clusterSize++ {
				if encodingArray[i+clusterSize] != encodingInt0 {
					break
				}
			}

			lastHeaderIndex = -1
			dst = append(dst, makeClusterHeader(encoding, clusterSize))

		case encodingInt8:
			for clusterSize = 1; clusterSize < maxClusterSize; clusterSize++ {
				if encodingArray[i+clusterSize] != encodingInt8 {
					break
				}
			}

			lastHeaderIndex = -1
			dst = append(dst, makeClusterHeader(encoding, clusterSize))
			for j := 0; j < clusterSize; j++ {
				v := int64(src[i+j])
				dst = append(dst, byte(v))
			}

		case encodingIntX:
			payload := uint64(int64(src[i]))
			headerByte := byte(encodingIntX << 5)

			if int64(payload) < 0 {
				payload = 0 - payload
				headerByte |= byte(1 << 4)
			}

			payloadLength := (64 + 7 - bits.LeadingZeros64(payload)) >> 3
			headerByte |= byte(payloadLength - 1)

			lastHeaderIndex = -1
			dst = append(dst, headerByte)
			dst = binary.LittleEndian.AppendUint64(dst, payload)
			dst = dst[0 : len(dst)-8+int(payloadLength)]

		default:
			for clusterSize < maxClusterSize {
				encoding2 := encodingArray[i+clusterSize]

				// quickly consume values of the same encoding
				if encoding == encoding2 {
					clusterSize++
					continue
				}

				// cannot combine floats and integers
				if isIntegerEncoding(encoding2) {
					break
				}

				if encoding2 > encoding {
					// If the new encoding is FP64, also bail as we don't want to lose that many bytes
					// by turning the existing cluster to FP64. In this case saving bytes has greater
					// benefit than forming an non-compressible FP64 cluster.
					if encoding2 == encodingFP64 {
						if clusterSize >= 2 {
							break
						}

						// If there is only a single value, turn the cluster to FP64 and see what's next
						encoding = encoding2
						clusterSize++
						continue
					}

					// If we have already reached an acceptable size of the cluster, use it as is and
					// don't go to a common encoding (which consumes more bytes).
					if clusterSize >= clusterSizeHint[encoding] {
						break
					}

					// Use the common encoding for this cluster - this means consuming more space.
					encoding = encoding2
					clusterSize++
					continue
				}

				// This assumes encoding2 < encoding - thus the new encoding has a potential to
				// start a cluster, which would consume less bytes. What we want to do in this
				// case is to look ahead a bit and decide whether it makes sense or not. If the
				// new [and smaller] cluster would not form a cluster of sufficient size, then
				// just merge it with the current one.
				countAhead := countFloatsToMerge(encodingArray[i+1+clusterSize:i+maxClusterSize], 8, encoding)
				if countAhead == 0 {
					break
				}

				clusterSize += countAhead
			}

			// CF12 and CF16 encoding must encode pairs of values, so verify the cluster is
			// encodable.
			if encoding <= encodingCF16 && (clusterSize&1) != 0 {
				if clusterSize == 1 {
					encoding = encodingCF24
				} else {
					clusterSize--
				}
			}

			if encoding <= encodingCF16 {
				if lastHeaderIndex != -1 && lastHeaderEncoding == encoding {
					// Coalesce by patching the previous cluster length
					dst[lastHeaderIndex] += byte(clusterSize >> 1)
					// Since we have coalesced two clusters there is no point in maintaining
					// the `lastHeaderIndex` value as it's guaranteed we cannot coalesce anymore.
					lastHeaderIndex = -1
				} else {
					lastHeaderIndex = -1
					if clusterSize == 32 {
						// Special case - CF12 or CF16 cluster having the maximum size - let's
						// set lastHeaderIndex and lastHeaderEncoding to indicate that another
						// run of the same encoding can be merged with this run.
						lastHeaderIndex = len(dst)
						lastHeaderEncoding = encoding
					}
					dst = append(dst, makeClusterHeader(encoding, clusterSize>>1))
				}
			} else {
				lastHeaderIndex = -1
				dst = append(dst, makeClusterHeader(encoding, clusterSize))
			}

			switch encoding {
			case encodingCF12:
				for j := 0; j < clusterSize; j += 2 {
					payload1, ok1 := encodeCF12(src[i+j])
					payload2, ok2 := encodeCF12(src[i+j+1])
					if !ok1 || !ok2 {
						panic("cannot encode CF12 cluster after previous analysis")
					}
					dst = write24bits(dst, payload1|(payload2<<12))
				}
			case encodingCF16:
				for j := 0; j < clusterSize; j += 2 {
					payload1, ok1 := encodeCF16(src[i+j])
					payload2, ok2 := encodeCF16(src[i+j+1])
					if !ok1 || !ok2 {
						panic("cannot encode CF16 cluster after previous analysis")
					}
					dst = write32bits(dst, payload1|(payload2<<16))
				}
			case encodingCF24:
				for j := 0; j < clusterSize; j++ {
					payload, ok := encodeCF24(src[i+j])
					if !ok {
						panic("cannot encode CF24 cluster after previous analysis")
					}
					dst = write24bits(dst, payload)
				}
			case encodingCF32:
				for j := 0; j < clusterSize; j++ {
					payload, ok := encodeCF32(src[i+j])
					if !ok {
						panic("cannot encode CF32 cluster after previous analysis")
					}
					dst = write32bits(dst, payload)
				}
			case encodingFP64:
				for j := 0; j < clusterSize; j++ {
					dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(src[i+j]))
				}
			}
		}

		i += clusterSize
	}

	return dst, true
}

func compressNumericList(src, dst []byte) ([]byte, bool) {
	var lbl []byte
	floats := []float64{}

	for len(src) > 0 {
		p := 0
		for p < len(src) && src[p]&0x80 == 0 {
			p++
		}

		p++
		if p >= len(src) {
			return nil, false
		}

		if lbl == nil {
			lbl = src[:p]
			dst = append(dst, lbl...)
		} else if !bytes.Equal(src[:p], lbl) {
			// not the same label as we expected
			return nil, false
		}

		t := ion.TypeOf(src[p:])
		if t != ion.ListType {
			// not a list? bail
			return nil, false
		}

		var lst []byte
		lst, src = ion.Contents(src[p:])

		floats = floats[0:0]
		listSize := len(lst)

		for len(lst) > 0 {
			var f float64
			var err error

			f, lst, err = ion.ReadCoerceFloat64(lst)
			if err != nil {
				// not a float? bail
				return nil, false
			}
			floats = append(floats, f)
		}

		ok := false
		dst, ok = compressFloatValues(floats, dst, listSize)

		if !ok {
			return nil, false
		}
	}

	return dst, true
}

func decompressNumericList(src, dst []byte) ([]byte, error) {
	lp := 0
	for lp < len(src) && src[lp]&0x80 == 0 {
		lp++
	}

	lp++
	label := src[:lp]
	src = src[lp:]

	for len(src) >= 2 {
		var itemCount uint
		var listSize uint

		src, itemCount = decodeVarUInt(src)
		src, listSize = decodeVarUInt(src)

		dst = append(dst, label...)
		dst = slices.Grow(dst, int(listSize+256))
		dst = ion.UnsafeAppendTag(dst, ion.ListType, listSize)

		if cpu.X86.HasAVX512VBMI {
			writtenCount, readCount := decodeNumericVecAVX512VBMI(dst, src, itemCount)
			if writtenCount >= 0 {
				dst = dst[0 : len(dst)+writtenCount]
				src = src[readCount:]
				continue
			}
		} else if cpu.X86.HasAVX512 {
			writtenCount, readCount := decodeNumericVecAVX512(dst, src, itemCount)
			if writtenCount >= 0 {
				dst = dst[0 : len(dst)+writtenCount]
				src = src[readCount:]
				continue
			}
		}

		for itemCount > 0 {
			if len(src) == 0 {
				return nil, fmt.Errorf("buffer truncated: %d floats pending to decode", itemCount)
			}

			clusterHeader := src[0]
			src = src[1:]

			clusterEncoding := encodingID(clusterHeader >> 5)
			clusterSize := int(clusterHeader&31) + 1

			switch clusterEncoding {
			case encodingInt0:
				for i := 0; i < clusterSize; i++ {
					dst = append(dst, 0x20)
				}

				itemCount -= uint(clusterSize)

			case encodingIntX:
				sign := (clusterHeader >> 4) & 1
				valueSize := int(clusterHeader&0x7) + 1

				if len(src) < valueSize {
					return nil, fmt.Errorf("IntX buffer truncated: required %d bytes, got %d", valueSize, len(src))
				}

				dst = append(dst, 0x20+(sign<<4)+byte(valueSize))
				for i := 0; i < valueSize; i++ {
					dst = append(dst, src[valueSize-i-1])
				}

				src = src[valueSize:]
				itemCount--

			case encodingInt8:
				requiredBytes := clusterSize
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("Int8 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i++ {
					val := int8(src[0])
					src = src[1:]

					if val >= 0 {
						dst = append(dst, 0x21, uint8(val))
					} else {
						dst = append(dst, 0x31, uint8(-val))
					}
				}

				itemCount -= uint(clusterSize)

			case encodingCF12:
				// since we always store a pair of CF12 values, we have to double
				// the clusterSize to get the number of floats this cluster has
				clusterSize *= 2

				requiredBytes := (clusterSize >> 1) * 3
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("CF12 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i += 2 {
					f1, f2 := decode2xCF12(src)
					src = src[3:]

					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f1))
					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f2))
				}

				itemCount -= uint(clusterSize)

			case encodingCF16:
				// since we always store a pair of CF16 values, we have to double
				// the clusterSize to get the number of floats this cluster has
				clusterSize *= 2

				requiredBytes := clusterSize * 2
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("CF16 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i += 2 {
					f1, f2 := decode2xCF16(src)
					src = src[4:]

					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f1))
					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f2))
				}

				itemCount -= uint(clusterSize)

			case encodingCF24:
				requiredBytes := clusterSize * 3
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("CF24 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i++ {
					f := decodeCF24(src)
					src = src[3:]

					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f))
				}

				itemCount -= uint(clusterSize)

			case encodingCF32:
				requiredBytes := clusterSize * 4
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("CF32 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i++ {
					f := decodeCF32(src)
					src = src[4:]

					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(f))
				}

				itemCount -= uint(clusterSize)

			case encodingFP64:
				requiredBytes := clusterSize * 8
				if len(src) < requiredBytes {
					return nil, fmt.Errorf("FP64 buffer truncated: required %d bytes, got %d", requiredBytes, len(src))
				}

				for i := 0; i < clusterSize; i++ {
					bits := binary.LittleEndian.Uint64(src)
					src = src[8:]

					dst = append(dst, 0x48)
					dst = binary.BigEndian.AppendUint64(dst, bits)
				}

				itemCount -= uint(clusterSize)
			}
		}

	}

	return dst, nil
}
