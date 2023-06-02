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
	"unsafe"

	"github.com/SnellerInc/sneller/ints"
	"golang.org/x/exp/slices"
)

// Decoder is a stateless decoder for iguana-compressed data.
// The zero value of Decoder is ready to use via Decompress or DecompressTo.
//
// It is not safe to use a Decoder from multiple goroutines simultaneously.
type Decoder struct {
	pack      streamPack
	anstab    ANSDenseTable
	ansnibtab ANSNibbleDenseTable
	entbuf    []byte
	lastOffs  int
}

func (d *Decoder) reset() {
	d.pack = streamPack{}
	d.anstab = ANSDenseTable{}
	d.ansnibtab = ANSNibbleDenseTable{}
}

// Decompress returns the decompressed result of src as a new slice.
func (d *Decoder) Decompress(src []byte) ([]byte, error) {
	cursor := len(src) - 1
	if cursor < 0 {
		return nil, errs[ecOutOfInputData]
	}
	uncompressedLen, cursor, ec := readControlVarUint(src, cursor)
	if ec != ecOK {
		return nil, errs[ec]
	}
	if uncompressedLen == 0 {
		return nil, nil
	}
	dst := make([]byte, 0, ints.AlignUp64(uncompressedLen, 64))
	dst, ec = d.decode(uncompressedLen, cursor, dst, src)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return dst, nil
}

// DecompressTo decompresses the data in src and appends it to dst,
// returning the enlarged slice or the first encountered error.
func (d *Decoder) DecompressTo(dst []byte, src []byte) ([]byte, error) {
	cursor := len(src) - 1
	if cursor < 0 {
		return dst, errs[ecOutOfInputData]
	}
	uncompressedLen, cursor, ec := readControlVarUint(src, cursor)
	if ec != ecOK {
		return dst, errs[ec]
	}
	if uncompressedLen == 0 {
		return dst, nil
	}
	c := uint64(cap(dst))
	n := uint64(len(dst))
	if rem := c - n; uncompressedLen > rem {
		dst = slices.Grow(dst, int(uncompressedLen))
	}
	dst, ec = d.decode(uncompressedLen, cursor, dst, src)
	if ec != ecOK {
		return dst, errs[ec]
	}
	return dst, nil
}

// we'd like to allow 64-byte loads at the final byte offset
// for each of the streams, so we need (64 - 1) bytes of valid memory
// past the end of the buffer
const padSize = (64 - 1)

// are addr and (addr+addend) in the same page?
func samePage(addr, addend uintptr) bool {
	return (addr &^ 4095) == ((addr + addend) &^ 4095)
}

func (d *Decoder) padStream(id stridType, buf []byte) []byte {
	if cap(buf)-len(buf) >= padSize {
		return buf
	}
	// if we don't cross a page boundary,
	// then the load is safe (but may read garbage):
	ptr := unsafe.Pointer(unsafe.SliceData(buf))
	end := uintptr(ptr) + uintptr(len(buf))
	if samePage(end, padSize) {
		return buf
	}
	// last resort: allocate new memory
	ret := make([]byte, len(buf), len(buf)+padSize)
	copy(ret, buf)
	return ret
}

func (d *Decoder) decode(uncompressedLen uint64, ctrlCursor int, dst []byte, src []byte) ([]byte, errorCode) {
	d.reset()
	var ec errorCode

	// Fetch the header
	if ctrlCursor < 0 {
		return dst, ecOutOfInputData
	}
	dataCursor := uint64(0)

	for {
		if ctrlCursor < 0 {
			return dst, ecOutOfInputData
		}
		cmd := src[ctrlCursor]
		ctrlCursor--

		switch cmd & cmdMask {
		case cmdCopyRaw:
			var n uint64
			n, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			dst = append(dst, src[dataCursor:dataCursor+n]...)
			dataCursor += n

		case cmdDecodeANS32:
			var lenUncompressed, lenCompressed uint64
			lenUncompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			lenCompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			ans := src[dataCursor : dataCursor+lenCompressed]
			encoded, ec := ansDecodeTable(&d.anstab, ans)
			if ec != ecOK {
				return dst, ec
			}
			dst, ec = ans32DecodeExplicit(encoded, &d.anstab, int(lenUncompressed), dst)
			if ec != ecOK {
				return dst, ec
			}
			dataCursor += lenCompressed

		case cmdDecodeANS1:
			var lenUncompressed, lenCompressed uint64
			lenUncompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			lenCompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			ans := src[dataCursor : dataCursor+lenCompressed]
			encoded, ec := ansDecodeTable(&d.anstab, ans)
			if ec != ecOK {
				return dst, ec
			}
			dst, ec = ans1DecodeExplicit(encoded, &d.anstab, int(lenUncompressed), dst)
			if ec != ecOK {
				return dst, ec
			}
			dataCursor += lenCompressed

		case cmdDecodeANSNibble:
			var lenUncompressed, lenCompressed uint64
			lenUncompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			lenCompressed, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}
			ans := src[dataCursor : dataCursor+lenCompressed]
			encoded, ec := ansNibbleDecodeTable(&d.ansnibtab, ans)
			if ec != ecOK {
				return dst, ec
			}
			dst, ec = ansNibbleDecodeExplicit(encoded, &d.ansnibtab, int(lenUncompressed), dst)
			if ec != ecOK {
				return dst, ec
			}
			dataCursor += lenCompressed

		case cmdDecodeIguana:
			// Fetch the header byte
			if ctrlCursor < 0 {
				return dst, ecOutOfInputData
			}

			var hdr uint64
			hdr, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
			if ec != ecOK {
				return dst, ec
			}

			// Fetch the uncompressed streams' lengths
			if hdr == 0 {
				for i := stridType(0); i < streamCount; i++ {
					var uLen uint64
					uLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
					if ec != ecOK {
						return dst, ec
					}
					d.pack[i].data = src[dataCursor : dataCursor+uLen]
					dataCursor += uLen
				}
			} else {
				var ulens [streamCount]uint64
				entropyBufferSize := uint64(0)

				for i := stridType(0); i < streamCount; i++ {
					var uLen uint64
					uLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
					if ec != ecOK {
						return dst, ec
					}
					ulens[i] = uLen
					if entropyMode := EntropyMode((hdr >> (i * 4)) & 0x0f); entropyMode != EntropyNone {
						entropyBufferSize += uLen
					}
				}
				if uint64(cap(d.entbuf)) < entropyBufferSize+padSize {
					// ensure the output is appropriately padded:
					d.entbuf = make([]byte, entropyBufferSize, entropyBufferSize+padSize)
				}
				entOffs := uint64(0)

				for i := stridType(0); i < streamCount; i++ {
					uLen := ulens[i]
					if entropyMode := EntropyMode((hdr >> (i * 4)) & 0x0f); entropyMode == EntropyNone {
						d.pack[i].data = d.padStream(i, src[dataCursor:dataCursor+uLen])
						dataCursor += uLen
					} else {
						var cLen uint64
						cLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
						if ec != ecOK {
							return dst, ec
						}
						switch entropyMode {
						case EntropyANS32:
							ans := src[dataCursor : dataCursor+cLen]
							dataCursor += cLen

							encoded, ec := ansDecodeTable(&d.anstab, ans)
							if ec != ecOK {
								return dst, ec
							}

							buf := d.entbuf[entOffs:entOffs]
							d.pack[i].data, ec = ans32DecodeExplicit(encoded, &d.anstab, int(uLen), buf)
							if ec != ecOK {
								return dst, ec
							}
							entOffs += uLen

						case EntropyANS1:
							ans := src[dataCursor : dataCursor+cLen]
							dataCursor += cLen

							encoded, ec := ansDecodeTable(&d.anstab, ans)
							if ec != ecOK {
								return dst, ec
							}

							buf := d.entbuf[entOffs:entOffs]
							d.pack[i].data, ec = ans1DecodeExplicit(encoded, &d.anstab, int(uLen), buf)
							if ec != ecOK {
								return dst, ec
							}
							entOffs += uLen

						case EntropyANSNibble:
							ansNib := src[dataCursor : dataCursor+cLen]
							dataCursor += cLen

							encoded, ec := ansNibbleDecodeTable(&d.ansnibtab, ansNib)
							if ec != ecOK {
								return dst, ec
							}

							buf := d.entbuf[entOffs:entOffs]
							d.pack[i].data, ec = ansNibbleDecodeExplicit(encoded, &d.ansnibtab, int(uLen), buf)
							if ec != ecOK {
								return dst, ec
							}
							entOffs += uLen

						default:
							panic("unrecognized entropy mode")
						}
					}
				}
			}

			d.lastOffs = -initLastOffset
			dst, ec = decompressIguana(dst, &d.pack, &d.lastOffs)

			if ec != ecOK {
				return dst, ec
			}

		default:
			return dst, ecUnrecognizedCommand
		}

		if (cmd & lastCommandMarker) != 0 {
			return dst, ecOK
		}
	}
}

func readControlVarUint(s []byte, cursor int) (uint64, int, errorCode) {
	r := uint64(0)
	for cursor >= 0 {
		v := s[cursor]
		cursor--
		r = (r << 7) | uint64(v&0x7f)
		if (v & 0x80) != 0 {
			return r, cursor, ecOK
		}
	}
	return 0, -1, ecOutOfInputData
}

func decompressIguanaReference(dst []byte, streams *streamPack, lastOffset *int) ([]byte, errorCode) {
	// [0_MMMM_LLL] - 16-bit offset, 4-bit match length (4-15+), 3-bit literal length (0-7+)
	// [1_MMMM_LLL] -   last offset, 4-bit match length (0-15+), 3-bit literal length (0-7+)
	// flag 31      - 24-bit offset,        match length (47+),    no literal length
	// flag 0-30    - 24-bit offset,  31 match lengths (16-46),    no literal length

	lastOffs := *lastOffset

	// Main Loop : decode sequences
	for !streams[stridTokens].empty() {
		//get literal length
		matchLen := 0
		token, ec := streams[stridTokens].fetch8()
		if ec != ecOK {
			return dst, ec
		}

		if token >= 32 {
			litLen := int(token & maxShortLitLen)
			if litLen == maxShortLitLen {
				val, ec := streams[stridVarLitLen].fetchVarUInt()
				if ec != ecOK {
					return dst, ec
				}
				litLen = val + maxShortLitLen
			}
			if litLen > 0 {
				if x, ec := streams[stridLiterals].fetchSequence(litLen); ec != ecOK {
					return dst, ec
				} else {
					dst = append(dst, x...)
				}
			}

			// get offset
			if (token & 0x80) == 0 {
				// [0_MMMM_LLL] - 16-bit offset, 4-bit match length (4-15+), 3-bit literal length (0-7+)
				newOffs, ec := streams[stridOffset16].fetch16()
				if ec != ecOK {
					return dst, ec
				}
				lastOffs = -int(newOffs)
			}

			// get matchlength
			matchLen = int((token >> literalLenBits) & maxShortMatchLen)
			if matchLen == maxShortMatchLen {
				val, ec := streams[stridVarMatchLen].fetchVarUInt()
				if ec != ecOK {
					return dst, ec
				}
				matchLen = val + maxShortMatchLen
			}
		} else if token < lastLongOffset {
			// token < 31
			matchLen = int(token + mmLongOffsets)
			x, ec := streams[stridOffset24].fetch24()
			if ec != ecOK {
				return dst, ec
			}
			lastOffs = -int(x)
		} else {
			// token == 31
			val, ec := streams[stridVarMatchLen].fetchVarUInt()
			if ec != ecOK {
				return dst, ec
			}
			matchLen = val + lastLongOffset + mmLongOffsets
			x, ec := streams[stridOffset24].fetch24()
			if ec != ecOK {
				return dst, ec
			}
			lastOffs = -int(x)
		}
		match := len(dst) + lastOffs
		dst = iguanaWildCopy(dst, dst[match:match+matchLen])
	}

	// last literals
	remainderLen := streams[stridLiterals].remaining()
	if remainderLen > 0 {
		if x, ec := streams[stridLiterals].fetchSequence(remainderLen); ec != ecOK {
			return dst, ec
		} else {
			dst = append(dst, x...)
		}
	}

	// end of decoding
	*lastOffset = lastOffs
	return dst, ecOK
}

func iguanaWildCopy(dst []byte, match []byte) []byte {
	// Emulates the SIMD register-wide copies
	for {
		n := len(match)
		if n == 0 {
			break
		}
		rem := ints.Min(iguanaChunkSize, n)
		var tmp [iguanaChunkSize]byte
		copy(tmp[:], match[:rem])
		dst = append(dst, tmp[:rem]...)
		match = match[rem:]
	}
	return dst
}

var decompressIguana func(dst []byte, streams *streamPack, lastOffs *int) ([]byte, errorCode) = decompressIguanaReference
