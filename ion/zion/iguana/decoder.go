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

package iguana

import (
	"github.com/SnellerInc/sneller/ints"
	"golang.org/x/exp/slices"
)

func Decompress(src []byte) ([]byte, error) {
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
	dst, ec = decode(uncompressedLen, cursor, dst, src)
	if ec != ecOK {
		return nil, errs[ec]
	}
	return dst, nil
}

func DecompressTo(dst []byte, src []byte) ([]byte, error) {
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
	dst, ec = decode(uncompressedLen, cursor, dst, src)
	if ec != ecOK {
		return dst, errs[ec]
	}
	return dst, nil
}

func decode(uncompressedLen uint64, ctrlCursor int, dst []byte, src []byte) ([]byte, errorCode) {
	var ec errorCode
	var ansBuffer []byte

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

		case cmdDecodeANS:
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
			var tab AnsDenseTable
			encoded, ec := ansDecodeTable(&tab, ans)
			if ec != ecOK {
				return dst, ec
			}
			dst, ec = ansDecodeExplicit(encoded, &tab, int(lenUncompressed), dst)
			if ec != ecOK {
				return dst, ec
			}
			dataCursor += lenCompressed

		case cmdDecodeIguana:
			// Fetch the header byte
			if ctrlCursor < 0 {
				return dst, ecOutOfInputData
			}
			hdr := src[ctrlCursor]
			ctrlCursor--

			// Fetch the uncompressed streams' lengths
			var streams streamPack

			if hdr == 0 {
				for i := stridType(0); i < streamCount; i++ {
					var uLen uint64
					uLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
					if ec != ecOK {
						return dst, ec
					}
					streams[i].data = src[dataCursor : dataCursor+uLen]
					dataCursor += uLen
				}
			} else {
				var ulens [streamCount]uint64
				ansBufferSize := uint64(0)

				for i := stridType(0); i < streamCount; i++ {
					var uLen uint64
					uLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
					if ec != ecOK {
						return dst, ec
					}
					ulens[i] = uLen
					if hdr&(1<<i) != 0 {
						ansBufferSize = ints.AlignUp64(ansBufferSize+uLen, 64)
					}
				}
				if uint64(cap(ansBuffer)) < ansBufferSize {
					ansBuffer = make([]byte, ansBufferSize)
				}
				ansOffs := uint64(0)

				for i := stridType(0); i < streamCount; i++ {
					uLen := ulens[i]
					if hdr&(1<<i) == 0 {
						streams[i].data = src[dataCursor : dataCursor+uLen]
						dataCursor += uLen
					} else {
						var cLen uint64
						cLen, ctrlCursor, ec = readControlVarUint(src, ctrlCursor)
						if ec != ecOK {
							return dst, ec
						}
						ans := src[dataCursor : dataCursor+cLen]
						dataCursor += cLen

						var tab AnsDenseTable
						encoded, ec := ansDecodeTable(&tab, ans)
						if ec != ecOK {
							return dst, ec
						}

						buf := ansBuffer[ansOffs:ansOffs]
						streams[i].data, ec = ansDecodeExplicit(encoded, &tab, int(uLen), buf)
						if ec != ecOK {
							return dst, ec
						}
						ansOffs = ints.AlignUp64(ansOffs+uLen, 64)
					}
				}
			}

			lastOffs := -initLastOffset
			dst, ec = decompressIguana(dst, &streams, &lastOffs)
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

			if x, ec := streams[stridLiterals].fetchSequence(litLen); ec != ecOK {
				return dst, ec
			} else {
				dst = append(dst, x...)
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
		dst = append(dst, dst[match:match+matchLen]...)
	}

	// last literals
	remainderLen := streams[stridLiterals].remaining()

	if x, ec := streams[stridLiterals].fetchSequence(remainderLen); ec != ecOK {
		return dst, ec
	} else {
		dst = append(dst, x...)
	}
	// end of decoding
	*lastOffset = lastOffs
	return dst, ecOK
}

var decompressIguana func(dst []byte, streams *streamPack, lastOffs *int) ([]byte, errorCode) = decompressIguanaReference
