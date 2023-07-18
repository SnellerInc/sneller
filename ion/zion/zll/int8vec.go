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

package zll

import (
	"fmt"

	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
	"golang.org/x/sys/cpu"
)

func appendUvarint(dst []byte, size int) []byte {
	uv := ion.Uvsize(uint(size))
	for uv > 1 {
		uv--
		shift := uv * 7
		dst = append(dst, byte((size>>shift)&0x7f))
	}
	dst = append(dst, byte(size&0x7f)|0x80)
	return dst
}

func tryInt8Vector(src, dst []byte) ([]byte, bool) {
	var sym, lbl ion.Symbol
	var err error
	elems := 0
	for len(src) > 0 {
		lbl, src, err = ion.ReadLabel(src)
		if err != nil {
			return nil, false
		}
		if elems == 0 {
			sym = lbl
			dst = appendUvarint(dst, int(sym))
		} else if lbl != sym {
			return nil, false
		}
		t := ion.TypeOf(src)
		if t != ion.ListType {
			return nil, false
		}
		var intbits []byte
		intbits, src = ion.Contents(src)
		dst = appendUvarint(dst, len(intbits))
		for len(intbits) > 0 {
			switch intbits[0] {
			case 0x20:
				dst = append(dst, 0)
				intbits = intbits[1:]
			case 0x21:
				if intbits[1] > 127 {
					return nil, false
				}
				dst = append(dst, byte(int8(intbits[1])))
				intbits = intbits[2:]
			case 0x31:
				if intbits[1] > 128 {
					return nil, false
				}
				dst = append(dst, byte(-int8(intbits[1])))
				intbits = intbits[2:]
			default:
				return nil, false // not a 1-byte integer
			}
		}
		elems++
	}
	return dst, true
}

//go:noescape
func unpackInt8VBMI2(src, dst []byte) int

//go:noescape
func unpackInt8AVX512(src, dst []byte) int

func decodeInt8Vec(dst, src []byte) ([]byte, error) {
	sym, src, err := ion.ReadLabel(src)
	if err != nil {
		return nil, fmt.Errorf("int8vec: reading initial label: %w", err)
	}
	for len(src) > 0 {
		dst = appendUvarint(dst, int(sym))
		var count ion.Symbol
		count, src, err = ion.ReadLabel(src)
		if err != nil {
			return nil, fmt.Errorf("int8vec: reading size: %w", err)
		}
		dst = ion.UnsafeAppendTag(dst, ion.ListType, uint(count))
		if count == 0 {
			continue
		}
		target := len(dst) + int(count)
		if cpu.X86.HasAVX512 {
			tail := len(dst)
			dst = slices.Grow(dst, int(count))
			dst = dst[:len(dst)+int(count)]
			var used int
			switch {
			case cpu.X86.HasAVX512VBMI2:
				// need VPCOMPRESSB
				used = unpackInt8VBMI2(src, dst[tail:])
			default:
				used = unpackInt8AVX512(src, dst[tail:])
			}
			if used >= 0 {
				src = src[used:]
				continue
			}
			// fallthrough; produce a more meaningful error
			// by using the portable Go code
		}
		n := 0
		for i, b := range src {
			if len(dst) >= target {
				break
			}
			v := int8(b)
			if v == 0 {
				dst = append(dst, 0x20)
			} else if v < 0 {
				dst = append(dst, 0x31, byte(-v))
			} else {
				dst = append(dst, 0x21, b)
			}
			n = i
		}
		if len(dst) != target {
			return nil, fmt.Errorf("corrupt int8vec encoding: got %d bytes instead of %d", len(dst), count)
		}
		src = src[n+1:]
	}
	return dst, nil
}
