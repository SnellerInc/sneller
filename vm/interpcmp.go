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

package vm

import (
	"bytes"
	"math"

	"github.com/SnellerInc/sneller/ion"
)

const (
	cmpTypeNullFirst     = 0x00
	cmpTypeBool          = 0x01
	cmpTypeNumber        = 0x02
	cmpTypeTimestamp     = 0x03
	cmpTypeString        = 0x04
	cmpTypeNullLast      = 0x0F
	cmpTypeInvalid       = 0x7F
	cmpTypeNonComparable = 0x40
	cmpTypeSort          = 0x80
)

// A lookup table that is used to convert ION type to our own type.
var ionTypeToCmpTypeTable = [48]byte{
	// opcmpv - a non-sorting compare, different types compare to MISSING.
	cmpTypeNullFirst, // null
	cmpTypeBool,      // bool
	cmpTypeNumber,    // uint
	cmpTypeNumber,    // int
	cmpTypeNumber,    // float
	cmpTypeInvalid,   // decimal
	cmpTypeTimestamp, // timestamp
	cmpTypeString,    // symbol
	cmpTypeString,    // string
	cmpTypeInvalid,   // clob
	cmpTypeInvalid,   // blob
	cmpTypeInvalid,   // list
	cmpTypeInvalid,   // sexp
	cmpTypeInvalid,   // struct
	cmpTypeInvalid,   // annotation
	cmpTypeInvalid,   // reserved

	// opsortcmpvnf - NULLs are sorted before any other value.
	cmpTypeSort | cmpTypeNullFirst, // null
	cmpTypeSort | cmpTypeBool,      // bool
	cmpTypeSort | cmpTypeNumber,    // uint
	cmpTypeSort | cmpTypeNumber,    // int
	cmpTypeSort | cmpTypeNumber,    // float
	cmpTypeSort | cmpTypeInvalid,   // decimal
	cmpTypeSort | cmpTypeTimestamp, // timestamp
	cmpTypeSort | cmpTypeString,    // symbol
	cmpTypeSort | cmpTypeString,    // string
	cmpTypeSort | cmpTypeInvalid,   // clob
	cmpTypeSort | cmpTypeInvalid,   // blob
	cmpTypeSort | cmpTypeInvalid,   // list
	cmpTypeSort | cmpTypeInvalid,   // sexp
	cmpTypeSort | cmpTypeInvalid,   // struct
	cmpTypeSort | cmpTypeInvalid,   // annotation
	cmpTypeSort | cmpTypeInvalid,   // reserved

	// opsortcmpvnl - NULLs are sorted after any other value.
	cmpTypeSort | cmpTypeNullLast,  // null
	cmpTypeSort | cmpTypeBool,      // bool
	cmpTypeSort | cmpTypeNumber,    // uint
	cmpTypeSort | cmpTypeNumber,    // int
	cmpTypeSort | cmpTypeNumber,    // float
	cmpTypeSort | cmpTypeInvalid,   // decimal
	cmpTypeSort | cmpTypeTimestamp, // timestamp
	cmpTypeSort | cmpTypeString,    // symbol
	cmpTypeSort | cmpTypeString,    // string
	cmpTypeSort | cmpTypeInvalid,   // clob
	cmpTypeSort | cmpTypeInvalid,   // blob
	cmpTypeSort | cmpTypeInvalid,   // list
	cmpTypeSort | cmpTypeInvalid,   // sexp
	cmpTypeSort | cmpTypeInvalid,   // struct
	cmpTypeSort | cmpTypeInvalid,   // annotation
	cmpTypeSort | cmpTypeInvalid,   // reserved
}

// comparison helpers that implement the same semantics we use in optimized code.
func cmpf64eq(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }
func cmpf64lt(a, b float64) bool { return a < b || (!math.IsNaN(a) && math.IsNaN(b)) }
func cmpf64le(a, b float64) bool { return a <= b || math.IsNaN(b) }
func cmpf64gt(a, b float64) bool { return a > b || (math.IsNaN(a) && !math.IsNaN(b)) }
func cmpf64ge(a, b float64) bool { return a >= b || math.IsNaN(a) }

func cmpi64(a, b int64) int64 {
	if a > b {
		return 1
	}

	if a < b {
		return -1
	}

	return 0
}

func cmpu64(a, b uint64) int64 {
	if a > b {
		return 1
	}

	if a < b {
		return -1
	}

	return 0
}

func cmpf64(a, b float64) int64 {
	if a > b {
		return 1
	}

	if a < b {
		return -1
	}

	aNaN := int64(0)
	bNaN := int64(0)
	if math.IsNaN(a) {
		aNaN = 1
	}
	if math.IsNaN(b) {
		bNaN = 1
	}
	return aNaN - bNaN
}

func cmpvi64f64(mem []byte, typeL byte, immi64 int64, immf64 float64) (int64, uint16) {
	result := int64(0)

	switch ion.Type(typeL >> 4) {
	case ion.FloatType:
		val, _, _ := ion.ReadFloat64(mem)
		if val < immf64 {
			result = -1
		} else if val > immf64 {
			result = 1
		}
		return result, 1
	case ion.IntType:
		val, _, _ := ion.ReadInt(mem)
		if val < immi64 {
			result = -1
		} else if val > immi64 {
			result = 1
		}
		return result, 1
	case ion.UintType:
		val, _, _ := ion.ReadUint(mem)
		if immi64 < 0 {
			result = 1
		} else if val < uint64(immi64) {
			result = -1
		} else if val > uint64(immi64) {
			result = 1
		}
		return result, 1
	}

	return 0, 0
}

func bccmpvgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	val2 := argptr[vRegData](bc, pc+6)
	mask := argptr[kRegData](bc, pc+8).mask

	dst := i64RegData{}
	retmask := uint16(0)

	op := bcword(bc, pc-2)
	internalTypeOffset := (int(op) - int(opcmpv)) * 16

	if internalTypeOffset < 0 || internalTypeOffset > 32 {
		panic("invalid opcode mapping: internalTypeOffset is greater than 32")
	}

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		tlv1 := val1.typeL[i]
		tlv2 := val2.typeL[i]

		ionType1 := ion.Type(tlv1 >> 4)
		ionType2 := ion.Type(tlv2 >> 4)

		internalType1 := ionTypeToCmpTypeTable[internalTypeOffset+int(ionType1)]
		internalType2 := ionTypeToCmpTypeTable[internalTypeOffset+int(ionType2)]
		combined := internalType1 | internalType2

		if (combined & cmpTypeNonComparable) != 0 {
			// Non-comparable types yield NOTHING.
			continue
		}

		if internalType1 == internalType2 {
			result := int64(0)
			internalType := internalType1 & 0x0F

			mem1 := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			mem2 := vmref{val2.offsets[i], val2.sizes[i]}.mem()

			switch internalType {
			case cmpTypeBool:
				result = int64(tlv1&1) - int64(tlv2&1)

			case cmpTypeNumber:
				if ionType1 == ion.FloatType {
					val1, _, _ := ion.ReadFloat64(mem1)
					if ionType2 == ion.FloatType {
						val2, _, _ := ion.ReadFloat64(mem2)
						result = cmpf64(val1, val2)
					} else if ionType2 == ion.IntType {
						val2, _, _ := ion.ReadInt(mem2)
						result = cmpf64(val1, float64(val2))
					} else if ionType2 == ion.UintType {
						val2, _, _ := ion.ReadUint(mem2)
						result = cmpf64(val1, float64(val2))
					}
				} else if ionType1 == ion.IntType {
					val1, _, _ := ion.ReadInt(mem1)
					if ionType2 == ion.FloatType {
						val2, _, _ := ion.ReadFloat64(mem2)
						result = cmpf64(float64(val1), val2)
					} else if ionType2 == ion.IntType {
						val2, _, _ := ion.ReadInt(mem2)
						result = cmpi64(val1, val2)
					} else if ionType2 == ion.UintType {
						result = -1
					}
				} else if ionType1 == ion.UintType {
					val1, _, _ := ion.ReadUint(mem1)
					if ionType2 == ion.FloatType {
						val2, _, _ := ion.ReadFloat64(mem2)
						result = cmpf64(float64(val1), val2)
					} else if ionType2 == ion.IntType {
						result = 1
					} else if ionType2 == ion.UintType {
						val2, _, _ := ion.ReadUint(mem2)
						result = cmpu64(val1, val2)
					}
				}

			case cmpTypeTimestamp, cmpTypeString:
				data1 := mem1[val1.headerSize[i]:]
				data2 := mem2[val2.headerSize[i]:]

				// Unsymbolize first.
				if ionType1 == ion.SymbolType {
					symbol1, _, _ := ion.ReadSymbol(mem1)
					mem1 = bc.symtab[symbol1].mem()
					data1, _ = ion.Contents(mem1)
				}

				if ionType2 == ion.SymbolType {
					symbol2, _, _ := ion.ReadSymbol(mem2)
					mem2 = bc.symtab[symbol2].mem()
					data2, _ = ion.Contents(mem2)
				}

				result = int64(bytes.Compare(data1, data2))
			}

			dst.values[i] = result
			retmask |= 1 << i
			continue
		}

		if combined >= cmpTypeSort {
			dst.values[i] = min(max(int64(internalType1)-int64(internalType2), -1), 1)
			retmask |= 1 << i
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 10
}

func bccmpeqvgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+2)
	val2 := argptr[vRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		tlv1 := val1.typeL[i]
		tlv2 := val2.typeL[i]

		ionType1 := ion.Type(tlv1 >> 4)
		ionType2 := ion.Type(tlv2 >> 4)

		mem1 := vmref{val1.offsets[i], val1.sizes[i]}.mem()
		mem2 := vmref{val2.offsets[i], val2.sizes[i]}.mem()

		// Unsymbolize only if one datum is symbol and the other string.
		if ionType1 == ion.SymbolType && ionType2 == ion.StringType {
			symbol1, _, _ := ion.ReadSymbol(mem1)
			mem1 = bc.symtab[symbol1].mem()
		} else if ionType1 == ion.StringType && ionType2 == ion.SymbolType {
			symbol2, _, _ := ion.ReadSymbol(mem2)
			mem2 = bc.symtab[symbol2].mem()
		}

		if bytes.Equal(mem1, mem2) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpeqvimmgo(bc *bytecode, pc int) int {
	val := argptr[vRegData](bc, pc+2)

	immOffset := bcword32(bc, pc+4) + bc.scratchoff
	immSize := bcword32(bc, pc+8)
	tlv2 := bcword8(bc, pc+12)
	immType := ion.Type(tlv2 >> 4)
	immMem := vmref{immOffset, immSize}.mem()

	immUnsymbolized := immMem
	if immType == ion.SymbolType {
		immSymbol, _, _ := ion.ReadSymbol(immMem)
		immUnsymbolized = bc.symtab[immSymbol].mem()
	}

	mask := argptr[kRegData](bc, pc+14).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		valTLV := val.typeL[i]
		valType := ion.Type(valTLV >> 4)
		valMem := vmref{val.offsets[i], val.sizes[i]}.mem()

		if valType == ion.SymbolType {
			if immType == ion.StringType {
				// Unsymbolize only if the first datum is
				// a symbol and the second is a string.
				symbol1, _, _ := ion.ReadSymbol(valMem)
				valMem = bc.symtab[symbol1].mem()
			} else {
				if bytes.Equal(valMem, immUnsymbolized) {
					retmask |= 1 << i
					continue
				}
			}
		}

		if bytes.Equal(valMem, immMem) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 16
}

func bccmpvkgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	val2 := argptr[kRegData](bc, pc+6).mask
	mask := argptr[kRegData](bc, pc+8).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		tlv1 := val1.typeL[i]
		if mask&(1<<i) == 0 || ion.Type(tlv1>>4) != ion.BoolType {
			continue
		}

		a := byte(tlv1 & 0x1)
		b := byte((val2 >> i) & 0x1)

		dst.values[i] = int64(a) - int64(b)
		retmask |= 1 << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 10
}

func bccmpvkimmgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	imm2 := bcword(bc, pc+6) & 0x1
	mask := argptr[kRegData](bc, pc+8).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		tlv1 := val1.typeL[i]
		if mask&(1<<i) == 0 || ion.Type(tlv1>>4) != ion.BoolType {
			continue
		}

		a := byte(tlv1 & 0x1)

		dst.values[i] = int64(a) - int64(imm2)
		retmask |= 1 << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 10
}

func bccmpvi64go(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)
	mask := argptr[kRegData](bc, pc+8).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		start := val1.offsets[i]
		width := val1.sizes[i]
		if width == 0 {
			continue
		}

		mem := vmref{start, width}.mem()
		rv, rm := cmpvi64f64(mem, val1.typeL[i], val2.values[i], float64(val2.values[i]))

		dst.values[i] = rv
		retmask |= rm << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 10
}

func bccmpvi64immgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	immi64 := int64(bcword64(bc, pc+6))
	immf64 := float64(immi64)
	mask := argptr[kRegData](bc, pc+14).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		start := val1.offsets[i]
		width := val1.sizes[i]
		if width == 0 {
			continue
		}

		mem := vmref{start, width}.mem()
		rv, rm := cmpvi64f64(mem, val1.typeL[i], immi64, immf64)

		dst.values[i] = rv
		retmask |= rm << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 16
}

func bccmpvf64go(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	val2 := argptr[f64RegData](bc, pc+6)
	mask := argptr[kRegData](bc, pc+8).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		start := val1.offsets[i]
		width := val1.sizes[i]
		if width == 0 {
			continue
		}

		mem := vmref{start, width}.mem()
		rv, rm := cmpvi64f64(mem, val1.typeL[i], int64(val2.values[i]), val2.values[i])

		dst.values[i] = rv
		retmask |= rm << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 10
}

func bccmpvf64immgo(bc *bytecode, pc int) int {
	val1 := argptr[vRegData](bc, pc+4)
	immf64 := bcfloat64(bc, pc+6)
	immi64 := int64(immf64)
	mask := argptr[kRegData](bc, pc+14).mask

	dst := i64RegData{}
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}

		start := val1.offsets[i]
		width := val1.sizes[i]
		if width == 0 {
			continue
		}

		mem := vmref{start, width}.mem()
		rv, rm := cmpvi64f64(mem, val1.typeL[i], immi64, immf64)

		dst.values[i] = rv
		retmask |= rm << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}
	return pc + 16
}

func bccmpeqslicego(bc *bytecode, pc int) int {
	val1 := argptr[sRegData](bc, pc+2)
	val2 := argptr[sRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask & (1 << i)) != 0 {
			a := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			b := vmref{val2.offsets[i], val2.sizes[i]}.mem()
			if bytes.Equal(a, b) {
				retmask |= 1 << i
			}
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpltstrgo(bc *bytecode, pc int) int {
	val1 := argptr[sRegData](bc, pc+2)
	val2 := argptr[sRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask & (1 << i)) != 0 {
			a := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			b := vmref{val2.offsets[i], val2.sizes[i]}.mem()
			if bytes.Compare(a, b) < 0 {
				retmask |= 1 << i
			}
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmplestrgo(bc *bytecode, pc int) int {
	val1 := argptr[sRegData](bc, pc+2)
	val2 := argptr[sRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask & (1 << i)) != 0 {
			a := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			b := vmref{val2.offsets[i], val2.sizes[i]}.mem()
			if bytes.Compare(a, b) <= 0 {
				retmask |= 1 << i
			}
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgtstrgo(bc *bytecode, pc int) int {
	val1 := argptr[sRegData](bc, pc+2)
	val2 := argptr[sRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask & (1 << i)) != 0 {
			a := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			b := vmref{val2.offsets[i], val2.sizes[i]}.mem()
			if bytes.Compare(a, b) > 0 {
				retmask |= 1 << i
			}
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgestrgo(bc *bytecode, pc int) int {
	val1 := argptr[sRegData](bc, pc+2)
	val2 := argptr[sRegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask & (1 << i)) != 0 {
			a := vmref{val1.offsets[i], val1.sizes[i]}.mem()
			b := vmref{val2.offsets[i], val2.sizes[i]}.mem()
			if bytes.Compare(a, b) >= 0 {
				retmask |= 1 << i
			}
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

// `A < B` => C (simplify to C = !A & B)
// ------------
// `0 < 0` => 0
// `0 < 1` => 1
// `1 < 0` => 0
// `1 < 1` => 0
func bccmpltkgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	val2 := argptr[kRegData](bc, pc+4).mask
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(^val1 & val2) & mask}
	return pc + 8
}

func bccmpltkimmgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	imm2 := uint16(bcword(bc, pc+4))
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(^val1 & imm2) & mask}
	return pc + 8
}

// `A <= B` => C (simplify to C = !A | B)
// -------------
// `0 <= 0` => 1
// `0 <= 1` => 1
// `1 <= 0` => 0
// `1 <= 1` => 1
func bccmplekgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	val2 := argptr[kRegData](bc, pc+4).mask
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(^val1 | val2) & mask}
	return pc + 8
}

func bccmplekimmgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	imm2 := uint16(bcword(bc, pc+4))
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(^val1 | imm2) & mask}
	return pc + 8
}

// `A > B` => C (simplify to C = A & !B)
// ------------
// `0 > 0` => 0
// `0 > 1` => 0
// `1 > 0` => 1
// `1 > 1` => 0
func bccmpgtkgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	val2 := argptr[kRegData](bc, pc+4).mask
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(val1 & ^val2) & mask}
	return pc + 8
}

func bccmpgtkimmgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	imm2 := uint16(bcword(bc, pc+4))
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(val1 & ^imm2) & mask}
	return pc + 8
}

// `A >= B` => C (simplify to C = A | !B)
// -------------
// `0 >= 0` => 1
// `0 >= 1` => 0
// `1 >= 0` => 1
// `1 >= 1` => 1
func bccmpgekgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	val2 := argptr[kRegData](bc, pc+4).mask
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(val1 | ^val2) & mask}
	return pc + 8
}

func bccmpgekimmgo(bc *bytecode, pc int) int {
	val1 := argptr[kRegData](bc, pc+2).mask
	imm2 := uint16(bcword(bc, pc+4))
	mask := argptr[kRegData](bc, pc+6).mask

	*argptr[kRegData](bc, pc) = kRegData{(val1 | ^imm2) & mask}
	return pc + 8
}

func bccmpeqf64go(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	val2 := argptr[f64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && cmpf64eq(val1.values[i], val2.values[i]) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpeqf64immgo(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	imm2 := bcfloat64(bc, pc+4)
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] == imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpltf64go(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	val2 := argptr[f64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && cmpf64lt(val1.values[i], val2.values[i]) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpltf64immgo(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	imm2 := bcfloat64(bc, pc+4)
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] < imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmplef64go(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	val2 := argptr[f64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && cmpf64le(val1.values[i], val2.values[i]) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmplef64immgo(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	imm2 := bcfloat64(bc, pc+4)
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] <= imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpgtf64go(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	val2 := argptr[f64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && cmpf64gt(val1.values[i], val2.values[i]) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgtf64immgo(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	imm2 := bcfloat64(bc, pc+4)
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] > imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpgef64go(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	val2 := argptr[f64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && cmpf64ge(val1.values[i], val2.values[i]) {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgef64immgo(bc *bytecode, pc int) int {
	val1 := argptr[f64RegData](bc, pc+2)
	imm2 := bcfloat64(bc, pc+4)
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] >= imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpeqi64go(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	val2 := argptr[i64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] == val2.values[i] {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpeqi64immgo(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	imm2 := int64(bcword64(bc, pc+4))
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] == imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmplti64go(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	val2 := argptr[i64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] < val2.values[i] {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmplti64immgo(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	imm2 := int64(bcword64(bc, pc+4))
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] < imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmplei64go(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	val2 := argptr[i64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] <= val2.values[i] {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmplei64immgo(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	imm2 := int64(bcword64(bc, pc+4))
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] <= imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpgti64go(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	val2 := argptr[i64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] > val2.values[i] {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgti64immgo(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	imm2 := int64(bcword64(bc, pc+4))
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] > imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}

func bccmpgei64go(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	val2 := argptr[i64RegData](bc, pc+4)
	mask := argptr[kRegData](bc, pc+6).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] >= val2.values[i] {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 8
}

func bccmpgei64immgo(bc *bytecode, pc int) int {
	val1 := argptr[i64RegData](bc, pc+2)
	imm2 := int64(bcword64(bc, pc+4))
	mask := argptr[kRegData](bc, pc+12).mask

	retmask := uint16(0)
	for i := 0; i < bcLaneCount; i++ {
		if (mask&(1<<i)) != 0 && val1.values[i] >= imm2 {
			retmask |= 1 << i
		}
	}

	*argptr[kRegData](bc, pc) = kRegData{retmask}
	return pc + 14
}
