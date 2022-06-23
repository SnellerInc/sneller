// Copyright (C) 2022 Sneller, Inc.
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

package sorting

import (
	"bytes"
	"fmt"

	"github.com/SnellerInc/sneller/ion"
)

type ionTuple struct {
	rawFields [][]byte
}

// Compare two arbitrary tuples and returns an int indicating relation:
// < 0 -- less
// = 0 -- equal
// > 0 -- greater
// Returns relation and the index of element if not equal (when the tuples
// are equal, index is unspecified)
func compareEquallySizedTuples(t1, t2 ionTuple, directions []Direction, nullsOrder []NullsOrder) (relation int, index int) {
	if len(t1.rawFields) != len(t2.rawFields) {
		panic("trying to compare tuples of different sizes")
	}

	if len(t1.rawFields) != len(nullsOrder) {
		panic("trying to compare tuples with nulls order having different size")
	}

	if len(t1.rawFields) != len(directions) {
		panic("trying to compare tuples with directions having different size")
	}

	return compareEquallySizedTuplesUnsafe(t1, t2, directions, nullsOrder)
}

type typesRelation uint8

const (
	alwaysLess typesRelation = iota
	alwaysGreater
	unsupportedRelation
	compareSameType
	compareDifferentTypes
)

// type(val1) rel type(val2)
// type1 is saved on the higher nibble
var typesRelations [256]typesRelation = [256]typesRelation{compareSameType,
	compareDifferentTypes, compareDifferentTypes, compareDifferentTypes,
	compareDifferentTypes, compareDifferentTypes, compareDifferentTypes,
	unsupportedRelation, compareDifferentTypes, compareDifferentTypes,
	compareDifferentTypes, compareDifferentTypes, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	compareDifferentTypes, compareSameType, alwaysLess, alwaysLess, alwaysLess,
	alwaysLess, alwaysLess, unsupportedRelation, alwaysLess, alwaysLess,
	alwaysLess, alwaysLess, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, compareDifferentTypes, alwaysGreater,
	compareSameType, alwaysGreater, compareDifferentTypes, compareDifferentTypes,
	alwaysLess, unsupportedRelation, alwaysLess, alwaysLess, alwaysLess,
	alwaysLess, unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, compareDifferentTypes, alwaysGreater, alwaysLess,
	compareSameType, compareDifferentTypes, compareDifferentTypes, alwaysLess,
	unsupportedRelation, alwaysLess, alwaysLess, alwaysLess, alwaysLess,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, compareDifferentTypes, alwaysGreater,
	compareDifferentTypes, compareDifferentTypes, compareSameType,
	compareDifferentTypes, alwaysLess, unsupportedRelation, alwaysLess, alwaysLess,
	alwaysLess, alwaysLess, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, compareDifferentTypes, alwaysGreater,
	compareDifferentTypes, compareDifferentTypes, compareDifferentTypes,
	compareSameType, alwaysLess, unsupportedRelation, alwaysLess, alwaysLess,
	alwaysLess, alwaysLess, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, compareDifferentTypes, alwaysGreater,
	alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater, compareSameType,
	unsupportedRelation, alwaysLess, alwaysLess, alwaysLess, alwaysLess,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, compareDifferentTypes, alwaysGreater,
	alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater,
	unsupportedRelation, compareSameType, alwaysLess, alwaysLess, alwaysLess,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, compareDifferentTypes, alwaysGreater, alwaysGreater,
	alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater,
	unsupportedRelation, alwaysGreater, compareSameType, compareDifferentTypes,
	alwaysLess, unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, compareDifferentTypes, alwaysGreater, alwaysGreater,
	alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater,
	unsupportedRelation, alwaysGreater, compareDifferentTypes, compareSameType,
	alwaysLess, unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, compareDifferentTypes, alwaysGreater, alwaysGreater,
	alwaysGreater, alwaysGreater, alwaysGreater, alwaysGreater,
	unsupportedRelation, alwaysGreater, alwaysGreater, alwaysGreater,
	compareSameType, unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation, unsupportedRelation,
	unsupportedRelation, unsupportedRelation}

func compareEquallySizedTuplesUnsafe(t1, t2 ionTuple, directions []Direction, nullsOrder []NullsOrder) (relation int, index int) {
	for i := 0; i < len(t1.rawFields); i++ {
		rel := compareIonValues(t1.rawFields[i], t2.rawFields[i], directions[i], nullsOrder[i])
		if rel != 0 {
			return rel, i
		}
	}

	return 0, -1
}

// Ordering defines an ordering for ion values.
type Ordering struct {
	// Nulls determines whether null values
	// are returned first or last in the ordering.
	Nulls NullsOrder
	// Direction determines whether non-null values
	// are sorted in ascending or descending order
	// according to PartiQL global comparison semantics.
	Direction
}

// Compare compares two ion values according
// using the Ordering o.
// Similarly to bytes.Compare, Compare returns
// -1 if a < b, 0 if a == b, or 1 if a > b
func (o Ordering) Compare(a, b []byte) int {
	return int(o.Direction) * compareIonValues(a, b, o.Direction, o.Nulls)
}

func compareIonValues(raw1, raw2 []byte,
	direction Direction, nullsOrder NullsOrder) int {

	type1, l1 := ion.DecodeTLV(raw1[0])
	type2, l2 := ion.DecodeTLV(raw2[0])

	// handle nulls
	if l1 == ionNullIndicator || l2 == ionNullIndicator {
		if l1 == ionNullIndicator && l2 == ionNullIndicator {
			return 0 // null == null
		}
		// null cmp non-null or non-null cmp null
		rel := 1
		if l1 == ionNullIndicator {
			rel = -1
		}

		// change direction only if null last xor descending order
		if (nullsOrder == NullsLast) != (direction == Descending) {
			rel = -rel
		}

		return rel
	}

	// here we're sure no null would be passed down the comparators
	if type1 == type2 {
		switch type1 {
		case ion.BoolType:
			b1 := boolFromLen(l1)
			b2 := boolFromLen(l2)
			if b1 == b2 {
				return 0
			} else if !b1 && b2 {
				return -1
			}
			return 1

		case ion.IntType:
			x1 := ionParseIntMagnitude(raw1)
			x2 := ionParseIntMagnitude(raw2)
			if x1 < x2 {
				return 1 // Note: it's correct, we're comparing magnitudes of negative ints
			} else if x1 > x2 {
				return -1
			}
			return 0

		case ion.UintType:
			var x1 uint64 = 0
			if l1 != 0 {
				x1 = ionParseIntMagnitude(raw1)
			}
			var x2 uint64 = 0
			if l2 != 0 {
				x2 = ionParseIntMagnitude(raw2)
			}

			if x1 < x2 {
				return -1
			} else if x1 > x2 {
				return 1
			}
			return 0

		case ion.FloatType:
			var x1 = 0.0
			if l1 == ionFloat32 {
				x1 = float64(ionParseFloat32(raw1))
			} else if l1 == ionFloat64 {
				x1 = ionParseFloat64(raw1)
			}

			var x2 = 0.0
			if l2 == ionFloat32 {
				x2 = float64(ionParseFloat32(raw2))
			} else if l2 == ionFloat64 {
				x2 = ionParseFloat64(raw2)
			}

			if x1 < x2 {
				return -1
			} else if x1 > x2 {
				return 1
			}
			return 0

		case ion.TimestampType:
			val1, ok1 := ionParseSimplifiedTimestamp(raw1)
			val2, ok2 := ionParseSimplifiedTimestamp(raw2)
			if ok1 && ok2 {
				if val1 < val2 {
					return -1
				} else if val1 > val2 {
					return 1
				}
				return 0
			}

			ts1 := ionParseTimestamp(raw1)
			ts2 := ionParseTimestamp(raw2)
			if ts1.Equal(ts2) {
				return 0
			} else if ts1.Before(ts2) {
				return -1
			}
			return 1

		case ion.StringType:
			// Note: do not create strings, just take views on raw UTF-8 bytes and compare
			s1, _ := ion.Contents(raw1)
			s2, _ := ion.Contents(raw2)
			return int(bytes.Compare(s1, s2))
		}
	}

	codeword := (uint8(type1) << 4) | uint8(type2)
	relation := typesRelations[codeword]
	switch relation {
	case alwaysLess:
		return -1
	case alwaysGreater:
		return 1
	case unsupportedRelation:
		// FIXME: wrong input Ion or types we don't support yet --- panic?
		return 0
	}

	// fallback to a generic runtime comparison (likely: mixed numeric, unlikely: bug)
	switch type1 {
	case ion.IntType:
		return compareNegintWithIonValue(ionParseIntMagnitude(raw1), raw2)

	case ion.UintType:
		if l1 != 0 {
			return comparePosintWithIonValue(ionParseIntMagnitude(raw1), raw2)
		}
		return compareZeroWithIonValue(raw2)

	case ion.FloatType:
		if l1 == ionFloat32 {
			return compareFloat32WithIonValue(ionParseFloat32(raw1), raw2)
		} else if l1 == ionFloat64 {
			return compareFloat64WithIonValue(ionParseFloat64(raw1), raw2)
		} else if l1 == ionFloatPositiveZero {
			return compareZeroWithIonValue(raw2)
		}
		panic("Wrong Ion float encoding")

	default:
		panic(fmt.Sprintf("Unsupported Ion type 0x%02x", type1))
	}
}

func boolFromLen(L byte) bool {
	if L == ionBoolFalse {
		return false
	} else if L == ionBoolTrue {
		return true
	}
	panic(fmt.Sprintf("Wrong Ion bool encoding L=%d", L))
}

func compareZeroWithIonValue(raw2 []byte) int {
	T, L := ion.DecodeTLV(raw2[0])

	switch T {
	case ion.IntType:
		return 1 // 0 > -x

	case ion.UintType:
		mag := ionParseIntMagnitude(raw2)
		if mag == 0 {
			return 0
		}
		return -1 // 0 < x

	case ion.FloatType:
		if L == ionFloat32 {
			x := ionParseFloat32(raw2)
			if 0.0 > x {
				return 1
			} else if 0.0 < x {
				return -1
			}
			return 0
		} else if L == ionFloat64 {
			x := ionParseFloat64(raw2)
			if 0.0 > x {
				return 1
			} else if 0.0 < x {
				return -1
			}
			return 0
		} else if L == ionFloatPositiveZero {
			return 0
		}
		panic("Wrong Ion float encoding")

	default:
		panic(fmt.Sprintf("Unsupported Ion type 0x%02x", T))
	}
}

func compareNegintWithIonValue(x uint64, raw2 []byte) int {
	T, L := ion.DecodeTLV(raw2[0])

	switch T {
	case ion.IntType:
		y := ionParseIntMagnitude(raw2)
		if x == y {
			return 0
		} else if x < y {
			return 1
		}
		return -1

	case ion.UintType:
		return -1 // -x < +y

	case ion.FloatType:
		var y float64
		if L == ionFloat32 {
			y = float64(ionParseFloat32(raw2))
		} else if L == ionFloat64 {
			y = ionParseFloat64(raw2)
		} else if L == ionFloatPositiveZero {
			y = 0.0
		} else {
			panic("Wrong Ion float encoding")
		}

		x := -float64(x)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0

	default:
		panic(fmt.Sprintf("Unsupported Ion type 0x%02x", T))
	}
}

func comparePosintWithIonValue(x uint64, raw2 []byte) int {
	T, L := ion.DecodeTLV(raw2[0])

	switch T {
	case ion.UintType:
		var y uint64
		if L != 0 {
			y = ionParseIntMagnitude(raw2)
		} else {
			y = 0.0
		}

		if x > y {
			return 1
		} else if x < y {
			return -1
		}
		return 0

	case ion.FloatType:
		var y float64
		if L == ionFloat32 {
			y = float64(ionParseFloat32(raw2))
		} else if L == ionFloat64 {
			y = ionParseFloat64(raw2)
		} else if L == ionFloatPositiveZero {
			y = 0.0
		} else {
			panic("Wrong Ion float encoding")
		}

		x := float64(x)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0

	default:
		panic(fmt.Sprintf("Unsupported Ion type 0x%02x", T))
	}
}

func compareFloat32WithIonValue(x float32, raw2 []byte) int {
	return compareFloat64WithIonValue(float64(x), raw2)
}

func compareFloat64WithIonValue(x float64, raw2 []byte) int {
	T, L := ion.DecodeTLV(raw2[0])

	var y float64

	switch T {
	case ion.IntType:
		y = -float64(ionParseIntMagnitude(raw2))

	case ion.UintType:
		if L != 0 {
			y = float64(ionParseIntMagnitude(raw2))
		}

	case ion.FloatType:
		if L == ionFloat32 {
			y = float64(ionParseFloat32(raw2))
		} else if L == ionFloat64 {
			y = ionParseFloat64(raw2)
		} else if L == ionFloatPositiveZero {
			y = 0.0
		} else {
			panic("Wrong Ion float encoding")
		}

	default:
		panic(fmt.Sprintf("Unsupported Ion type 0x%02x", T))
	}

	// compare numeric values
	if x < y {
		return -1
	} else if x > y {
		return 1
	}
	return 0
}
