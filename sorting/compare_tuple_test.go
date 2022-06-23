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
	"testing"
)

type Testcase struct {
	raw1     []byte
	raw2     []byte
	relation int
}

func TestIonValueCompare(t *testing.T) {
	ionNull := []byte{0x0f}
	ionFalse := []byte{0x10}
	ionTrue := []byte{0x11}
	ionPosintVal0 := []byte{0x20}
	ionPosintVal42 := []byte{0x21, 0x2a}
	ionPosintVal123 := []byte{0x21, 0x7b}
	ionNegintVal81 := []byte{0x31, 0x51}
	ionNegintVal981 := []byte{0x32, 0x03, 0xd5}
	ionFloatVal0 := []byte{0x40}
	ionFloat32Valp1 := []byte{0x44, 0x3f, 0x80, 0x00, 0x00}
	ionFloat32Valp10 := []byte{0x44, 0x41, 0x20, 0x00, 0x00}
	ionFloat32Valn2 := []byte{0x44, 0x40, 0x00, 0x00, 0x00}
	ionFloat32Valn20 := []byte{0x44, 0x41, 0xa0, 0x00, 0x00}
	ionFloat64Val0 := []byte{0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ionFloat64Valp1 := []byte{0x48, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ionFloat64Valp10 := []byte{0x48, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ionFloat64Valn2 := []byte{0x48, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ionFloat64Valn20 := []byte{0x48, 0x40, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ionStrTest := []byte{0x84, 0x74, 0x65, 0x73, 0x74}                           // "test"
	ionStrCat := []byte{0x83, 0x63, 0x61, 0x74}                                  // "cat"
	ionTimestamp := []byte{0x68, 0x80, 0x0F, 0xD0, 0x81, 0x81, 0x80, 0x80, 0x80} // 2000-01-01T00:00:00Z with no fractional seconds

	// null
	{
		testcases := []Testcase{
			{ionNull, ionNull, 0},           // null = null
			{ionNull, ionTrue, -1},          // null < true
			{ionNull, ionFalse, -1},         // null < false
			{ionNull, ionPosintVal0, -1},    // null < 0
			{ionNull, ionPosintVal42, -1},   // null < 42
			{ionNull, ionPosintVal123, -1},  // null < 123
			{ionNull, ionNegintVal81, -1},   // null < -81
			{ionNull, ionNegintVal981, -1},  // null < -981
			{ionNull, ionFloatVal0, -1},     // null < float32(0.0)
			{ionNull, ionFloat32Valp1, -1},  // null < float32(1.0)
			{ionNull, ionFloat32Valp10, -1}, // null < float32(10.0)
			{ionNull, ionFloat32Valn2, -1},  // null < float32(2.0)
			{ionNull, ionFloat32Valn20, -1}, // null < float32(20.0)
			{ionNull, ionFloat64Val0, -1},   // null < float64(0.0)
			{ionNull, ionFloat64Valp1, -1},  // null < float64(1.0)
			{ionNull, ionFloat64Valp10, -1}, // null < float64(10.0)
			{ionNull, ionFloat64Valn2, -1},  // null < float64(2.0)
			{ionNull, ionFloat64Valn20, -1}, // null < float64(20.0)
			{ionNull, ionStrTest, -1},       // null < "test"
			{ionNull, ionStrCat, -1},        // null < "cat"
			{ionNull, ionTimestamp, -1},     // null < timestamp
		}

		testRelations(t, testcases)
	}

	// false
	{
		testcases := []Testcase{
			{ionFalse, ionNull, 1},           // false > null
			{ionFalse, ionTrue, -1},          // false < true
			{ionFalse, ionFalse, 0},          // false = false
			{ionFalse, ionPosintVal0, -1},    // false < 0
			{ionFalse, ionPosintVal42, -1},   // false < 42
			{ionFalse, ionPosintVal123, -1},  // false < 123
			{ionFalse, ionNegintVal81, -1},   // false < -81
			{ionFalse, ionNegintVal981, -1},  // false < -981
			{ionFalse, ionFloatVal0, -1},     // false < float32(0.0)
			{ionFalse, ionFloat32Valp1, -1},  // false < float32(1.0)
			{ionFalse, ionFloat32Valp10, -1}, // false < float32(10.0)
			{ionFalse, ionFloat32Valn2, -1},  // false < float32(2.0)
			{ionFalse, ionFloat32Valn20, -1}, // false < float32(20.0)
			{ionFalse, ionFloat64Val0, -1},   // false < float64(0.0)
			{ionFalse, ionFloat64Valp1, -1},  // false < float64(1.0)
			{ionFalse, ionFloat64Valp10, -1}, // false < float64(10.0)
			{ionFalse, ionFloat64Valn2, -1},  // false < float64(2.0)
			{ionFalse, ionFloat64Valn20, -1}, // false < float64(20.0)
			{ionFalse, ionStrTest, -1},       // false < "test"
			{ionFalse, ionStrCat, -1},        // false < "cat"
			{ionFalse, ionTimestamp, -1},     // false < timestamp
		}

		testRelations(t, testcases)
	}

	// true
	{
		testcases := []Testcase{
			{ionTrue, ionNull, 1},           // true > null
			{ionTrue, ionTrue, 0},           // true = true
			{ionTrue, ionFalse, 1},          // true > false
			{ionTrue, ionPosintVal0, -1},    // true < 0
			{ionTrue, ionPosintVal42, -1},   // true < 42
			{ionTrue, ionPosintVal123, -1},  // true < 123
			{ionTrue, ionNegintVal81, -1},   // true < -81
			{ionTrue, ionNegintVal981, -1},  // true < -981
			{ionTrue, ionFloatVal0, -1},     // true < float32(0.0)
			{ionTrue, ionFloat32Valp1, -1},  // true < float32(1.0)
			{ionTrue, ionFloat32Valp10, -1}, // true < float32(10.0)
			{ionTrue, ionFloat32Valn2, -1},  // true < float32(2.0)
			{ionTrue, ionFloat32Valn20, -1}, // true < float32(20.0)
			{ionTrue, ionFloat64Val0, -1},   // true < float64(0.0)
			{ionTrue, ionFloat64Valp1, -1},  // true < float64(1.0)
			{ionTrue, ionFloat64Valp10, -1}, // true < float64(10.0)
			{ionTrue, ionFloat64Valn2, -1},  // true < float64(2.0)
			{ionTrue, ionFloat64Valn20, -1}, // true < float64(20.0)
			{ionTrue, ionStrTest, -1},       // true < "test"
			{ionTrue, ionStrCat, -1},        // true < "cat"
			{ionTrue, ionTimestamp, -1},     // false < timestamp
		}

		testRelations(t, testcases)
	}

	// integer zero
	{
		testcases := []Testcase{
			{ionPosintVal0, ionNull, 1},           // 0 > null
			{ionPosintVal0, ionTrue, 1},           // 0 > true
			{ionPosintVal0, ionFalse, 1},          // 0 > false
			{ionPosintVal0, ionPosintVal0, 0},     // 0 = 0
			{ionPosintVal0, ionPosintVal42, -1},   // 0 < 42
			{ionPosintVal0, ionPosintVal123, -1},  // 0 < 123
			{ionPosintVal0, ionNegintVal81, 1},    // 0 < -81
			{ionPosintVal0, ionNegintVal981, 1},   // 0 < -981
			{ionPosintVal0, ionFloatVal0, 0},      // 0 = float32(0.0)
			{ionPosintVal0, ionFloat32Valp1, -1},  // 0 < float32(1.0)
			{ionPosintVal0, ionFloat32Valp10, -1}, // 0 < float32(10.0)
			{ionPosintVal0, ionFloat32Valn2, -1},  // 0 < float32(2.0)
			{ionPosintVal0, ionFloat32Valn20, -1}, // 0 < float32(20.0)
			{ionPosintVal0, ionFloat64Val0, 0},    // 0 = float64(0.0)
			{ionPosintVal0, ionFloat64Valp1, -1},  // 0 < float64(1.0)
			{ionPosintVal0, ionFloat64Valp10, -1}, // 0 < float64(10.0)
			{ionPosintVal0, ionFloat64Valn2, -1},  // 0 < float64(2.0)
			{ionPosintVal0, ionFloat64Valn20, -1}, // 0 < float64(20.0)
			{ionPosintVal0, ionStrTest, -1},       // 0 < "test"
			{ionPosintVal0, ionStrCat, -1},        // 0 < "cat"
			{ionPosintVal0, ionTimestamp, -1},     // 0 < timestamp
		}

		testRelations(t, testcases)
	}

	// positive integer
	{
		testcases := []Testcase{
			{ionPosintVal42, ionNull, 1},          // 42 > null
			{ionPosintVal42, ionTrue, 1},          // 42 > true
			{ionPosintVal42, ionFalse, 1},         // 42 > false
			{ionPosintVal42, ionPosintVal0, 1},    // 42 > 0
			{ionPosintVal42, ionPosintVal42, 0},   // 42 = 42
			{ionPosintVal42, ionPosintVal123, -1}, // 42 < 123
			{ionPosintVal42, ionNegintVal81, 1},   // 42 > -81
			{ionPosintVal42, ionNegintVal981, 1},  // 42 > -981
			{ionPosintVal42, ionFloatVal0, 1},     // 42 > float32(0.0)
			{ionPosintVal42, ionFloat32Valp1, 1},  // 42 > float32(1.0)
			{ionPosintVal42, ionFloat32Valp10, 1}, // 42 > float32(10.0)
			{ionPosintVal42, ionFloat32Valn2, 1},  // 42 > float32(2.0)
			{ionPosintVal42, ionFloat32Valn20, 1}, // 42 > float32(20.0)
			{ionPosintVal42, ionFloat64Val0, 1},   // 42 > float64(0.0)
			{ionPosintVal42, ionFloat64Valp1, 1},  // 42 > float64(1.0)
			{ionPosintVal42, ionFloat64Valp10, 1}, // 42 > float64(10.0)
			{ionPosintVal42, ionFloat64Valn2, 1},  // 42 > float64(2.0)
			{ionPosintVal42, ionFloat64Valn20, 1}, // 42 > float64(20.0)
			{ionPosintVal42, ionStrTest, -1},      // 42 < "test"
			{ionPosintVal42, ionStrCat, -1},       // 42 < "cat"
			{ionPosintVal42, ionTimestamp, -1},    // 42 < timestamp
		}

		testRelations(t, testcases)
	}

	// negative integer
	{
		testcases := []Testcase{
			{ionNegintVal81, ionNull, 1},           // -81 > null
			{ionNegintVal81, ionTrue, 1},           // -81 > true
			{ionNegintVal81, ionFalse, 1},          // -81 > false
			{ionNegintVal81, ionPosintVal0, -1},    // -81 < 0
			{ionNegintVal81, ionPosintVal42, -1},   // -81 < 42
			{ionNegintVal81, ionPosintVal123, -1},  // -81 < 123
			{ionNegintVal81, ionNegintVal81, 0},    // -81 = -81
			{ionNegintVal81, ionNegintVal981, 1},   // -81 > -981
			{ionNegintVal81, ionFloatVal0, -1},     // -81 < float32(0.0)
			{ionNegintVal81, ionFloat32Valp1, -1},  // -81 < float32(1.0)
			{ionNegintVal81, ionFloat32Valp10, -1}, // -81 < float32(10.0)
			{ionNegintVal81, ionFloat32Valn2, -1},  // -81 < float32(2.0)
			{ionNegintVal81, ionFloat32Valn20, -1}, // -81 < float(20.0)
			{ionNegintVal81, ionFloat64Val0, -1},   // -81 < float64(0.0)
			{ionNegintVal81, ionFloat64Valp1, -1},  // -81 < float64(1.0)
			{ionNegintVal81, ionFloat64Valp10, -1}, // -81 < float64(10.0)
			{ionNegintVal81, ionFloat64Valn2, -1},  // -81 < float64(2.0)
			{ionNegintVal81, ionFloat64Valn20, -1}, // -81 < float64(20.0)
			{ionNegintVal81, ionStrTest, -1},       // -81 < "test"
			{ionNegintVal81, ionStrCat, -1},        // -81 < "cat"
			{ionNegintVal81, ionTimestamp, -1},     // -81 < "cat"
		}

		testRelations(t, testcases)
	}

	// float32
	{
		testcases := []Testcase{
			{ionFloat32Valp1, ionNull, 1},           // float32(1.0) > null
			{ionFloat32Valp1, ionTrue, 1},           // float32(1.0) > true
			{ionFloat32Valp1, ionFalse, 1},          // float32(1.0) > false
			{ionFloat32Valp1, ionPosintVal0, 1},     // float32(1.0) > 0
			{ionFloat32Valp1, ionPosintVal42, -1},   // float32(1.0) < 42
			{ionFloat32Valp1, ionPosintVal123, -1},  // float32(1.0) < 123
			{ionFloat32Valp1, ionNegintVal81, 1},    // float32(1.0) > -81
			{ionFloat32Valp1, ionNegintVal981, 1},   // float32(1.0) > -981
			{ionFloat32Valp1, ionFloatVal0, 1},      // float32(1.0) > float32(0.0)
			{ionFloat32Valp1, ionFloat32Valp1, 0},   // float32(1.0) = float32(1.0)
			{ionFloat32Valp1, ionFloat32Valp10, -1}, // float32(1.0) < float32(10.0)
			{ionFloat32Valp1, ionFloat32Valn2, -1},  // float32(1.0) < float32(2.0)
			{ionFloat32Valp1, ionFloat32Valn20, -1}, // float32(1.0) < float(20.0)
			{ionFloat32Valp1, ionFloat64Val0, 1},    // float32(1.0) > float64(0.0)
			{ionFloat32Valp1, ionFloat64Valp1, 0},   // float32(1.0) = float64(1.0)
			{ionFloat32Valp1, ionFloat64Valp10, -1}, // float32(1.0) < float64(10.0)
			{ionFloat32Valp1, ionFloat64Valn2, -1},  // float32(1.0) < float64(2.0)
			{ionFloat32Valp1, ionFloat64Valn20, -1}, // float32(1.0) < float(20.0)
			{ionFloat32Valp1, ionStrTest, -1},       // float32(1.0) < "test"
			{ionFloat32Valp1, ionStrCat, -1},        // float32(1.0) < "cat"
			{ionFloat32Valp1, ionTimestamp, -1},     // float32(1.0) < timestamp
		}

		testRelations(t, testcases)
	}

	// float64
	{
		testcases := []Testcase{
			{ionFloat64Valp10, ionNull, 1},           // float64(10.0) > null
			{ionFloat64Valp10, ionTrue, 1},           // float64(10.0) > true
			{ionFloat64Valp10, ionFalse, 1},          // float64(10.0) > false
			{ionFloat64Valp10, ionPosintVal0, 1},     // float64(10.0) > 0
			{ionFloat64Valp10, ionPosintVal42, -1},   // float64(10.0) < 42
			{ionFloat64Valp10, ionPosintVal123, -1},  // float64(10.0) < 123
			{ionFloat64Valp10, ionNegintVal81, 1},    // float64(10.0) > -81
			{ionFloat64Valp10, ionNegintVal981, 1},   // float64(10.0) > -981
			{ionFloat64Valp10, ionFloatVal0, 1},      // float64(10.0) > float32(0.0)
			{ionFloat64Valp10, ionFloat32Valp1, 1},   // float64(10.0) > float32(1.0)
			{ionFloat64Valp10, ionFloat32Valp10, 0},  // float64(10.0) = float32(10.0)
			{ionFloat64Valp10, ionFloat32Valn2, 1},   // float64(10.0) > float32(2.0)
			{ionFloat64Valp10, ionFloat32Valn20, -1}, // float64(10.0) < float32(20.0)
			{ionFloat64Valp10, ionFloat64Val0, 1},    // float64(10.0) > float64(0.0)
			{ionFloat64Valp10, ionFloat64Valp1, 1},   // float64(10.0) > float64(1.0)
			{ionFloat64Valp10, ionFloat64Valp10, 0},  // float64(10.0) = float64(10.0)
			{ionFloat64Valp10, ionFloat64Valn2, 1},   // float64(10.0) > float64(2.0)
			{ionFloat64Valp10, ionFloat64Valn20, -1}, // float64(10.0) < float64(20.0)
			{ionFloat64Valp10, ionStrTest, -1},       // float64(10.0) < "test"
			{ionFloat64Valp10, ionStrCat, -1},        // float64(10.0) < "cat"
			{ionFloat64Valp10, ionTimestamp, -1},     // float64(10.0) < timestamp
		}

		testRelations(t, testcases)
	}

	// string
	{
		testcases := []Testcase{
			{ionStrCat, ionNull, 1},          // "cat" > null
			{ionStrCat, ionTrue, 1},          // "cat" > true
			{ionStrCat, ionFalse, 1},         // "cat" > false
			{ionStrCat, ionPosintVal0, 1},    // "cat" > 0
			{ionStrCat, ionPosintVal42, 1},   // "cat" > 42
			{ionStrCat, ionPosintVal123, 1},  // "cat" > 123
			{ionStrCat, ionNegintVal81, 1},   // "cat" > -81
			{ionStrCat, ionNegintVal981, 1},  // "cat" > -981
			{ionStrCat, ionFloatVal0, 1},     // "cat" > float32(0.0)
			{ionStrCat, ionFloat32Valp1, 1},  // "cat" > float32(1.0)
			{ionStrCat, ionFloat32Valp10, 1}, // "cat" > float32(10.0)
			{ionStrCat, ionFloat32Valn2, 1},  // "cat" > float32(2.0)
			{ionStrCat, ionFloat32Valn20, 1}, // "cat" > float32(20.0)
			{ionStrCat, ionFloat64Val0, 1},   // "cat" > float64(0.0)
			{ionStrCat, ionFloat64Valp1, 1},  // "cat" > float64(1.0)
			{ionStrCat, ionFloat64Valp10, 1}, // "cat" > float64(10.0)
			{ionStrCat, ionFloat64Valn2, 1},  // "cat" > float64(2.0)
			{ionStrCat, ionFloat64Valn20, 1}, // "cat" > float64(20.0)
			{ionStrCat, ionStrTest, -1},      // "cat" < "test"
			{ionStrCat, ionStrCat, 0},        // "cat" = "cat"
			{ionStrCat, ionTimestamp, 1},     // "cat" > timestamp
		}

		testRelations(t, testcases)
	}

	// timestamp
	{
		testcases := []Testcase{
			{ionTimestamp, ionNull, 1},          // timestamp > null
			{ionTimestamp, ionTrue, 1},          // timestamp > true
			{ionTimestamp, ionFalse, 1},         // timestamp > false
			{ionTimestamp, ionPosintVal0, 1},    // timestamp > 0
			{ionTimestamp, ionPosintVal42, 1},   // timestamp > 42
			{ionTimestamp, ionPosintVal123, 1},  // timestamp > 123
			{ionTimestamp, ionNegintVal81, 1},   // timestamp > -81
			{ionTimestamp, ionNegintVal981, 1},  // timestamp > -981
			{ionTimestamp, ionFloatVal0, 1},     // timestamp > float32(0.0)
			{ionTimestamp, ionFloat32Valp1, 1},  // timestamp > float32(1.0)
			{ionTimestamp, ionFloat32Valp10, 1}, // timestamp > float32(10.0)
			{ionTimestamp, ionFloat32Valn2, 1},  // timestamp > float32(2.0)
			{ionTimestamp, ionFloat32Valn20, 1}, // timestamp > float32(20.0)
			{ionTimestamp, ionFloat64Val0, 1},   // timestamp > float64(0.0)
			{ionTimestamp, ionFloat64Valp1, 1},  // timestamp > float64(1.0)
			{ionTimestamp, ionFloat64Valp10, 1}, // timestamp > float64(10.0)
			{ionTimestamp, ionFloat64Valn2, 1},  // timestamp > float64(2.0)
			{ionTimestamp, ionFloat64Valn20, 1}, // timestamp > float64(20.0)
			{ionTimestamp, ionStrTest, -1},      // timestamp < "test"
			{ionTimestamp, ionTimestamp, 0},     // timestamp == timestamp
		}

		testRelations(t, testcases)
	}

}

func testRelations(t *testing.T, testcases []Testcase) {
	for i, testcase := range testcases {
		rel := compareIonValues(testcase.raw1, testcase.raw2, Ascending, NullsFirst)
		if rel != testcase.relation {
			t.Errorf("#%d Comparison of %02x with %02x yielded relation %d, should be %d",
				i, testcase.raw1, testcase.raw2, rel, testcase.relation)
		}
	}
}

func TestTupleCompare(t *testing.T) {
	ionNull := []byte{0x0f}
	ionFalse := []byte{0x10}
	ionTrue := []byte{0x11}
	ionPosintVal0 := []byte{0x20}
	ionPosintVal42 := []byte{0x21, 0x2a}
	ionPosintVal123 := []byte{0x21, 0x7b}
	ionFloatVal0 := []byte{0x40}
	ionFloat32Valp10 := []byte{0x44, 0x41, 0x20, 0x00, 0x00}
	ionStrTest := []byte{0x84, 0x74, 0x65, 0x73, 0x74} // "test"
	ionStrCat := []byte{0x83, 0x63, 0x61, 0x74}        // "cat"

	type Testcase struct {
		tuple1     ionTuple
		tuple2     ionTuple
		directions []Direction
		nullsOrder []NullsOrder
		relation   int
		index      int
	}

	testcases := []Testcase{
		// ("test", 42, true) = ("test", 42, true)
		{makeionTuple(ionStrTest, ionPosintVal42, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionTrue),
			[]Direction{Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst},
			0, -1},

		// ("test", 42, true) > ("test", 42, false)
		{makeionTuple(ionStrTest, ionPosintVal42, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFalse),
			[]Direction{Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst},
			1, 2},

		// ("test", 42, 0.0, true) < ("test", 42, 10.0, true)
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloat32Valp10, ionTrue),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			-1, 2},

		// ("test", 42, 0.0, null) > ("test", 42, 0.0, true) [nulls last]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsLast},
			1, 3},

		// ("test", 42, 0.0, null) < ("test", 42, 0.0, true) [asc nulls first]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionFalse),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			-1, 3},

		// (0, 42, 123, 0, 42, 123) > (0, 42, 123, 0, 42, 42)
		{makeionTuple(ionPosintVal0, ionPosintVal42, ionPosintVal123,
			ionPosintVal0, ionPosintVal42, ionPosintVal123),
			makeionTuple(ionPosintVal0, ionPosintVal42, ionPosintVal123,
				ionPosintVal0, ionPosintVal42, ionPosintVal42),
			[]Direction{Ascending, Ascending, Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			1, 5},

		// (0, 42, "cat", 123) < (0, 42, "test", 123)
		{makeionTuple(ionPosintVal0, ionPosintVal42, ionStrCat, ionPosintVal123),
			makeionTuple(ionPosintVal0, ionPosintVal42, ionStrTest, ionPosintVal123),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			-1, 2},

		// ("test", 42, 0.0, true) > ("test", 42, 0.0, null) [asc nulls first]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			1, 3},

		// ("test", 42, 0.0, true) < ("test", 42, 0.0, null) [asc nulls last]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			[]Direction{Ascending, Ascending, Ascending, Ascending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsLast},
			-1, 3},

		// ("test", 42, 0.0, true) < ("test", 42, 0.0, null) [desc nulls first]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			[]Direction{Ascending, Ascending, Ascending, Descending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsFirst},
			-1, 3},

		// ("test", 42, 0.0, true) > ("test", 42, 0.0, null) [desc nulls last]
		{makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionTrue),
			makeionTuple(ionStrTest, ionPosintVal42, ionFloatVal0, ionNull),
			[]Direction{Ascending, Ascending, Ascending, Descending},
			[]NullsOrder{NullsFirst, NullsFirst, NullsFirst, NullsLast},
			1, 3},
	}

	for i, testcase := range testcases {
		rel, index := compareEquallySizedTuples(testcase.tuple1, testcase.tuple2,
			testcase.directions, testcase.nullsOrder)

		if rel != testcase.relation {
			t.Errorf("#%d Comparison of %v with %v yielded relation %d, should be %d",
				i, testcase.tuple1, testcase.tuple2, rel, testcase.relation)
		}

		if rel != 0 && index != testcase.index {
			t.Errorf("#%d Comparison of %v with %v yielded index %d, should be %d",
				i, testcase.tuple1, testcase.tuple2, index, testcase.index)
		}
	}
}

func makeionTuple(b ...[]byte) ionTuple {
	return ionTuple{rawFields: b}
}
