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
	"encoding/binary"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

const ionFloat32 byte = 0x04
const ionFloat64 byte = 0x08
const ionFloatPositiveZero byte = 0x00
const ionBoolFalse byte = 0x00
const ionBoolTrue byte = 0x01
const ionNullIndicator byte = 0x0f

// --------------------------------------------------

func ionParseIntMagnitude(raw []byte) uint64 {
	value, _, err := ion.ReadIntMagnitude(raw)
	if err != nil {
		panic(err)
	}

	return value
}

func ionParseFloat32(raw []byte) float32 {
	value, _, err := ion.ReadFloat32(raw)
	if err != nil {
		panic(err)
	}

	return value
}

func ionParseFloat64(raw []byte) float64 {
	value, _, err := ion.ReadFloat64(raw)
	if err != nil {
		panic(err)
	}

	return value
}

// ionParseSimplifiedTimestamp tries to parse timestamp as a big-endian 8-byte value.
//
// Caveat: if a timestamp has some fields missing, we complete them with zeros.
// Such completed timestamps converted back to `date.Time` (see: `simplifiedTimestampToTime`)
// may substantially different the original timestamp. The reason is `date.Stamp`
// function tries to fix ill-formed timestamps to something more reasonable.
func ionParseSimplifiedTimestamp(raw []byte) (uint64, bool) {
	// The "ideal" format is:
	// - TLV
	// - offset=0   1 byte  (0b1000_0000)
	// - year       2 bytes (0b0yyy_yyyy 0b1yyy_yyyy)
	// - month      1 byte  (0b1mmm_mmmm)
	// - day        1 byte  (0b1ddd_dddd)
	// - hour       1 byte  (0b1hhh_hhhh)
	// - minute     1 byte  (0b1MMM_MMMM)
	// - second     1 byte  (0b1sss_ssss)
	// - no second fractions
	if len(raw) == 0 {
		return 0, false // let the slow-path to set the error
	}

	t, L := ion.DecodeTLV(raw[0])
	if t != ion.TimestampType || L > 8 {
		return 0, false // let the slow-path to set the error
	}

	// the fastest of fast-paths
	var val uint64
	if L == 8 {
		val = binary.BigEndian.Uint64(raw[1:])

		if (val & 0xff80808080808080) == 0x8000808080808080 {
			return val, true
		}
	}

	if cap(raw) > 8 {
		val = binary.BigEndian.Uint64(raw[1:9])
		// there are missing some fields, force them to 0 (0x80 varint)
		switch L {
		case 7: // missing second
			val &= 0xffffffffffffff00
			val |= 0x0000000000000080
		case 6: // missing minute and second
			val &= 0xffffffffffff0000
			val |= 0x0000000000008080
		case 5: // missing hour, minute, second
			val &= 0xffffffffff000000
			val |= 0x0000000000808080
		case 4: // missing day, hour, minute, second
			val &= 0xffffffff00000000
			val |= 0x0000000080808080
		case 3: // missing month, day, hour, minute, second
			val &= 0xffffff0000000000
			val |= 0x0000008080808080
		}

		if (val & 0xff80808080808080) == 0x8000808080808080 {
			return val, true
		}
	}

	// FIXME: deal with shorter buffers (cap < 8)

	return 0, false
}

// simplifiedTimestampToTime interprets integer value as raw Ion timestamp.
func simplifiedTimestampToTime(ts uint64) date.Time {
	tmp := ts >> (5 * 8)
	year := ((tmp & 0xff00) >> 1) | (tmp & 0x7f)
	month := (ts >> (4 * 8)) & 0x7f
	day := (ts >> (3 * 8)) & 0x7f
	hour := (ts >> (2 * 8)) & 0x7f
	min := (ts >> (1 * 8)) & 0x7f
	sec := ts & 0x7f
	nsec := 0

	return date.Date(int(year), int(month), int(day), int(hour),
		int(min), int(sec), nsec)
}

func ionParseTimestamp(raw []byte) date.Time {
	value, _, err := ion.ReadTime(raw)
	if err != nil {
		panic(err)
	}

	return value
}

func ionParseString(raw []byte) string {
	value, _, err := ion.ReadString(raw)
	if err != nil {
		panic(err)
	}

	return value
}
