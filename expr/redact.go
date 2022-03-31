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

package expr

import (
	"encoding/base32"
	"encoding/binary"
	"math"

	"github.com/dchest/siphash"
)

const (
	k0, k1 = 0, 1
)

func redactBuf(buf []byte) uint64 {
	return siphash.Hash(k0, k1, buf)
}

func redactInt(i int64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i))
	res := redactBuf(buf[:])
	return int64(res)
}

func redactFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// don't redact these; couldn't possibly be secret...
		return f
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
	res := redactBuf(buf[:])
	// map onto the range [0, 1)
	return float64(int64(res)) / float64(1<<63)
}

func redactString(s string) string {
	res := redactBuf([]byte(s))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], res)
	return base32.StdEncoding.EncodeToString(buf[:])
}
