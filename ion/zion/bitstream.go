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

package zion

import (
	"encoding/binary"

	"golang.org/x/exp/slices"
)

// shapeEncoder encodes the shape
// of a structure by emitting bucket
// references and shape descriptors
type shapeEncoder struct {
	pos      uint8 // 0 <= pos <= 16
	contents uint64
	output   []byte

	startpos, class int
}

func (s *shapeEncoder) start(sizeclass int) {
	s.startpos = len(s.output)
	s.class = sizeclass
}

// push a byte of data with associated type bits
func (s *shapeEncoder) emit(u byte) {
	s.contents |= uint64(u) << (4 * s.pos)
	s.pos++
	if s.pos == 16 {
		s.flush()
	}
}

func (s *shapeEncoder) finish() {
	s.flush()
	s.output[s.startpos] |= byte(s.class << 6)
	s.startpos = -1
	s.class = -1
}

func (s *shapeEncoder) flush() {
	extra := 1 + 8
	start := len(s.output)
	s.output = slices.Grow(s.output, extra)
	s.output = s.output[:len(s.output)+extra]
	s.output[start] = byte(s.pos)
	binary.LittleEndian.PutUint64(s.output[start+1:], s.contents)
	s.output = s.output[:len(s.output)-(8-((int(s.pos)+1)/2))]
	s.contents = 0
	s.pos = 0
}
