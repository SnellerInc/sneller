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

package zion

import (
	"encoding/binary"
	"slices"
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
