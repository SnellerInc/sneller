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

type stream struct {
	data   []byte
	cursor int
}

func (s *stream) empty() bool {
	return s.cursor >= len(s.data)
}

func (s *stream) remaining() int {
	return len(s.data) - s.cursor
}

func (s *stream) checkFetch(n int) errorCode {
	if k := len(s.data); s.cursor+n > k {
		return ecOutOfInputData
	}
	return ecOK
}

func (s *stream) fetch8() (byte, errorCode) {
	if ec := s.checkFetch(1); ec != ecOK {
		return 0, ec
	}
	r := s.data[s.cursor]
	s.cursor++
	return r, ecOK
}

func (s *stream) fetch16() (uint16, errorCode) {
	if ec := s.checkFetch(2); ec != ecOK {
		return 0, ec
	}
	a := s.data[s.cursor+0]
	b := s.data[s.cursor+1]
	s.cursor += 2
	return (uint16(a) + uint16(b)<<8), ecOK
}

func (s *stream) fetch24() (uint32, errorCode) {
	if ec := s.checkFetch(3); ec != ecOK {
		return 0, ec
	}
	a := s.data[s.cursor+0]
	b := s.data[s.cursor+1]
	c := s.data[s.cursor+2]
	s.cursor += 3
	return (uint32(a) + uint32(b)<<8 + uint32(c)<<16), ecOK
}

func (s *stream) fetchVarUInt() (int, errorCode) {
	a, ec := s.fetch8()
	if ec != ecOK {
		return 0, ec
	}
	if a < 0xfe {
		return int(a), ecOK
	} else if a == 0xfe {
		b, ec := s.fetch16()
		if ec != ecOK {
			return 0, ec
		}
		x0 := int(b & 0xff)
		x1 := int(b >> 8)
		return (x1 * 254) + x0, ecOK
	} else {
		b, ec := s.fetch24()
		if ec != ecOK {
			return 0, ec
		}
		x0 := int(b & 0xff)
		x1 := int((b >> 8) & 0xff)
		x2 := int(b >> 16)
		return (((x2 * 254) + x1) * 254) + x0, ecOK
	}
}

func (s *stream) fetchSequence(n int) ([]byte, errorCode) {
	if ec := s.checkFetch(1); ec != ecOK {
		return nil, ec
	}
	r := s.data[s.cursor : s.cursor+n]
	s.cursor += n
	return r, ecOK
}

type stridType uint8
type streamPack [streamCount]stream

const (
	stridTokens stridType = iota
	stridOffset16
	stridOffset24
	stridVarLitLen
	stridVarMatchLen
	stridLiterals
	streamCount
)
