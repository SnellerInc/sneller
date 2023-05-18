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

package s3

import (
	"encoding/binary"
	"fmt"
	"io"
)

type s3SelectReader struct {
	src   io.ReadCloser
	frame []byte
	pos   int
	atEOF bool
}

func (s *s3SelectReader) skip(n int) error {
	_, err := io.CopyN(io.Discard, s.src, int64(n))
	return err
}

func (s *s3SelectReader) realloc(size int) []byte {
	if cap(s.frame) >= size {
		s.frame = s.frame[:size]
		return s.frame[:size]
	}
	s.frame = make([]byte, size)
	return s.frame
}

func (s *s3SelectReader) load(n int) ([]byte, error) {
	buf := s.realloc(n)
	_, err := io.ReadFull(s.src, buf)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return buf, err
}

func (s *s3SelectReader) readOneFrame() (bool, error) {
	be32 := binary.BigEndian.Uint32
	be16 := binary.BigEndian.Uint16

	s.pos = 0
	prelude, err := s.load(12)
	if err != nil {
		return false, err
	}
	total := int(be32(prelude))
	headerlen := int(be32(prelude[4:]))
	_ = be32(prelude[8:]) // crc

	headers, err := s.load(headerlen)
	if err != nil {
		return false, err
	}

	var errmsg, errcode string
	isError := false
	discard := false

	for len(headers) > 0 {
		namelen := int(headers[0])
		name := headers[1 : namelen+1]
		if headers[namelen+1] != 7 {
			return false, fmt.Errorf("unexpected header type %d", headers[namelen+1])
		}
		vlen := int(be16(headers[namelen+2:]))
		val := headers[namelen+4 : namelen+4+vlen]
		headers = headers[namelen+4+vlen:]

		switch string(name) {
		case ":message-type":
			if string(val) == "error" {
				isError = true
			} // otherwise "event"
		case ":event-type":
			switch string(val) {
			default:
				return false, fmt.Errorf("unexpected :event-type header %q", val)
			case "Cont", "Progress", "Stats":
				discard = true // ignore these
			case "Records":
				// this is the one we actually want
			case "End":
				s.pos = 0
				s.frame = s.frame[:0]
				s.atEOF = true
			}
		case ":error-code":
			errcode = string(val)
		case ":error-message":
			errmsg = string(val)
		case ":content-type":
			// ignored
		default:
			return false, fmt.Errorf("unexpected header %q", name)
		}
		if isError {
			return false, fmt.Errorf("s3 select error: code %s message %s", errcode, errmsg)
		}
	}
	s.frame = s.frame[:0]
	s.pos = 0
	payload := total - headerlen - 16
	if payload == 0 {
		return !discard, s.skip(4)
	}
	if discard {
		return false, s.skip(payload + 4)
	}

	// actually set s.frame to the payload contents
	_, err = s.load(payload + 4)
	if err != nil {
		return false, err
	}
	s.frame = s.frame[:len(s.frame)-4] // eat final crc
	return true, nil
}

func (s *s3SelectReader) readFrame() error {
	for !s.atEOF {
		data, err := s.readOneFrame()
		if err != nil {
			return err
		}
		if data {
			break
		}
	}
	return nil
}

func (s *s3SelectReader) Close() error { return s.src.Close() }

func (s *s3SelectReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) && !s.atEOF {
		if s.pos >= len(s.frame) {
			err := s.readFrame()
			if err != nil {
				return 0, err
			}
			if s.atEOF {
				break
			}
		}
		c := copy(p[n:], s.frame[s.pos:])
		n += c
		s.pos += c
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
