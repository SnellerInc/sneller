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
