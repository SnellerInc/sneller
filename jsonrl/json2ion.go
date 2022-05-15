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

package jsonrl

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

func isUnfinished(n int, err error, buf []byte) bool {
	return n == len(buf) && errors.Is(err, ErrNoMatch)
}

var startObjectSize = 1024

const MaxObjectSize = 1024 * 1024

var (
	// ErrTooLarge is returned from Convert
	// when the input would require more than
	// MaxObjectSize bytes of buffering in order
	// for a complete object to be parsed.
	ErrTooLarge = errors.New("jsonrl: object too large")
)

// Scanner is the interface used
// by Convert to parse objects.
//
// Ordinarily, this interface is
// implemented by a bufio.Reader,
// but anything that wraps a bufio.Reader
// (for example, io.NopCloser) will also
// work fine.
//
// See Convert.
type Scanner interface {
	// Peek should behave identically
	// to bufio.Reader.Peek
	Peek(n int) ([]byte, error)
	// Discard should behave identically
	// to bufio.Reader.Discard
	Discard(n int) (int, error)
	// Size should behave identically
	// to bufio.Reader.Size
	Size() int
}

// 1-bit state machine for list tokens
type listState int

const (
	// waiting for '['
	listNone listState = iota
	listStart
	listNext
	listObject

	// encountered an error
	listInvalid
)

// determine if we should skip the next
// non-space character and update the
// internal state accordingly
func (l *listState) skip(next byte) bool {
	switch *l {
	case listNone:
		// search for '['
		switch next {
		case '[':
			*l = listStart
			return true
		default:
			// ignore
			return false
		}
	case listStart:
		switch next {
		case ']':
			*l = listNone
			return true
		default:
			*l = listNext
			return false
		}
	case listNext:
		// expect ',' or ']'
		switch next {
		case ',':
			*l = listObject
			return true
		case ']':
			*l = listNone
			return true
		default:
			*l = listInvalid
			return false
		}
	case listObject:
		*l = listNext
		return false
	}
	*l = listInvalid
	return false
}

// Convert converts JSON data from src and writes the data into dst.
// If hints is non-nil, Convert will use hints to determine which portions of
// the original JSON data to write into dst.
//
// If src is a Scanner (i.e. an io.Reader that has been wrapped in a bufio.Reader),
// and the Scanner has a buffer capacity of at least MaxObjectSize,
// then the Scanner will be used directly.
// Otherwise, src will be wrapped in a bufio.Reader.
//
// Convert does not support translating objects above MaxObjectSize.
// If Convert can not find the terminating token of an object
// in src after scanning MaxObjectSize bytes, it will return ErrTooLarge.
func Convert(src io.Reader, dst *ion.Chunker, hints *Hint) error {
	st := newState(dst)
	st.UseHints(hints)
	var rd Scanner
	if s, ok := src.(Scanner); ok && s.Size() >= MaxObjectSize {
		rd = s
	} else {
		rd = bufio.NewReaderSize(src, MaxObjectSize)
	}
	lstate := listNone
	startsize := startObjectSize
	var snapshot ion.Snapshot
	objn := 0
	for {
		buf, err := rd.Peek(startsize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(buf) == 0 {
					if lstate != listNone {
						return fmt.Errorf("%w (no matching ending ']')", ErrNoMatch)
					}
					return nil
				}
			} else if err == bufio.ErrBufferFull {
				return ErrTooLarge
			} else {
				return fmt.Errorf("jsonrl.Convert: %w", err)
			}
		}
		tail := bytes.TrimLeft(buf, " \t\n\v\f\r\u0085\u00A0")
		// permanently drop the leading whitespace
		rd.Discard(len(buf) - len(tail))
		if len(tail) == 0 {
			// we got only whitespace; just
			// keep seeking forwards
			continue
		}
		buf = tail
		if lstate.skip(buf[0]) {
			// if we're processing a list token,
			// continue searching for actual object text
			rd.Discard(1)
			continue
		} else if lstate == listInvalid {
			return fmt.Errorf("%w (unexpected list token %c)", ErrNoMatch, buf[0])
		}
		var n int
		for {
			st.out.Save(&snapshot)
			n, err = parseObject(st, buf)
			if err == nil {
				break
			}
			if !isUnfinished(n, err, buf) {
				return fmt.Errorf("jsonrl.Convert: parsing object %d: %w", objn, err)
			}
			st.rewind(&snapshot)
			startsize *= 2
			buf, err = rd.Peek(startsize)
			if err != nil {
				if errors.Is(err, io.EOF) {
					if len(buf) < startsize/2 {
						return fmt.Errorf("jsonrl.Convert: object %d: pos %d %w", objn, len(buf), ErrNoMatch)
					}
					// otherwise, continue; we got more data
				} else if errors.Is(err, bufio.ErrBufferFull) {
					return ErrTooLarge
				} else {
					return fmt.Errorf("jsonrl.Convert: %w", err)
				}
			}
		}
		err = st.Commit()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // here so that JSON can be ingested inline
			}
			return fmt.Errorf("jsonrl.Convert: committing object: %w", err)
		}
		objn++
		_, err = rd.Discard(n)
		if err != nil {
			// should never happen; we buffered at least
			// as many bytes as we read, so we should
			// be able to discard the appropriate number
			// of bytes here
			panic(err)
		}
	}
	return nil
}
