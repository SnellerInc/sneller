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

// Convert converts json data from src
// into aligned ion chunks in dst using
// the provided alignment.
//
// If src is a Scanner (i.e. an io.Reader
// that has been wrapped in a bufio.Reader),
// and the Scanner has a buffer capacity of
// at least MaxObjectSize,
// then the Scanner will be used directly.
// Otherwise, src will be wrapped in a bufio.Reader.
//
// Convert does not support translating
// objects above MaxObjectSize.
func Convert(src io.Reader, dst *ion.Chunker, schema *SchemaState) error {
	st := NewState(dst)
	st.schemaState = schema
	var rd Scanner
	if s, ok := src.(Scanner); ok && s.Size() >= MaxObjectSize {
		rd = s
	} else {
		rd = bufio.NewReaderSize(src, MaxObjectSize)
	}
	startsize := startObjectSize
	var snapshot ion.Snapshot
	objn := 0
	for {
		buf, err := rd.Peek(startsize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(buf) == 0 {
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
		var n int
		for {
			st.out.Save(&snapshot)
			n, err = ParseObject(st, buf)
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
