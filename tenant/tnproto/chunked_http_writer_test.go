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

package tnproto

import (
	"bytes"
	"io"
	"testing"
)

type nopWriterCloser struct {
	io.Writer
}

func NopWriterCloser(w io.Writer) io.WriteCloser {
	return &nopWriterCloser{w}
}

func (w *nopWriterCloser) Close() error { return nil }

func TestEmpty(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))
	chunkedWriter := &chunkedHTTPWriter{w: NopWriterCloser(buf), sendFinal: true}
	chunkedWriter.Close()

	if buf.String() != "0\r\n\r\n" {
		t.Fatal("Invalid data")
	}
}

func TestBasic(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))
	chunkedWriter := &chunkedHTTPWriter{w: NopWriterCloser(buf), sendFinal: true}
	chunkedWriter.Write([]byte("HELLO WORLD"))
	chunkedWriter.Close()

	if buf.String() != "b\r\nHELLO WORLD\r\n0\r\n\r\n" {
		t.Fatal("Invalid data")
	}
}
func TestDoubleClose(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))
	chunkedWriter := &chunkedHTTPWriter{w: NopWriterCloser(buf), sendFinal: true}
	chunkedWriter.Write([]byte("HELLO"))
	chunkedWriter.Close()
	chunkedWriter.Close()

	if buf.String() != "5\r\nHELLO\r\n0\r\n\r\n" {
		t.Fatal("Invalid data")
	}
}
