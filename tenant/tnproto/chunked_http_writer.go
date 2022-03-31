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
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

type chunkedHTTPWriter struct {
	w        io.WriteCloser
	isClosed bool
	// because the net/http package
	// sends a final 0\r\n\r\n when
	// a handler returns, we only send
	// the final bit of data in unit testing
	sendFinal bool
}

func (chw *chunkedHTTPWriter) Write(p []byte) (int, error) {
	bytes := len(p)
	if bytes == 0 {
		// don't emit the end-of-stream marker accidentally
		return 0, nil
	}
	if _, err := fmt.Fprintf(chw.w, "%x\r\n", bytes); err != nil {
		return 0, err
	}
	if bytes > 0 {
		if _, err := chw.w.Write(p); err != nil {
			return 0, err
		}
	}
	if _, err := io.WriteString(chw.w, "\r\n"); err != nil {
		return 0, err
	}
	return bytes, nil
}

func (chw *chunkedHTTPWriter) Close() error {
	if chw.isClosed {
		return nil
	}
	var err error
	if chw.sendFinal {
		_, err = io.WriteString(chw.w, "0\r\n\r\n")
	}
	chw.isClosed = true
	chw.w.Close()
	return err
}

type writerCloser struct {
	io.Writer
	io.Closer
}

func httpChunkedJSON(dst io.WriteCloser) io.WriteCloser {
	return &writerCloser{
		Writer: ion.NewJSONWriter(&chunkedHTTPWriter{
			w: dst,
		}, '\n'),
		Closer: dst,
	}
}

type arrayWriter struct {
	*ion.JSONWriter
	final io.Closer
}

func httpJSONArray(dst io.WriteCloser) io.WriteCloser {
	inner := &chunkedHTTPWriter{w: dst}
	return &arrayWriter{
		JSONWriter: ion.NewJSONWriter(inner, ','),
		final:      dst,
	}
}

func (a *arrayWriter) Close() error {
	err := a.JSONWriter.Close()
	err2 := a.final.Close()
	if err == nil {
		err = err2
	}
	return err
}
