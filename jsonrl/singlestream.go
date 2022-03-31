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
	"fmt"
	"io"
	"sync"
)

// MultiWriter is an interface
// satisfied by ion output destinations
// that support multi-stream output.
type MultiWriter interface {
	// Open should open a new output stream.
	// All calls to the Write method of
	// the output stream are guaranteed to
	// be of a fixed block alignment.
	// Close will be called on each stream
	// when the blocks are done being written.
	//
	// Calls to Write on the returned io.Writer
	// are allowed to return io.EOF if they
	// would no longer like to receive input.
	Open() (io.WriteCloser, error)
	io.Closer
	CloseError(error)
}

// SimpleWriter is a MultiWriter
// that wraps a single output io.Writer.
type SimpleWriter struct {
	W io.WriteCloser

	lock     sync.Mutex
	refcount int
}

// Open implements MultiWriter.Open
func (s *SimpleWriter) Open() (io.WriteCloser, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.refcount++
	return s, nil
}

// Write implements io.Writer
func (s *SimpleWriter) Write(p []byte) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.refcount == 0 {
		return 0, fmt.Errorf("call to SimpleWriter.Write after Close")
	}
	return s.W.Write(p)
}

// Close implements io.Closer
func (s *SimpleWriter) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.refcount--
	if s.refcount < 0 {
		return s.W.Close()
	}
	return nil
}

func (s *SimpleWriter) CloseError(err error) {}
