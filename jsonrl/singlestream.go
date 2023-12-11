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
