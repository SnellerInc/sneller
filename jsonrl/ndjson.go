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
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/SnellerInc/sneller/ion"
)

// Splitter is configuration
// for splitting newline-delimited json
type Splitter struct {
	// WindowSize is the window with which
	// the Splitter searches for newlines
	// on which to split its input
	WindowSize int
	// MaxParallel is the maximum parallelism
	// with which the input ndjson is translated
	MaxParallel int
	// Alignment is the alignment of output
	// chunks written to Output
	Alignment int
	// Output is the multi-stream output
	// of the translation
	Output MultiWriter

	errc chan error
}

func trim(buf []byte) []byte {
	if len(buf) > 20 {
		buf = buf[:20]
	}
	return buf
}

func (s *Splitter) process(st *state, buf []byte) (int, error) {
	end := bytes.LastIndexByte(buf, '\n')
	if end < 0 {
		// TODO: grow the window if this happens?
		return 0, fmt.Errorf("no trailing newline in buffer")
	}

	buf = buf[:end]
	c := len(buf) + 1 // we are counting the trailing '\n' as processed
	buf = bytes.TrimSpace(buf)
	off := 0
	for off < len(buf) {
		n, err := parseObject(st, buf[off:])
		if err != nil {
			return 0, fmt.Errorf("in text %q %w", trim(buf[off:]), err)
		}
		off += n
		err = st.Commit()
		if err != nil {
			return off, err
		}
	}
	return c, nil
}

func (s *Splitter) splitAt(r io.ReaderAt, off, end int64, rem []int64) error {
	local := make([]byte, s.WindowSize)
	n, err := r.ReadAt(local, off)
	if n == 0 && err != nil {
		return err
	}
	w, err := s.Output.Open()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	defer w.Close()
	cn := &ion.Chunker{W: w, Align: s.Alignment}
	st := newState(cn)
	start := 0
	if off != 0 {
		start = bytes.IndexByte(local[:n], '\n')
		if start < 0 {
			return fmt.Errorf("missing newline inside windowlen %d (objects too large)", n)
		}
		pend := off + int64(start) + 1 // include trailing '\n'
		pstart := rem[len(rem)-1]
		rem := rem[:len(rem)-1]
		go func() {
			s.errc <- s.splitAt(r, pstart, pend, rem)
		}()
	}
	// process can handle *up to* n bytes,
	// but may handle fewer if we read across
	// an object boundary
	n, err = s.process(st, local[start:n])
	if err != nil {
		// EOF from Commit
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("at window offset %d: %w", off+int64(start), err)
	}
	off += int64(n) + int64(start)
	for off < end {
		n, err = r.ReadAt(local, off)
		if n == 0 && err != nil {
			return err
		}
		inner := local[:n]
		if int(end-off) < len(inner) {
			inner = inner[:end-off]
		}
		n, err = s.process(st, inner)
		if err != nil {
			// EOF from Commit
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("at window offset %d: %w", off, err)
		}
		off += int64(n)
	}
	err = cn.Flush()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("flushing at window offset %d %w", off, err)
	}
	return nil
}

// Split processes the given io.ReaderAt
// up to (but not including) the byte at index 'size'
// as newline-delimited JSON
//
// The input data is processed in parallel;
// the value of s.MaxParallel determines
// the maximum level of parallelism at which
// the data is processed.
//
// The s.WindowSize variable determines how
// much data is read from r at once.
// The window size should be significantly
// larger than the maximum size of a line.
func (s *Splitter) Split(r io.ReaderAt, size int64) error {
	if s.MaxParallel == 0 {
		s.MaxParallel = runtime.GOMAXPROCS(0)
	}
	if s.WindowSize == 0 {
		s.WindowSize = 1024 * 1024
	}
	if s.Alignment == 0 {
		s.Alignment = 1024 * 1024
	}
	if s.errc == nil || cap(s.errc) < s.MaxParallel {
		s.errc = make(chan error, s.MaxParallel)
	}
	p := s.MaxParallel
	if windows := int(size / int64(s.WindowSize)); windows < p {
		p = windows
	}
	if p <= 0 {
		p = 1
	}
	offsets := make([]int64, p)
	stride := size / int64(p)
	for i := range offsets {
		offsets[i] = int64(i) * stride
	}
	// each splitting operation starts the preceding
	// one in turn once it discovers an appropriate offset
	// for splitting the data
	go func() {
		s.errc <- s.splitAt(r, offsets[p-1], size, offsets[:p-1])
	}()

	var err error
	for range offsets {
		err1 := <-s.errc
		if err == nil && err1 != nil {
			err = err1
			// do not assume all threads have been spawned, break immediately
			break
		}
	}
	if err != nil {
		s.Output.CloseError(err)
		return err
	}
	return s.Output.Close()
}
