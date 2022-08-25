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

package vm

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Table represents an ion-encoded collection of rows
type Table interface {
	// Chunks should return the
	// number of chunks of data present
	// in the table, or -1 if the number
	// of chunks is not known in advance.
	Chunks() int

	// WriteChunks should write the table
	// contents into dst using the provided
	// parallelism hint.
	//
	// Each output stream should be created
	// with dst.Open(), followed by zero or
	// more calls to io.WriteCloser.Write, followed
	// by exactly one call to io.WriteCloser.Close.
	// See QuerySink.Open. Each call to io.WriteCloser.Write
	// must be at a "chunk boundary" -- the provided
	// data must begin with an ion BVM plus an ion symbol table
	// and be followed by zero or more ion structures.
	//
	// Typically callers will implement
	// WriteChunks in terms of SplitInput.
	WriteChunks(dst QuerySink, parallel int) error
}

// NewStreamTable turns a stream of unknown length into a Table.
// The alignment boundaries of the chunks must be
// known in advance.
func NewStreamTable(src io.Reader, align int) *StreamTable {
	return &StreamTable{
		src:   src,
		align: align,
	}
}

// StreamTable is a Table implementation
// that wraps an arbitrary-length stream of bytes.
type StreamTable struct {
	lock  sync.Mutex
	src   io.Reader
	ateof bool
	align int
}

// Chunks implements Table.Chunks
func (s *StreamTable) Chunks() int { return -1 }

func readToEOF(r io.Reader, dst []byte) (int, error) {
	n := 0
	for n < len(dst) {
		nn, err := r.Read(dst[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// SplitInput is a helper function for
// writing the implementation of Table.WriteChunks.
// SplitInput calls dst.Open() up to parallel times,
// and then passes the destination to separate calls
// to into() in different goroutines. SplitInput takes
// care of closing the outputs returned from dst.Open()
// and waits for each goroutine to return.
func SplitInput(dst QuerySink, parallel int, into func(io.Writer) error) error {
	merge := func(first, second error) error {
		ret := first
		if ret == nil || errors.Is(ret, io.EOF) {
			ret = second
		}
		return ret
	}
	if parallel <= 1 {
		// Don't use goroutines if there is no parallelism - this makes debugging a bit easier.
		w, err := dst.Open()
		if err != nil {
			return err
		}

		err = into(w)
		return merge(err, w.Close())
	}
	var wg sync.WaitGroup
	errlist := make([]error, parallel)
	opendone := make(chan struct{}, 1)
	for i := 0; i < parallel; i++ {
		w, err := dst.Open()
		if err != nil {
			if i == 0 {
				return err
			}
			// just stop opening
			// more parallel streams
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := into(w)
			// make sure w.Close() is safe to call
			<-opendone
			errlist[i] = merge(err, w.Close())
		}(i)
	}
	// we don't start any children goroutines
	// in earnest until we've completed our calls
	// to Open(); this guarantees that the QuerySink
	// does not have to manage calls to Open() concurrently
	// with calls to Close() for each child thread.
	close(opendone)
	wg.Wait()

	for i := range errlist {
		if errlist[i] != nil {
			return errlist[i]
		}
	}
	return nil
}

// WriteChunks implements Table.WriteChunks
func (s *StreamTable) WriteChunks(dst QuerySink, parallel int) error {
	into := func(w io.Writer) error {
		chunk := Malloc()
		defer Free(chunk)
		for {
			s.lock.Lock()
			if s.ateof {
				s.lock.Unlock()
				return nil
			}
			n, err := readToEOF(s.src, chunk)
			s.ateof = errors.Is(err, io.EOF)
			s.lock.Unlock()
			if n == 0 {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			_, err = w.Write(chunk[:n])
			if err != nil {
				return err
			}
		}
	}
	return SplitInput(dst, parallel, into)
}

// NewReaderAtTable table constructs a ReaderAtTable
// that reads from the provided ReaderAt
// at the specified alignment and up to size bytes.
func NewReaderAtTable(src io.ReaderAt, size int64, align int) *ReaderAtTable {
	return &ReaderAtTable{
		src:   src,
		size:  size,
		align: align,
	}
}

// ReaderAtTable is a Table implementation
// that wraps an io.ReaderAt.
type ReaderAtTable struct {
	src   io.ReaderAt
	size  int64
	off   int64
	align int
}

func (r *ReaderAtTable) Hits() int64   { return 1 }
func (r *ReaderAtTable) Misses() int64 { return 0 }
func (r *ReaderAtTable) Bytes() int64  { return r.size }

// Size returns the number of bytes in the table
func (r *ReaderAtTable) Size() int64 { return r.size }

// Align returns the configured alignment for chunks in the table
func (r *ReaderAtTable) Align() int { return r.align }

// Chunks returns the number of chunks in the table
func (r *ReaderAtTable) Chunks() int { return int((r.size + int64(r.align-1)) / int64(r.align)) }

func (r *ReaderAtTable) run(dst io.Writer) error {
	if r.align > PageSize {
		return fmt.Errorf("align %d < PageSize (%d)", r.align, PageSize)
	}
	chunk := Malloc()[:r.align]
	defer Free(chunk)
	step := int64(r.align)
	for {
		off := atomic.AddInt64(&r.off, step) - step
		if r.size != -1 && off >= r.size {
			return nil
		}
		n, err := r.src.ReadAt(chunk, off)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if n == 0 {
					return nil
				}
				// otherwise, use the bytes
				// and then keep looping; we will
				// hit (0, io.EOF) on the next iteration
			} else {
				return err
			}
		}
		_, err = dst.Write(chunk[:n])
		if err != nil {
			return err
		}
	}
}

// WriteChunks implements Table.WriteChunks
func (r *ReaderAtTable) WriteChunks(dst QuerySink, parallel int) error {
	if c := r.Chunks(); c < parallel && c > 0 {
		parallel = c
	}
	return SplitInput(dst, parallel, r.run)
}

// BufferedTable is a Table implementation
// that uses bytes that are present in memory.
type BufferedTable struct {
	buf   []byte
	align int
	off   int64
}

func (b *BufferedTable) Hits() int64   { return 1 }
func (b *BufferedTable) Misses() int64 { return 0 }
func (b *BufferedTable) Bytes() int64  { return b.Size() }

// BufferTable converts a buffer with a known
// chunk alignment into a Table
func BufferTable(buf []byte, align int) *BufferedTable {
	return &BufferedTable{buf: buf, align: align, off: 0}
}

// Size returns the number of bytes in the table
func (b *BufferedTable) Size() int64 { return int64(len(b.buf)) }

// Chunks implements Table.Chunks
func (b *BufferedTable) Chunks() int { return (len(b.buf) + b.align - 1) / b.align }

func (b *BufferedTable) run(w io.Writer) error {
	tmp := Malloc()
	defer Free(tmp)
	for {
		off := atomic.AddInt64(&b.off, int64(b.align)) - int64(b.align)
		if off >= int64(len(b.buf)) {
			return nil
		}
		size := int64(b.align)
		if off+size > int64(len(b.buf)) {
			size = int64(len(b.buf)) - off
		}
		copy(tmp, b.buf[off:off+size])
		_, err := w.Write(tmp[:size])
		if err != nil {
			return err
		}
		HintEndSegment(w)
	}
}

// WriteChunks implements Table.WriteChunks
func (b *BufferedTable) WriteChunks(dst QuerySink, parallel int) error {
	return SplitInput(dst, parallel, b.run)
}

// Reset resets the current read offset of
// the table so that another call to WriteChunks can be made.
func (b *BufferedTable) Reset() {
	b.off = 0
}
