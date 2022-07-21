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
	"io"
)

// TeeWriter is an io.Writer that writes
// to multiple streams simultaneously,
// taking care to handle the errors from
// each stream separately.
type TeeWriter struct {
	pos      int64
	splitter int // either -1, or writters[splitter] is a rowSplitter
	writers  []io.Writer
	final    []func(int64, error)
}

// NewTeeWriter constructs a new TeeWriter with
// an io.Writer and an error handler.
// The returned TeeWriter does not return errors
// on calls to Write unless all of its constituent
// io.Writers have returned with errors, at which
// point it will return io.EOF.
func NewTeeWriter(out io.Writer, final func(int64, error)) *TeeWriter {
	tw := &TeeWriter{splitter: -1}
	tw.Add(out, final)
	return tw
}

// CloseError calls the final function for
// all the remaining writers with the provided
// error value, then resets the content of t.
func (t *TeeWriter) CloseError(err error) {
	for i := range t.final {
		if t.final[i] != nil {
			t.final[i](t.pos, err)
		}
	}
	t.splitter = -1
	t.final = t.final[:0]
	t.writers = t.writers[:0]
}

// Close calls final(nil) for each of
// the remaining writers added via Add
// and then resets the content of t.
func (t *TeeWriter) Close() error {
	for i := range t.writers {
		HintEndSegment(t.writers[i])
	}
	t.CloseError(nil)
	return nil
}

// Add adds a writer to the TeeWriter.
// Calls to t.Write will be forwarded to w
// for as long as it does not return an error.
// On the first encountered error, final(err) will
// be called and the writer will be disabled.
// If no errors are encountered, then final(nil) will
// be called at the point that t.Close (or t.CloseError)
// is called.
//
// The final function provided to Add should not block;
// it is called synchronously with respect to calls to Write.
func (t *TeeWriter) Add(w io.Writer, final func(int64, error)) {
	if rs, ok := w.(*rowSplitter); ok {
		if t.splitter < 0 {
			t.splitter = len(t.writers)
			// produce a new splitter so that a caller
			// calling rs.Close() does not close the
			// splitter we are actually using for (potentially)
			// multiple outputs
			sp := splitter(rs.rowConsumer)
			t.writers = append(t.writers, sp)
			t.final = append(t.final, final)
			return
		}
		split := t.writers[t.splitter].(*rowSplitter)
		ts, ok := split.rowConsumer.(*teeSplitter)
		if ok {
			// we are already tee-ing a splitter,
			// so just push the entries down into the tee
			if tee2, ok := rs.rowConsumer.(*teeSplitter); ok {
				ts.dst = append(ts.dst, tee2.dst...)
				ts.final = append(ts.final, tee2.final...)
			} else {
				ts.dst = append(ts.dst, rs.rowConsumer)
				ts.final = append(ts.final, final)
			}
		} else {
			// create a new teeSplitter that shares
			// a symbol table with one top-level rowSplitter
			ts := &teeSplitter{
				dst:   []rowConsumer{split.rowConsumer, rs},
				final: []func(int64, error){t.final[t.splitter], final},
			}
			split.rowConsumer = ts
			t.final[t.splitter] = func(i int64, e error) {
				ts.close(i, e)
			}
		}
		return
	}
	if tw, ok := w.(*TeeWriter); ok {
		if tw.splitter >= 0 {
			// don't "leak" this splitter; we are going
			// to eat its contents in Add
			rs := tw.writers[tw.splitter].(*rowSplitter)
			rs.drop()
		}
		// flatten multiple TeeWriters into one
		for i := range tw.writers {
			t.Add(tw.writers[i], tw.final[i])
		}
		return
	}
	t.writers = append(t.writers, w)
	t.final = append(t.final, final)
}

// Write implements io.Writer
func (t *TeeWriter) Write(p []byte) (int, error) {
	any := false
	for i := 0; i < len(t.writers); i++ {
		n, err := t.writers[i].Write(p)
		if err != nil {
			t.final[i](int64(n)+t.pos, err)
			t.final = deleteOne(t.final, i)
			t.writers = deleteOne(t.writers, i)
			switch t.splitter {
			case i:
				t.splitter = -1
			case len(t.writers):
				t.splitter = i
			}
			i--
			continue
		}
		any = true
	}
	if !any {
		return 0, io.EOF
	}
	t.pos += int64(len(p))
	return len(p), nil
}

// teeSplitter is a rowConsumer that can
// live under a rowSplitter to pass rows to
// multiple query operators at once
type teeSplitter struct {
	pos   int64 // updated by rowSplitter.Write
	dst   []rowConsumer
	cache []vmref
	final []func(int64, error)
}

func (t *teeSplitter) clone(refs []vmref) []vmref {
	if cap(t.cache) < len(refs) {
		t.cache = make([]vmref, len(refs))
	}
	t.cache = t.cache[:len(refs)]
	copy(t.cache, refs)
	return t.cache
}

func deleteOne[T any](src []T, i int) []T {
	src[i] = src[len(src)-1]
	src = src[:len(src)-1]
	return src
}

func (t *teeSplitter) symbolize(st *symtab) error {
	any := false
	for i := 0; i < len(t.dst); i++ {
		// XXX: we are really relying here on the
		// fact that rowConsumers don't destructively
		// modify the symbol table; they can add to it
		// (which is fine; they are allowed to see each
		// other's symbols) but they cannot remove anything
		err := t.dst[i].symbolize(st)
		if err != nil {
			t.final[i](t.pos, err)
			t.dst = deleteOne(t.dst, i)
			t.final = deleteOne(t.final, i)
			i--
			continue
		}
		any = true
	}
	if !any {
		return io.EOF
	}
	return nil
}

func (t *teeSplitter) writeRows(delims []vmref) error {
	any := false
	for i := 0; i < len(t.dst); i++ {
		// we have to clone the delimiter slice,
		// since callees are allowed to use it
		// as scratch space during execution
		err := t.dst[i].writeRows(t.clone(delims))
		if err != nil {
			t.final[i](t.pos, err)
			t.dst = deleteOne(t.dst, i)
			t.final = deleteOne(t.final, i)
			i--
			continue
		}
		any = true
	}
	if !any {
		return io.EOF
	}
	return nil
}

func (t *teeSplitter) next() rowConsumer { return nil }

func (t *teeSplitter) close(pos int64, err error) {
	for i := range t.final {
		t.final[i](pos, err)
	}
	t.dst = t.dst[:0]
	t.final = t.final[:0]
}

func (t *teeSplitter) Close() error {
	return nil
}

func (t *teeSplitter) EndSegment() {
	for i := range t.dst {
		for rc := t.dst[i]; rc != nil; rc = rc.next() {
			if esw, ok := rc.(EndSegmentWriter); ok {
				esw.EndSegment()
			}
		}
	}
}
