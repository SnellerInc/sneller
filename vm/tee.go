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

var _ EndSegmentWriter = (*TeeWriter)(nil)

// TeeWriter is an io.Writer that writes
// to multiple streams simultaneously,
// taking care to handle the errors from
// each stream separately.
type TeeWriter struct {
	pos      int64
	splitter int // either -1, or writters[splitter] is a rowSplitter
	state    []teeState
}

type teeState struct {
	w     io.Writer
	final func(int64, error)
}

func (t *TeeWriter) ConfigureZion(fields []string) bool {
	if len(t.state) > 1 || t.splitter == -1 {
		return false
	}
	return t.state[t.splitter].w.(*rowSplitter).ConfigureZion(fields)
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
	for i := range t.state {
		if t.state[i].final != nil {
			t.state[i].final(t.pos, err)
		}
	}
	t.splitter = -1
	t.state = t.state[:0]
}

// Close calls final(nil) for each of
// the remaining writers added via Add
// and then resets the content of t.
func (t *TeeWriter) Close() error {
	for i := range t.state {
		HintEndSegment(t.state[i].w)
	}
	t.CloseError(nil)
	return nil
}

// EndSegment implements EndSegmentWriter.EndSegment.
// This calls HintEndSegment on all the
// remaining writers.
func (t *TeeWriter) EndSegment() {
	for i := range t.state {
		HintEndSegment(t.state[i].w)
	}
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
			t.splitter = len(t.state)
			// create a new teeSplitter that shares
			// a symbol table with one top-level rowSplitter
			ts := &teeSplitter{
				state: []splitState{{
					dst:   rs.rowConsumer,
					final: final,
				}},
			}
			// produce a new splitter so that a caller
			// calling rs.Close() does not close the
			// splitter we are actually using for (potentially)
			// multiple outputs
			sp := splitter(ts)
			// have the rowSplitter update ts.pos
			// on each call to Write:
			sp.pos = &ts.pos
			sp.rowConsumer = ts
			t.state = append(t.state, teeState{
				w: sp,
				final: func(i int64, e error) {
					ts.close(i, e)
					sp.Close()
				},
			})
			return
		}
		split := t.state[t.splitter].w.(*rowSplitter)
		ts := split.rowConsumer.(*teeSplitter)
		if tee2, ok := rs.rowConsumer.(*teeSplitter); ok {
			ts.state = append(ts.state, tee2.state...)
		} else {
			ts.state = append(ts.state, splitState{
				dst:   rs.rowConsumer,
				final: final,
			})
		}
		return
	}
	if tw, ok := w.(*TeeWriter); ok {
		if tw.splitter >= 0 {
			// don't "leak" this splitter; we are going
			// to eat its contents in Add
			rs := tw.state[tw.splitter].w.(*rowSplitter)
			rs.drop()
		}
		// flatten multiple TeeWriters into one
		for i := range tw.state {
			t.Add(tw.state[i].w, tw.state[i].final)
		}
		// add a nil writer to call finalizer
		if final != nil {
			t.state = append(t.state, teeState{final: final})
		}
		return
	}
	t.state = append(t.state, teeState{w: w, final: final})
}

// Write implements io.Writer
func (t *TeeWriter) Write(p []byte) (int, error) {
	any := false
	for i := 0; i < len(t.state); i++ {
		if t.state[i].w == nil {
			continue
		}
		n, err := t.state[i].w.Write(p)
		if err != nil {
			t.state[i].final(int64(n)+t.pos, err)
			t.state = deleteOne(t.state, i)
			switch t.splitter {
			case i:
				t.splitter = -1
			case len(t.state):
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
	state []splitState
	cache []vmref
	zion  int // +1 = yes, -1 = no, 0 = not yet known
}

type splitState struct {
	aux   auxbindings
	dst   rowConsumer
	final func(int64, error)
	zout  zionConsumer
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

func (t *teeSplitter) zionOk() bool {
	switch t.zion {
	case 1:
		return true
	case -1:
		return false
	}
	var ok bool
	for i := range t.state {
		t.state[i].zout, ok = t.state[i].dst.(zionConsumer)
		if !ok || !t.state[i].zout.zionOk() {
			t.zion = -1
			return false
		}
	}
	t.zion = 1
	return true
}

func (t *teeSplitter) symbolize(st *symtab, aux *auxbindings) error {
	any := false
	for i := 0; i < len(t.state); i++ {
		t.state[i].aux.set(aux)
		// XXX: we are really relying here on the
		// fact that rowConsumers don't destructively
		// modify the symbol table; they can add to it
		// (which is fine; they are allowed to see each
		// other's symbols) but they cannot remove anything
		err := t.state[i].dst.symbolize(st, &t.state[i].aux)
		if err != nil {
			t.state[i].final(t.pos, err)
			t.state = deleteOne(t.state, i)
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

func (t *teeSplitter) writeZion(state *zionState) error {
	if t.zion != 1 {
		panic("teeSplitter.writeZion: unexpected call (nozion flag is set)")
	}
	any := false
	for i := 0; i < len(t.state); i++ {
		err := t.state[i].zout.writeZion(state)
		if err != nil {
			t.state[i].final(t.pos, err)
			t.state = deleteOne(t.state, i)
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

func (t *teeSplitter) writeRows(delims []vmref, params *rowParams) error {
	any := false
	for i := 0; i < len(t.state); i++ {
		// we have to clone the delimiter slice,
		// since callees are allowed to use it
		// as scratch space during execution
		err := t.state[i].dst.writeRows(t.clone(delims), params)
		if err != nil {
			t.state[i].final(t.pos, err)
			t.state = deleteOne(t.state, i)
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
	for i := range t.state {
		t.state[i].final(pos, err)
	}
	t.state = t.state[:0]
}

func (t *teeSplitter) Close() error {
	return nil
}

func (t *teeSplitter) EndSegment() {
	for i := range t.state {
		for rc := t.state[i].dst; rc != nil; rc = rc.next() {
			if esw, ok := rc.(EndSegmentWriter); ok {
				esw.EndSegment()
			}
		}
	}
}
