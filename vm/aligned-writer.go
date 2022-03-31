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
	"fmt"
	"io"
	"sync"

	"github.com/SnellerInc/sneller/ion"
)

const pageSlack = 16

// buffer pool of buffers that have size defaultAlign
var alignedbufs = sync.Pool{}

const defaultAlign = 1024 * 1024

// calloc returns a zeroed buffer
// with length equal to size and capacity
// equal to size plus the required padding
// for the assembly code to perform unaligned reads.
//
// Buffers passed to core procedures ought to be
// allocated with calloc.
func calloc(size int) []byte {
	if size == defaultAlign {
		g := alignedbufs.Get()
		if g != nil {
			buf := g.([]byte)
			return buf[:size]
		}
	}
	return make([]byte, size, size+pageSlack)
}

// free "frees" a buffer allocated by calloc.
// The caller may not use any part of buf
// once free has been callled.
func free(buf []byte) {
	if cap(buf) == defaultAlign+pageSlack {
		//lint:ignore SA6002 inconsequential
		alignedbufs.Put(buf)
	}
}

// alignedWriter buffers writes up to len(buf)
// and flushes them to 'out'
type alignedWriter struct {
	out       io.WriteCloser
	buf       []byte
	off, save int
}

func (a *alignedWriter) init(out io.WriteCloser, pre []byte, align int) {
	a.out = out
	a.buf = calloc(align)
	if pre != nil {
		a.off = copy(a.buf, pre)
		a.save = a.off
	}
}

func (a *alignedWriter) space() int {
	return len(a.buf) - a.off
}

func (a *alignedWriter) reserve(n int) []byte {
	buf := a.buf[a.off : a.off+n]
	a.off += n
	return buf
}

func (a *alignedWriter) flush() (int, error) {
	if a.off == a.save {
		return 0, nil
	}
	contents := a.buf[:a.off]
	a.off = a.save
	return a.out.Write(contents)
}

func (a *alignedWriter) setpre(st *ion.Symtab) error {
	if a.off != a.save {
		_, err := a.flush()
		if err != nil {
			return err
		}
	}

	// marshal the symbol table into the
	// initial bytes of the buffer
	orig := len(a.buf)
	var b ion.Buffer
	b.Set(a.buf[:0])
	st.Marshal(&b, true)
	a.save = len(b.Bytes())
	if a.save > orig {
		return fmt.Errorf("cannot fit %d bytes into alignment", a.save)
	}
	a.off = a.save
	return nil
}

func (a *alignedWriter) Close() error {
	if a.buf == nil {
		if a.out != nil {
			return a.out.Close()
		}
		return nil
	}
	_, err := a.flush()
	free(a.buf)
	a.buf = nil
	err2 := a.out.Close()
	if err == nil {
		err = err2
	}
	return err
}
