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

package vm

import (
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

const (
	defaultAlign = 1024 * 1024
)

// alignedWriter buffers writes up to len(buf)
// and flushes them to 'out'
type alignedWriter struct {
	out       io.WriteCloser
	buf       []byte
	off, save int
}

func (a *alignedWriter) init(out io.WriteCloser) {
	a.out = out
	a.buf = Malloc()
}

func (a *alignedWriter) space() int {
	return len(a.buf) - a.off
}

func (a *alignedWriter) flush() (int, error) {
	if a.off == a.save {
		return 0, nil
	}
	contents := a.buf[:a.off]
	a.off = a.save
	return a.out.Write(contents)
}

// if there is no data in the buffer, then
// free it and let it be re-allocated lazily
// by the caller
func (a *alignedWriter) maybeDrop() {
	if a.buf == nil || a.off != a.save {
		return
	}
	Free(a.buf)
	a.buf = nil
	a.off = 0
	a.save = 0
}

func (a *alignedWriter) setpre(st *symtab) error {
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
	Free(a.buf)
	a.buf = nil
	err2 := a.out.Close()
	if err == nil {
		err = err2
	}
	return err
}
