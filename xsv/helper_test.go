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

package xsv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

const alignment = 1024 * 1024
const testFolder = "testdata"

func testConvert(t *testing.T, file string, ch RowChopper, h *Hint) {
	f, err := os.Open(testFolder + "/" + file)
	if err != nil {
		t.Fatalf("cannot open %q: %s", file, err)
	}
	defer f.Close()

	base := testFolder + "/" + strings.TrimSuffix(file, filepath.Ext(file))
	dst := ion.Chunker{Align: alignment}

	ionFile := base + "-" + filepath.Ext(file)[1:] + ".ion.json"
	i, err := os.Open(ionFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			w, err := os.Create(ionFile)
			if err != nil {
				t.Fatalf("cannot create %q: %s", ionFile, err)
			}
			defer w.Close()

			dst.W = ion.NewJSONWriter(w, '\n')

			t.Errorf("no ion.json file, so we'll create one")
		} else {
			t.Fatalf("cannot read %q: %s", ionFile, err)
		}
	} else {
		cw := compareWriter{R: i}
		defer func() {
			if err := cw.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		}()
		dst.W = ion.NewJSONWriter(&cw, '\n')
	}

	err = Convert(f, &dst, ch, h)
	if err != nil {
		t.Fatalf("cannot convert: %s", err)
	}
}

// compareWriter implements an io.Writer that
// bails out with an error if it detects when
// different data is written compared to the
// reference file
type compareWriter struct {
	R         io.Reader
	BufSize   int
	line, col int
	buffer    []byte
	len       int64
}

func (cw *compareWriter) Write(p []byte) (int, error) {
	// allocate a buffer for reading
	if cw.buffer == nil {
		bufSize := cw.BufSize
		if bufSize <= 0 {
			bufSize = 8192
		}
		cw.buffer = make([]byte, bufSize)
	}

	for n := 0; n < len(p); {
		maxRead := len(p) - n
		if maxRead > cap(cw.buffer) {
			maxRead = cap(cw.buffer)
		}
		r, err := cw.R.Read(cw.buffer[:maxRead])
		if err != nil {
			return 0, err
		}
		if r != maxRead {
			return 0, fmt.Errorf("writing more data than expected")
		}

		for i := 0; i < r; i++ {
			if p[n+i] != cw.buffer[i] {
				return 0, fmt.Errorf("bytes differ at line %d, col %d", cw.line+1, cw.col+1)
			}
			if cw.buffer[i] == '\n' {
				cw.line++
				cw.col = 0
			} else {
				cw.col++
			}
		}

		n += r
	}

	// read the same amount of bytes of the input
	cw.len += int64(len(p))
	return len(p), nil
}

func (cw *compareWriter) Close() error {
	buf := []byte{0}
	n, err := cw.R.Read(buf)
	if err == nil && n > 0 {
		return fmt.Errorf("expected end at index %d", cw.len)
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
