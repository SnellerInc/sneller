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

package main

import (
	"io"
	"sync/atomic"

	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
)

type readerTable struct {
	t   *blockfmt.Trailer
	src io.ReaderAt
	clo io.Closer

	block int64
}

var allBytes int64

type byteTracker struct {
	io.Writer
}

func (b *byteTracker) Write(p []byte) (int, error) {
	atomic.AddInt64(&allBytes, int64(len(p)))
	return b.Writer.Write(p)
}

func (f *readerTable) Chunks() int {
	n := 0
	for i := range f.t.Blocks {
		n += f.t.Blocks[i].Chunks
	}
	return n
}

// satisfied by s3.Reader
type rangeReader interface {
	RangeReader(off, width int64) (io.ReadCloser, error)
}

func (f *readerTable) write(dst io.Writer) error {
	var d blockfmt.Decoder
	dst = &byteTracker{dst}
	for n := atomic.AddInt64(&f.block, 1) - 1; int(n) < len(f.t.Blocks); n = atomic.AddInt64(&f.block, 1) - 1 {
		nt := f.t.Slice(int(n), int(n+1))
		d.Trailer = nt
		pos := nt.Blocks[0].Offset
		end := nt.Offset
		if rr, ok := f.src.(rangeReader); ok {
			src, err := rr.RangeReader(pos, end-pos)
			if err != nil {
				return err
			}
			_, err = d.Copy(dst, src)
			src.Close()
			if err != nil {
				return err
			}
		} else {
			src := io.NewSectionReader(f.src, pos, end-pos)
			_, err := d.Copy(dst, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *readerTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	err := vm.SplitInput(dst, parallel, f.write)
	if f.clo != nil {
		f.clo.Close()
	}
	return err
}

func srcTable(f io.ReaderAt, size int64) (vm.Table, error) {
	tr, err := blockfmt.ReadTrailer(f, size)
	if err != nil {
		return nil, err
	}
	rt := &readerTable{t: tr, src: f}
	if c, ok := f.(io.Closer); ok {
		rt.clo = c
	}
	return rt, nil
}
