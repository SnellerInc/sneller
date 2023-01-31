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

package blob

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"net/http"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestCompressedRange(t *testing.T) {
	buf := make([]byte, 16*1024)
	rand.Read(buf)

	var dst blockfmt.BufferUploader
	cw := blockfmt.CompressionWriter{
		Output:     &dst,
		Comp:       blockfmt.CompressorByName("zstd"),
		InputAlign: 512,
		TargetSize: 512,
	}
	cw.SkipChecks()
	in := buf
	for len(in) > 0 {
		_, err := cw.Write(in[:cw.InputAlign])
		if err != nil {
			t.Fatal(err)
		}
		err = cw.Flush()
		if err != nil {
			t.Fatal(err)
		}
		in = in[cw.InputAlign:]
	}
	err := cw.Close()
	if err != nil {
		t.Fatal(err)
	}

	srv := server(dst.Bytes())
	defer srv.Close()
	listening := make(chan struct{}, 1)
	go func() {
		l, err := net.Listen("tcp", "localhost:9101")
		if err != nil {
			panic(err)
		}
		close(listening)
		err = srv.Serve(l)
		if err != http.ErrServerClosed {
			panic(err)
		}
	}()
	<-listening

	all := &Compressed{
		From: &URL{
			Value: "http://localhost:9101/backing",
			Info: Info{
				Size:  int64(len(dst.Bytes())),
				Align: cw.InputAlign,
			},
		},
		Trailer: cw.Trailer,
	}
	testRead(t, all, buf)

	out := make([]byte, len(buf))
	blocks := len(buf) / cw.InputAlign
	var ibuf ion.Buffer
	var st ion.Symtab
	for i := 0; i < blocks-1; i++ {
		for j := i + 1; j < blocks; j++ {
			off := int64(i * cw.InputAlign)
			size := int64((j - i) * cw.InputAlign)
			part := &CompressedPart{
				Parent:     all,
				StartBlock: i,
				EndBlock:   j,
			}
			rd, err := part.Decompressor()
			if err != nil {
				t.Fatal(err)
			}
			n, err := io.ReadFull(rd, out[:size])
			rd.Close()
			if err != nil {
				t.Fatalf("block %d outsize %d: %s", i, size, err)
			}
			if n != int(size) {
				t.Fatalf("read %d bytes instead of %d", n, size)
			}
			if !bytes.Equal(out[:size], buf[off:off+size]) {
				t.Fatalf("read@[%d:+%d] not equivalent to input", off, size)
			}
			ibuf.Reset()
			l := List{Contents: []Interface{part}}
			l.Encode(&ibuf, &st)
			lout, err := DecodeList(readDatum(t, &st, &ibuf))
			if err != nil {
				t.Fatal(err)
			}
			_ = lout

		}
	}
}
