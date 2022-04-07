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
	"io/fs"
	"math/rand"
	"net"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func testRead(t *testing.T, src Interface, backing []byte) {
	dst := make([]byte, len(backing))
	info, err := src.Stat()
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	reader := func(src Interface, info *Info) io.ReadCloser {
		var r io.ReadCloser
		var err error
		if c, ok := src.(*Compressed); ok {
			r, err = c.Decompressor()
		} else {
			r, err = src.Reader(0, info.Size)
		}
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		return r
	}

	r := reader(src, info)
	_, writerTo := r.(io.WriterTo)
	defer r.Close()
	_, err = io.ReadFull(r, dst)
	if err != nil {
		t.Helper()
		t.Error(err)
	}
	if !bytes.Equal(dst, backing) {
		t.Helper()
		t.Error("output not equal")
	}
	if !writerTo {
		return
	}
	r2 := reader(src, info)
	defer r2.Close()
	wt := r2.(io.WriterTo)
	var buf bytes.Buffer
	nn, err := wt.WriteTo(&buf)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if int(nn) != len(backing) {
		t.Helper()
		t.Errorf("wrote %d bytes instead of %d", nn, len(backing))
	}
	if !bytes.Equal(buf.Bytes(), backing) {
		t.Helper()
		t.Error("WriteTo returned bad data")
	}
}

// implements http.FileSystem
type bufServer struct {
	backing []byte
}

// implements http.File and fs.FileInfo
type httpFile struct {
	*bytes.Reader
}

func (h *httpFile) Name() string               { return "backing" }
func (h *httpFile) Size() int64                { return int64(h.Reader.Size()) }
func (h *httpFile) Mode() os.FileMode          { return 0644 }
func (h *httpFile) ModTime() time.Time         { return time.Time{} }
func (h *httpFile) IsDir() bool                { return false }
func (h *httpFile) Sys() interface{}           { return nil }
func (h *httpFile) Stat() (fs.FileInfo, error) { return h, nil }
func (h *httpFile) Close() error               { return nil }

func (h *httpFile) Readdir(count int) ([]fs.FileInfo, error) {
	panic("Readdir on non-dir http.File")
	return nil, nil
}

func (b *bufServer) Open(name string) (http.File, error) {
	if name != "/backing" {
		return nil, os.ErrNotExist
	}
	return &httpFile{Reader: bytes.NewReader(b.backing)}, nil
}

func server(buf []byte) *http.Server {
	return &http.Server{
		Handler: http.FileServer(&bufServer{buf}),
	}
}

func TestBlobs(t *testing.T) {
	backing := make([]byte, 2*1024*1024)
	rand.Read(backing)

	s := server(backing)
	listening := make(chan struct{}, 1)
	go func() {
		l, err := net.Listen("tcp", "localhost:9100")
		if err != nil {
			panic(err)
		}
		close(listening)
		err = s.Serve(l)
		if err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// wait until the socket is bound
	// before testing
	<-listening

	b := &URL{
		Value: "http://localhost:9100/backing",
		Info: Info{
			Size:  int64(len(backing)),
			Align: 1024 * 1024,
		},
	}
	testRead(t, b, backing)
	s.Close()
}

func TestSerialization(t *testing.T) {
	now := date.Now().Truncate(time.Microsecond)
	lst := &List{
		Contents: []Interface{
			&Compressed{
				From: &URL{
					Value: "http://abc.xyz/012",
					Info: Info{
						Size:         rand.Int63(),
						Align:        100,
						LastModified: now,
					},
				},
				Trailer: &blockfmt.Trailer{Algo: "zstd"},
			},
			&URL{
				Value: "http://foo.bar/baz",
				Info: Info{
					Size:         rand.Int63(),
					Align:        1000,
					LastModified: now,
				},
			},
		},
	}
	var buf ion.Buffer
	var st ion.Symtab
	lst.Encode(&buf, &st)

	got, err := DecodeList(&st, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, lst) {
		t.Errorf("%#v != %#v", got, lst)
	}
}
