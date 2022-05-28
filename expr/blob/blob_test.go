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
	"fmt"
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
				Trailer: &blockfmt.Trailer{Version: 1, Algo: "zstd"},
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

func TestSerializationCompressed(t *testing.T) {
	now := date.Now().Truncate(time.Microsecond)
	lst := &List{}
	for i := 0; i < 2000; i++ {
		b := &Compressed{
			From: &URL{
				Value: "http://foo.bar/baz",
				Info: Info{
					Size:         50 * 1024 * 1024,
					Align:        1024 * 1024,
					LastModified: now,
				},
			},
			Trailer: &blockfmt.Trailer{
				Version:    1,
				Algo:       "zstd",
				BlockShift: 20,
				Offset:     500000,
				Blocks: []blockfmt.Blockdesc{
					{Offset: 0, Chunks: 100},
					{Offset: 100000, Chunks: 101},
					{Offset: 200000, Chunks: 101},
				},
			},
		}
		parts, err := b.Split(80 * 1024 * 1024)
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != 3 {
			t.Errorf("got %d parts?", len(parts))
		}
		lst.Contents = append(lst.Contents, b)
		for j := range parts {
			lst.Contents = append(lst.Contents, &parts[j])
		}
	}
	var compressed, uncompressed ion.Buffer
	var compressed2 ion.Buffer
	var st, st2 ion.Symtab

	// tickle race detector:
	done := make(chan struct{}, 1)
	go func() {
		lst.Encode(&compressed2, &st2)
		close(done)
	}()
	lst.Encode(&compressed, &st)
	<-done

	lst.encode(&uncompressed, &st)
	if ion.TypeOf(compressed.Bytes()) != ion.StructType {
		t.Fatal("not encoded as struct")
	}
	if compressed.Size() >= uncompressed.Size() {
		t.Errorf("list suspiciously big (%d bytes) after compression", compressed.Size())
	}
	got, err := DecodeList(&st, compressed.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, lst) {
		t.Fatal("not equal")
		// t.Errorf("%#v != %#v", got, lst)
	}
	// make sure we do snapshotting properly so
	// EndStruct doesn't panic when writing a blob
	// list as a struct member
	var buf ion.Buffer
	buf.BeginStruct(-1)
	buf.BeginField(123)
	lst.Encode(&buf, &st)
	buf.EndStruct()
}

func BenchmarkSerializationCompressed(b *testing.B) {
	now := date.Now().Truncate(time.Microsecond)
	lst := &List{
		Contents: make([]Interface, 2000),
	}
	rng := func(n int) []blockfmt.Range {
		return []blockfmt.Range{
			blockfmt.NewRange([]string{"timestamp"},
				ion.Timestamp(now.Add(-time.Duration(n)*time.Hour)),
				ion.Timestamp(now.Add(-time.Duration(n+1)*time.Hour))),
		}
	}
	// 2000 items * 5 blocks each ~= 10000 * 100MiB ~= 1TiB worth of blocks
	for i := range lst.Contents {
		lst.Contents[i] = &Compressed{
			From: &URL{
				Value: fmt.Sprintf("https://bucket.name.s3.amazonaws.com/path/to/object-%d", rand.Int63()),
				Info: Info{
					Size:         50*1024*1024 + rand.Int63n(1024*1024),
					Align:        1024 * 1024,
					LastModified: now,
				},
			},
			Trailer: &blockfmt.Trailer{
				Version:    1,
				Offset:     50*1024*1024 - rand.Int63n(1024*1024),
				Algo:       "zstd",
				BlockShift: 20,
				Blocks: []blockfmt.Blockdesc{
					{Offset: 0, Chunks: 100, Ranges: rng(5)},
					{Offset: 10*1024*1024 + rand.Int63n(50000), Chunks: 100, Ranges: rng(4)},
					{Offset: 20*1024*1024 + rand.Int63n(50000), Chunks: 100, Ranges: rng(3)},
					{Offset: 30*1024*1024 + rand.Int63n(50000), Chunks: 100, Ranges: rng(2)},
					{Offset: 40*1024*1024 + rand.Int63n(50000), Chunks: 100, Ranges: rng(1)},
				},
			},
		}
	}
	b.Run("encode", func(b *testing.B) {
		var buf ion.Buffer
		var st ion.Symtab
		lst.Encode(&buf, &st)
		b.SetBytes(int64(len(buf.Bytes())))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			st.Reset()
			lst.Encode(&buf, &st)
		}
		b.ReportMetric(float64(int64(len(buf.Bytes())))/2000, "bytes/record")
	})
	b.Run("decode", func(b *testing.B) {
		var buf ion.Buffer
		var st ion.Symtab
		lst.Encode(&buf, &st)
		b.SetBytes(int64(len(buf.Bytes())))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := DecodeList(&st, buf.Bytes())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
