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

// Package compr provides a unified interface wrapping
// third-party compression libraries.
package compr

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Compressor describes the interface
// that CompressionWriter needs a compression
// algorithm to implement.
type Compressor interface {
	// Name is the name of the compression algorithm.
	Name() string
	// Compress should append the compressed contents
	// of src to dst and return the result.
	Compress(src, dst []byte) []byte
}

// Decompressor is the interface that a
// CompressionReader uses to decompress blocks.
type Decompressor interface {
	// Name is the name of the compression algorithm.
	// See also Compressor.Name.
	Name() string
	// Decompress decompresses source data
	// into dst. It should error out if
	// dst is not large enough to fit the
	// encoded source data.
	//
	// It must be safe to make multiple
	// calls to Decompress simultaneously
	// from different goroutines.
	Decompress(src, dst []byte) error
}

type zstdCompressor struct {
	enc *zstd.Encoder
}

func (z zstdCompressor) Compress(src, dst []byte) []byte {
	return z.enc.EncodeAll(src, dst)
}

func (z zstdCompressor) Name() string { return "zstd" }

var (
	zstdDecoder     *zstd.Decoder
	zstdFastDecoder *zstd.Decoder
)

func init() {
	// by default, concurrency is set to min(4, GOMAXPROCS);
	// we'd like it to *always* be GOMAXPROCS
	z, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		panic(err)
	}
	zstdDecoder = z
	z, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.IgnoreChecksum(true))
	if err != nil {
		panic(err)
	}
	zstdFastDecoder = z
}

// DecodeZstd calls DecodeAll on the global zstd
// decoder.
//
// See: (*zstd.Decoder).DecodeAll
func DecodeZstd(src, dst []byte) ([]byte, error) {
	return zstdDecoder.DecodeAll(src, dst)
}

type zstdDecompressor zstd.Decoder

func (z *zstdDecompressor) Name() string { return "zstd" }

func (z *zstdDecompressor) Decompress(src, dst []byte) error {
	into := dst[:0:len(dst)]
	ret, err := (*zstd.Decoder)(z).DecodeAll(src, into)
	if err != nil {
		return err
	}
	if len(ret) != len(dst) {
		return fmt.Errorf("expected %d bytes decompressed; got %d", len(dst), len(ret))
	}
	// the decoder should not have had to
	// realloc the buffer
	if &ret[0] != &dst[0] {
		return fmt.Errorf("zstd decompress: output buffer realloc'd")
	}
	return nil
}

type s2Compressor struct{}

func (s2Compressor) Compress(src, dst []byte) []byte {
	tail := dst[len(dst):cap(dst)]
	// s2 requires non-overlapping src and dst
	if overlaps(src, tail) {
		tail = nil
	}
	got := s2.Encode(tail, src)
	if len(dst) == 0 {
		return got
	}
	if len(tail) > 0 && len(got) > 0 && &tail[0] == &got[0] {
		return dst[:len(dst)+len(got)]
	}
	return append(dst, got...)
}

func (s2Compressor) Decompress(src, dst []byte) error {
	into := dst[:0:len(dst)]
	ret, err := s2.Decode(into, src)
	if err != nil {
		return err
	}
	if len(ret) != len(dst) {
		return fmt.Errorf("expected %d bytes decompressed; got %d", len(dst), len(ret))
	}
	// the decoder should not have had to
	// realloc the buffer
	if &ret[0] != &dst[0] {
		return fmt.Errorf("s2 decompress: output buffer realloc'd")
	}
	return nil
}

func (s2Compressor) Name() string { return "s2" }

// Compression selects a compression algorithm by name.
// The returned Compressor will return the same value
// for Compressor.Name as the specified name.
func Compression(name string) Compressor {
	switch name {
	case "zstd-better":
		z, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
			zstd.WithEncoderConcurrency(1))
		return zstdCompressor{z}
	case "zstd":
		z, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		return zstdCompressor{z}
	case "s2":
		return s2Compressor{}
	default:
		return nil
	}
}

func Decompression(name string) Decompressor {
	switch name {
	case "zstd":
		return (*zstdDecompressor)(zstdDecoder)
	case "zstd-nocrc":
		return (*zstdDecompressor)(zstdFastDecoder)
	case "s2":
		return s2Compressor{}
	default:
		return nil
	}
}

func overlaps(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	a0 := uintptr(unsafe.Pointer(&a[0]))
	a1 := a0 + uintptr(len(a))
	b0 := uintptr(unsafe.Pointer(&b[0]))
	b1 := b0 + uintptr(len(b))
	return a0 < b1 && b0 < a1
}
