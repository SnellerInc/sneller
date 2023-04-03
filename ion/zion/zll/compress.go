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

package zll

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/SnellerInc/sneller/ion/zion/iguana"
	"github.com/klauspost/compress/zstd"
)

const useIguana = false // TODO: implement it properly

var dec *zstd.Decoder
var enc *zstd.Encoder

// magic is the (little-endian) magic number
// that begins zion compressed chunks
//
// (the magic number is "zip" in ion encoding)
var magic = []byte{0x83, 'z', 'i', 'p'}

// IsMagic returns true if x begins with
// the 4-byte magic number for zion-encoded
// streams, or false otherwise.
func IsMagic(x []byte) bool {
	return len(x) >= 4 &&
		bytes.Equal(x[:4], magic)
}

// AppendMagic appends the zion magic bytes plus the
// seed bits to dst.
//
// NOTE: currently only the lowest 4 bits of seed should be set.
// The rest are reserved for future use.
func AppendMagic(dst []byte, seed uint32) []byte {
	return append(append(dst, magic...),
		byte(seed), byte(seed>>8), byte(seed>>16), byte(seed>>24))
}

func init() {
	dec, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.IgnoreChecksum(true))
	enc, _ = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
}

// MaxBucketSize is the maximum size of a compressed bucket.
const MaxBucketSize = 1 << 21

func le24(x []byte) int {
	return int(x[0]) + (int(x[1]) << 8) + (int(x[2]) << 16)
}

func put24(i int, dst []byte) {
	dst[0] = byte(i)
	dst[1] = byte(i >> 8)
	dst[2] = byte(i >> 16)
}

// Compress compresses data from src and appends it to dst,
// returning the new dst slice or an error.
func Compress(src, dst []byte) ([]byte, error) {
	off := len(dst)
	dst = append(dst, 0, 0, 0)

	if useIguana {
		var err error
		dst, err = iguana.Compress(src, dst, iguana.DefaultANSThreshold)
		if err != nil {
			return dst, err
		}
	} else {
		dst = enc.EncodeAll(src, dst)
	}
	size := len(dst) - off - 3
	if size >= MaxBucketSize {
		return nil, fmt.Errorf("compressed segment length %d exceeds max size %d", size, MaxBucketSize)
	}
	put24(size, dst[off:])
	return dst, nil
}

// FrameSize returns the number compressed bytes
// within the next frame. This is the same number
// that Decompress would return as the number of bytes
// consumed if called on src.
func FrameSize(src []byte) (int, error) {
	if len(src) < 3 {
		return 0, fmt.Errorf("zion.frameSize: illegal frame size")
	}
	size := le24(src) + 3
	if size > len(src) {
		return 0, fmt.Errorf("zion.frameSize: size %d > len %d", size, len(src))
	}
	return size, nil
}

// Decompress decompressed data from src, appending it to dst.
// Decompress returns the new dst, the number of compressed bytes consumed,
// and the first error encountered, if any.
func Decompress(src, dst []byte) ([]byte, int, error) {
	if len(src) < 3 {
		return nil, 0, fmt.Errorf("zion.decompress: illegal frame size")
	}
	size := le24(src) + 3
	if size > len(src) {
		return nil, 0, fmt.Errorf("zion.decompress: segment size %d exceeds slice len %d", size, len(src))
	}
	if useIguana {
		out, err := iguana.DecompressTo(dst, src[3:size])
		if err != nil {
			return nil, 0, err
		}
		return out, size, nil
	} else {
		out, err := dec.DecodeAll(src[3:size], dst)
		if err != nil {
			return nil, 0, err
		}
		return out, size, nil
	}
}
