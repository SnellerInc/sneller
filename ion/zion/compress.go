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

package zion

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

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

// NOTE: we append a 4-byte value to every magic marker,
// but right now we only use the lowest 4 bits for the
// descriptor selector in sym2bucket(). The other bits
// are free for future feature flags, etc.
func appendMagic(dst []byte, seed uint32) []byte {
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

const maxSize = 1 << 21

func le24(x []byte) int {
	return int(x[0]) + (int(x[1]) << 8) + (int(x[2]) << 16)
}

func put24(i int, dst []byte) {
	dst[0] = byte(i)
	dst[1] = byte(i >> 8)
	dst[2] = byte(i >> 16)
}

// compress compressed data, appending to dst
func compress(src, dst []byte) ([]byte, error) {
	off := len(dst)
	dst = append(dst, 0, 0, 0)
	dst = enc.EncodeAll(src, dst)
	size := len(dst) - off - 3
	if size >= maxSize {
		return nil, fmt.Errorf("compressed segment length %d exceeds max size %d", size, maxSize)
	}
	put24(size, dst[off:])
	return dst, nil
}

func frameSize(src []byte) (int, error) {
	if len(src) < 3 {
		return 0, fmt.Errorf("zion.frameSize: illegal frame size")
	}
	size := le24(src) + 3
	if size > len(src) {
		return 0, fmt.Errorf("zion.frameSize: size %d > len %d", size, len(src))
	}
	return size, nil
}

// decompress decompresses data, appending to dst
func decompress(src, dst []byte) ([]byte, int, error) {
	if len(src) < 3 {
		return nil, 0, fmt.Errorf("zion.decompress: illegal frame size")
	}
	size := le24(src) + 3
	if size > len(src) {
		return nil, 0, fmt.Errorf("zion.decompress: segment size %d exceeds slice len %d", size, len(src))
	}
	out, err := dec.DecodeAll(src[3:size], dst)
	if err != nil {
		return nil, 0, err
	}
	return out, size, nil
}
