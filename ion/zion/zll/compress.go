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
	"sync"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/iguana"
	"github.com/klauspost/compress/zstd"
)

var dec *zstd.Decoder
var enc *zstd.Encoder

var iguanaPool = sync.Pool{
	New: func() any { return &iguana.Decoder{} },
}

func iguanaDec() *iguana.Decoder {
	return iguanaPool.Get().(*iguana.Decoder)
}

func dropIguana(dec *iguana.Decoder) {
	iguanaPool.Put(dec)
}

// BucketAlgo is an algorithm used to compress buckets.
type BucketAlgo uint8

var iguanaEncoders = sync.Pool{
	New: func() any { return &iguana.Encoder{} },
}

func iguanaEnc() *iguana.Encoder {
	return iguanaEncoders.Get().(*iguana.Encoder)
}

func dropIguanaEnc(enc *iguana.Encoder) {
	iguanaEncoders.Put(enc)
}

const (
	// CompressZstd indicates that buckets are compressed
	// using vanilla zstd compression.
	CompressZstd BucketAlgo = iota
	// CompressIguanaV0 indicates that buckets are
	// compressed using the experimental iguana compression.
	CompressIguanaV0
	// CompressIguanaV0Specialized indicates
	// that buckets are compressed using the experimental
	// iguana compression OR a specialized algorithm given
	// by the first byte of the data.
	// If the first byte of the data is a null byte, then
	// IguanaV0 is used.
	CompressIguanaV0Specialized
)

func (a BucketAlgo) String() string {
	switch a {
	case CompressZstd:
		return "zstd"
	case CompressIguanaV0:
		return "iguana_v0"
	case CompressIguanaV0Specialized:
		return "iguana_v0/specialized"
	default:
		return fmt.Sprintf("BucketAlgo(%X)", uint8(a))
	}
}

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
func AppendMagic(dst []byte, algo BucketAlgo, seed uint8) []byte {
	lo8 := (seed & 0xf) | (uint8(algo&0xf) << 4)
	hi8 := uint8(algo >> 4)
	return append(append(dst, magic...), lo8, hi8, 0, 0)
}

// BucketHints is a set of hints to be provided for compression.
// The zero value of BucketHints implies there are no hints available.
type BucketHints struct {
	// Elements is the number of (symbol, value) pairs in the bucket.
	Elements int
	// TypeSet is a bitmap containing all of the
	// possible ion types for values in this bucket.
	TypeSet uint16
	// ListTypeSet is a bitmap of all the possible
	// ion types for sub-elements of the top level type
	// when the top-level type is an ion list type.
	// (This may be zero even when the top-level type
	// is only a list type iif the top-level lists are all empty.)
	ListTypeSet uint16
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

const (
	// regular iguana compression:
	iguanaUnspecialized = iota
	// uvarint list lengths + identity-encoded int8 values:
	iguanaRawInt8Vector
	// specialized encoding: for future use
)

// Compress compresses data from src and appends it to dst,
// returning the new dst slice or an error.
// If [hints] is non-nil, it may be used to improve
// the quality of the compression performed.
func (a BucketAlgo) Compress(hints *BucketHints, src, dst []byte) ([]byte, error) {
	off := len(dst)
	dst = append(dst, 0, 0, 0)
	if len(src) == 0 {
		return dst, nil
	}
	var err error
	switch a {
	case CompressIguanaV0Specialized:
		if hints != nil && hints.Elements > 0 &&
			hints.TypeSet == uint16(1)<<ion.ListType &&
			hints.ListTypeSet == (uint16(1)<<ion.IntType)|(uint16(1)<<ion.UintType) {
			out := append(dst, iguanaRawInt8Vector)
			out, ok := tryInt8Vector(src, out)
			if ok {
				dst = out
				break
			}
		}
		// TODO: use hints
		dst = append(dst, iguanaUnspecialized) // leading null byte
		fallthrough
	case CompressIguanaV0:
		enc := iguanaEnc()
		dst, err = enc.Compress(src, dst, iguana.DefaultEntropyRejectionThreshold)
		dropIguanaEnc(enc)
	case CompressZstd:
		dst = enc.EncodeAll(src, dst)
	default:
		panic("BucketAlgo.Compress: unknown BucketAlgo")
	}
	if err != nil {
		return nil, err
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
func (a BucketAlgo) Decompress(src, dst []byte) ([]byte, int, error) {
	if len(src) < 3 {
		return nil, 0, fmt.Errorf("zion.decompress: illegal frame size")
	}
	size := le24(src) + 3
	if size > len(src) {
		return nil, 0, fmt.Errorf("zion.decompress: segment size %d exceeds slice len %d", size, len(src))
	}
	if size == 3 {
		// empty bucket
		return dst, size, nil
	}
	var err error
	var out []byte
	switch a {
	case CompressZstd:
		out, err = dec.DecodeAll(src[3:size], dst)
	case CompressIguanaV0Specialized:
		if size < 4 {
			return nil, 0, fmt.Errorf("size %d does not fit bucket specialization byte", size)
		}
		switch src[3] {
		default:
			return nil, 0, fmt.Errorf("bad iguana/specialized bucket encoding %d", src[3])
		case iguanaRawInt8Vector:
			out, err = decodeInt8Vec(dst, src[4:size])
		case iguanaUnspecialized:
			dec := iguanaDec()
			out, err = dec.DecompressTo(dst, src[4:size])
			dropIguana(dec)
		}
	case CompressIguanaV0:
		dec := iguanaDec()
		out, err = dec.DecompressTo(dst, src[3:size])
		dropIguana(dec)
	default:
		err = fmt.Errorf("zll.BucketAlgo.Decompress: unrecognized algo %X", a)
	}
	return out, size, err
}
