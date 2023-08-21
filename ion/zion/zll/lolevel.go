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

// Package zll exposes types and procedures related
// to low-level zion decoding. Callers should prefer
// the high-level zion package.
package zll

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/SnellerInc/sneller/ion"
)

type Symtab interface {
	Unmarshal([]byte) ([]byte, error)
	Symbolize(x string) (ion.Symbol, bool)
}

// Shape manages the stateful part of
// decoding relevant to the "shape" portion of
// the stream.
type Shape struct {
	// Symtab is the current symbol table.
	// Callers may plug in their own symbol table implementation.
	Symtab Symtab
	// Bits is the raw shape bitsream, including the leading symbol table.
	Bits []byte
	// Start is the position within Bits that the actual shape bits start.
	// This may be non-zero if the Bits stream has a leading symbol table.
	Start int
	// Seed is the 32-bit seed stored in the shape preamble;
	// it contains the selector used to hash symbols and the
	// algorithm used to compress buckets. All the other bits
	// are reserved and should be zero.
	Seed uint32
}

// Algo returns the bucket compression algorithm
// as indicated by s.Seed.
func (s *Shape) Algo() BucketAlgo {
	return BucketAlgo(s.Seed >> 4)
}

//go:noescape
func shapecount(shape []byte) int

// Count returns the number of records implied by the shape bitstream.
// The caller should have already called s.Decode at least once to
// populate the shape bits.
func (s *Shape) Count() (int, error) {
	count := shapecount(s.Bits[s.Start:])
	if count < 0 {
		return 0, fmt.Errorf("zll.Shape.Count: corrupt data")
	}
	return count, nil
}

// SymbolBucket determines the bucket in which the
// top-level fields associated with sym would be encoded.
func (s *Shape) SymbolBucket(sym ion.Symbol) int {
	return int(SymbolBucket(0, uint8(s.Seed&0xf), sym))
}

func (s *Shape) checkMagic(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("zll.Shape: len(input)=%d; missing magic", len(src))
	}
	if !bytes.Equal(src[:4], magic) {
		return nil, fmt.Errorf("zll.Shape: bad magic bytes %x", src[:len(magic)])
	}
	s.Seed = binary.LittleEndian.Uint32(src[4:])
	// bits[0:4] = seed
	// bits[4:12] = algo
	// bits[12:32] = reserved
	if (s.Seed >> 12) != 0 {
		return nil, fmt.Errorf("zll.Shape: unexpected Seed bits 0x%x (unsupported features?)", s.Seed)
	}
	return src[8:], nil
}

// Decode decodes the shape portion of src into
// s.Symtab and s.Bits and returns the buckets portion.
// Note that zion streams tend to be stateful, so the
// order in which Decode is called on sequences of blocks
// will change how s.Symtab is computed.
func (s *Shape) Decode(src []byte) ([]byte, error) {
	src, err := s.checkMagic(src)
	if err != nil {
		return nil, err
	}
	s.Start = 0
	var skip int
	s.Bits, skip, err = s.Algo().Decompress(src, s.Bits[:0])
	if err != nil {
		return nil, err
	}
	if ion.IsBVM(s.Bits) || ion.TypeOf(s.Bits) == ion.AnnotationType {
		rest, err := s.Symtab.Unmarshal(s.Bits)
		if err != nil {
			return nil, err
		}
		s.Start = len(s.Bits) - len(rest)
	}
	return src[skip:], nil
}

// Buckets represents the decompression state of
// the "buckets" portion of a zion block.
type Buckets struct {
	// Shape is used to determine the seed and
	// symbol table used for populating the right buckets.
	Shape *Shape

	// Pos is the starting position of each
	// bucket within Decompressed, or -1 if the bucket
	// has not yet been decoded.
	Pos [NumBuckets]int32
	// Decompressed contains the raw decompressed buckets
	Decompressed []byte
	// Compressed contains the compressed buckets
	Compressed []byte
	// SymbolBits is a bitmap of symbol IDs;
	// only top-level symbols that need to be
	// extracted have their corresponding bit set.
	SymbolBits []uint64
	// BucketBits is a bitmap of buckets;
	// bit N == 1 implies that bucket N has been
	// decompressed.
	BucketBits uint32
	// Decomps is the number of individual bucket
	// decompression operations that have been performed.
	Decomps int

	// SkipPadding, if set, causes the calls to
	// Select and SelectSymbols to omit padding
	// Decompressed. If SkipPadding is not set,
	// then Decompressed is padded so that its
	// capacity allows the byte at len(Decompressed)-1
	// to be read with an 8-byte load.
	SkipPadding bool
}

// Reset resets the state of b to point to the given
// shape and compressed buckets.
func (b *Buckets) Reset(shape *Shape, compressed []byte) {
	b.SymbolBits = b.SymbolBits[:0]
	for i := 0; i < NumBuckets; i++ {
		b.Pos[i] = -1
	}
	b.Decompressed = b.Decompressed[:0]
	b.Compressed = compressed
	b.BucketBits = 0
	b.Shape = shape
}

func (b *Buckets) want(bucket int) bool {
	return b.BucketBits&(1<<bucket) != 0
}

func (b *Buckets) clearBits() {
	b.SymbolBits = b.SymbolBits[:0]
	b.BucketBits = 0
}

func (b *Buckets) setBit(sym ion.Symbol) {
	v := uint(sym)
	word := int(v >> 6)
	for len(b.SymbolBits) <= word {
		b.SymbolBits = append(b.SymbolBits, 0)
	}
	b.SymbolBits[word] |= 1 << (v & 63)
	b.BucketBits |= 1 << b.Shape.SymbolBucket(sym)
}

// Selected indicates whether or not the symbol is one
// of the symbols selected by Select for the current symbol table.
func (b *Buckets) Selected(sym ion.Symbol) bool {
	v := uint(sym)
	word := int(v >> 6)
	if len(b.SymbolBits) < word {
		return false
	}
	return (b.SymbolBits[word] & (1 << (v & 63))) != 0
}

// Select ensures that all the buckets corresponding
// to the selected components are already decompressed.
// Supplying a nil list of components causes all buckets
// to be decompressed. Select may be called more than once
// with different sets of components. Each time Select is
// called, it resets b.BucketBits and b.SymbolBits to
// correspond to the most-recently-selected components,
// but it does not reset the b.Pos displacements into
// decompressed data.
func (b *Buckets) Select(components []string) error {
	b.clearBits()
	if components == nil {
		b.SelectAll()
	}
	for _, comp := range components {
		sym, ok := b.Shape.Symtab.Symbolize(comp)
		if !ok {
			continue
		}
		b.setBit(sym)
	}
	return b.decompSelected()
}

// SelectSymbols works identically to Select, but it picks
// the top-level path components by their symbol IDs rather
// than the names of the path components.
func (b *Buckets) SelectSymbols(syms []ion.Symbol) error {
	b.clearBits()
	for _, sym := range syms {
		b.setBit(sym)
	}
	return b.decompSelected()
}

// ensure the final byte in buf
// can be loaded with a MOVQ
func pad8(buf []byte) []byte {
	l := (len(buf) + 8) & 7
	return slices.Grow(buf, l)
}

func (b *Buckets) decompSelected() error {
	parts := b.Compressed
	algo := b.Shape.Algo()
	for i := 0; i < NumBuckets; i++ {
		var skip int
		var err error
		if !b.want(i) || b.Pos[i] >= 0 {
			// either not wanted or already decompressed -> continue
			skip, err = FrameSize(parts)
		} else {
			b.Pos[i] = int32(len(b.Decompressed))
			b.Decompressed, skip, err = algo.Decompress(parts, b.Decompressed)
			b.Decomps++
		}
		if err != nil {
			return err
		}
		parts = parts[skip:]
	}
	if !b.SkipPadding {
		b.Decompressed = pad8(b.Decompressed)
	}
	return nil
}

// SelectAll is equivalent to b.Select(nil)
func (b *Buckets) SelectAll() error {
	b.BucketBits = (1 << NumBuckets) - 1
	return b.decompSelected()
}
