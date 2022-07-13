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
	"fmt"

	"github.com/SnellerInc/sneller/ion"
)

type bucket struct {
	mem  []byte // raw bytes belonging to this bucket
	base int    // base offset for this structure
}

func (b *bucket) append(mem []byte) {
	b.mem = append(b.mem, mem...)
}

// Encoder is used to compress sequential blocks
// of ion data. See Encoder.Encode and Decoder.Decode.
type Encoder struct {
	st         ion.Symtab
	sym2bucket []uint8
	shape      []byte

	// the encoded format of a "frame" is simply
	// buckets+1 compressed frames; the first frame
	// is the symbol table plus the "shape" and the
	// remaining frames are the buckets, in-order
	enc  shapeEncoder
	buck [buckets]bucket
	seed uint32
}

// Reset resets the Encoder's internal
// symbol table and its seed (see SetSeed).
func (e *Encoder) Reset() {
	e.st.Reset()
	e.sym2bucket = e.sym2bucket[:0]
	e.shape = e.shape[:0]
	e.seed = 0
	e.enc = shapeEncoder{}
}

// SetSeed sets the seed used for hashing
// symbol IDs to buckets.
func (e *Encoder) SetSeed(x uint32) {
	// precomputed symbol->bucket mapping
	// is invalidated each time the seed
	// is changed:
	e.sym2bucket = e.sym2bucket[:0]
	e.seed = x
}

// Encode encodes ion data from src by appending it to dst.
// Encode parses ion symbol tables from src as they appear,
// so the output stream may not be order-independent (the chunks
// encoded via Encode should be decoded via Decoder.Decode in the
// same order in which they are encoded).
//
// Note that the compression format does not preserve
// nop padding in ion data. In other words, data passed
// to Encode may not be bit-identical to data received
// from Decode if the input data contains nop pads.
func (e *Encoder) Encode(src, dst []byte) ([]byte, error) {
	for i := 0; i < buckets; i++ {
		e.buck[i].mem = e.buck[i].mem[:0]
		e.buck[i].base = 0
	}
	if ion.IsBVM(src) {
		// reset precomputed bucket numbers
		// if the symbol table isn't a strict append
		e.sym2bucket = e.sym2bucket[:0]
	}
	var body []byte
	var err error
	if ion.IsBVM(src) || ion.TypeOf(src) == ion.AnnotationType {
		body, err = e.st.Unmarshal(src)
		if err != nil {
			return nil, err
		}
		// shape starts with the symbol table
		e.shape = append(e.shape[:0], src[:len(src)-len(body)]...)
	} else {
		body = src
		e.shape = e.shape[:0]
	}
	e.precompute()
	// walk for shape, pushing fields into buckets,
	// appending to shape for metadata
	err = e.walk(body)
	if err != nil {
		return nil, err
	}
	// TODO: try multiple seed values and pick
	// the one that produces the most even distribution
	// of compressed bucket sizes?
	dst = appendMagic(dst, e.seed)
	dst, err = compress(e.shape, dst)
	if err != nil {
		return nil, err
	}
	for i := 0; i < buckets; i++ {
		dst, err = compress(e.buck[i].mem, dst)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// precompute a look-up-table for symbol IDs to buckets
func (e *Encoder) precompute() {
	syms := e.st.MaxID()
	for len(e.sym2bucket) < syms {
		n := len(e.sym2bucket)
		e.sym2bucket = append(e.sym2bucket, uint8(sym2bucket(uint64(e.seed), ion.Symbol(n))))
	}
}

func skipOne(mem []byte) ([]byte, error) {
	s := ion.SizeOf(mem)
	if s <= 0 || s > len(mem) {
		return nil, fmt.Errorf("zion: illegal ion object size %d (buf size %d)", s, len(mem))
	}
	return mem[s:], nil
}

func (e *Encoder) walk(mem []byte) error {
	var err error
	for len(mem) > 0 {
		mem, err = e.walkOne(mem)
		if err != nil {
			return err
		}
	}
	return nil
}

func class(x int) int {
	switch {
	case x < 0xe:
		return 0
	case x < 1<<7:
		return 1
	case x < 1<<14:
		return 2
	case x < 1<<21:
		return 3
	default:
		panic("illegal size class")
	}
}

func (e *Encoder) walkOne(mem []byte) ([]byte, error) {
	t := ion.TypeOf(mem)
	switch t {
	default:
		// make sure we don't run into a symbol table
		// or something else that would be semantically important!
		return nil, fmt.Errorf("zion.Encoder.Encode: top-level value of type %s", t)
	case ion.NullType:
		// nop pad
		return skipOne(mem)
	case ion.StructType:
		// okay
	}
	self, rest := ion.Contents(mem)
	if self == nil {
		return nil, fmt.Errorf("invalid ion body")
	}
	if len(mem)-len(rest) >= maxSize {
		return nil, fmt.Errorf("structure size %d exceeds max size %d", len(mem)-len(rest), maxSize)
	}
	var err error
	e.enc.output = e.shape
	e.enc.start(class(len(self)))
	for len(self) > 0 {
		self, err = e.encodeField(self)
		if err != nil {
			return nil, err
		}
	}
	e.enc.finish()
	e.shape = e.enc.output
	return rest, nil
}

func (e *Encoder) encodeFlat(sym ion.Symbol, fieldval []byte) {
	b := e.sym2bucket[sym]
	e.enc.emit(b)
	e.buck[b].append(fieldval)
}

func (e *Encoder) encodeField(mem []byte) ([]byte, error) {
	sym, rest, err := ion.ReadLabel(mem)
	if err != nil {
		return nil, err
	}
	s := ion.SizeOf(rest)
	if s <= 0 || s > len(mem) {
		return nil, fmt.Errorf("zion.Encoder.encodeField: illegal ion object size %d (buf size %d)", s, len(mem))
	}
	// encode a terminal value
	s += len(mem) - len(rest)
	e.encodeFlat(sym, mem[:s])
	return mem[s:], nil
}
