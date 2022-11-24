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
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
)

// faults are error codes returned from the assembly
type fault int32

const (
	noFault       fault = iota // no error encountered
	faultTooLarge              // not enough room in the output buffer
	faultBadData               // unexpected data in the input
)

const (
	DefaultTargetWrite = 128 * 1024
)

// Decoder is a stateful decoder of compressed
// data produced with Encoder.Encode.
//
// The zero value of Decoder has the "wildcard"
// flag set, which means it decodes 100% of the
// structure fields in the input.
// Calls to Decoder.SetComponents can pick a subset
// of the fields that are to be projected, and calls
// to Decoder.SetWildcard can re-enable the wildcard flag.
type Decoder struct {
	TargetWriteSize int

	//lint:ignore U1000 used in assembly as a scratch buffer
	nums [buckets]uint8 // unpacked bucket references

	out []byte
	mem []byte
	tmp []byte
	dst io.Writer
	nn  int64

	// these are broken up so that
	// they can be adjusted with a
	// single vpaddd instruction
	pos  [buckets]int32
	base [buckets]int32

	fault fault

	st         symtab
	set        pathset
	components []string // if precise, decode these fields
	precise    bool     // if !precise, decode everything

	decomps int
	// seed is the full 32-bit value
	// from the input stream; currently
	// only the lowest nibble is used
	seed uint32
}

func pad8(buf []byte) []byte {
	l := (len(buf) + 8) & 7
	return slices.Grow(buf, l)
}

// Reset resets the internal decoder state,
// including the internal symbol table.
func (d *Decoder) Reset() {
	d.TargetWriteSize = 0
	d.components = nil
	d.st.reset()
	d.set.clear()
	d.mem = d.mem[:0]
	d.out = d.out[:0]
	d.tmp = d.tmp[:0]
	d.dst = nil
	d.fault = 0
	d.decomps = 0
}

// SetWildcard tells the decoder to decode
// all input fields. This clears any field
// selection made by SetComponents.
//
// The zero value of Decoder has the wildcard flag set.
func (d *Decoder) SetWildcard() {
	d.precise = false
	d.components = d.components[:0]
	d.st.components = nil
}

// Wildcard reads the status of the decoder wildcard flag.
// See also SetWildcard and SetComponents.
func (d *Decoder) Wildcard() bool { return !d.precise }

// SetComponents sets the leading path components
// that should be copied out during calls to Decode.
// SetComponents may be overridden by another call
// to SetComponents or SetWildcard.
//
// The "leading path component" is the first component
// of a path, so the path x.y.z has x as its first component.
func (d *Decoder) SetComponents(x []string) {
	// for safety, make a copy of x so that
	// the caller doesn't have to worry about
	// us sorting+compacting x
	d.components = slices.Grow(d.components[:0], len(x))
	d.components = d.components[:len(x)]
	copy(d.components, x)
	slices.Sort(d.components)
	d.components = slices.Compact(d.components)
	d.precise = true

	d.st.components = make([]component, len(x))
	for i := range d.st.components {
		d.st.components[i].name = x[i]
		d.st.components[i].symbol = ^ion.Symbol(0)
	}
}

func (d *Decoder) checkMagic(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("zion.Decoder: len(input)=%d; missing magic", len(src))
	}
	if !bytes.Equal(src[:4], magic) {
		return nil, fmt.Errorf("zion.Decoder: bad magic bytes %x", src[:len(magic)])
	}
	d.seed = binary.LittleEndian.Uint32(src[4:])
	d.set.selector = uint8(d.seed & 0xf)
	return src[8:], nil
}

func (d *Decoder) prepare(src, dst []byte) ([]byte, error) {
	src, err := d.checkMagic(src)
	if err != nil {
		return nil, err
	}
	var skip int
	d.mem, skip, err = decompress(src, d.mem[:0])
	if err != nil {
		return nil, fmt.Errorf("zion.Decoder: getting shape: %w", err)
	}
	src = src[skip:]
	var shape []byte
	if ion.IsBVM(d.mem) || ion.TypeOf(d.mem) == ion.AnnotationType {
		shape, err = d.st.Unmarshal(d.mem)
		if err != nil {
			return nil, fmt.Errorf("zion.Decoder: parsing symbol table: %w", err)
		}
		// copy symbol table into output
		dst = append(dst, d.mem[:len(d.mem)-len(shape)]...)
	} else {
		shape = d.mem
	}
	d.set.clear()
	for i := range d.st.components {
		sym := d.st.components[i].symbol
		if sym == ^ion.Symbol(0) {
			continue
		}
		d.set.set(sym)
	}
	d.out = dst

	// we can avoid decompressing any buckets
	// at all if none of the symbols we care about
	// are present in any buckets
	for i := 0; i < buckets; i++ {
		d.base[i] = 0
		if d.precise && !d.set.useBucket(i) {
			skip, err = frameSize(src)
			if err != nil {
				return nil, err
			}
			d.pos[i] = -1
		} else {
			d.pos[i] = int32(len(d.mem))
			d.mem, skip, err = decompress(src, d.mem)
			if err != nil {
				return nil, err
			}
			d.decomps++
		}
		src = src[skip:]
	}
	// always ensure any reference to d.mem
	// can be loaded with a MOVQ:
	d.mem = pad8(d.mem)
	return shape, nil
}

// Decode performs a statefull decoding of src
// by appending into dst. If a particular field selection
// has been selected via d.SetComponents, then Decode *may*
// omit fields that are not part of the selection.
// Sequential calls to Decode build an ion symbol table internally,
// so the order in which blocks are presented to Decode as src
// should match the order in which they were presented to Encoder.Encode.
func (d *Decoder) Decode(src, dst []byte) ([]byte, error) {
	shape, err := d.prepare(src, dst)
	if err != nil {
		return nil, err
	}
	err = d.walk(shape)
	ret := d.out
	d.out = nil
	return ret, err
}

// CopyBytes writes ion data into dst as it is decoded from src.
// CopyBytes works similarly to Decode except that it does not
// require as much data to be buffered at once.
func (d *Decoder) CopyBytes(dst io.Writer, src []byte) (int64, error) {
	if d.tmp == nil {
		size := d.TargetWriteSize
		if size <= 0 {
			size = DefaultTargetWrite
		}
		d.tmp = make([]byte, size)
	}
	shape, err := d.prepare(src, d.tmp[:0])
	if err != nil {
		return 0, err
	}
	d.dst = dst
	err = d.walk(shape)
	nn := d.nn
	d.nn = 0
	d.dst = nil
	d.tmp = d.out[:0]
	d.out = nil
	return nn, err
}

// Count counts the number of structures in src
// rather than decompressing the body of src.
// Note that Count is stateful (it processes symbol
// tables) so that it may be substituted for a call
// to Decode where only the number of stored records
// is of interest.
func (d *Decoder) Count(src []byte) (int, error) {
	src, err := d.checkMagic(src)
	if err != nil {
		return 0, err
	}
	d.mem, _, err = decompress(src, d.mem[:0])
	if err != nil {
		return 0, fmt.Errorf("zion.Decoder.Count: getting shape: %w", err)
	}
	var shape []byte
	shape, err = d.st.Unmarshal(d.mem)
	if err != nil {
		return 0, fmt.Errorf("zion.Decoder.Count: parsing symbol table: %w", err)
	}
	ret, ok := shapecount(shape)
	if !ok {
		return 0, errCorrupt
	}
	return ret, nil
}

// count the number of records in shape
//
//go:noescape
func shapecount(shape []byte) (int, bool)

//go:noescape
func zipfast(src, dst []byte, d *Decoder) (int, int)

//go:noescape
func zipall(src, dst []byte, d *Decoder) (int, int)

//go:noescape
func zipfast1(src, dst []byte, d *Decoder, sym ion.Symbol, count int) (int, int)

var (
	errCorrupt    = errors.New("corrupt input")
	errNoProgress = errors.New("zion.zipfast says noFault but 0 bytes of progress")
)

func (d *Decoder) zip(shape, dst []byte) (int, int) {
	if !d.precise {
		return zipall(shape, dst, d)
	}
	if len(d.st.components) != 1 {
		return zipfast(shape, dst, d)
	}
	// fast-path for single-symbol scan is basically
	// to perform a memcpy of the source bucket
	c, ok := shapecount(shape)
	if !ok {
		d.fault = faultBadData
		return 0, 0
	}
	// extract the decompressed bucket memory
	sym := d.st.components[0].symbol
	if sym == ^ion.Symbol(0) {
		// we're searching for a path that
		// doesn't have an associated symbol,
		// so we just need to produce the empty struct
		// for each of the input shapes
		if len(dst) < c {
			d.fault = faultTooLarge
			return 0, 0
		}
		for i := 0; i < c; i++ {
			dst[i] = 0xd0
		}
		return len(shape), c
	}
	bucket := d.set.bucket(sym)
	pos := d.pos[bucket]
	mem := d.mem[pos:]
	if bucket < buckets-1 && d.pos[bucket+1] >= 0 {
		mem = d.mem[:d.pos[bucket+1]]
	}
	// pre-compute the bounds check:
	// the destination must fit N descriptors
	// of a particular size class, plus the bucket size,
	// plus 7 so that we can copy 8-byte chunks of data:
	if len(dst) < (class(len(mem))+1)*c+len(mem)+7 {
		d.fault = faultTooLarge
		return 0, 0
	}
	consumed, wrote := zipfast1(mem, dst, d, sym, c)
	if consumed > len(mem) {
		panic("read out-of-bounds")
	}
	if wrote > len(dst) {
		panic("zipfast1 wrote out-of-bounds")
	}
	return len(shape), wrote
}

// walk walks objects in shape and appends them to d.out
func (d *Decoder) walk(shape []byte) error {
	for len(shape) > 0 {
		consumed, wrote := d.zip(shape, d.out[len(d.out):cap(d.out)])
		if consumed > 0 {
			avail := cap(d.out) - len(d.out)
			// these two checks here are panics
			// because they indicate that the assembly
			// code has gone entirely off the rails:
			if wrote > avail {
				println("wrote", wrote, "of", avail)
				panic("wrote out-of-bounds")
			}
			if consumed > len(shape) {
				println("read", consumed, "of", len(shape))
				panic("read out-of-bounds")
			}
			d.out = d.out[:len(d.out)+wrote]
			shape = shape[consumed:]
			if d.dst != nil {
				// flush the fields we've got so far
				n, err := d.dst.Write(d.out)
				d.out = d.out[:0]
				d.nn += int64(n)
				if err != nil {
					return err
				}
			}
		} else if wrote > 0 {
			// shouldn't happen; we need to consume
			// at least 1 byte in order to produce results
			panic("consumed == 0 but wrote > 0")
		}
		switch d.fault {
		case faultBadData:
			return errCorrupt
		case noFault:
			if consumed == 0 {
				// this shouldn't happen; if we didn't consume
				// any data, then we should get a fault
				return errNoProgress
			}
		case faultTooLarge:
			if d.dst == nil || consumed == 0 {
				// grow the buffer if we couldn't
				// make progress otherwise
				avail := cap(d.out) - len(d.out)
				if avail >= maxSize {
					return errNoProgress
				}
				if avail == 0 {
					avail = 1024 // start here at least
				}
				d.out = slices.Grow(d.out, avail*2)
			}
		}
	}
	return nil
}
