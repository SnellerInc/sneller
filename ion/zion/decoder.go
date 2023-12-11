// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package zion

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"

	"slices"
)

// faults are error codes returned from the assembly
type fault int32

const (
	noFault        fault = iota // no error encountered
	faultTooLarge               // not enough room in the output buffer
	faultBadData                // unexpected data in the input
	faultTruncated              // input data truncated (or Ion corrupted)
)

func (f fault) String() string {
	switch f {
	default:
		return "unknown fault"
	case noFault:
		return "no fault"
	case faultTooLarge:
		return "not enough room in the output buffer"
	case faultBadData:
		return "unexpected data in the input"
	case faultTruncated:
		return "input data truncated"
	}
}

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
	nums [zll.NumBuckets]uint8 // unpacked bucket references
	// used in assembly to track the current decoding displacement
	base [zll.NumBuckets]int32

	shape   zll.Shape
	buckets zll.Buckets

	out []byte
	tmp []byte
	dst io.Writer
	nn  int64

	fault fault

	st symtab

	// if precise is true, then components is
	// the list of top-level fields to extract;
	// if !precise then all fields should be extracted
	components []string
	precise    bool
	portable   bool // don't use arch-specific acceleration
}

// Reset resets the internal decoder state,
// including the internal symbol table.
func (d *Decoder) Reset() {
	d.TargetWriteSize = 0
	d.components = nil
	d.st.reset()
	d.out = d.out[:0]
	d.tmp = d.tmp[:0]
	d.dst = nil
	d.fault = noFault
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

func (d *Decoder) prepare(src, dst []byte) ([]byte, error) {
	d.shape.Symtab = &d.st
	body, err := d.shape.Decode(src)
	if err != nil {
		return nil, err
	}
	// copy symbol table bits into output
	dst = append(dst[:0], d.shape.Bits[:d.shape.Start]...)
	d.buckets.Reset(&d.shape, body)
	for i := range d.base {
		d.base[i] = 0
	}
	if d.precise {
		err = d.buckets.SelectSymbols(d.st.selected)
	} else {
		err = d.buckets.SelectAll()
	}
	if err != nil {
		return nil, err
	}
	d.out = dst
	return d.shape.Bits[d.shape.Start:], nil
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
	d.shape.Symtab = &d.st
	_, err := d.shape.Decode(src)
	if err != nil {
		return 0, err
	}
	return d.shape.Count()
}

// SetPortable sets the decoder's portability flag.
// If the portability flag is set, then the Decoder
// uses a pure-Go decoding routine even if an architecture-specific
// non-portable decoding routine is available.
func (d *Decoder) SetPortable(p bool) {
	d.portable = p
}

func load64(buf []byte) uint64 {
	if cap(buf) >= 8 {
		return binary.LittleEndian.Uint64(buf[:8])
	}
	u := uint64(0)
	for i, b := range buf {
		u |= uint64(b) << (i * 8)
	}
	return u
}

func (d *Decoder) walk(shape []byte) error {
	if !d.portable && d.haveasm() {
		return d.walkasm(shape)
	}

	d.base = [zll.NumBuckets]int32{}
	instruct := false
	var result ion.Buffer
	result.Set(d.out)
	for len(shape) > 0 {
		fc := shape[0] & 0x1f
		if fc > 16 {
			return fmt.Errorf("zion.Decoder.walk: fc = %x", fc)
		}
		skip := int((fc + 3) / 2)
		if len(shape) < skip {
			return fmt.Errorf("zion.Decoder.walk: skip %d > len(shape)=%d", skip, len(shape))
		}
		if !instruct {
			result.BeginStruct(-1)
			instruct = true
		}

		// decode nibbles into structure fields
		nibbles := load64(shape[1:])
		shape = shape[skip:]
		for i := 0; i < int(fc); i++ {
			b := nibbles & 0xf
			nibbles >>= 4
			if d.buckets.Pos[b] < 0 {
				continue // bucket not decompressed
			}
			buf := d.buckets.Decompressed[d.buckets.Pos[b]+d.base[b]:]
			if len(buf) == 0 {
				return fmt.Errorf("zion.Decoder.walk: unexpected bucket EOF")
			}
			sym, rest, err := ion.ReadLabel(buf)
			if err != nil {
				return fmt.Errorf("zion.Decoder.walk: %w (%d bytes remaining)", err, len(buf))
			}
			fieldsize := ion.SizeOf(rest)
			if fieldsize <= 0 || fieldsize > len(rest) {
				return fmt.Errorf("zion.Decoder.walk: SizeOf=%d", fieldsize)
			}
			size := fieldsize + (len(buf) - len(rest))
			d.base[b] += int32(size)
			if d.precise && !d.buckets.Selected(sym) {
				continue
			}
			result.BeginField(sym)
			result.UnsafeAppend(rest[:fieldsize])
		}

		if fc < 16 {
			result.EndStruct()
			instruct = false
		}
	}
	if instruct {
		return fmt.Errorf("zion.Decoder.walk: missing terminal 0x10 fc marker")
	}
	d.out = result.Bytes()
	if d.dst != nil {
		// flush the fields we've got so far
		n, err := d.dst.Write(d.out)
		d.out = d.out[:0]
		d.nn += int64(n)
		if err != nil {
			return err
		}
	}
	return nil
}
