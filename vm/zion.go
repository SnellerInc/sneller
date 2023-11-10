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

package vm

import (
	"encoding/binary"
	"fmt"
	"slices"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"

	"github.com/SnellerInc/sneller/internal/memops"
)

type zionState struct {
	shape     zll.Shape
	buckets   zll.Buckets
	blocksize int64
}

type zionConsumer interface {
	symbolize(st *symtab, aux *auxbindings) error
	zionOk(fields []string) bool
	writeZion(state *zionState) error
}

// zionFlattener is a wrapper for rowConsumers
// that do not implement zionConsumer
type zionFlattener struct {
	rowConsumer // inherit writeRows, Close(), next(), etc.
	infields    []string

	// cached structures:
	myaux   auxbindings
	strided []vmref
	params  rowParams
	tape    []ion.Symbol
	empty   []vmref
}

// we only flatten when the number of fields is small;
// otherwise we have to allocate a bunch of space to
// write out all the vmrefs (columns * rows, 8 bytes each)
// which might actually be *larger* than the data we have to copy...
const maxFlatten = 8

func (z *zionFlattener) zionOk(fields []string) bool {
	if len(fields) > 0 && len(fields) < maxFlatten {
		z.infields = append(z.infields[:0], fields...)
		return true
	}
	return false
}

func (z *zionFlattener) symbolize(st *symtab, aux *auxbindings) error {
	if len(aux.bound) != 0 {
		panic("zionFlattener not the top element in the rowConsumer chain?")
	}
	z.tape = z.tape[:0]
	for _, name := range z.infields {
		sym, ok := st.Symbolize(name)
		if !ok {
			continue
		}
		z.tape = append(z.tape, sym)
	}
	slices.Sort(z.tape)

	// we're going to bind auxbound in symbol order
	z.myaux.reset()
	for i := range z.tape {
		z.myaux.push(st.Get(z.tape[i]))
	}
	return z.rowConsumer.symbolize(st, &z.myaux)
}

const (
	//lint:ignore U1000 used in assembly
	zllBucketPos          = unsafe.Offsetof(zll.Buckets{}.Pos)
	zllBucketDecompressed = unsafe.Offsetof(zll.Buckets{}.Decompressed)

	// We try to process zionStride rows at a time from the shape. Must be a power of 2.
	zionStrideLog2 = 8
	zionStride     = 1 << zionStrideLog2
)

func empty(src []vmref, n int) []vmref {
	if cap(src) >= n {
		for i := range src {
			src[i] = vmref{}
		}
		return src[:n]
	}

	return make([]vmref, n)
}

// convert a writeZion into a writeRows
// by projecting into auxparams
func (z *zionFlattener) writeZion(state *zionState) error {
	if len(z.tape) == 0 {
		// unusual edge-case: none of the matched symbols
		// are part of the symbol table; just count
		// the number of rows and emit empty rows
		n, err := state.shape.Count()
		if err != nil {
			return err
		}
		z.params.auxbound = z.params.auxbound[:0]
		z.empty = empty(z.empty, n)
		return z.writeRows(z.empty, &z.params)
	}

	// force decompression of the buckets we want
	err := state.buckets.SelectSymbols(z.tape)
	if err != nil {
		return err
	}

	// allocate space for up to zionStride rows * columns;
	// each "column" starts at z.strided[column * zionStride:]
	// which simplifies the assembly a bit
	z.strided = sanitizeAux(z.strided, len(z.tape)*zionStride)
	posn := state.buckets.Pos
	// set slice sizes for all the fields
	z.params.auxbound = shrink(z.params.auxbound, len(z.tape))
	pos := state.shape.Start
	for pos < len(state.shape.Bits) {

		in, out := zionflatten(state.shape.Bits[pos:], &state.buckets, z.strided, z.tape)

		pos += in
		if pos > len(state.shape.Bits) {
			panic("read out-of-bounds")
		}
		if out > zionStride {
			panic("write out-of-bounds")
		}
		if out <= 0 || in <= 0 {
			err = fmt.Errorf("couldn't copy out zion data (data corruption?)")
			break
		}
		for i := range z.params.auxbound {
			z.params.auxbound[i] = sanitizeAux(z.strided[i*zionStride:], out)
		}
		// the callee is allowed to clobber this,
		// so it has to be re-zeroed on each iteration
		z.empty = empty(z.empty, out)
		err = z.writeRows(z.empty, &z.params)
		if err != nil {
			break
		}
	}
	state.buckets.Pos = posn // restore bucket positions
	return err
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

func zionflattenReference(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (in, out int) {
	memops.ZeroMemory(fields)

	// TODO: should be provided for testing purposes and for the non-x64 platforms
	initShapeLen := len(shape)
	outCnt := 0
	fieldOffs := 0
	tapeOffs := 0

	for len(shape) > 0 {
		fc := shape[0] & 0x1f
		if fc > 16 {
			fmt.Printf("zion.Decoder.walk: fc = %x", fc)
			return 0, outCnt
		}
		skip := int((fc + 3) / 2)
		if len(shape) < skip {
			fmt.Printf("zion.Decoder.walk: skip %d > len(shape)=%d", skip, len(shape))
			return 0, outCnt
		}

		// decode nibbles into structure fields
		nibbles := load64(shape[1:])
		shape = shape[skip:]
		for i := 0; i < int(fc); i++ {
			b := nibbles & 0xf
			nibbles >>= 4
			if buckets.Pos[b] < 0 {
				continue // bucket not decompressed
			}
			buf := buckets.Decompressed[buckets.Pos[b]:]
			if len(buf) == 0 {
				fmt.Printf("zion.Decoder.walk: unexpected bucket EOF")
				return 0, outCnt
			}
			sym, rest, err := ion.ReadLabel(buf)

			if err != nil {
				fmt.Printf("zion.Decoder.walk: %s (%d bytes remaining)", err, len(buf))
				return 0, outCnt
			}

			for {
				if tapeOffs >= len(tape) || sym < tape[tapeOffs] {
					fieldsize := ion.SizeOf(rest)
					if fieldsize <= 0 || fieldsize > len(rest) {
						fmt.Printf("zion.Decoder.walk: SizeOf=%d", fieldsize)
						return 0, outCnt
					}
					size := fieldsize + (len(buf) - len(rest))
					buckets.Pos[b] += int32(size)
					break
				} else if sym == tape[tapeOffs] {
					tapeOffs++
					fieldsize := ion.SizeOf(rest)
					if fieldsize <= 0 || fieldsize > len(rest) {
						fmt.Printf("zion.Decoder.walk: SizeOf=%d", fieldsize)
						return 0, outCnt
					}
					size := fieldsize + (len(buf) - len(rest))
					buckets.Pos[b] += int32(size)

					d, ok := vmdispl(rest)

					if !ok {
						fmt.Printf("vmdispl failed")
						return 0, outCnt
					}
					fields[fieldOffs+outCnt] = vmref{d, uint32(fieldsize)}
					fieldOffs += zionStride
					break
				} else { // sym > tape[tapeOffs]
					fields[fieldOffs+outCnt] = vmref{0, 0}
					fieldOffs += zionStride
					tapeOffs++
				}
			}
		}

		if fc < 16 {
			for tapeOffs < len(tape) {
				fields[fieldOffs+outCnt] = vmref{0, 0}
				fieldOffs += zionStride
				tapeOffs++
			}

			outCnt++
			if outCnt == zionStride {
				break
			}

			fieldOffs = 0
			tapeOffs = 0
		}
	}

	return initShapeLen - len(shape), outCnt
}

// zionflatten unpacks the contents of buckets that match 'tape'
// into the corresponding vmref slices
//
// prerequisites:
//   - len(fields) == len(tape)*zionStride
//   - len(shape) > 0
//   - len(tape) > 0
func zionflatten(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (int, int) {
	if portable.Load() {
		return zionflattenReference(shape, buckets, fields, tape)
	}
	return zionFlattenAsm(shape, buckets, fields, tape)
}
