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
	"fmt"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"

	"golang.org/x/exp/slices"
)

type zionState struct {
	shape      zll.Shape
	buckets    zll.Buckets
	components []string
}

type zionConsumer interface {
	zionOk(fields []string) bool
	writeZion(state *zionState) error
}

// zionFlattener is a wrapper for rowConsumers
// that do not implement zionConsumer
type zionFlattener struct {
	rowConsumer // inherit writeRows, Close(), next(), etc.
	infields    []string

	// cached structures:
	myaux  auxbindings
	params rowParams
	tape   []ion.Symbol
	empty  []vmref
}

// we only flatten when the number of fields is small;
// otherwise we have to allocate a bunch of space to
// write out all the vmrefs (columns * rows, 8 bytes each)
// which might actually be *larger* than the data we have to copy...
const maxFlatten = 8

func (z *zionFlattener) zionOk(fields []string) bool {
	return len(fields) > 0 && len(fields) < maxFlatten
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

// zionflatten unpacks the contents of buckets that match 'tape'
// into the corresponding vmref slices
//
// prerequisites:
//   - len(fields) == len(tape)
//   - len(fields[*]) == shape count
//
//go:noescape
func zionflatten(shape []byte, buckets *zll.Buckets, fields [][]vmref, tape []ion.Symbol) int

const (
	//lint:ignore U1000 used in assembly
	zllBucketPos          = unsafe.Offsetof(zll.Buckets{}.Pos)
	zllBucketDecompressed = unsafe.Offsetof(zll.Buckets{}.Decompressed)
)

// convert a writeZion into a writeRows
// by projecting into auxparams
func (z *zionFlattener) writeZion(state *zionState) error {
	n, err := state.shape.Count()
	if err != nil {
		return err
	}
	// force decompression of the buckets we want
	err = state.buckets.SelectSymbols(z.tape)
	if err != nil {
		return err
	}

	// set slice sizes for all the fields
	z.empty = sanitizeAux(z.empty, n)
	z.params.auxbound = shrink(z.params.auxbound, len(z.tape))
	for i := range z.params.auxbound {
		z.params.auxbound[i] = sanitizeAux(z.params.auxbound[i], n)
	}

	count := zionflatten(state.shape.Bits[state.shape.Start:], &state.buckets, z.params.auxbound, z.tape)
	if count < n {
		return fmt.Errorf("couldn't copy out zion data (data corruption?)")
	} else if count > n {
		println(count, ">", n)
		panic("write out-of-bounds")
	}
	return z.writeRows(z.empty, &z.params)
}
