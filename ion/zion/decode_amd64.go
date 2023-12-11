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

//go:build amd64

package zion

import (
	"errors"
	"slices"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"

	"golang.org/x/sys/cpu"
)

const (
	//lint:ignore U1000 used in asm
	posOff = unsafe.Offsetof(zll.Buckets{}.Pos)
	//lint:ignore U1000 used in asm
	decompOff = unsafe.Offsetof(zll.Buckets{}.Decompressed)
	//lint:ignore U1000 used in asm
	bitsOff = unsafe.Offsetof(zll.Buckets{}.SymbolBits)
)

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
	bucket := d.shape.SymbolBucket(sym)
	pos := d.buckets.Pos[bucket]
	mem := d.buckets.Decompressed[pos:]
	if bucket < zll.NumBuckets-1 && d.buckets.Pos[bucket+1] >= 0 {
		mem = d.buckets.Decompressed[:d.buckets.Pos[bucket+1]]
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

func (d *Decoder) haveasm() bool {
	return cpu.X86.HasAVX512
}

// walk walks objects in shape and appends them to d.out
func (d *Decoder) walkasm(shape []byte) error {
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
		case faultBadData, faultTruncated:
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
				if avail >= zll.MaxBucketSize {
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
