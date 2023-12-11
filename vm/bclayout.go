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

package vm

import (
	"unsafe"

	"github.com/SnellerInc/sneller/internal/asmutils"
)

type kRegData struct {
	mask uint16
}

type bRegData struct {
	offsets [bcLaneCount]uint32
	sizes   [bcLaneCount]uint32
}

type vRegData struct {
	offsets    [bcLaneCount]uint32
	sizes      [bcLaneCount]uint32
	typeL      [bcLaneCount]byte
	headerSize [bcLaneCount]byte
}

type sRegData struct {
	offsets [bcLaneCount]uint32
	sizes   [bcLaneCount]uint32
}

func (s *sRegData) fill(v uint32) {
	for i := 0; i < bcLaneCount; i++ {
		s.offsets[i] = v
		s.sizes[i] = v
	}
}

type hRegData struct {
	lo, hi [bcLaneCount]uint64
}

type i64RegData struct {
	values [bcLaneCount]int64
}

type f64RegData struct {
	values [bcLaneCount]float64
}

func (k *kRegData) getBit(idx int) bool {
	return (k.mask>>idx)&1 == 1
}

func (k *kRegData) setBit(idx int) {
	k.mask |= 1 << idx
}

type litref struct {
	offset uint32
	length uint32
	tlv    uint8
	hLen   uint8
}

const (
	kRegSize = 2
	bRegSize = 128
	vRegSize = 160
	sRegSize = 128
	hRegSize = 256
	lRegSize = 128
)

// assert that the size of each reg struct matches the sizes understood by VM
const (
	_ = ^uintptr(0) + (uintptr(kRegSize) - unsafe.Sizeof(kRegData{}))
	_ = ^uintptr(0) + (uintptr(bRegSize) - unsafe.Sizeof(bRegData{}))
	_ = ^uintptr(0) + (uintptr(vRegSize) - unsafe.Sizeof(vRegData{}))
	_ = ^uintptr(0) + (uintptr(sRegSize) - unsafe.Sizeof(sRegData{}))
	_ = ^uintptr(0) + (uintptr(sRegSize) - unsafe.Sizeof(i64RegData{}))
	_ = ^uintptr(0) + (uintptr(sRegSize) - unsafe.Sizeof(f64RegData{}))
	_ = ^uintptr(0) + (uintptr(hRegSize) - unsafe.Sizeof(hRegData{}))
)

func vRegDataFromVStackCast(vstack *[]uint64, count int) (out []vRegData) {
	hdr := (*asmutils.SliceHeader)(unsafe.Pointer(vstack))
	outhdr := (*asmutils.SliceHeader)(unsafe.Pointer(&out))
	outhdr.Data = hdr.Data
	outhdr.Len = count
	outhdr.Cap = hdr.Cap / vRegSize
	return
}

// item returns the i'th item in the vReg
func (v *vRegData) item(i int) vmref {
	return vmref{v.offsets[i], v.sizes[i]}
}

// getdelim produces the delimiter associated
// with a particular field in a particular
// record returned by bcfind() based on the
// record and field indices and the number
// of fields produced by bcfind
func getdelim(out []vRegData, record, field, nfields int) vmref {
	// each group of nfields registers corresponds to 16 records
	blk := (nfields * (record / 16)) + field
	// with in each register, lanes are 1:1
	lane := (record & 15)
	return out[blk].item(lane)
}
