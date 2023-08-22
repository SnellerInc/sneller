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
