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
	"reflect"
	"unsafe"
)

type vRegLayout struct {
	offsets [16]uint32
	sizes   [16]uint32
}

const kRegSize = 2
const sRegSize = 128
const vRegSize = 128
const bRegSize = 128
const hRegSize = 256
const lRegSize = 128

func vRegLayoutFromVStackCast(vstack *[]uint64, count int) (out []vRegLayout) {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(vstack))
	outhdr := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outhdr.Data = hdr.Data
	outhdr.Len = count
	outhdr.Cap = hdr.Cap / vRegSize
	return
}

// item returns the i'th item in the vReg
func (v *vRegLayout) item(i int) vmref {
	return vmref{v.offsets[i], v.sizes[i]}
}

// getdelim produces the delimiter associated
// with a particular field in a particular
// record returned by bcfind() based on the
// record and field indices and the number
// of fields produced by bcfind
func getdelim(out []vRegLayout, record, field, nfields int) vmref {
	// each group of nfields registers corresponds to 16 records
	blk := (nfields * (record / 16)) + field
	// with in each register, lanes are 1:1
	lane := (record & 15)
	return out[blk].item(lane)
}
