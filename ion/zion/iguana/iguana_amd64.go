// Copyright (C) 2023 Sneller, Inc.
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

// Package iguana implements a Lizard-derived compression/decompression pipeline
package iguana

import (
	"reflect"
	"unsafe"

	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX512VPOPCNTDQ {
		pickBestMatch = pickBestMatchVPOPCNTDQ
	}
	if cpu.X86.HasAVX512VBMI2 && cpu.X86.HasAVX512VBMI {
		decompressIguana = decompressIguanaVBMI2
	}
}

const offsSliceHeaderData = unsafe.Offsetof(reflect.SliceHeader{}.Data) //lint:ignore U1000 used in assembly
const offsSliceHeaderLen = unsafe.Offsetof(reflect.SliceHeader{}.Len)   //lint:ignore U1000 used in assembly
const offsSliceHeaderCap = unsafe.Offsetof(reflect.SliceHeader{}.Cap)   //lint:ignore U1000 used in assembly
const sizeSliceHeader = unsafe.Sizeof(reflect.SliceHeader{})            //lint:ignore U1000 used in assembly

//go:noescape
//go:nosplit
func decompressIguanaVBMI2(dst []byte, streams *streamPack, lastOffs *int) ([]byte, errorCode)

//gox:noescape
//go:nosplit
func pickBestMatchVPOPCNTDQ(ec *iguanaEncodingContext, src []byte, candidates []uint32) matchDescriptor
