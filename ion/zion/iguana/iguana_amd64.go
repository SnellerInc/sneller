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

// Package iguana implements a Lizard-derived compression/decompression pipeline
package iguana

import (
	"reflect"
	"unsafe"

	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX512 {
		decompressIguana = decompressIguanaAVX512Generic

		if cpu.X86.HasAVX512VBMI2 && cpu.X86.HasAVX512VBMI {
			decompressIguana = decompressIguanaAVX512VBMI2
		}

		if cpu.X86.HasAVX512CD {
			pickBestMatch = pickBestMatchAVX512CD
		}
	}
}

const offsSliceHeaderData = unsafe.Offsetof(reflect.SliceHeader{}.Data) //lint:ignore U1000 used in assembly
const offsSliceHeaderLen = unsafe.Offsetof(reflect.SliceHeader{}.Len)   //lint:ignore U1000 used in assembly
const offsSliceHeaderCap = unsafe.Offsetof(reflect.SliceHeader{}.Cap)   //lint:ignore U1000 used in assembly
const sizeSliceHeader = unsafe.Sizeof(reflect.SliceHeader{})            //lint:ignore U1000 used in assembly

//go:noescape
func decompressIguanaAVX512Generic(dst []byte, streams *streamPack, lastOffs *int) ([]byte, errorCode)

//go:noescape
func decompressIguanaAVX512VBMI2(dst []byte, streams *streamPack, lastOffs *int) ([]byte, errorCode)

//go:noescape
func pickBestMatchAVX512CD(ec *encodingContext, src []byte, candidates []uint32) matchDescriptor
