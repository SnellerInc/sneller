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

#include "bc_constant.h"

// ION size decoding per https://amzn.github.io/ion-docs/docs/binary.html

#define ION_SIZE_VARUINT $0x80
#define ION_SIZE_INVALID $0x40

// NOP
CONST_DATA_U8(consts_byte_ion_size_b, 0x00, $0)
CONST_DATA_U8(consts_byte_ion_size_b, 0x01, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x02, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x03, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x04, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x05, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x06, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x07, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x08, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x09, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x0f, $0) // null.null
// bool
CONST_DATA_U8(consts_byte_ion_size_b, 0x10, $0) // bool.false
CONST_DATA_U8(consts_byte_ion_size_b, 0x11, $0) // bool.true
CONST_DATA_U8(consts_byte_ion_size_b, 0x12, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x13, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x14, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x15, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x16, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x17, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x18, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x19, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1a, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1b, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1c, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1d, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1e, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x1f, $0) // null.bool
// positive int
CONST_DATA_U8(consts_byte_ion_size_b, 0x20, $0) // positive int 0
CONST_DATA_U8(consts_byte_ion_size_b, 0x21, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x22, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x23, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x24, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x25, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x26, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x27, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x28, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x29, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x2f, $0) // null.int
// negative int
CONST_DATA_U8(consts_byte_ion_size_b, 0x30, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x31, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x32, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x33, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x34, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x35, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x36, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x37, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x38, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x39, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x3f, $0) // null.int
//float
CONST_DATA_U8(consts_byte_ion_size_b, 0x40, $0) // float 0.0
CONST_DATA_U8(consts_byte_ion_size_b, 0x41, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x42, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x43, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x44, $4) // IEEE754 float32
CONST_DATA_U8(consts_byte_ion_size_b, 0x45, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x46, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x47, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x48, $8) // IEEE754 float64
CONST_DATA_U8(consts_byte_ion_size_b, 0x49, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4a, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4b, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4c, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4d, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4e, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x4f, $0) // null.float
// decimal
CONST_DATA_U8(consts_byte_ion_size_b, 0x50, $0) // decimal 0.0
CONST_DATA_U8(consts_byte_ion_size_b, 0x51, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x52, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x53, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x54, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x55, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x56, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x57, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x58, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x59, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x5f, $0) // null.decimal
// timestamp
CONST_DATA_U8(consts_byte_ion_size_b, 0x60, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x61, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x62, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x63, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x64, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x65, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x66, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x67, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x68, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x69, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x6f, $0) // null.timestamp
// symbol
CONST_DATA_U8(consts_byte_ion_size_b, 0x70, $0) // symbol 0
CONST_DATA_U8(consts_byte_ion_size_b, 0x71, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x72, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x73, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x74, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x75, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x76, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x77, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x78, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x79, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x7f, $0) // null.symbol
// string
CONST_DATA_U8(consts_byte_ion_size_b, 0x80, $0) // string empty
CONST_DATA_U8(consts_byte_ion_size_b, 0x81, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x82, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x83, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x84, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x85, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x86, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x87, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x88, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x89, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x8f, $0) // null.string
// clob
CONST_DATA_U8(consts_byte_ion_size_b, 0x90, $0) // clob empty
CONST_DATA_U8(consts_byte_ion_size_b, 0x91, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0x92, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0x93, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0x94, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0x95, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0x96, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0x97, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0x98, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0x99, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9a, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9b, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9c, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9d, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9e, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0x9f, $0) // null.clob
// blob
CONST_DATA_U8(consts_byte_ion_size_b, 0xa0, $0) // blob empty
CONST_DATA_U8(consts_byte_ion_size_b, 0xa1, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa2, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa3, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa4, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa5, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa6, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa7, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa8, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0xa9, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0xaa, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0xab, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0xac, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0xad, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0xae, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0xaf, $0) // null.blob
// list
CONST_DATA_U8(consts_byte_ion_size_b, 0xb0, $0) // list empty
CONST_DATA_U8(consts_byte_ion_size_b, 0xb1, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb2, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb3, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb4, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb5, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb6, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb7, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb8, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0xb9, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0xba, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0xbb, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0xbc, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0xbd, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0xbe, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0xbf, $0) // null.list
// sexp
CONST_DATA_U8(consts_byte_ion_size_b, 0xc0, $0) // sexp empty
CONST_DATA_U8(consts_byte_ion_size_b, 0xc1, $1)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc2, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc3, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc4, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc5, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc6, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc7, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc8, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0xc9, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0xca, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0xcb, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0xcc, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0xcd, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0xce, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0xcf, $0) // null.sexp
// struct
CONST_DATA_U8(consts_byte_ion_size_b, 0xd0, $0) // struct empty
CONST_DATA_U8(consts_byte_ion_size_b, 0xd1, ION_SIZE_VARUINT) // the struct has at least one symbol/value pair, the length field exists, and the field name integers are sorted in increasing order
CONST_DATA_U8(consts_byte_ion_size_b, 0xd2, $2)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd3, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd4, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd5, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd6, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd7, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd8, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0xd9, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0xda, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0xdb, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0xdc, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0xdd, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0xde, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0xdf, $0) // null.struct
// version marker
CONST_DATA_U8(consts_byte_ion_size_b, 0xe0, $0)
// annotation
CONST_DATA_U8(consts_byte_ion_size_b, 0xe1, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe2, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe3, $3)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe4, $4)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe5, $5)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe6, $6)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe7, $7)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe8, $8)
CONST_DATA_U8(consts_byte_ion_size_b, 0xe9, $9)
CONST_DATA_U8(consts_byte_ion_size_b, 0xea, $10)
CONST_DATA_U8(consts_byte_ion_size_b, 0xeb, $11)
CONST_DATA_U8(consts_byte_ion_size_b, 0xec, $12)
CONST_DATA_U8(consts_byte_ion_size_b, 0xed, $13)
CONST_DATA_U8(consts_byte_ion_size_b, 0xee, ION_SIZE_VARUINT)
CONST_DATA_U8(consts_byte_ion_size_b, 0xef, ION_SIZE_INVALID)
// reserved
CONST_DATA_U8(consts_byte_ion_size_b, 0xf0, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf1, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf2, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf3, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf4, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf5, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf6, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf7, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf8, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xf9, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xfa, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xfb, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xfc, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xfd, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xfe, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0xff, ION_SIZE_INVALID)
// 4 guard bytes to allow undisturbed reading past the LUT above, should the linker put it at the page boundary
CONST_DATA_U8(consts_byte_ion_size_b, 0x100, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x101, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x102, ION_SIZE_INVALID)
CONST_DATA_U8(consts_byte_ion_size_b, 0x103, ION_SIZE_INVALID)
CONST_GLOBAL(consts_byte_ion_size_b, $260)
