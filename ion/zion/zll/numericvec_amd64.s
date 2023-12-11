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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"

#define DECODE_CONST(offset) decodeNumericVecData<>+(offset)(SB)

// CF12 exponents
DATA decodeNumericVecData<>+(0 + 0)(SB)/8, $0x408f400000000000   // 1e3
DATA decodeNumericVecData<>+(0 + 8)(SB)/8, $0x40c3880000000000   // 1e4

// CF16 exponents
DATA decodeNumericVecData<>+(16 + 0)(SB)/8, $0x40c3880000000000  // 1e4
DATA decodeNumericVecData<>+(16 + 8)(SB)/8, $0x40f86a0000000000  // 1e5

// CF24 exponents
DATA decodeNumericVecData<>+(32 + 0)(SB)/8, $0x40f86a0000000000  // 1e5
DATA decodeNumericVecData<>+(32 + 8)(SB)/8, $0x412e848000000000  // 1e6
DATA decodeNumericVecData<>+(32 + 16)(SB)/8, $0x416312d000000000 // 1e7
DATA decodeNumericVecData<>+(32 + 24)(SB)/8, $0x4197d78400000000 // 1e8
DATA decodeNumericVecData<>+(32 + 32)(SB)/8, $0x41cdcd6500000000 // 1e9
DATA decodeNumericVecData<>+(32 + 40)(SB)/8, $0x4202a05f20000000 // 1e10
DATA decodeNumericVecData<>+(32 + 48)(SB)/8, $0x42374876e8000000 // 1e11
DATA decodeNumericVecData<>+(32 + 56)(SB)/8, $0x42a2309ce5400000 // 1e13

// CF32 exponents
DATA decodeNumericVecData<>+(96 + 0)(SB)/8, $0x41cdcd6500000000  // 1e9
DATA decodeNumericVecData<>+(96 + 8)(SB)/8, $0x42a2309ce5400000  // 1e13

// VPSHUFB predicate that expands CF12 bits loaded into a 128-bit
// register into two 64-bit halves that can be further processed.
DATA decodeNumericVecData<>+(112 + 0)(SB)/8, $0x0504040302010100
DATA decodeNumericVecData<>+(112 + 8)(SB)/8, $0x0B0A0A0908070706

// VPSHUFB predicate that expands CF24 bits loaded into a 128-bit register.
DATA decodeNumericVecData<>+(128 + 0)(SB)/8, $0xFF050403FF020100
DATA decodeNumericVecData<>+(128 + 8)(SB)/8, $0xFF0B0A09FF080706

// VPSHUFB predicate that is used for bswap64
DATA decodeNumericVecData<>+(144 + 0)(SB)/8, $0x0001020304050607
DATA decodeNumericVecData<>+(144 + 8)(SB)/8, $0x08090A0B0C0D0E0F

DATA decodeNumericVecData<>+(160 + 0)(SB)/4, $4                  // constant(4)
DATA decodeNumericVecData<>+(160 + 4)(SB)/4, $0x48484848         // constant(0x48484848)
DATA decodeNumericVecData<>+(160 + 8)(SB)/4, $0x20202020         // constant(0x20202020)
DATA decodeNumericVecData<>+(160 + 12)(SB)/4, $0x00210021        // constant(0x00210021)

// SCATTER increments by 9 (for encoding 64-bit ION floats)
DATA decodeNumericVecData<>+(192 + 0)(SB)/8, $0
DATA decodeNumericVecData<>+(192 + 8)(SB)/8, $9
DATA decodeNumericVecData<>+(192 + 16)(SB)/8, $18
DATA decodeNumericVecData<>+(192 + 24)(SB)/8, $27
DATA decodeNumericVecData<>+(192 + 32)(SB)/8, $36
DATA decodeNumericVecData<>+(192 + 40)(SB)/8, $45
DATA decodeNumericVecData<>+(192 + 48)(SB)/8, $54
DATA decodeNumericVecData<>+(192 + 56)(SB)/8, $63

// VPERMB predicate to shuffle 8-byte chunks to 9-byte chunks (7 values can be shuffled like this)
DATA decodeNumericVecData<>+(256 +  0)(SB)/8, $0x06050403020100FF
DATA decodeNumericVecData<>+(256 +  8)(SB)/8, $0x0D0C0B0A0908FF07
DATA decodeNumericVecData<>+(256 + 16)(SB)/8, $0x1413121110FF0F0E
DATA decodeNumericVecData<>+(256 + 24)(SB)/8, $0x1B1A1918FF171615
DATA decodeNumericVecData<>+(256 + 32)(SB)/8, $0x222120FF1F1E1D1C
DATA decodeNumericVecData<>+(256 + 40)(SB)/8, $0x2928FF2726252423
DATA decodeNumericVecData<>+(256 + 48)(SB)/8, $0x30FF2F2E2D2C2B2A
DATA decodeNumericVecData<>+(256 + 56)(SB)/8, $0xFF37363534333231

// ION FP64 headers in every 9th byte
DATA decodeNumericVecData<>+(320 +  0)(SB)/8, $0x0000000000000048
DATA decodeNumericVecData<>+(320 +  8)(SB)/8, $0x0000000000004800
DATA decodeNumericVecData<>+(320 + 16)(SB)/8, $0x0000000000480000
DATA decodeNumericVecData<>+(320 + 24)(SB)/8, $0x0000000048000000
DATA decodeNumericVecData<>+(320 + 32)(SB)/8, $0x0000004800000000
DATA decodeNumericVecData<>+(320 + 40)(SB)/8, $0x0000480000000000
DATA decodeNumericVecData<>+(320 + 48)(SB)/8, $0x0048000000000000
DATA decodeNumericVecData<>+(320 + 56)(SB)/8, $0x4800000000000000

GLOBL decodeNumericVecData<>(SB), RODATA|NOPTR, $384

#define FUNC_NAME ·decodeNumericVecAVX512
#include "numericvec_amd64_impl.h"
#undef FUNC_NAME

#define FUNC_NAME ·decodeNumericVecAVX512VBMI
#define AVX512_VBMI_ENABLED
#include "numericvec_amd64_impl.h"
#undef AVX512_VBMI_ENABLED
#undef FUNC_NAME

#undef DECODE_CONST
