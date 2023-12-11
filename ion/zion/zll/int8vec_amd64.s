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

#define FUNC_NAME ·unpackInt8VBMI2
#define VPCOMPRESSB_IMPL_Z0_K1_DI_R15() \
    VPCOMPRESSB Z0, K1, Z0              \
    VMOVDQU32   Z0, 0(DI)               \
    POPCNTQ     R15, R15                \
    ADDQ        R15, DI

#include "int8vec_amd64.h"

// without VBMI2 we're stuck emulating VPCOMPRESSB
// by using VPCOMPRESSD and doing 16-byte stores
#undef FUNC_NAME
#undef VPCOMPRESSB_IMPL_Z0_K1_DI_R15
#define FUNC_NAME ·unpackInt8AVX512
#define VPCOMPRESSB_IMPL_Z0_K1_DI_R15()     \
    VEXTRACTI32X4 $1, Z0, X21  \
    VEXTRACTI32X4 $2, Z0, X22  \
    VEXTRACTI32X4 $3, Z0, X23  \
    VPMOVZXBD     X0, Z0       \
    VPMOVZXBD     X21, Z21     \
    VPMOVZXBD     X22, Z22     \
    VPMOVZXBD     X23, Z23     \
    VPCOMPRESSD   Z0, K1, Z0   \
    VPMOVDB       Z0, X0       \
    VMOVDQU32     X0, 0(DI)    \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI      \
    SHRQ          $16, R15     \
    KSHIFTRQ      $16, K1, K7  \
    VPCOMPRESSD   Z21, K7, Z21 \
    VPMOVDB       Z21, X21     \
    VMOVDQU32     X21, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI      \
    SHRQ          $16, R15     \
    KSHIFTRQ      $16, K7, K7  \
    VPCOMPRESSD   Z22, K7, Z22 \
    VPMOVDB       Z22, X22     \
    VMOVDQU32     X22, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI      \
    SHRQ          $16, R15     \
    KSHIFTRQ      $16, K7, K7  \
    VPCOMPRESSD   Z23, K7, Z23 \
    VPMOVDB       Z23, X23     \
    VMOVDQU32     X23, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI

#include "int8vec_amd64.h"
