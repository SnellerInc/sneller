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
    KSHIFTRQ      $16, K1, K2  \
    VPCOMPRESSD   Z21, K2, Z21 \
    VPMOVDB       Z21, X21     \
    VMOVDQU32     X21, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI      \
    SHRQ          $16, R15     \
    KSHIFTRQ      $16, K2, K2  \
    VPCOMPRESSD   Z22, K2, Z22 \
    VPMOVDB       Z22, X22     \
    VMOVDQU32     X22, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI      \
    SHRQ          $16, R15     \
    KSHIFTRQ      $16, K2, K2  \
    VPCOMPRESSD   Z23, K2, Z23 \
    VPMOVDB       Z23, X23     \
    VMOVDQU32     X23, 0(DI)   \
    POPCNTW       R15, R10     \
    ADDQ          R10, DI

#include "int8vec_amd64.h"
