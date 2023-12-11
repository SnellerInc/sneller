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


// func zeroMemoryPointerless(p unsafe.Pointer, n uintptr)
// Requires AVX-512 support
TEXT ·zeroMemoryPointerless(SB), NOSPLIT|NOFRAME, $0-0
    NO_LOCAL_POINTERS
    MOVQ            n+8(FP), AX                         // AX := uint64{n}
    MOVQ            ptr+0(FP), SI                       // SI := uint64{p}
    MOVQ            $-1, CX
    VPXORQ          X0, X0, X0                          // Z0 := {0*}
    BZHIQ           AX, CX, CX                          // CX := uint64{set n least significant bits}, valid for 0 <= n <= 64
    LEAQ            -64(SI)(AX*1), DI
    CMPQ            AX, $64
    JA              multiple_chunks

    // 0 <= n <= 64, so a single masked store suffices to fill the entire buffer
    KMOVQ           CX, K1
    VMOVDQU8        Z0, K1, (SI)
    RET

multiple_chunks:
    // n > 64, so the last (possibly unaligned) chunk can be filled unconditionally
    VMOVDQU8        Z0, (DI)

    // Prepare a cursor for aligned stores by zeroing the 6 least significant bits of the address.
    // There are two possibilities:
    //
    //  1. The store address above was unaligned. The aligned store will then overlap
    //     imperfectly with the unaligned store, so both are required.
    //  2. The store address was aligned. The unaligned and aligned stores would then overlap perfectly, making
    //     the aligned store redundant. This can be avoided by adjusting the store cursor to skip the already
    //     executed store above.

    LEAQ            -1(DI), AX
    TESTB           $0x3f, DI
    CMOVQEQ         AX, DI
    ANDQ            $-64, DI

aligned_loop:
    CMPQ            DI, SI
    JCS             aligned_loop_completed
    VMOVDQA64       Z0, (DI)
    SUBQ            $64, DI
    JMP             aligned_loop

aligned_loop_completed:
    // n > 64, so the finishing store cannot write out of the buffer
    VMOVDQU8        Z0, (SI)
    RET
