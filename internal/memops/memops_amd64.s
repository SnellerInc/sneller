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


// func zeroMemoryPointerless(p unsafe.Pointer, n uintptr)
// Requires AVX-512 support
TEXT ·zeroMemoryPointerless(SB), NOSPLIT|NOFRAME, $0-0
    NO_LOCAL_POINTERS
    MOVQ            n+8(FP), AX                         // AX := uint64{n}
    MOVQ            p+0(FP), SI                         // SI := uint64{p}
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
