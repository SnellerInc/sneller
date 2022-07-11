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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"
#include "bc_amd64.h"


// func bctest_run_aux(bc *bytecode, ctx *bctestContext)
TEXT ·bctest_run_aux(SB), NOSPLIT, $0
    MOVQ    ctx+8(FP), CX

    // setup regs for bytecode
    VMOVDQU64   bctestContext_structBase(CX), Z0
    VMOVDQU64   bctestContext_structLen(CX), Z1
    VMOVDQU64   bctestContext_scalar+0(CX), Z2
    VMOVDQU64   bctestContext_scalar+64(CX), Z3
    VMOVDQU64   bctestContext_valueBase(CX), Z30
    VMOVDQU64   bctestContext_valueLen(CX), Z31

    MOVW        bctestContext_current(CX), AX
    KMOVW       AX, K1
    MOVW        bctestContext_valid(CX), AX
    KMOVW       AX, K7

    // run the VM
    MOVQ    bc+0(FP), VIRT_BCPTR
    MOVQ    ·vmm+0(SB), SI  // real static base
    VMENTER()

    // gather results
    MOVQ        ctx+8(FP), CX
    VMOVDQU64   Z0, bctestContext_structBase(CX)
    VMOVDQU64   Z1, bctestContext_structLen(CX)
    VMOVDQU64   Z2, bctestContext_scalar+0(CX)
    VMOVDQU64   Z3, bctestContext_scalar+64(CX)
    VMOVDQU64   Z30, bctestContext_valueBase(CX)
    VMOVDQU64   Z31, bctestContext_valueLen(CX)

    KMOVW       K1, AX
    MOVW        AX, bctestContext_current(CX)
    KMOVW       K7, AX
    MOVW        AX, bctestContext_valid(CX)

    RET
