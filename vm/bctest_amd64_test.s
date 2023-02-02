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

// func bctest_run_aux(bc *bytecode, ctx *bctestContext, activeLanes uint64)
TEXT ·bctest_run_aux(SB), NOSPLIT, $0
    // prepare the necessary environment for invoking the VM
    MOVQ ctx+8(FP), CX
    KMOVQ activeLanes+16(FP), K7

    MOVQ bc+0(FP), VIRT_BCPTR  // DI
    MOVQ ·vmm+0(SB), VIRT_BASE // SI real static base
    BCCLEARSCRATCH(VIRT_PCREG)
    MOVQ bytecode_compiled(VIRT_BCPTR), VIRT_PCREG
    MOVQ bytecode_vstack(VIRT_BCPTR), VIRT_VALUES // R12

    // execute the bytecode
    VMINVOKE()

    RET
