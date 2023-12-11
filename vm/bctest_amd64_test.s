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
#include "bc_amd64.h"

// func bctest_run_aux(bc *bytecode, ctx *bctestContext, activeLanes uint64)
TEXT ·bctest_run_aux(SB), NOSPLIT, $0
    // prepare the necessary environment for invoking the VM
    MOVQ ctx+8(FP), CX
    KMOVQ activeLanes+16(FP), K7

    MOVQ bc+0(FP), VIRT_BCPTR  // DI
    MOVQ ·vmm+0(SB), VIRT_BASE // SI real static base
    BC_CLEAR_SCRATCH(VIRT_PCREG)
    MOVQ bytecode_compiled(VIRT_BCPTR), VIRT_PCREG
    MOVQ bytecode_vstack(VIRT_BCPTR), VIRT_VALUES // R12

    // execute the bytecode
    BC_INVOKE()

    RET
