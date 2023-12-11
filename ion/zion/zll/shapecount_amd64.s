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

/*
    The procedure has three paths:
    1. fast_path for fc = 15: we know that skip=9, count += 1, no check for data corruption
    2. slow path for fc >= 15: we know that skip=9, count is unchanged,
       and only then we check for fc > 16
    3. common path for fc < 15: skip=(fc+3)/2, count += 1, no check for data corruption
*/
TEXT ·shapecount(SB), NOSPLIT, $0
    MOVQ shape_base+0(FP), SI
    MOVQ shape_len+8(FP), DX
    ADDQ SI, DX       // end-of-source
    XORL CX, CX       // count
    JMP  loop_tail
loop:
    MOVBLZX 0(SI), AX
    ANDL    $0x1f, AX // fc = shape[0] & 0x1f
    CMPL    AX, $15
    JZ      fast_path
    JA      slow_path
    INCQ    CX
    ADDQ    $3, AX
    SHRQ    $1, AX    // skip = (fc + 3)/2
    ADDQ    AX, SI    // shape = shape[skip:]
loop_tail:
    CMPQ    SI, DX    // bounds check
    JB      loop
    JA      corrupt
    MOVQ    CX, ret+24(FP)
    RET
fast_path: // fc = 15
    INCQ    CX
slow_path_tail:
    ADDQ    $9, SI
    CMPQ    SI, DX
    JB      loop
    JA      corrupt
    MOVQ    CX, ret+24(FP)
    RET
slow_path: // fc >= 16 is rare
    CMPL    AX, $16   // assert(fc <= 16)
    JA      corrupt
    JMP     slow_path_tail

corrupt:
    MOVQ    $-1, ret+24(FP)
    RET
