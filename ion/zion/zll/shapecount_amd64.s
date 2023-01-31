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

TEXT ·shapecount(SB), NOSPLIT, $0
    MOVQ shape_base+0(FP), SI
    MOVQ shape_len+8(FP), DX
    ADDQ SI, DX       // end-of-source
    XORL CX, CX       // count
    XORL BX, BX       // zero
    JMP  loop_tail
loop:
    MOVBLZX 0(SI), AX
    ANDL    $0x1f, AX // fc = shape[0] & 0x1f
    CMPL    AX, $16   // assert(fc <= 16)
    JA      corrupt
    ADCXQ   BX, CX    // count++ if fc < 16
    ADDQ    $3, AX
    SHRQ    $1, AX    // skip = (fc + 3)/2
    ADDQ    AX, SI    // shape = shape[skip:]
loop_tail:
    CMPQ    SI, DX    // bounds check
    JB      loop
    JA      corrupt
    MOVQ    CX, ret+24(FP)
    RET
corrupt:
    MOVQ    $-1, ret+24(FP)
    RET
