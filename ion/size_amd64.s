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

TEXT ·fastuv(SB), NOSPLIT, $0
    MOVQ    buf+0(FP), SI
    MOVQ    $0x8080808080808080, DX
    MOVQ    0(SI), R10
    MOVQ    R10, AX
    ANDQ    DX, AX       // AX = bytes & 0x8080...
    BLSMSKQ AX, R8       // R8 = mask up to lowest matched
    ANDQ    R8, R10      // R10 = bytes & mask up to bit
    BSWAPQ  R10          // R10 = bswap64(bytes & mask)
    ANDNQ   R8, DX, DX   // DX = mask &^ 0x80... = mask & 0x7f...
    BSWAPQ  DX
    PEXTQ   DX, R10, R10 // R10 = pext(DX)
    MOVQ    R8, CX
    INCQ    CX           // mask+1 should be a single bit
    TZCNTQ  CX, CX       // tzcnt(mask+1) is the width in bits
    SHRL    $3, CX       // tzcnt(mask+1)>>3 = width in bytes
    MOVQ    R10, ret+24(FP)
    MOVQ    CX, ret1+32(FP)
    RET
