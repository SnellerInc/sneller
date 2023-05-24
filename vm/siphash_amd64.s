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

#include "go_asm.h"
#include "textflag.h"
#include "funcdata.h"

TEXT ·siphashx8(SB), NOSPLIT, $0
  MOVQ      base+16(FP), R15
  MOVQ      ends+24(FP), R10
  VPXORQ    Y10, Y10, Y10
  VMOVDQU32 0(R10), Y11       // Y11 = end positions
  VALIGND   $7, Y10, Y11, Y10 // Y10 = offsets = lengths[lane-1]
  VPSUBD    Y10, Y11, Y11     // Y11 = lengths (end position - offset[lane-1])
  VPBROADCASTQ k0+0(FP), Z9   // Z9 = k0
  VPBROADCASTQ k1+8(FP), Z8   // Z8 = k1
  KXNORB    K1, K1, K1        // lanes = 0xff
  CALL      siphashx8(SB)
  VMOVDQU32 Z9, ret+32(FP)     // lo 64 bits x 8
  VMOVDQU32 Z10, ret+32+64(FP) // hi 64 bits x 8
  RET
