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
  VMOVDQU32 Z9, ret+32(FP)      // lo 64 bits x 8
  VMOVDQU32 Z10, ret_1_0+96(FP) // hi 64 bits x 8
  RET
