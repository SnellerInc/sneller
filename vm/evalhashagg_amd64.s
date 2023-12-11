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
#include "bc_imm_amd64.h"

TEXT ·evalhashaggbc(SB), NOSPLIT, $8
  NO_LOCAL_POINTERS

  MOVQ bc+0(FP), VIRT_BCPTR
  MOVQ ·vmm+0(SB), VIRT_BASE
  XORQ R9, R9           // R9 = rows consumed
  MOVQ tree+32(FP), VIRT_AGG_BUFFER

  // Clear missing bucket mask here so it's always zero before executing BC.
  MOVW $0, bytecode_missingBucketMask(VIRT_BCPTR)

loop:
  MOVQ delims_len+16(FP), CX
  MOVL $16, R8

  SUBQ R9, CX
  JZ end

  // Make sure ECX has at most 16 rows.
  CMPQ CX, R8
  CMOVQ_AE R8, CX

  // Prepare K1 mask based on how many rows we are gonna process.
  MOVL $-1, R8
  SHLXL CX, R8, R8
  NOTL R8
  KMOVW R8, K1

  // Unpack the next 16 (or fewer) delims into Z0 (indices) and Z1 (lengths).
  MOVQ         delims+8(FP), DX
  KSHIFTRW     $8, K1, K2
  VMOVDQU64.Z  0(DX)(R9*8), K1, Z2
  VMOVDQU64.Z  64(DX)(R9*8), K2, Z3
  ADDQ         CX, R9

  VPMOVQD      Z2, Y0
  VPSRLQ       $32, Z2, Z2
  VPMOVQD      Z2, Y2

  VPMOVQD      Z3, Y1
  VPSRLQ       $32, Z3, Z3
  VPMOVQD      Z3, Y3

  VINSERTI32X8 $1, Y1, Z0, Z0
  VINSERTI32X8 $1, Y3, Z2, Z1

  BC_ENTER()
  JC     early_end
  JMP    loop

early_end:
  KMOVW     K7, R8
  POPCNTL   R8, R8
  SUBQ      R8, R9

end:
  MOVQ      R9, ret+40(FP)
  RET
