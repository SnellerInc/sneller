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
#include "bc_imm_amd64.h"

TEXT ·evalaggregatebc(SB), NOSPLIT, $8
  NO_LOCAL_POINTERS
  MOVQ w+0(FP), DI                     // RDI = &w
  XORQ R9, R9                          // R9  = rows consumed
  MOVQ aggregateDataBuffer+32(FP), R10 // R10 = aggregate data buffer

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

  // Enter bytecode interpretation
  VPXORD Z30, Z30, Z30
  VPXORD Z31, Z31, Z31
  MOVQ   ·vmm+0(SB), VIRT_BASE
  VMENTER()

  JMP loop

trap:
  BYTE $0xCC

end:
  // The function returns an integer, but at the moment we just return zero as each aggregate value has a separate counter.
  XORL R10, R10
  MOVQ R10, ret+56(FP)
  RET
