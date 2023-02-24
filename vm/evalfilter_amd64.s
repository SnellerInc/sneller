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

TEXT ·evalfilterbc(SB), NOSPLIT, $8
  NO_LOCAL_POINTERS
  MOVQ w+0(FP), VIRT_BCPTR
  XORQ R9, R9         // R9 = rows consumed
  XORQ R10, R10       // R10 = rows out
  JMP  tail
loop:
  // load delims
  MOVQ         R9, 0(SP) // save for later
  CMPQ         CX, $16
  JLT          genmask
  KXNORW       K0, K0, K1
doit:
  // unpack the next 16 (or fewer) delims
  // into Z0=indices, Z1=lengths
  MOVQ         delims+8(FP), DX
  VMOVDQU64.Z  0(DX)(R9*8), K1, Z2
  KSHIFTRW     $8, K1, K2
  VMOVDQU64.Z  64(DX)(R9*8), K2, Z3
  VPMOVQD      Z2, Y0
  VPMOVQD      Z3, Y1
  VINSERTI32X8 $1, Y1, Z0, Z0
  VPROLQ       $32, Z2, Z2
  VPROLQ       $32, Z3, Z3
  VPMOVQD      Z2, Y1
  VPMOVQD      Z3, Y2
  VINSERTI32X8 $1, Y2, Z1, Z1
  ADDQ         $16, R9

  // enter bytecode interpretation
  MOVQ         ·vmm+0(SB), VIRT_BASE
  BC_ENTER()
  JC bytecode_error // break the loop on error

  // compress output into delims
  MOVQ          delims+8(FP), DX
  KMOVW         K1, K2
  KSHIFTRW      $8, K2, K2
  KMOVB         K1, K1

  // first 8 words: compress w/ K1
  MOVQ          R10, R14
  KMOVD         K1, R8
  POPCNTL       R8, R8
  VPMOVZXDQ     Y1, Z2               // Z2 = first 8 lengths
  VPMOVZXDQ     Y0, Z3               // Z3 = first 8 offsets
  VEXTRACTI32X8 $1, Z0, Y0
  VEXTRACTI32X8 $1, Z1, Y1
  VPROLQ        $32, Z2, Z2
  VPORD         Z2, Z3, Z2           // Z2 = first 8 qword(length << 32 | offset)
  VPCOMPRESSQ   Z2, K1, 0(DX)(R10*8)
  ADDQ          R8, R10

  // second 8 words: compress w/ k2
  MOVQ          R10, R15
  KMOVW         K2, R8
  POPCNTL       R8, R8
  VPMOVZXDQ     Y1, Z2
  VPMOVZXDQ     Y0, Z3
  VPROLQ        $32, Z2, Z2
  VPORD         Z2, Z3, Z2
  VPCOMPRESSQ   Z2, K2, 0(DX)(R10*8)
  ADDQ          R8, R10

  MOVQ          bytecode_auxvals+8(DI), CX
  TESTQ         CX, CX
  JZ            tail
  MOVQ          bytecode_auxvals+0(DI), AX
  MOVQ          0(SP), R11             // R11 = old rows consumed
compress_auxvals:
  KMOVB         K7, K3                 // K7 = original input mask
  KSHIFTRW      $8, K7, K4
  MOVQ          0(AX), R8
  VMOVDQU64.Z   0(R8)(R11*8), K3, Z2   // load from rows consumed
  VPCOMPRESSQ   Z2, K1, 0(R8)(R14*8)
  VMOVDQU64.Z   64(R8)(R11*8), K4, Z2  // load more rows consumed
  VPCOMPRESSQ   Z2, K2, 0(R8)(R15*8)
  ADDQ          $24, AX
  DECQ          CX
  JNZ           compress_auxvals
tail:
  MOVQ delims_len+16(FP), CX
  SUBQ R9, CX
  JG   loop             // should be JLT, but operands are reversed
  MOVQ R10, ret+32(FP)
  RET
genmask:
  // K1 = (1 << CX)-1
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  KMOVW       R8, K1
  JMP         doit
bytecode_error:
  RET
