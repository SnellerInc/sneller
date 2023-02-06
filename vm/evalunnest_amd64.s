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

TEXT ·evalsplat(SB), NOSPLIT, $16
  NO_LOCAL_POINTERS
  XORQ         R9, R9         // # rows consumed
  XORQ         R10, R10       // # delims output
  JMP          loop_tail
loop:
  CMPQ         CX, $16
  JLT          genmask
  KXNORW       K0, K0, K1
  MOVQ         $16, CX        // plan on consuming 16 rows
vmenter:
  // unpack the next 16 (or fewer) delims
  // into Z0=indices, Z1=lengths
  MOVQ         indelims+8(FP), DX
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
  ADDQ         CX, R9           // provisional rows consumed += cx

  // enter bytecode interpretation
  MOVQ   bc+0(FP), DI
  MOVQ   ·vmm+0(SB), SI  // real static base
  BC_ENTER()
  JC bytecode_error  // break the loop on error

  // now we need to scan (z2:z3).k1 as arrays
  // for distinct ion elements
  MOVQ    ret+80(FP), AX        // lane number
  KMOVW   K1, R15
  MOVQ    perm+56(FP), R14      // R14 = &perm[0]
  MOVQ    outdelims+32(FP), BX  // BX  = &outdelims[0]
  TESTL   R15, R15              // no lanes are arrays?
  JZ      loop_tail             // then we are trivially done
splat_lane:
  VMOVD   X2, R12               // R12 = base pointer
  VMOVD   X3, R13               // R13 = length
  VALIGND $1, Z2, Z2, Z2        // shift away dword in z2 and z3
  VALIGND $1, Z3, Z3, Z3
  TESTL   $1, R15               //
  JZ      next_lane             // skip to next lane if not a list
  ADDQ    R12, R13              // R13 = end-of-array offset
  JMP     splat_array_tail
splat_array:
  CMPQ    R10, outdelims_len+40(FP)
  JAE     out_of_space          // no more space if out >= len(outdelims)
  MOVL    R12, 0(BX)(R10*8)     // store offset
  MOVL    0(SI)(R12*1), DX      // DX = element bits
  MOVL    DX, R8
  ANDB    $0x0f, R8
  CMPB    R8, $0x0f             // size bits = 0x0f? size = 1
  JNE     not_null
  MOVQ    $1, DX
  JMP     size_done
not_null:
  CMPB    R8, $0x0e             // size bits = 0x0e? varint
  JNE     fixed_size
  XORL    R8, R8                // R8 = uvarint accumulator
  MOVL    $1, DI                // DI = header size (currently 1)
varint:
  INCL    DI                    // header bytes++
  SHLL    $7, R8                // uvarint <<= 7
  SHRQ    $8, DX                // input >>= 8
  MOVQ    DX, CX
  ANDL    $0x7f, CX
  ADDL    CX, R8                // uvarint += (input & 0x7f)
  TESTL   $0x80, DX             // loop while (input & 0x80) == 0
  JZ      varint
  MOVQ    R8, DX                // output = uvarint
  ADDQ    DI, DX                // output += header bytes
  JMP     size_done
fixed_size:
  ANDL    $0x0f, DX             // size = descriptor&0x0f
  INCQ    DX                    // + descriptor byte
size_done:
  MOVL    DX, 4(BX)(R10*8)      // store length
  ADDQ    DX, R12               // input pointer += length
  MOVL    AX, 0(R14)(R10*4)     // perm[n] = lane number
  INCQ    R10
splat_array_tail:
  CMPQ    R12, R13
  JLT     splat_array           // continue while R12 < R13
next_lane:
  INCL    AX
  SHRL    $1, R15
  JNZ     splat_lane
loop_tail:
  // all input lanes handled and
  // written to output, so R9 and R10 are in-sync
  // with their respective return values
  MOVQ    R9, ret+80(FP)
  MOVQ    R10, ret1+88(FP)
  MOVQ    indelims_len+16(FP), CX
  SUBQ    R9, CX
  JNZ     loop                 // continue while len(delims) > r9
  RET
genmask:
  MOVL    $1, R8
  SHLL    CX, R8
  SUBL    $1, R8
  KMOVW   R8, K1
  JMP     vmenter
out_of_space:
  MOVQ    AX, ret+80(FP) // AX = # lanes processed
  MOVQ    R10, ret1+88(FP)
  RET
bytecode_error:
  RET
