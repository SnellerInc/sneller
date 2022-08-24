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

DATA indexb<>+0(SB)/1, $0
DATA indexb<>+1(SB)/1, $1
DATA indexb<>+2(SB)/1, $2
DATA indexb<>+3(SB)/1, $3
DATA indexb<>+4(SB)/1, $4
DATA indexb<>+5(SB)/1, $5
DATA indexb<>+6(SB)/1, $6
DATA indexb<>+7(SB)/1, $7
DATA indexb<>+8(SB)/1, $8
DATA indexb<>+9(SB)/1, $9
DATA indexb<>+10(SB)/1, $10
DATA indexb<>+11(SB)/1, $11
DATA indexb<>+12(SB)/1, $12
DATA indexb<>+13(SB)/1, $13
DATA indexb<>+14(SB)/1, $14
DATA indexb<>+15(SB)/1, $15
GLOBL indexb<>(SB), RODATA|NOPTR, $16

TEXT ·compress(SB), NOSPLIT, $0
  NO_LOCAL_POINTERS
  MOVQ  delims+0(FP), SI
  MOVQ  SI, DI
  MOVQ  delims_len+8(FP), CX
  XORL  AX, AX
  XORL  DX, DX
  JMP   tail
loop:
  KXNORB      K0, K0, K1
  VMOVDQU64.Z 0(SI), K1, Z0
  VPTESTMQ    Z0, Z0, K1, K1
  KMOVB       K1, R15
  VPCOMPRESSQ Z0, K1, 0(DI)
  POPCNTL     R15, R15
  ADDL        R15, AX
  LEAQ        0(DI)(R15*8), DI
  ADDQ        $64, SI
  SUBL        $8, CX
tail:
  CMPL        CX, $8
  JG          loop
  TESTL       CX, CX
  JZ          ret
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  KMOVB       R8, K1
  VMOVDQU64.Z 0(SI), K1, Z0
  VPTESTMQ    Z0, Z0, K1, K1
  KMOVB       K1, R15
  VPCOMPRESSQ Z0, K1, 0(DI)
  POPCNTL     R15, R15
  ADDL        R15, AX
ret:
  MOVQ  AX, ret+24(FP)
  RET

TEXT ·evalsplat(SB), NOSPLIT, $16
  NO_LOCAL_POINTERS
  MOVQ         indelims_len+16(FP), CX
  CMPQ         CX, $16
  JLT          genmask
  KXNORW       K0, K0, K1
vmenter:
  // unpack the next 16 (or fewer) delims
  // into Z0=indices, Z1=lengths
  MOVQ         indelims+8(FP), DX
  VMOVDQU64.Z  0(DX), K1, Z2
  KSHIFTRW     $8, K1, K2
  VMOVDQU64.Z  64(DX), K2, Z3
  VPMOVQD      Z2, Y0
  VPMOVQD      Z3, Y1
  VINSERTI32X8 $1, Y1, Z0, Z0
  VPROLQ       $32, Z2, Z2
  VPROLQ       $32, Z3, Z3
  VPMOVQD      Z2, Y1
  VPMOVQD      Z3, Y2
  VINSERTI32X8 $1, Y2, Z1, Z1

  // enter bytecode interpretation
  MOVQ   bc+0(FP), DI
  MOVQ   ·vmm+0(SB), SI  // real static base
  VMENTER()

  // now we need to scan (z2:z3).k1 as arrays
  // for distinct ion elements
  XORL    AX, AX                // lane number
  KMOVW   K1, R15
  MOVQ    perm+56(FP), R9       // R9 = &perm[0]
  MOVQ    outdelims+32(FP), R10 // R10 = &outdelims[0]
  MOVQ    outdelims_len+40(FP), R11
  LEAQ    0(R10)(R11*8), R11    // R11 = &outdelims[len(outdelims)]
  TESTL   R15, R15              // no lanes are arrays?
  JZ      done
splat_lane:
  CMPQ    R10, R11              // no more output space?
  JGE     done
  VMOVD   X2, R12               // R12 = base pointer
  VMOVD   X3, R13               // R13 = length
  ADDQ    R12, R13              // R13 = end-of-array offset
  VALIGND $1, Z2, Z2, Z2        // shift away dword in z2 and z3
  VALIGND $1, Z3, Z3, Z3
  TESTL   $1, R15               // don't do anything here
  JZ      next_lane
  JMP     splat_array_tail
splat_array:
  MOVL    R12, 0(R10)           // store offset
  MOVQ    0(SI)(R12*1), DX      // DX = element bits
  MOVL    DX, BX
  ANDB    $0x0f, BX
  CMPB    BX, $0x0f             // size bits = 0x0f? size = 1
  JNE     not_null
  MOVQ    $1, DX
  JMP     size_done
not_null:
  CMPB    BX, $0x0e             // size bits = 0x0e? varint
  JNE     fixed_size
  XORL    BX, BX                // BX = uvarint accumulator
  MOVL    $1, R8                // R8 = header size (currently 1)
varint:
  INCL    R8                    // header bytes++
  SHLL    $7, BX                // uvarint <<= 7
  SHRQ    $8, DX                // input >>= 8
  MOVQ    DX, CX
  ANDL    $0x7f, CX
  ADDL    CX, BX                // uvarint += (input & 0x7f)
  TESTQ   $0x80, DX             // loop while (input & 0x80) == 0
  JZ      varint
  MOVQ    BX, DX                // output = uvarint
  ADDQ    R8, DX                // output += header bytes
  JMP     size_done
fixed_size:
  ANDL    $0x0f, DX             // size = descriptor&0x0f
  INCQ    DX                    // + descriptor byte
size_done:
  MOVL    DX, 4(R10)            // store length
  ADDQ    DX, R12               // input pointer += length
  MOVL    AX, 0(R9)             // perm[n] = lane number
  LEAQ    8(R10), R10           // *outdelims[n++] = length
  LEAQ    4(R9), R9             // perm++
  CMPQ    R10, R11              // no more space?
  JGE     done                  // then we're really done
splat_array_tail:
  CMPQ    R12, R13
  JLT     splat_array           // continue while R12 < R13
next_lane:
  INCL    AX
  SHRL    $1, R15
  JNZ     splat_lane
done:
  MOVQ    AX, ret+80(FP)         // ret0 = # lanes output
  SUBQ    outdelims+32(FP), R10
  SHRQ    $3, R10                // R10 = (R10-&outdelims[0])/8
  MOVQ    R10, ret1+88(FP)       // ret1 = # delims output
  RET
genmask:
  MOVL    $1, R8
  SHLL    CX, R8
  SUBL    $1, R8
  KMOVW   R8, K1
  JMP     vmenter
