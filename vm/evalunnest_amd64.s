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

// project fields into an output buffer
// using the stack slots produced by a bytecode
// program invocation
TEXT ·evalunnest(SB), NOSPLIT, $16
  NO_LOCAL_POINTERS
  XORL R9, R9             // R9 = rows consumed
  MOVQ dst+56(FP), DI
  MOVQ DI, ret+104(FP)    // ret0 = # bytes written
  MOVQ R9, ret1+112(FP)   // ret1 = # rows consumed
  JMP  tail
loop:
  // load delims
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
  VPMOVQD      Z2, Y30
  VPMOVQD      Z3, Y31
  VINSERTI32X8 $1, Y31, Z30, Z30
  VPROLQ       $32, Z2, Z2
  VPROLQ       $32, Z3, Z3
  VPMOVQD      Z2, Y31
  VPMOVQD      Z3, Y2
  VINSERTI32X8 $1, Y2, Z31, Z31

  MOVQ         perm+32(FP), DX
  VMOVDQU32    0(DX)(R9*4), K1, Z3
  MOVQ         bc+0(FP), VIRT_BCPTR
  VMOVDQU32    Z3, bytecode_perm(VIRT_BCPTR)

  // enter bytecode interpretation
  MOVQ    ·vmm+0(SB), SI  // real static base
  VMENTER()
  KMOVW   K1, R15         // R15 = active rows bitmask
  MOVQ    ret+104(FP), DI // DI = output location

project_objects:
  TESTL   $1, R15
  JZ      clear_delimiter
  MOVQ    symbols_len+88(FP), R8
  MOVQ    symbols+80(FP), BX
  MOVQ    VIRT_VALUES, DX
  XORL    CX, CX
get_size:
  MOVL    64(DX), AX
  TESTL   AX, AX
  JZ      empty_cell
  ADDL    AX, CX
  MOVBLZX syminfo_size(BX), AX
  ADDL    AX, CX
empty_cell:
  ADDQ    $syminfo__size, BX
  ADDQ    $VREG_SIZE, DX
  DECL    R8
  JNZ     get_size

  // determine if there is
  // enough space in the output buffer
  // for an object of the given size
  // plus 13 bytes slack
  // (we need 7 bytes for the copy
  // and up to 4 bytes for the structure
  // header)
  MOVQ    dst_len+64(FP), DX   // DX = len(dst)
  SUBQ    $13, DX              // DX = len(dst) - slack
  SUBQ    CX, DX               // DX = space = len(dst) - slack - sizeof(obj)
  ADDQ    dst+56(FP), DX       // DX = &dst[0] + space = max dst ptr
  CMPQ    DI, DX               // current offset >= space?
  JG      ret                  // if so, return early

  MOVQ    delims+8(FP), R8
  MOVL    CX, 4(R8)(R9*8)

  // compute output descriptor in DX
  // and descriptor size in BX
  MOVL    $2, BX
  MOVL    CX, DX
  ANDL    $0x7f, DX
  ORL     $0x80, DX          // final byte |= 0x80
  SHRL    $8, CX
  JZ      writeheader
moreheader:
  INCL    BX
  SHLL    $8, DX
  MOVL    CX, AX
  ANDL    $0x7f, AX
  ORL     AX, DX
  SHRL    $7, CX
  JNZ     moreheader
  CMPL    BX, $4
  JG      trap               // assert no more than 4 descriptor bytes
writeheader:
  SHLL    $8, DX
  ORL     $0xde, DX          // insert 0xde byte
  MOVL    DX, 0(DI)
  ADDQ    BX, DI             // move forward descriptor size

  MOVQ    DI, DX
  SUBQ    ·vmm+0(SB), DX  // DX = absolute address (32-bit)
  MOVL    DX, 0(R8)(R9*8) // rewrite delims[R9].offset = (DI - dst)

  // actually project
  MOVQ    symbols+80(FP), BX
  MOVQ    symbols_len+88(FP), R8
  MOVQ    VIRT_VALUES, DX
copy_field:
  MOVL    64(DX), CX
  TESTL   CX, CX
  JZ      next_field

  // write encoded symbol
  MOVL    syminfo_encoded(BX), AX
  MOVL    AX, 0(DI)
  MOVBQZX syminfo_size(BX), AX
  ADDQ    AX, DI

  // memcpy(DI, SI, CX),
  // falling back to 'rep movsb' for very large copies
  MOVQ    ·vmm+0(SB), SI  // real static base
  MOVL    0(DX), AX
  ADDQ    AX, SI
  CMPL    CX, $256
  JGE     rep_movsb
eight:
  // the caller has arranged for the target buffer
  // to have at least 7 bytes of extra space
  // for trailing garbage (it will get overwritten
  // with a nop pad)
  //
  // most ion objects are less than 8 bytes,
  // so typically we do not loop here
  MOVQ    0(SI), AX
  MOVQ    AX, 0(DI)
  ADDQ    $8, DI
  ADDQ    $8, SI
  SUBL    $8, CX
  JG      eight
  // add a possibly-negative CX
  // back to DI to adjust for any
  // extra copying we may have done
  MOVLQSX CX, CX
  ADDQ    CX, DI
next_field:
  ADDQ    $VREG_SIZE, DX
  ADDQ    $syminfo__size, BX
  DECL    R8
  JNZ     copy_field
next_lane:
  ADDQ    $4, VIRT_VALUES
  INCL    R9
  SHRL    $1, R15
  JNZ     project_objects
  MOVQ    DI, ret+104(FP)    // accumulate destination offset
tail:
  MOVQ    delims_len+16(FP), CX
  SUBQ    R9, CX
  JNZ     loop
ret:
  MOVQ    dst+56(FP), DI
  SUBQ    DI, ret+104(FP)
  MOVQ    R9, ret1+112(FP)
  RET
genmask:
  // K1 = (1 << CX)-1
  MOVL    $1, R8
  SHLL    CX, R8
  SUBL    $1, R8
  KMOVW   R8, K1
  JMP     doit
clear_delimiter:  // branch target from projection loop entry
  MOVQ    delims+8(FP), R8
  MOVQ    $0, 0(R8)(R9*8)
  JMP     next_lane
rep_movsb:
  // memcpy "fast-path" for large objects
  REP; MOVSB;
  JMP  next_field
trap:
  BYTE $0xCC
  RET
