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

TEXT ·evalfindbc(SB), NOSPLIT, $16
  NO_LOCAL_POINTERS
  MOVQ w+0(FP), DI        // DI = &w
  XORL R9, R9             // R9 = rows consumed
  MOVQ stride+32(FP), R10
  MOVQ R10, 0(SP)         // 0(SP) = stack pointer incrementor
  XORL R10, R10           // R10 = current stack pointer addend
  BCCLEARSCRATCH(R15)
  BCCLEARERROR()
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
  KMOVW   K1, K7
  MOVQ    ·vmm+0(SB), SI  // real static base
  MOVQ    bytecode_compiled(DI), VIRT_PCREG
  MOVQ    bytecode_vstack(DI), VIRT_VALUES
  ADDQ    R10, VIRT_VALUES                     // stack offset += rows out
  VMINVOKE()
  JC      opcode_failed                        // break on error

  ADDQ    0(SP), R10                           // moar stack
tail:
  MOVQ delims_len+16(FP), CX
  SUBQ R9, CX
  JG   loop             // should be JLT, but operands are reversed
  RET
genmask:
  // K1 = (1 << CX)-1
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  KMOVW       R8, K1
  JMP         doit
opcode_failed:
  RET
trap:
  BYTE $0xCC
  RET

// project fields into an output buffer
// using the stack slots produced by a bytecode
// program invocation
TEXT ·evalproject(SB), NOSPLIT, $16
  NO_LOCAL_POINTERS
  XORL R9, R9             // R9 = rows consumed
  MOVQ dst+32(FP), DI
  MOVQ DI, ret+80(FP)    // ret0 = # bytes written
  MOVQ R9, ret1+88(FP)   // ret1 = # rows consumed
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
  VPMOVQD      Z2, Y0
  VPMOVQD      Z3, Y1
  VINSERTI32X8 $1, Y1, Z0, Z0
  VPROLQ       $32, Z2, Z2
  VPROLQ       $32, Z3, Z3
  VPMOVQD      Z2, Y1
  VPMOVQD      Z3, Y2
  VINSERTI32X8 $1, Y2, Z1, Z1

  MOVQ    bc+0(FP), VIRT_BCPTR
  MOVQ    ·vmm+0(SB), SI       // real static base
  // enter bytecode interpretation
  VMENTER()
  JCS     did_abort

  KMOVW   K1, R15         // R15 = active rows bitmask
  MOVQ    ret+80(FP), DI  // DI = output location

project_objects:
  TESTL   $1, R15
  JZ      next_lane
  MOVQ    symbols_len+64(FP), R8
  MOVQ    symbols+56(FP), BX
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
  MOVQ    dst_len+40(FP), DX   // DX = len(dst)
  SUBQ    $13, DX              // DX = len(dst) - slack
  SUBQ    CX, DX               // DX = space = len(dst) - slack - sizeof(obj)
  ADDQ    dst+32(FP), DX       // DX = &dst[0] + space = max dst ptr
  CMPQ    DI, DX               // current offset >= space?
  JG      ret                  // if so, return early

  // rewrite delims[R9].size
  MOVQ    delims+8(FP), R8
  MOVL    CX, 4(R8)(R9*8)

  // compute output descriptor in DX
  // and descriptor size in BX
  MOVL    $2, BX
  MOVL    CX, DX
  ANDL    $0x7f, DX
  ORL     $0x80, DX          // final byte |= 0x80
  SHRL    $7, CX
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

  // rewrite delims[R9++].offset = (DI - vmm)
  MOVQ    DI, DX
  SUBQ    ·vmm+0(SB), DX  // DX = absolute address (32-bit)
  MOVL    DX, 0(R8)(R9*8)
  INCL    R9

  // actually project
  MOVQ    symbols+56(FP), BX
  MOVQ    symbols_len+64(FP), R8
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
  MOVQ    ·vmm+0(SB), SI
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
  SHRL    $1, R15
  JNZ     project_objects
  MOVQ    DI, ret+80(FP)    // accumulate destination offset
tail:
  MOVQ    delims_len+16(FP), CX
  SUBQ    R9, CX
  JNZ     loop
ret:
  MOVQ    dst+32(FP), DI
  SUBQ    DI, ret+80(FP)
  MOVQ    R9, ret1+88(FP)
  RET
genmask:
  // K1 = (1 << CX)-1
  MOVL    $1, R8
  SHLL    CX, R8
  SUBL    $1, R8
  KMOVW   R8, K1
  JMP     doit
rep_movsb:
  // memcpy "fast-path" for large objects
  REP; MOVSB;
  JMP  next_field
trap:
  BYTE $0xCC
  RET
did_abort:
  MOVQ $0, ret+80(FP)
  MOVQ $0, ret1+88(FP)
  RET
