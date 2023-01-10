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

TEXT ·zipall(SB), NOSPLIT, $24
    NO_LOCAL_POINTERS
    MOVQ  src_base+0(FP), SI  // SI = &src[0]
    MOVQ  dst_base+24(FP), DI // DI = &dst[0]
    MOVQ  $0, ret+56(FP)      // consumed = 0
    MOVQ  $0, ret1+64(FP)     // wrote = 0
    MOVQ  d+48(FP), R9        // R9 = &Decoder
    MOVQ  SI, 0(SP)           // set up initial saved start
    MOVQ  DI, 8(SP)           // set up initial saved dst
begin_struct:
    XORL  R11, R11
    MOVQ  R11, 16(SP)         // set up current size class
    MOVQ  8(SP), DI           // load dst ptr
    // cache old Decoder[*].base values
    // in zmm0 so that we can restore them
    // if we have to abort processing this struct
    VMOVDQU32 Decoder_base(R9), Z0
    // add one byte of reserved space
    // for the descriptor; we may add more
    INCQ    DI
top:
    MOVQ    0(SP), SI          // SI = current source pointer
    MOVQ    src_len+8(FP), AX
    ADDQ    src_base+0(FP), AX // AX = &src[0] + len(src)
    SUBQ    SI, AX             // AX = remaining bytes
    TESTQ   AX, AX
    JZ      ret_ok      // assert len(src) != 0
    MOVBLZX 0(SI), R10  // R10 = descriptor
    MOVL    R10, R11
    SHRL    $6, R11     // R11 = 2-bit size class
    ADDQ    R11, DI     // reserve destination space for descriptor
    ADDL    R11, 16(SP) // save latest size class; should be zero except for last structure
    ANDL    $0x1f, R10
    CMPL    R10, $16
    JG      ret_err     // assert desc <= 0x10
    MOVL    R10, R11
    MOVL    R11, R13
    INCL    R13
    SHRL    $1, R13     // R13 = (desc[0]+1)/2 = # needed bytes
    DECQ    AX          // consumed 1 byte
    INCQ    SI          // advance by 1 byte
    CMPL    R13, AX
    JA      ret_err     // assert len(src)-1 >= # needed bytes
    MOVQ    0(SI), R12             // R12 = descriptor bits
    ADDQ    R13, SI                // adjust source base
    MOVQ    SI, 0(SP)              // save SI; will restore later
    TESTL   R11, R11
    JZ      done
unpack_loop:
    MOVQ    R12, R13
    ANDL    $0xf, R13                    // R13 = bucket number
    MOVQ    Decoder_buckets+const_decompOff+0(R9), SI        // SI = &Decoder.buckets.Decompressed[0]
    MOVL    Decoder_buckets+const_posOff(R9)(R13*4), R14  // R14 = bucket pos
    TESTL   R14, R14
    JS      trap                         // assert(pos >= 0)
    ADDL    Decoder_base(R9)(R13*4), R14 // R14 = bucket.pos + bucket.base
    LEAQ    0(SI)(R14*1), SI             // SI = actual ion field+value

    // now we have SI pointing to an ion label + value;
    // we need to decode the # of bytes in this memory
    // and add it back to bucket.base

    // first, parse label varint:
    MOVL    0(SI), R14
    ANDL    $0x00808080, R14
    JZ      ret_err
    TZCNTL  R14, CX           // cx = tzcnt(stop bits)
    ADDL    $1, CX            // cx = tzcnt(stop bits)+1 = valid bits
    SHRL    $3, CX            // cx/8 = field label width
    MOVL    0(SI)(CX*1), R14  // load first 4 bytes of value
    CMPB    R14, $0x11
    JE      just_one_byte
    MOVL    R14, R15
    ANDL    $0x0f, R15
    CMPL    R15, $0x0f
    JE      just_one_byte
    CMPL    R15, $0x0e
    JNE     end_varint // will add R15 to CX
value_is_varint:
    INCL    CX
    SHRL    $8, R14
    TESTL   $0x00808080, R14  // if there isn't a stop bit, we have a problem
    JZ      ret_err
    MOVL    R14, R15
    ANDL    $0x7f, R15        // accum = desc[0]&0x7f
    TESTL   $0x80, R14
    JNZ     end_varint
varint_loop:
    INCL    CX
    SHLL    $7, R15
    SHRL    $8, R14
    MOVL    R14, R8
    ANDL    $0x7f, R8
    ADDL    R8, R15
    TESTL   $0x80, R14
    JZ      varint_loop
end_varint:
    ADDL    R15, CX          // size += sizeof(object)
just_one_byte:
    INCL   CX                          // size += descriptor byte
    MOVQ   dst_len+32(FP), R8          // r8 = len - (addr - baseaddr) = remaining
    ADDQ   dst_base+24(FP), R8
    SUBQ   DI, R8
    CMPQ   CX, R8
    JG     ret_toolarge                // error if size > remaining(dst)
    ADDL   CX, Decoder_base(R9)(R13*4) // bucket base += size
    SUBQ   $8, R8
    CMPQ   CX, R8                      // size > remaining(dst)-8
    JG     memcpy_slow                 // have to copy precisely
do_memcpy:
    // memcpy(di, si, cx); di += cx; si += cx;
    MOVQ    0(SI), R8
    ADDQ    $8, SI
    MOVQ    R8, 0(DI)
    ADDQ    $8, DI
    SUBQ    $8, CX
    JG      do_memcpy
    ADDQ    CX, DI            // add negative CX component
    ADDQ    CX, SI            // back into SI, DI to get corrected amt
skip_bucket:
    SHRQ    $4, R12           // bits >>= 4
    DECL    R11               // elements--
    JNZ     unpack_loop       // continue while elements > 0
    CMPQ    R10, $16          // loop again if shape[0] == 16
    JEQ     top
done:
    // structure is complete; commit updates
    MOVQ      8(SP), R10       // get a copy of the original dst ptr
    MOVQ      DI, 8(SP)        // we are about to clobber DI
    MOVQ      0(SP), SI
    SUBQ      src+0(FP), SI    // ret0 = # bytes consumed
    MOVQ      SI, ret+56(FP)
    SUBQ      R10, DI          // DI = (cur dst - original dst) = #bytes written
    ADDQ      DI, ret1+64(FP)  // ret1 += #bytes written

    // encode structure bits, taking care
    // to respect the original size class
    // of the object
    MOVL      16(SP), R11
    TESTL     R11, R11
    JZ        size_class_0
    CMPL      R11, $1
    JEQ       size_class_1
    CMPL      R11, $2
    JEQ       size_class_2
    CMPL      R11, $3
    JEQ       size_class_3
    JMP       ret_err
size_class_0:
    // encode 1-byte descriptor 0xd0 to 0xdd
    DECQ      DI
    JS        ret_err
    CMPQ      DI, $0xd
    JA        ret_err
    MOVL      $0xd0, R8
    ORL       DI, R8
    MOVB      R8, 0(R10)
    JMP       begin_struct
size_class_1:
    // encode 2-byte descriptor 0xde80 to 0xdeff
    SUBQ      $2, DI
    JS        ret_err
    CMPQ      DI, $127
    JA        ret_err
    MOVL      $0x80de, R8
    SHLL      $8, DI
    ORL       DI, R8
    MOVW      R8, 0(R10)
    JMP       begin_struct
size_class_2:
    // encode 3-byte descriptor 0xde0080 to 0xde7fff
    SUBQ      $3, DI
    JS        ret_err
    CMPQ      DI, $(1<<14)
    JAE       ret_err
    MOVL      $0x8000de, R8
    MOVL      DI, R11
    ANDL      $(0x7f << 7), R11
    SHLL      $1, R11
    ORL       R11, R8              // desc |= (size & (0x7f << 7)) << 1
    MOVL      DI, R11
    ANDL      $0x7f, R11
    SHLL      $16, R11
    ORL       R11, R8              // desc |= (size & 0x7f)
    MOVL      0(R10), R11          // get leading 4 bytes
    ANDL      $0xff000000, R11
    ORL       R8, R11
    MOVL      R11, 0(R10)          // write (word & 0xff000000) | bits
    JMP       begin_struct
size_class_3:
    SUBQ  $4, DI
    JS    ret_err
    CMPQ  DI, $(1<<21)
    JAE   ret_err
    MOVL  $0x800000de, R8
    MOVL  DI, R11
    ANDL  $0x7f, R11
    SHLL  $(24 - 0), R11
    ORL   R11, R8              // desc |= (size & 0x7f)
    MOVL  DI, R11
    ANDL  $(0x7f << 7), R11
    SHLL  $(16 - 7), R11
    ORL   R11, R8              // desc |= (size & (0x7f << 7)) << 1
    ANDL  $(0x7f << 14), DI
    SHRL  $(14 - 8), DI        // shift "left" by (8 - 14), so shift right by 6
    ORL   DI, R8               // desc |= (size & (0x7f << 14)) << 2
    MOVL  R8, 0(R10)           // save desc in original dst ptr
    JMP   begin_struct
ret_ok:
    MOVL  $0, Decoder_fault(R9)
    RET
ret_toolarge:
    // error path for decoding a structure
    // that does not fit in dst
    MOVL     $const_faultTooLarge, Decoder_fault(R9)
    // fallthrough
ret_restore:
    // restore old Decoder.base[*] values
    VMOVDQU32 Z0, Decoder_base(R9)
    RET
ret_err:
    // error path when we encounter bad data
    MOVL     $const_faultBadData, Decoder_fault(R9)
    JMP      ret_restore
memcpy_slow:
    MOVBQZX  0(SI), R8
    INCQ     SI
    MOVB     R8, 0(DI)
    INCQ     DI
    DECL     CX
    JNZ      memcpy_slow
    JMP      skip_bucket
trap:
    BYTE $0xCC
    JMP  ret_err
