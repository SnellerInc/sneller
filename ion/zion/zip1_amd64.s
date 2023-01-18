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

TEXT ·shapecount(SB), NOSPLIT, $0
    MOVQ shape_base+0(FP), SI
    MOVQ shape_len+8(FP), DX
    ADDQ SI, DX       // end-of-source
    XORL CX, CX       // count
    XORL AX, AX       // fc
    XORL BX, BX       // zero
    MOVB $1, ret1+32(FP)
    JMP  loop_tail
loop:
    MOVBLZX 0(SI), AX
    ANDL    $0x1f, AX // fc = shape[0] & 0x1f
    CMPL    AX, $16   // assert(fc <= 16)
    JA      corrupt
    ADCXQ   BX, CX    // count++ if fc < 16
    ADDQ    $3, AX
    SHRQ    $1, AX    // skip = (fc + 3)/2
    ADDQ    AX, SI    // shape = shape[skip:]
loop_tail:
    CMPQ    SI, DX    // bounds check
    JB      loop
    JA      corrupt
    MOVQ    CX, ret+24(FP)
    RET
corrupt:
    MOVB    $0, ret1+32(FP)
    RET

// Specialized path for decoding a single field:
//
// We know that a single field must live in a single bucket,
// so we can simply walk the bucket and extract a bunch of
// single-record fields. We use the pre-computed object count
// to emit the correct number of structures with MISSING fields
// once we have emitted all of the structures with non-MISSING fields.
// (One consequence of this is that we may permute the order of records,
// but that seems fine.)
TEXT ·zipfast1(SB), NOSPLIT, $0
    NO_LOCAL_POINTERS
    MOVQ  src_base+0(FP), SI  // SI = &src[0]
    MOVQ  dst_base+24(FP), DI // DI = &dst[0]
    MOVQ  $0, ret+72(FP)      // consumed = 0
    MOVQ  $0, ret1+80(FP)     // wrote = 0
    MOVQ  count+64(FP), BX    // BX = target count
    CMPQ  BX, $0
    JLE   ret

    MOVQ  sym+56(FP), DX      // DX = target symbol
    MOVQ  src_len+8(FP), R10
    ADDQ  SI, R10             // R10 = &src[len(src)] -- a past-end pointer
    MOVQ  dst_len+32(FP), R11
    ADDQ  DI, R11             // R11 = &dst[len(dst)] -- a past-end pointer

loop_top:
    // now we have SI pointing to an ion label + value;
    // we need to decode the # of bytes in this memory
    // and add it back to bucket.base
    // first, parse label varint:
    MOVL    0(SI), R14
    MOVL    R14, R15
    ANDL    $0x00808080, R15 // check for stop bit
    JZ      ret_baddata
    MOVL    $1, CX
    MOVL    R14, R8
    ANDL    $0x7f, R8
    JMP     test_stop_bit
more_label_bits:
    INCL    CX
    SHLL    $7, R8
    SHRL    $8, R14
    MOVL    R14, R15
    ANDL    $0x7f, R15
    ORL     R15, R8
test_stop_bit:
    TESTL   $0x80, R14
    JZ      more_label_bits
parse_value:
    // parse value
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
    JZ      ret_baddata
    MOVL    R14, R15
    ANDL    $0x7f, R15        // accum = desc[0]&0x7f
    TESTL   $0x80, R14
    JNZ     end_varint
varint_loop:
    INCL    CX
    SHLL    $7, R15
    SHRL    $8, R14
    MOVL    R14, R13
    ANDL    $0x7f, R13
    ORL     R13, R15
    TESTL   $0x80, R14
    JZ      varint_loop
end_varint:
    ADDL    R15, CX          // size += sizeof(object)
just_one_byte:
    INCL   CX                // size += descriptor byte

    // check src bounds
    LEAQ   (SI)(CX*1), AX    // AX = src pointer after copy/skip
    CMPQ   AX, R10
    JA     ret_truncated     // check if the future SI won't go outside `src`

    CMPQ   R8, DX
    JNE    skip_object
    CMPQ   CX, $0xe
    JGE    size2

    // check dst bounds
    LEAQ   1(DI)(CX*1), AX
    CMPQ   AX, R11
    JA     ret_toolarge

    MOVL   CX, AX
    ORL    $0xd0, AX
    MOVB   AX, 0(DI)
    INCQ   DI
    JMP    do_memcpy
size2:
    CMPQ   CX, $(1<<7)
    JGE    size3

    // check dst bounds
    LEAQ   2(DI)(CX*1), AX
    CMPQ   AX, R11
    JA     ret_toolarge

    MOVL   CX, AX
    SHLL   $8, AX
    ORL    $0x80de, AX
    MOVW   AX, 0(DI)
    ADDQ   $2, DI
    JMP    do_memcpy
size3:
    CMPQ   CX, $(1<<14)
    JGE    size4

    // check dst bounds
    LEAQ   3(DI)(CX*1), AX
    CMPQ   AX, R11
    JA     ret_toolarge

    MOVL   $0x8000de, AX
    MOVL   CX, R8
    ANDL   $(0x7f << 7), R8
    SHLL   $1, R8
    ORL    R8, AX
    MOVL   CX, R8
    ANDL   $0x7f, R8
    SHLL   $16, R8
    ORL    R8, AX
    MOVL   AX, 0(DI)
    ADDQ   $3, DI
    JMP    do_memcpy
size4:
    CMPQ  CX, $(1<<21)
    JGE   ret_baddata

    // check dst bounds
    LEAQ   3(DI)(CX*1), AX
    CMPQ   AX, R11
    JA     ret_toolarge

    MOVL  $0x800000de, AX
    MOVL  CX, R8
    ANDL  $0x7f, R8
    SHLL  $24, R8
    ORL   R8, AX
    MOVL  CX, R8
    ANDL  $(0x7f << 7), R8
    SHLL  $(16 - 7), R8
    ORL   R8, AX              // desc |= (size & (0x7f << 7)) << 1
    MOVL  CX, R8
    ANDL  $(0x7f << 14), R8
    SHRL  $(14 - 8), R8        // shift "left" by (8 - 14), so shift right by 6
    ORL   R8, AX
    MOVL  AX, 0(DI)
    ADDQ  $4, DI
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
memcpy_end:
    // count--; see if we are done
    DECL    BX
    JZ      ret
loop_tail:
    CMPQ    SI, R10
    JB      loop_top
write_zeros:
    // check dst capacity
    LEAQ    (DI)(BX*1), AX
    CMPQ    AX, R11
    JA      ret_toolarge
write_zeros_loop:
    // for all the elements we didn't consume,
    // write out the empty structure:
    MOVB    $0xd0, 0(DI)
    INCQ    DI
    DECL    BX
    JNZ     write_zeros_loop
ret:
    MOVQ    d+48(FP), R9
    MOVL    $0, Decoder_fault(R9)
    // compute consumed + wrote values
    SUBQ    src_base+0(FP), SI
    MOVQ    SI, ret+72(FP)
    SUBQ    dst_base+24(FP), DI
    MOVQ    DI, ret1+80(FP)
    RET
skip_object:
    ADDQ    CX, SI
    JMP     loop_tail
ret_baddata:
    MOVQ    d+48(FP), R9
    MOVL    $const_faultBadData, Decoder_fault(R9)
    RET
ret_toolarge:
    MOVQ    d+48(FP), R9
    MOVL    $const_faultTooLarge, Decoder_fault(R9)
    RET
ret_truncated:
    MOVQ    d+48(FP), R9
    MOVL    $const_faultTruncated, Decoder_fault(R9)
    RET
