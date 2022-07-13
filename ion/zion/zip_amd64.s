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

// Bucket decoding:
//
// Each Decoder.pos[*] field points to the offset
// within Decoder.mem[] at which the bucket starts,
// and Decoder.base[*] is the current addend (i.e.
// the cursor within the bucket). Buckets that
// do not contain anything we care about have Decoder.pos[*]
// set to -1.
//
// Before decoding each structure, we save Decoder.pos[*]
// in zmm0 so that we can restore it if it turns out that
// we do not have enough space for the complete structure
// in the output buffer. Then, we step through each of the
// fields sequentially and do the following:
//
//   1. If the field references a bucket that we have
//      not decompressed, go to the next field.
//   2. Decode the label and value in the bucket.
//   3. Update the bucket pointer: Decoder.base[bucket] += len(label)+len(value)
//   4. If the symbol from the decoded label is in
//      our symbol bitmap, then perform a bounds-check
//      on the output buffer and copy out the label+field
//      if there is enough space (otherwise return).
//
// When we are decoding the structure field size, we use
// the size class hint to reserve space at the beginning
// of the current output buffer position for the structure
// descriptor. Once we have finished copying out all the fields,
// we encode the structure descriptor. (We return an encoding
// error if the size class hint lied about the amount of space
// required for the descriptor. The size class should be large
// enough for *all* of the fields, so a subset of the fields should
// have no trouble fitting!)

TEXT ·zipfast(SB), NOSPLIT, $32
    NO_LOCAL_POINTERS
    MOVQ  src_base+0(FP), SI  // SI = &src[0]
    MOVQ  dst_base+24(FP), DI // DI = &dst[0]
    MOVQ  dst_len+32(FP), R8
    ADDQ  DI, R8              // R8 = &dst[len(dst)] = end-of-output
    MOVQ  R8, 24(SP)          // 24(SP) = end-of-output
    MOVQ  $0, ret+56(FP)      // consumed = 0
    MOVQ  $0, ret1+64(FP)     // wrote = 0
    MOVQ  d+48(FP), R9        // R9 = &Decoder
    MOVQ  Decoder_set+pathset_bits+0(R9), DX // DX = symbol ID bitmap
    MOVQ  SI, 0(SP)           // set up initial saved start
    MOVQ  DI, 8(SP)           // set up initial saved dst
    VMOVDQU32 Decoder_pos(R9), Z0
    VPMOVD2M  Z0, K1
    KNOTW     K1, K1          // K1 = valid bucket bits
    KMOVW     K1, R8
    VPBROADCASTW R8, Y7       // Y7 = valid bucket bits x16
    MOVL      $1, R8
    VPBROADCASTW R8, Y3       // Y3 = $1 (words)
    MOVL      $0xf, R8
    VPBROADCASTB R8, X6       // X6 = 0x0f0f0f0f...
begin_struct:
    XORL  R11, R11
    MOVL  R11, 16(SP)         // set up current size class
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
    LEAL    1(R11), R13
    SHRL    $1, R13     // R13 = (desc[0]+1)/2 = # needed bytes
    DECQ    AX          // consumed 1 byte
    INCQ    SI          // advance by 1 byte
    CMPL    R13, AX
    JA      ret_err     // assert len(src)-1 >= # needed bytes
    TESTL   R11, R11
    JZ      done
    MOVQ    0(SI), R12             // R12 = descriptor bits
    ADDQ    R13, SI                // adjust source base
    MOVQ    SI, 0(SP)              // save SI; will restore later

    // test all of the descriptor nibbles
    // against the buckets we've decoded
    // and eliminate any that don't match
    // (doing this without branches removes
    // a dozen+ unpredictable branches!)
    MOVL          $1, R8
    SHLXL         R11, R8, R8
    DECL          R8              // R8 = mask of valid bits
    KMOVW         R8, K1          // K1 = lanes to evaluate
    VMOVQ         R12, X1
    RORQ          $4, R12
    VMOVQ         R12, X2
    VPUNPCKLBW    X2, X1, X1      // X1 = interleave bytes and rol(bytes, 4)
    VPANDD        X6, X1, X1      // X1 &= 0x0f0f... (ignore upper nibble)
    VPMOVZXBW     X1, Y4          // Y4 = unpacked nibbles into 16 words
    VPSLLVW       Y4, Y3, Y4      // Y4 = 1<<nibbles in 16 words
    VPTESTMW      Y7, Y4, K1, K1  // K1 = (1 << nibble) & bucket mask x 16
    KTESTW        K1, K1
    JZ            check_outer_loop // continue parsing shape if zero matching fields

    // NOTE: this is just
    //   VPCOMPRESSB X1, K1, Decoder_nums(R9)
    // but without VBMI2 instructions;
    // it's possible PEXT from K1 could also
    // be used to compress the bits in R12...
    VPMOVZXBD     X1, Z1
    VPCOMPRESSD   Z1, K1, Z1
    VPMOVDB       Z1, Decoder_nums(R9)

    KMOVW         K1, R11
    XORL          R12, R12        // R12 = bucket index
    POPCNTL       R11, R11        // R11 = actual buckets to process
unpack_loop:
    MOVBQZX Decoder_nums(R9)(R12*1), R13 // R13 = bucket number
    MOVQ    Decoder_mem+0(R9), SI        // SI = &Decoder.mem[0]
    MOVL    Decoder_pos(R9)(R13*4), R14  // R14 = bucket pos
    ADDL    Decoder_base(R9)(R13*4), R14 // R14 = bucket.pos + bucket.base
    LEAQ    0(SI)(R14*1), SI             // SI = actual ion field+value

    // now we have SI pointing to an ion label + value;
    // we need to decode the # of bytes in this memory
    // and add it back to bucket.base

    // first, parse label varint:
    MOVL    0(SI), R14
    MOVL    R14, R15
    ANDL    $0x00808080, R15 // check for stop bit
    JZ      ret_err
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
label_done:
    // R8 is now the symbol ID; test against
    // the symbol bitmap:
    MOVL    R8, BX
    SHRL    $6, BX
    CMPQ    Decoder_set+pathset_bits+8(R9), BX
    JBE     parse_value     // skip if (bit >> 6) > len(set.bits)
    MOVQ    0(DX)(BX*8), BX // BX = words[bit>>6]
    ANDL    $63, R8
    MOVL    $1, R15
    SHLXQ   R8, R15, R8     // R8 = 1 << (bit & 63)
    XORQ    R8, BX          // flip bitmap bit
    ANDQ    R8, BX          // BX is non-zero if we should skip this entry
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
    ORL     R8, R15
    TESTL   $0x80, R14
    JZ      varint_loop
end_varint:
    ADDL    R15, CX          // size += sizeof(object)
just_one_byte:
    INCL   CX                          // size += descriptor byte
    ADDL   CX, Decoder_base(R9)(R13*4) // bucket base += size
    TESTQ  BX, BX
    JNZ    skip_bucket
    MOVQ   24(SP), R15
    SUBQ   DI, R15
    SUBQ   CX, R15
    JL     ret_toolarge
    CMPQ   R15, $8
    JL     memcpy_slow
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
    INCL    R12               // skip to next Decoder.nums[]
    DECL    R11               // elements--
    JNZ     unpack_loop       // continue while elements > 0
check_outer_loop:
    CMPQ    R10, $16          // loop again if shape[0] == 16
    JEQ     top
done:
    // it's possible that we haven't actually
    // done a bounds-check yet if we are outputting
    // exactly zero fields; in that case we still
    // need to check that the descriptor can fit!
    CMPQ      DI, 24(SP)
    JA        ret_toolarge

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
    ANDL      $0xf, DI
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
    ANDL      $0x7f, DI
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
