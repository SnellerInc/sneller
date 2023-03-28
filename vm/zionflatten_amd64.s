// Copyright (C) 2023 Sneller, Inc.
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

// func zionflatten(shape []byte, buckets *zll.Buckets, fields [][]vmref, tape []ion.Symbol) int
TEXT ·zionflatten(SB), NOSPLIT|NOFRAME, $16
    NO_LOCAL_POINTERS
    MOVQ  shape+0(FP), SI     // SI = &shape[0]
    MOVQ  buckets+24(FP), R9  // R9 = *zll.Buckets
    VMOVDQU32 const_zllBucketPos(R9), Z0 // Z0 = buckets.Pos
    VPMOVD2M  Z0, K1
    KNOTW     K1, K1          // K1 = valid bucket bits
    KMOVW     K1, R8
    VPBROADCASTW R8, Y7       // Y7 = valid bucket bits x16
    MOVL      $1, R8
    VPBROADCASTW R8, Y3       // Y3 = $1 (words)
    MOVL      $0xf, R8
    VPBROADCASTB R8, X6       // X6 = 0x0f0f0f0f...
    XORL      CX, CX
    MOVL      CX, 8(SP)       // 8(SP) = structures processed = 0
    MOVQ      SI, 0(SP)       // 0(SP) = saved shape addr
begin_struct:
    MOVQ  fields+32(FP), DI      // DI = &fields[0] (incremented later)
    MOVQ  tape_base+56(FP), DX   // DX = &tape[0] (first symbol to match)
    XORL  R11, R11               // source bits
top:
    MOVQ    0(SP), SI
    MOVQ    shape_len+8(FP), AX
    ADDQ    shape_base+0(FP), AX // AX = &src[0] + len(src)
    SUBQ    SI, AX               // AX = remaining bytes
    JZ      ret_ok               // assert len(src) != 0
    MOVBLZX 0(SI), R10           // R10 = descriptor
    ANDL    $0x1f, R10
    CMPL    R10, $16
    JG      ret_err              // assert desc <= 0x10
    MOVL    R10, R11
    LEAL    1(R11), R13
    SHRL    $1, R13              // R13 = (desc[0]+1)/2 = # needed bytes
    DECQ    AX                   // consumed 1 byte
    INCQ    SI                   // advance by 1 byte
    CMPL    R13, AX
    JA      ret_err              // assert len(src)-1 >= # needed bytes
    MOVQ    0(SI), R12           // R12 = descriptor bits
    ADDQ    R13, SI              // adjust source base
    MOVQ    SI, 0(SP)            // save shape pointer
    TESTL   R11, R11
    JZ      done

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

    VPMOVZXBD     X1, Z1
    VPCOMPRESSD   Z1, K1, Z1       // Z1 = buckets to scan

    KMOVW         K1, R11
    POPCNTL       R11, R11        // R11 = actual buckets to process
unpack_loop:
    MOVQ    const_zllBucketDecompressed+0(R9), SI // SI = &zll.Buckets.Decompressed[0]
    VMOVD   X1, R13                               // extract next bucket from Z1
    MOVL    const_zllBucketPos(R9)(R13*4), R14    // R14 = bucket pos
    LEAQ    0(SI)(R14*1), SI                      // SI = actual ion field+value
    VALIGND $1, Z1, Z1, Z1                        // shift to next bucket

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
    XORL    BX, BX
    MOVQ    tape_len+64(FP), AX
    SHLQ    $3, AX
    ADDQ    tape_base+56(FP), AX // AX = &tape[len(tape)] = end-of-tape
match_tape:
    CMPQ    DX, AX
    JGE     parse_value      // definitely not matching if tape exhausted
    CMPQ    0(DX), R8        // *tape == symbol
    JEQ     exact_match
    JA      parse_value      // symbol <= tape
    MOVL    8(SP), R14       // R14 = current struct
    SHLQ    $3, R14          //
    ADDQ    0(DI), R14       //
    MOVQ    BX, 0(R14)       // fields[struct].{off, len} = 0
    ADDQ    $24, DI          // fields += sizeof([]vmref)
    ADDQ    $8, DX           // tape += sizeof(symbol)
    JMP     match_tape
exact_match:
    MOVL    $1, BX
    ADDQ    $8, DX
parse_value:
    // parse value
    MOVL    CX, R12           // save label size
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
    INCL   CX                                // size += descriptor byte
    ADDL   CX, const_zllBucketPos(R9)(R13*4) // bucket base += size
    TESTL  BX, BX
    JZ     skip_bucket

    // adjust offset and size so we skip the label bits
    ADDQ   R12, SI
    SUBQ   R12, CX
    JS     trap              // size should always be positive

    // copy out the field into DI
    SUBQ   ·vmm+0(SB), SI
    JS     trap              // SI should always be within vmm...
    MOVL   8(SP), R8
    MOVQ   0(DI), AX
    MOVL   SI, 0(AX)(R8*8)   // fields[struct].off = (addr - vmm)
    MOVL   CX, 4(AX)(R8*8)   // fields[struct].len = rcx
    ADDQ   $24, DI           // fields += sizeof([]vmref)
skip_bucket:
    DECL    R11               // elements--
    JNZ     unpack_loop       // continue while elements > 0
check_outer_loop:
    CMPQ    R10, $16          // loop again if shape[0] == 16
    JEQ     top
done:
    INCL    8(SP)            // struct++
    JMP     begin_struct
ret_ok:
    VMOVDQU32 Z0, const_zllBucketPos(R9)
    MOVL    8(SP), AX
    MOVQ    AX, ret+80(FP)    // return # structures copied out
    RET
ret_err:
    VMOVDQU32 Z0, const_zllBucketPos(R9)
    MOVL    8(SP), AX
    MOVQ    AX, ret+80(FP)
    RET
trap:
    BYTE $0xCC
    RET
