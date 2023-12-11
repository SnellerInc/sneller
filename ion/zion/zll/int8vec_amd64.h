// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.


TEXT FUNC_NAME(SB), NOSPLIT|NOFRAME, $16
    MOVQ  $-1, ret+48(FP)   // default result = error
    MOVQ  src+0(FP), SI
    MOVQ  src_len+8(FP), BX
    ADDQ  SI, BX            // BX = end-of-input
    MOVQ  dst+24(FP), DI
    MOVQ  dst_len+32(FP), DX
    TESTQ DX, DX
    JZ    ret_okay
    ADDQ  DI, DX            // DX = end-of-output

    // populate loop bounds for 32x loop
    MOVQ      DX, R13
    SUBQ      $64, R13   // R13 = end-of-32x-loop-output
    JS        loop_1x    // can't do 32-byte loop if fewer than 64 bytes of space
    MOVQ      BX, R14
    SUBQ      SI, R14
    SHRQ      $5, R14    // R14 = max 32x loop trip counts
    JZ        loop_1x    // can't do 32-byte loop if fewer than 32 bytes of input

    // populate zmm constants for 32x loop
    MOVQ      $0xaaaaaaaaaaaaaaaa, R15
    KMOVQ     R15, K2    // K2 = odd lanes
    MOVL      $0x0020, R15
    VPBROADCASTW R15, Z1 // Z1 = 0x0020 repeated
    VPSRLW    $1, Z1, Z2 // Z2 = 0x0010 repeated
    VPSRLW    $4, Z2, Z3 // Z3 = 0x0001 repeated
loop_32x:
    VMOVDQU32   0(SI), Y0
    ADDQ        $32, SI
    VPMOVSXBW   Y0, Z0         // Z0 = sign-extended values
    VPSRLW      $11, Z0, Z4    // Z4 = sign bit at 0x0010 position
    VPANDD      Z4, Z2, Z4     // Z4 = 0x0010 for signed words
    VPABSW      Z0, Z0         // Z0 = abs(int16(byte))
    VPSLLW      $8, Z0, Z0     // Z0 = abs(int16(byte))<<8
    VPTERNLOGD  $0xFE, Z4, Z1, Z0 // Z0 = (abs(int16(byte))<<8) | 0x20 | 0x0010 for signed words
    VPTESTMB    Z0, Z0, K1     // K1 = non-zero bytes (also output bytes)
    KMOVQ       K1, R15
    KSHIFTRQ    $1, K1, K7     // K7 = non-zero byte positions shifted right by 1
    VPMOVM2B    K7, Z4         // Z4 = low bytes before non-zero bytes = 0xff
    VPTERNLOGD  $0xF8, Z3, Z4, Z0 // Z0 |= (low bytes before non-zero bytes & 0x01
    // store vpcompressb(z0, k1) into 0(DI)
    // and add popcntq(r15) to DI
    // clobbers allowed: z0, r10, r15, z20-31, k1, k7
    VPCOMPRESSB_IMPL_Z0_K1_DI_R15()
    CMPQ        DI, R13
    JA          loop_1x        // break loop if we're now at or past the last output location
    DECQ        R14
    JNZ         loop_32x       // continue while (--tripcount)
loop_1x:
    CMPQ     SI, BX
    JZ       ret_okay
    JA       ret_err
    MOVQ     $1, CX
    MOVBQZX  0(SI), R8
    INCQ     SI
    MOVB     R8, R11     // R11 will become negated byte
    MOVB     R8, R10     // R10 will become sign bit
    NEGB     R11
    SHRL     $3, R10     //
    ANDL     $0x10, R10  // R10 = extra bit to add to 0x20 if negative
    MOVL     $0x20, R9
    TESTB    R8, R8
    JZ       store_tag   // handle zero
    CMOVLLT  R11, R8     // R8 = -R8 if byte is negative
    INCL     CX          // output 2 bytes, not 1
    INCL     R9          // tag is 0x21
    ADDL     R10, R9     // add sign bit into tag (0x31 or 0x21)
    MOVB     R8, 1(DI)   // store byte value
store_tag:
    MOVB     R9, 0(DI)
    LEAQ     0(DI)(CX*1), DI
    CMPQ     DI, DX
    JB       loop_1x // continue if not at end
    JNE      ret_err // error if we didn't fill exactly the desired bytes
ret_okay:
    SUBQ     src+0(FP), SI  // compute # output bytes
    MOVQ     SI, ret+48(FP)
ret_err:
    RET
