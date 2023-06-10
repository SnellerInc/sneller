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

#include "textflag.h"
#include "go_asm.h"

// bestMatchAVX512(src []byte, litmin, pos int32, hist *[4]int32)
TEXT ·bestMatchAVX512(SB), NOSPLIT|NOFRAME, $0-0
    MOVQ      src_base+0(FP), SI
    MOVQ      src_len+8(FP), DX
    MOVL      litmin+24(FP), AX // AX = minimum target position
    MOVQ      hist+32(FP), R8   // R8 = addr of next match to examine
    MOVL      pos+28(FP), BX
    SUBQ      BX, DX            // DX = len(src)-pos = max match len
    CMPQ      DX, $32
    JLT       trap              // should never happen
    VMOVDQU32 0(SI)(BX*1), Y3   // Y3 = cached 1st 32 bytes of src

    // initial results are zero
    XORL      R9, R9
    MOVL      R9, ret+40(FP)
    MOVL      R9, ret1+44(FP)
    MOVL      R9, ret2+48(FP)

    // 1st loop iteration: unconditional
    MOVL      $const_histsize, CX
    MOVL      0(R8), R10        // R10 = matchpos
    ADDQ      $4, R8            // hist++
restart: // 2nd+ loops begin here
    MOVQ      src_base+0(FP), SI
    MOVL      pos+28(FP), R9    // R9 = targetpos
    XORL      R11, R11          // R11 = matchlen = 0
    VPXORD    0(SI)(R10*1), Y3, Y0
    VPTESTMB  Y0, Y0, K1        // K1 = non-matching bytes
    KTESTD    K1, K1
    JNZ       matchlen_done     // any non matching bytes -> done
    LEAQ      0(SI)(R10*1), DI  // DI = current matchpos to evaluate
    LEAQ      0(SI)(R9*1), SI   // SI = current targetpos to evaluate
    MOVQ      DX, R12           // R12 = max_matchlen
matchlen_loop:
    ADDQ      $32, R11
    SUBQ      $32, R12          // R12 = max_matchlen - matchlen
    JZ        extend_backwards  // max_matchlen - matchlen == 0 -> done
    CMPQ      R12, $32
    JLT       byte_by_byte      // (max_matchlen - matchlen) < 8 -> bytewise compare
    VMOVDQU32 0(SI)(R11*1), Y0
    VPXORD    0(DI)(R11*1), Y0, Y0
    VPTESTMB  Y0, Y0, K1
    KTESTD    K1, K1
    JZ        matchlen_loop
matchlen_done:
    KMOVD     K1, R14
    TZCNTL    R14, R14
    ADDL      R14, R11           // R11 = 8-byte matches + tzcnt(got^want)/8
extend_backwards:
    TESTL     R10, R10           // can't extend backwards if matchpos == 0
    JZ        test_legal
    MOVQ      src_base+0(FP), SI
continue_extending_backwards:
    CMPL    R9, AX
    JEQ     test_legal           // assert(targetpos > litmin)
    MOVBLZX -1(SI)(R9*1), R14
    CMPB    -1(SI)(R10*1), R14
    JNE     test_legal           // src[matchpos-1] == src[targetpos-1]
    DECL    R9                   // targetpos--
    INCL    R11                  // matchlen++
    DECL    R10                  // matchpos--
    JNZ     continue_extending_backwards // continue while matchpos > 0
test_legal:
    // make sure this match+length pair is legal:
    MOVL    R9,  R14
    SUBL    R10, R14           // R14 = (targetpos - matchpos) = offset
    SHRL    $16, R14           // R14 = (offset >> 16)
    JZ      test_better        // small offset -> legal
    CMPL    R11, $15
    JLE     loop_tail          // offset >= 1<<16 && matchlen <= 15 -> illegal
test_better:
    // finally, produce output:
    CMPL    R11, ret2+48(FP)   // continue if matchlen <= candidate
    JLE     loop_tail
    MOVL    R9,  ret+40(FP)
    MOVL    R10, ret1+44(FP)
    MOVL    R11, ret2+48(FP)
loop_tail:
    DECL    CX
    JZ      done             // --count == 0 -> done
    MOVL    0(R8), R10       // R10 = matchpos
    ADDQ    $4, R8           // hist++
    TESTL   R10, R10
    JNZ     restart          // continue while matchpos != 0
done:
    RET
byte_by_byte:
    MOVBLZX 0(SI)(R11*1), R13
    CMPB    R13, 0(DI)(R11*1)
    JNE     extend_backwards
    INCL    R11
    DECL    R12
    JNZ     byte_by_byte
    JMP     extend_backwards
trap:
    BYTE $0xCC
    RET
