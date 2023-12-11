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
#include "funcdata.h"
#include "go_asm.h"

// Expands a 128-bit key for AES128 encryption/decryption. The 11 resulting subkeys
// will be stored at the block pointed to by DI. For the single-lane case, the stride
// of $0x10 in DX will put the subkeys in consecutive memory locations. Larger strides
// are allowed if subkey interleaving is required, e.g. in the AVX512 four-lane AES
// processing blocks. Note: the expansion functions are not performance-critical and
// should be executable on a plain SSE-capable CPU. DO NOT use AVX512 encodings.
//
// Inputs:
//  X0 := the 128-bit key to be expanded
//  DI  := the expanded key buffer base address
//  DX  := the output buffer stride
//
// Clobbers:
//  DI, X1, X2
//
TEXT aesExpandKeyCore128<>(SB), NOSPLIT | NOFRAME, $0-0
    VMOVDQU             X0, (DI)
    ADDQ                DX, DI
    AESKEYGENASSIST     $0x01, X0, X1   // round 1
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x02, X0, X1   // round 2
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x04, X0, X1   // round 3
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x08, X0, X1   // round 4
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x10, X0, X1   // round 5
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x20, X0, X1   // round 6
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x40, X0, X1   // round 7
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x80, X0, X1   // round 8
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x1b, X0, X1   // round 9
    CALL                aesExpandKeyMixCore128<>(SB)
    AESKEYGENASSIST     $0x36, X0, X1   // round 10
    JMP                 aesExpandKeyMixCore128<>(SB)

TEXT aesExpandKeyMixCore128<>(SB), NOSPLIT | NOFRAME, $0-0
    PSHUFD      $0xff, X1, X1
    MOVOA       X0, X2
    PSLLDQ      $4, X2
    PXOR        X2, X0
    MOVOA       X0, X2
    PSLLDQ      $4, X2
    PXOR        X2, X0
    MOVOA       X0, X2
    PSLLDQ      $4, X2
    PXOR        X2, X0
    PXOR        X1, X0
    VMOVDQU     X0, (DI)
    ADDQ        DX, DI
    RET


// func auxExpandFromKey128(p *ExpandedKey128, key Key128)
// Expands a 128-bit key into the 11 AES round keys and stores the results in a block pointed to by p.
//
TEXT ·auxExpandFromKey128(SB), NOSPLIT | NOFRAME, $0-0
    MOVQ        p+0(FP), DI
    VMOVDQU     key+8(FP), X0
    MOVL        $0x10, DX
    JMP         aesExpandKeyCore128<>(SB)


// func auxExpandFromKey128Quad(p *ExpandedKey128Quad, quad Key128Quad)
// Expands a quad of 128-bit keys into their corresponding quads of 11 AES round keys
// and stores the results in a block pointed to by p. The round keys are interleaved
// in memory round1[key1], round1[key2], round1[key3], round1[key4], round2[key1]...
// to allow for parallel 4-lane AVX512 AES encryption/decryption. Note that while
// the 4-lane encryption/decryption is supported in hardware, the key expansion is not,
// so it needs to be done separately for every lane.
//
TEXT ·auxExpandFromKey128Quad(SB), NOSPLIT | NOFRAME, $0-0
    VMOVDQU     quad_0_0+8(FP), X0
    MOVQ        p+0(FP), SI
    MOVL        $0x40, DX
    MOVQ        SI, DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU     quad_1_0+24(FP), X0
    LEAQ        0x10(SI), DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU     quad_2_0+40(FP), X0
    LEAQ        0x20(SI), DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU     quad_3_0+56(FP), X0
    LEAQ        0x30(SI), DI
    JMP         aesExpandKeyCore128<>(SB)
