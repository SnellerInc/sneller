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

// Expands a 128-bit key for AES128 encryption/decryption. The 11 resulting subkeys
// will be stored at the block pointed to by DI. For the single-lane case, the stride
// of $0x10 in DX will put the subkeys in consecutive memory locations. Larger strides
// are allowed if subkey interleaving is required, e.g. in the AVX512 four-lane AES
// processing blocks.
//
// Inputs:
//  X0 := the 128-bit key to be expanded
//  DI  := the expanded key buffer base address
//  DX  := the output buffer stride
//
// Clobbers:
//  DI, X1, X2

TEXT aesExpandKeyCore128<>(SB), NOSPLIT | NOFRAME, $0-0
    VMOVDQU32           X0, (DI)
    ADDQ                DX, DI
    VAESKEYGENASSIST    $0x01, X0, X1   // round 1
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x02, X0, X1   // round 2
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x04, X0, X1   // round 3
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x08, X0, X1   // round 4
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x10, X0, X1   // round 5
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x20, X0, X1   // round 6
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x40, X0, X1   // round 7
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x80, X0, X1   // round 8
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x1b, X0, X1   // round 9
    CALL                aesExpandKeyMixCore128<>(SB)
    VAESKEYGENASSIST    $0x36, X0, X1   // round 10
    JMP                 aesExpandKeyMixCore128<>(SB)

TEXT aesExpandKeyMixCore128<>(SB), NOSPLIT | NOFRAME, $0-0
    VPSHUFD     $0xff, X1, X1
    VPSLLDQ     $4, X0, X2
    VPXORD      X2, X0, X0
    VPSLLDQ     $4, X0, X2
    VPXORD      X2, X0, X0
    VPSLLDQ     $4, X0, X2
    VPXORD      X2, X0, X0
    VPXORD      X1, X0, X0
    VMOVDQU32   X0, (DI)
    ADDQ        DX, DI
    RET


// func auxExpandFromKey128(p *ExpandedKey128, key Key128)
// Expands a 128-bit key into the 11 AES round keys and stores the results in a block pointed to by p.
//
TEXT ·auxExpandFromKey128(SB), NOSPLIT | NOFRAME, $0-0
    MOVQ        p+0(FP), DI
    VMOVDQU64   key+8(FP), X0
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
    VMOVDQU64   quad_0_0+8(FP), X0
    MOVQ        p+0(FP), SI
    MOVL        $0x40, DX
    MOVQ        SI, DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU64   quad_1_0+24(FP), X0
    LEAQ        0x10(SI), DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU64   quad_2_0+40(FP), X0
    LEAQ        0x20(SI), DI
    CALL        aesExpandKeyCore128<>(SB)
    VMOVDQU64   quad_3_0+56(FP), X0
    LEAQ        0x30(SI), DI
    JMP         aesExpandKeyCore128<>(SB)
