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

TEXT hashCoreVAES<>(SB), NOSPLIT | NOFRAME, $0-0
    // BX := the base address of the expanded 128-bit key quad
    // CX := the number of bytes to process
    // SI := the base address of the input data block

    NEGQ        CX
    JZ          empty_input
    MOVQ        $-1, AX

    // Observe the actual shift is (CL & 0x3f), so AX is 0xff..ff for every multiple of 64.
    // The n=0 case has been already excluded by the test above, so a full 64-byte chunk will
    // be fetched correctly with the 0xff..ff mask. That leaves only a possibly empty set of
    // the preceeding full chunks for further processing. Due to the chunks being either full
    // (the preceeding ones) or zero-padded to full (the last one), no lane masking is needed.
    SHRQ        CL, AX
    ANDQ        $-64, CX
    VMOVDQU64   0x00(BX), Z6        // The round 1 key quad
    KMOVQ       AX, K1              // K1 := the active lanes of the last chunk
    SUBQ        CX, SI              // move the input read cursor 64 bytes past the beginning of the last chunk
    VMOVDQU8.Z  -0x40(SI), K1, Z0   // Z0 := the zero-padded last chunk of input data
    MOVQ        $-0x200, DX         // DX := the ring cursor offset such that DX==0 when the wrap-around condition occurs
    VMOVDQU64   0x40(BX), Z7        // The round 2 key quad

    // CX := the negative byte count - 64, multiple of 64
    // SI := input read cursor, 64 bytes past the beginning of the last chunk
    // BX := the AES keys
    // Z0 := the zero-padded last chunk of input data
    // Z6 := the round 1 key quad
    // Z7 := the round 2 key quad

loop:
    // From "The Design of Rijndael", page 41, section 3.5:
    //
    // Two rounds of Rijndael provide 'full diffusion' in the following
    // sense: every state bit depends on all state bits two rounds ago,
    // or a change in one state bit is likely to affect half of the state
    // bits after two rounds.

    VAESENC     Z6, Z0, Z0          // latency 5, so use the time for prefetching and cursor arithmetic
    VMOVDQU64   0x280(BX)(DX*1), Z6 // fetch the round N+1 key quad
    SUBQ        $0x40, SI           // move the input read cursor to the preceding chunk
    VMOVDQU64   0x2c0(BX)(DX*1), Z8 // fetch the round N+2 key quad. Z8 is the next Z7.
    ADDQ        $0x40, CX
    VAESENC     Z7, Z0, Z0
    JNZ         append_chunk        // optimize branch prediction for the short input case

    // No more chunks to process. The in-lane diffusion should be quite good per
    // the Rijndael design, but there is absolutley no inter-lane diffusion so far!
    // This will be fixed by the non-linear mixing step that follows.
    //
    // BX := the AES keys
    // Z0 := the partial hash result in the ABCD lane order
    // Z6 := the round (n+3) key quad
    // Z8 := the round (n+4) key quad
    //
    // The "A" means the 128-bit chunk from lane 0, "B" from lane 1, etc. Inter-lane mixing
    // requires that the chunks from every other lane influence the chunk from a given lane.
    // For every pair (x, y) compute x^y and encrypt it with the i-th lane key K_i to ensure
    // diffusion. Then xor all the encrypted pairs obtained from K_i to form the final result
    // for the i-th lane. The code block below takes the full advantage of the 5-cycle latency,
    // 1-cycle throughput pipeline characteristics of VAESENC: there are 6 partial results to
    // compute, each in 2 encryption steps, so the mixing takes just ~12 cycles instead of ~60.
    // The remaining operations are expected to slide under the VAESENC latency, barely
    // contributing to the total execution time of the mixer. ENC(x)=ENC2(ENC1(x)).

    VSHUFI64X2  $((1 << 6) | (1 << 4) | (1 << 2) | (1 << 0)), Z0, Z0, Z1 // Z1 := BBBB
    VSHUFI64X2  $((2 << 6) | (2 << 4) | (2 << 2) | (2 << 0)), Z0, Z0, Z2 // Z2 := CCCC
    VSHUFI64X2  $((3 << 6) | (3 << 4) | (3 << 2) | (3 << 0)), Z0, Z0, Z3 // Z3 := DDDD
    VSHUFI64X2  $((0 << 6) | (0 << 4) | (0 << 2) | (0 << 0)), Z0, Z0, Z0 // Z0 := AAAA
    VPXORD      Z1, Z2, Z16     // Z16 := B^C
    VAESENC     Z6, Z16, Z16    // Z16 := ENC1(B^C,K_1), ENC1(B^C,K_2), ENC1(B^C,K_3), ENC1(B^C,K_4)
    VPXORD      Z1, Z3, Z17     // Z17 := B^D
    VAESENC     Z6, Z17, Z17    // Z17 := ENC1(B^D,K_1), ENC1(B^D,K_2), ENC1(B^D,K_3), ENC1(B^D,K_4)
    VPXORD      Z2, Z3, Z18     // Z18 := C^D
    VAESENC     Z6, Z18, Z18    // Z18 := ENC1(C^D,K_1), ENC1(C^D,K_2), ENC1(C^D,K_3), ENC1(C^D,K_4)
    VPXORD      Z0, Z1, Z19     // Z19 := A^B
    VAESENC     Z6, Z19, Z19    // Z19 := ENC1(A^B,K_1), ENC1(A^B,K_2), ENC1(A^B,K_3), ENC1(A^B,K_4)
    VPXORD      Z0, Z2, Z20     // Z20 := A^C
    VAESENC     Z6, Z20, Z20    // Z20 := ENC1(A^C,K_1), ENC1(A^C,K_2), ENC1(A^C,K_3), ENC1(A^C,K_4)
    VPXORD      Z0, Z3, Z21     // Z21 := A^D
    VAESENC     Z6, Z21, Z21    // Z21 := ENC1(A^D,K_1), ENC1(A^D,K_2), ENC1(A^D,K_3), ENC1(A^D,K_4)
    VAESENC     Z8, Z16, Z16    // Z16 := ENC2(Z16)
    VAESENC     Z8, Z17, Z17    // Z17 := ENC2(Z17)
    VAESENC     Z8, Z18, Z18    // Z18 := ENC2(Z18)
    VAESENC     Z8, Z19, Z19    // Z19 := ENC2(Z19)
    VAESENC     Z8, Z20, Z20    // Z20 := ENC2(Z20)
    VAESENC     Z8, Z21, Z21    // Z21 := ENC2(Z21)
    VPTERNLOGD  $0x96, Z18, Z17, Z16 // Z16 := ENC(B^C)^ENC(B^D)^ENC(C^D)
    VPTERNLOGD  $0x96, Z21, Z20, Z19 // Z19 := ENC(A^B)^ENC(A^C)^ENC(A^D)
    VPXORD      Z16, Z19, Z0         // Z0  := ENC(A^B)^ENC(A^C)^ENC(A^D)^ENC(B^C)^ENC(B^D)^ENC(C^D)
    // Z0 contains the final result
    RET


append_chunk:   // note: we are under the 5-cycle latency umbrella of the last VAESENC instruction
    VPXORD      -0x40(SI), Z0, Z0
    VMOVDQA64   Z8, Z7
    // move the ring cursor to the next pair of round keys (or wrap-around to the beginning)
    ADDQ        $0x80, DX
    MOVQ        $-0x280, AX
    CMOVQEQ     AX, DX
    JMP         loop

empty_input:
    VMOVDQU64   0x280(BX), Z0   // A hash value for an empty input
    RET


TEXT hashCoreAES<>(SB), NOSPLIT | NOFRAME, $0-0
    // BX := the base address of the expanded 128-bit key quad
    // CX := the number of bytes to process
    // SI := the base address of the input data block

    NEGQ            CX
    JZ              empty_input
    MOVQ            $-1, AX
    VMOVDQU64       0x00(BX), Z4        // The round 1 key quad

    // Observe the actual shift is (CL & 0x3f), so AX is 0xff..ff for every multiple of 64.
    // The n=0 case has been already excluded by the test above, so a full 64-byte chunk will
    // be fetched correctly with the 0xff..ff mask. That leaves only a possibly empty set of
    // the preceeding full chunks for further processing. Due to the chunks being either full
    // (the preceeding ones) or zero-padded to full (the last one), no lane masking is needed.
    //
    // Additionally, please note that on Skylake-X the VAESENC is an L4/T1 operation
    // that is executed as a single uOp at port 0. Shuffles and extracts are L3/T1
    // single uOps executed at port 5, so:
    //  a) there are no Reservation Station conflicts;
    //  b) the stream of VAESENC establishes the critical path, making extracts/shuffles
    //     easily go under the VAESENC latency;
    //  c) memory operations execute at ports 2 and 3 only, so it is better to
    //     prefetch 512-bit chunks as a single uOp and decompose it later using the otherwise
    //     idle port 5 resources than executing 4 independent 128-bit fetches in m128()
    //     operand mode. Additionally, decoupling the fetch and consumption phase allows
    //     fetching data significantly sooner than the consumption order would suggest,
    //     constituting a form of prefetching;
    //  d) 128-bit L1/T0.33 boolean operations can be executed at ports 0, 1, 5.
    //     Port 0 is constantly busy with VAESENC, so executing two boolean operations
    //     or a boolean operation and a shuffle per cycle is free relatively the execution
    //     of VAESENC.

    SHRQ            CL, AX
    ANDQ            $-64, CX
    VEXTRACTI32X4   $1, Z4, X5
    KMOVQ           AX, K1              // K1 := the active lanes of the last chunk
    SUBQ            CX, SI              // move the input read cursor 64 bytes past the beginning of the last chunk
    VEXTRACTI32X4   $2, Z4, X6
    VMOVDQU8.Z      -0x40(SI), K1, Z0   // Z0 := the zero-padded last chunk of input data
    MOVQ            $-0x200, DX         // DX := the ring cursor offset such that DX==0 when the wrap-around condition occurs
    VEXTRACTI32X4   $1, Z0, X1
    VEXTRACTI32X4   $3, Z4, X7
    VEXTRACTI32X4   $2, Z0, X2
    VMOVDQU64       0x40(BX), Z8        // The round 2 key quad
    VEXTRACTI32X4   $3, Z0, X3

    // CX    := the negative byte count - 64, multiple of 64
    // SI    := input read cursor, 64 bytes past the beginning of the last chunk
    // BX    := the AES keys
    // X3-X0 := the hash accumulator (Z0) decomposed into 128-bit chunks
    // X7-X4 := quad round 1 128-bit keys decomposed into 128-bit chunks
    // Z8    := quad round 2 128-bit keys

loop:
    VAESENC         X4, X0, X0
    VMOVDQU64       0x280(BX)(DX*1), Z4 // fetch the round N+1 key quad
    SUBQ            $0x40, SI           // move the input read cursor to the preceding chunk
    VAESENC         X5, X1, X1
    VEXTRACTI32X4   $1, Z8, X5
    VAESENC         X6, X2, X2
    VEXTRACTI32X4   $2, Z8, X6
    VAESENC         X7, X3, X3
    VEXTRACTI32X4   $3, Z8, X7
    VAESENC         X8, X0, X0
    VMOVDQU64       0x2c0(BX)(DX*1), Z8 // fetch the round N+2 key quad.
    ADDQ            $0x40, CX
    VAESENC         X5, X1, X1
    VEXTRACTI32X4   $1, Z4, X5
    VAESENC         X6, X2, X2
    VEXTRACTI32X4   $2, Z4, X6
    VAESENC         X7, X3, X3
    VEXTRACTI32X4   $3, Z4, X7
    JNZ             append_chunk

    // X0-X3 := the hash accumulator (Z0) decomposed into 128-bit chunks [A, B, C, D]
    // X4-X7 := quad round N+1 128-bit keys decomposed into 128-bit chunks [K_1, K_2, K_3, K_4]
    // Z8    := quad round N+2 128-bit keys [L_1, L_2, L_3, L_4]

    VSHUFI64X2      $((0 << 6) | (3 << 4) | (2 << 2) | (1 << 0)), Z8, Z8, Z8 // Rotate the round N+2 key to make L_2 accessible through X8
    VPXORD          X0, X1, X9      // X9  := A^B
    VPXORD          X0, X2, X10     // X10 := A^C
    VAESENC         X5, X9, X11     // X11 := ENC1(A^B,K_2)
    VPXORD          X0, X3, X0      // X0  := A^D
    VAESENC         X5, X10, X12    // X12 := ENC1(A^C,K_2)
    VPXORD          X1, X2, X1      // X1  := B^C
    VAESENC         X5, X0, X13     // X13 := ENC1(A^D,K_2)
    VPXORD          X2, X3, X3      // X3  := C^D
    VAESENC         X5, X1, X14     // X14 := ENC1(B^C,K_2)
    VPXORD          X1, X3, X2      // X2  := B^D
    VAESENC         X5, X3, X15     // X15 := ENC1(C^D,K_2)
    VAESENC         X5, X2, X5      // X5  := ENC1(B^D,K_2)
    VAESENC         X8, X11, X11    // X11 := ENC2(ENC1(A^B,K_2), L_2) := ENC(A^B, K_2, L_2)
    VAESENC         X8, X12, X12    // X12 := ENC2(ENC1(A^C,K_2), L_2) := ENC(A^C, K_2, L_2)
    VAESENC         X8, X13, X13    // X13 := ENC2(ENC1(A^D,K_2), L_2) := ENC(A^D, K_2, L_2)
    VAESENC         X8, X14, X14    // X14 := ENC2(ENC1(B^C,K_2), L_2) := ENC(B^C, K_2, L_2)
    VAESENC         X8, X15, X15    // X15 := ENC2(ENC1(C^D,K_2), L_2) := ENC(C^D, K_2, L_2)
    VAESENC         X8, X5, X5      // X5  := ENC2(ENC1(B^D,K_2), L_2) := ENC(B^D, K_2, L_2)
    VSHUFI64X2      $((0 << 6) | (3 << 4) | (2 << 2) | (1 << 0)), Z8, Z8, Z8 // Rotate the round N+2 key to make L_3 accessible through X8
    VPXORD          X11, X12, X16   // X16 := ENC(A^B, K_2, L_2)^ENC(A^C, K_2, L_2)
    VAESENC         X6, X9, X11     // X11 := ENC1(A^B,K_3)
    VAESENC         X6, X10, X12    // X12 := ENC1(A^C,K_3)
    VPXORD          X13, X14, X17   // X17 := ENC(A^D, K_2, L_2)^ENC(B^C, K_2, L_2)
    VAESENC         X6, X0, X13     // X13 := ENC1(A^D,K_3)
    VPXORD          X16, X17, X16   // X16 := ENC(A^B, K_2, L_2)^ENC(A^C, K_2, L_2)^ENC(A^D, K_2, L_2)^ENC(B^C, K_2, L_2)
    VAESENC         X6, X1, X14     // X14 := ENC1(B^C,K_3)
    VPXORD          X5, X15, X17    // X17 := ENC(B^D, K_2, L_2)^ENC(C^D, K_2, L_2)
    VAESENC         X6, X3, X15     // X15 := ENC1(C^D,K_3)
    VPXORD          X16, X17, X16   // X16 := ENC(A^B, K_2, L_2)^ENC(A^C, K_2, L_2)^ENC(A^D, K_2, L_2)^ENC(B^C, K_2, L_2)^ENC(B^D, K_2, L_2)^ENC(C^D, K_2, L_2)
    VAESENC         X6, X2, X5      // X5  := ENC1(B^D,K_3)
    VSHUFI64X2      $((1 << 6) | (1 << 4) | (0 << 2) | (1 << 0)), Z16, Z16, Z16 // Z16 := [0, 0, ENC_2, 0]
    VAESENC         X8, X11, X11    // X11 := ENC2(ENC1(A^B,K_3), L_3) := ENC(A^B, K_3, L_3)
    VAESENC         X8, X12, X12    // X12 := ENC2(ENC1(A^C,K_3), L_3) := ENC(A^C, K_3, L_3)
    VAESENC         X8, X13, X13    // X13 := ENC2(ENC1(A^D,K_3), L_3) := ENC(A^D, K_3, L_3)
    VAESENC         X8, X14, X14    // X14 := ENC2(ENC1(B^C,K_3), L_3) := ENC(B^C, K_3, L_3)
    VAESENC         X8, X15, X15    // X15 := ENC2(ENC1(C^D,K_3), L_3) := ENC(C^D, K_3, L_3)
    VAESENC         X8, X5, X5      // X5  := ENC2(ENC1(B^D,K_3), L_3) := ENC(B^D, K_3, L_3)
    VSHUFI64X2      $((0 << 6) | (3 << 4) | (2 << 2) | (1 << 0)), Z8, Z8, Z8 // Rotate the round N+2 key to make L_4 accessible through X8
    VPXORD          X11, X12, X17   // X17 := ENC(A^B, K_3, L_3)^ENC(A^C, K_3, L_3)
    VAESENC         X7, X9, X11     // X11 := ENC1(A^B,K_4)
    VAESENC         X7, X10, X12    // X12 := ENC1(A^C,K_4)
    VPXORD          X13, X14, X6    // X6  := ENC(A^D, K_3, L_3)^ENC(B^C, K_3, L_3)
    VAESENC         X7, X0, X13     // X13 := ENC1(A^D,K_4)
    VPXORD          X6, X17, X17    // X17 := ENC(A^B, K_3, L_3)^ENC(A^C, K_3, L_3)^ENC(A^D, K_3, L_3)^ENC(B^C, K_3, L_3)
    VAESENC         X7, X1, X14     // X14 := ENC1(B^C,K_4)
    VPXORD          X5, X15, X6     // X6  := ENC(B^D, K_3, L_3)^ENC(C^D, K_3, L_3)
    VAESENC         X7, X3, X15     // X15 := ENC1(C^D,K_4)
    VPXORD          X6, X17, X17    // X17 := ENC(A^B, K_3, L_3)^ENC(A^C, K_3, L_3)^ENC(A^D, K_3, L_3)^ENC(B^C, K_3, L_3)^ENC(B^D, K_3, L_3)^ENC(C^D, K_3, L_3)
    VAESENC         X7, X2, X5      // X5  := ENC1(B^D,K_4)
    VINSERTI32X4    $2, X17, Z16, Z16  // Z16 := [0, ENC_3, ENC_2, 0]
    VAESENC         X8, X11, X11    // X11 := ENC2(ENC1(A^B,K_4), L_4) := ENC(A^B, K_4, L_4)
    VAESENC         X8, X12, X12    // X12 := ENC2(ENC1(A^C,K_4), L_4) := ENC(A^C, K_4, L_4)
    VAESENC         X8, X13, X13    // X13 := ENC2(ENC1(A^D,K_4), L_4) := ENC(A^D, K_4, L_4)
    VAESENC         X8, X14, X14    // X14 := ENC2(ENC1(B^C,K_4), L_4) := ENC(B^C, K_4, L_4)
    VAESENC         X8, X15, X15    // X15 := ENC2(ENC1(C^D,K_4), L_4) := ENC(C^D, K_4, L_4)
    VAESENC         X8, X5, X5      // X5  := ENC2(ENC1(B^D,K_4), L_4) := ENC(B^D, K_4, L_4)
    VEXTRACTI32X4   $1, Z8, X8      // X8  := L_1
    VPXORD          X11, X12, X6    // X6  := ENC(A^B, K_4, L_4)^ENC(A^C, K_4, L_4)
    VAESENC         X4, X9, X11     // X11 := ENC1(A^B,K_1)
    VAESENC         X4, X10, X12    // X12 := ENC1(A^C,K_1)
    VPXORD          X13, X14, X7    // X7  := ENC(A^D, K_4, L_4)^ENC(B^C, K_4, L_4)
    VAESENC         X4, X0, X13     // X13 := ENC1(A^D,K_1)
    VPXORD          X6, X7, X6      // X6  := ENC(A^B, K_4, L_4)^ENC(A^C, K_4, L_4)^ENC(A^D, K_4, L_4)^ENC(B^C, K_4, L_4)
    VAESENC         X4, X1, X14     // X14 := ENC1(B^C,K_1)
    VPXORD          X5, X15, X7     // X7  := ENC(B^D, K_4, L_4)^ENC(C^D, K_4, L_4)
    VAESENC         X4, X2, X15     // X15 := ENC1(B^D,K_1)
    VPXORD          X6, X7, X6      // X6  := ENC(A^B, K_4, L_4)^ENC(A^C, K_4, L_4)^ENC(A^D, K_4, L_4)^ENC(B^C, K_4, L_4)^ENC(B^D, K_4, L_4)^ENC(C^D, K_4, L_4)
    VAESENC         X4, X3, X5      // X5  := ENC1(C^D,K_1)
    VINSERTI32X4    $3, X6, Z16, Z16  // Z16 := [ENC_4, ENC_3, ENC_2, 0]
    VAESENC         X8, X11, X11    // X11 := ENC2(ENC1(A^B,K_1), L_1) := ENC(A^B, K_1, L_1)
    VAESENC         X8, X12, X12    // X12 := ENC2(ENC1(A^C,K_1), L_1) := ENC(A^C, K_1, L_1)
    VAESENC         X8, X13, X13    // X13 := ENC2(ENC1(A^D,K_1), L_1) := ENC(A^D, K_1, L_1)
    VAESENC         X8, X14, X14    // X14 := ENC2(ENC1(B^C,K_1), L_1) := ENC(B^C, K_1, L_1)
    VAESENC         X8, X15, X15    // X15 := ENC2(ENC1(B^D,K_1), L_1) := ENC(B^D, K_1, L_1)
    VAESENC         X8, X5,  X0     // X0  := ENC2(ENC1(C^D,K_1), L_1) := ENC(C^D, K_1, L_1)
    VPTERNLOGD      $0x96, X11, X12, X13 // X13 := ENC(A^B, K_1, L_1)^ENC(A^C, K_1, L_1)^ENC(A^D, K_1, L_1)
    VPTERNLOGD      $0x96, X14, X15, X0  // X0  := ENC(B^C, K_1, L_1)^ENC(B^D, K_1, L_1)^ENC(C^D, K_1, L_1)
    VPTERNLOGD      $0x96, Z16, Z13, Z0  // Z0  := [ENC_4, ENC_3, ENC_2, ENC_1]
    // Z0 contains the final result
    RET

append_chunk:
    VPXORD      -0x40(SI), X0, X0
    ADDQ        $0x80, DX
    VPXORD      -0x30(SI), X1, X1
    MOVQ        $-0x280, AX
    VPXORD      -0x20(SI), X2, X2
    // move the ring cursor to the next pair of round keys (or wrap-around to the beginning)
    CMOVQEQ     AX, DX
    VPXORD      -0x10(SI), X3, X3
    JMP         loop

empty_input:
    VMOVDQU64   0x280(BX), Z0   // A hash value for an empty input
    RET


// Trampolines

// func aesHash64(quad *ExpandedKey128Quad, p *byte, n int) uint64
TEXT ·aesHash64(SB), NOSPLIT | NOFRAME, $0-24
    NO_LOCAL_POINTERS
    MOVQ        n+16(FP), CX    // CX := the input length in bytes
    MOVQ        quad+0(FP), BX  // BX := the base address of the expanded 128-bit key quad
    MOVQ        p+8(FP), SI     // SI := the base address of the input data block
    CALL        *hashCoreDispatcher<>+0(SB)
    VPEXTRQ     $0, X0, ret+24(FP)
    RET


// func aesHashWide(quad *ExpandedKey128Quad, p *byte, n int) WideHash
TEXT ·aesHashWide(SB), NOSPLIT | NOFRAME, $0-24
    NO_LOCAL_POINTERS
    MOVQ        n+16(FP), CX    // CX := the input length in bytes
    MOVQ        quad+0(FP), BX  // BX := the base address of the expanded 128-bit key quad
    MOVQ        p+8(FP), SI     // SI := the base address of the input data block
    CALL        *hashCoreDispatcher<>+0(SB)
    VMOVDQU64   Z0, ret+24(FP)
    RET


// func aesInitHashEngine()
TEXT ·aesInitHashEngine(SB), NOSPLIT | NOFRAME, $0-0
    CMPB    golang·org∕x∕sys∕cpu·X86+const_offsX86HasAVX512VAES(SB), $0
    JE      resolved_vaes
    LEAQ    hashCoreVAES<>(SB), AX
    MOVQ    AX, hashCoreDispatcher<>(SB)

resolved_vaes:
    RET

// Dynamic dispatch based on the features supported in hardware
DATA hashCoreDispatcher<>+0(SB)/8, $hashCoreAES<>(SB)
GLOBL hashCoreDispatcher<>(SB), NOPTR, $8
