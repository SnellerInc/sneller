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

// This file contains the implementation of APPROX_COUNT_DISTINCT opcode

// See evalaggregate_amd64.s
#define AggregateDataBuffer R10

TEXT bcaggapproxcount(SB), NOSPLIT|NOFRAME, $0
    MOVQ    R12, bytecode_spillArea(VIRT_BCPTR)

    // Note: The virtual hash registers are 128-bit ones, we use the higher 64 bits of each.
    MOVQ    0(VIRT_PCREG), R15
    ADDQ    AggregateDataBuffer, R15    // R15 -> aggregate buffer of size 1 << precision bytes

    MOVWQZX 8(VIRT_PCREG), R8
    ADDQ    bytecode_hashmem(VIRT_BCPTR), R8

    MOVBQZX 10(VIRT_PCREG), R12         // R12  = bits per bucket
    MOVQ    $64, R13
    SUBQ    R12, R13                    // R13 = 64 - R12 - hash bits

    MOVQ    $16, R14                    // the number of hashes
scalar_loop:
    MOVQ    (R8), DX    // DX = higher 64-bit of the 128-bit hash
    SHLXQ   R12, DX, CX // CX - hash
    LZCNTQ  CX, CX
    INCQ    CX          // CX = lzcnt(hash) + 1
    SHRXQ   R13, DX, DX // DX - bucket id

    // update HLL register
    MOVBQZX (R15)(DX*1), BX
    CMPQ    BX, CX
    CMOVQLT CX, BX      // BX = max(DX, BX)
    MOVB    BL, (R15)(DX*1)

    ADDQ    $16, R8
    DECQ    R14
    JNZ     scalar_loop

next:
    MOVQ    bytecode_spillArea(VIRT_BCPTR), R12

    NEXT_ADVANCE(12)

// bcaggapproxcountmerge implements buckets filled by bcaggapproxcount opcode.
//
// The merge operation is merely a max operation - please see
// aggApproxCountDistinctUpdateBuckets function from aggcountdistinct.go.
TEXT bcaggapproxcountmerge(SB), NOSPLIT|NOFRAME, $0
#define BUFFER_SIZE             BX
#define AGG_BUFFER_PTR_ORIG     CX
#define AGG_BUFFER_PTR          DX
#define VAL_OFFSETS             R8
#define VAL_BUFFER_PTR          R13
#define ACTIVE_MASK             R14
#define COUNTER                 R15

    /* BUFFER_SIZE = 1 << precision - the expected size of input buffers */
    MOVWQZX 8(VIRT_PCREG), CX           // CX -> precision
    XORQ    BUFFER_SIZE, BUFFER_SIZE
    BTSQ    CX, BUFFER_SIZE             // 1 << precision

    /* Check if all lengths equal to 1 << precision */
    VPBROADCASTQ    BUFFER_SIZE, Z29
    VPCMPQ          $VPCMP_IMM_NE, Z29, Z31, K1, K2
    KTESTQ          K2, K2
    JNZ wrong_input

    // Note: the minimum precision of APPROX_COUNT_DISTINCT is 4 (ApproxCountDistinctMinPrecision),
    //       thus we can safely process the buffers in 16-byte chunks.
    SHRQ        $4, BUFFER_SIZE

    /* Input buffers offsets (we already validated all have the correct size) */
    LEAQ        bytecode_spillArea(VIRT_BCPTR), VAL_OFFSETS
    VMOVDQU32   Z30, (VAL_OFFSETS)

    /* Aggregate buffer pointer */
    MOVQ    0(VIRT_PCREG), AGG_BUFFER_PTR_ORIG
    ADDQ    AggregateDataBuffer, AGG_BUFFER_PTR_ORIG

    KMOVW       K1, ACTIVE_MASK

main_loop:
    TESTQ   $1, ACTIVE_MASK
    JZ      skip

    // update n-th buffer
    MOVL    (VAL_OFFSETS), VAL_BUFFER_PTR
    ADDQ    VIRT_BASE, VAL_BUFFER_PTR
    MOVQ    BUFFER_SIZE, COUNTER
    MOVQ    AGG_BUFFER_PTR_ORIG, AGG_BUFFER_PTR
    update:
        // agg_buffer[j] := max(agg_buffer[j], val_buffer[k])
        VMOVDQU (AGG_BUFFER_PTR), X5
        VMOVDQU (VAL_BUFFER_PTR), X6
        VPMAXUB X6, X5, X5
        VMOVDQU X5, (AGG_BUFFER_PTR)

        // j++; k++
        ADDQ    $16, AGG_BUFFER_PTR
        ADDQ    $16, VAL_BUFFER_PTR
        DECQ    COUNTER
        JNZ     update

skip:
    ADDQ    $4, VAL_OFFSETS
    SHRQ    $1, ACTIVE_MASK
    JNZ     main_loop

end:
    NEXT_ADVANCE(10)

wrong_input:
    FAIL()

#undef BUFFER_SIZE
#undef AGG_BUFFER_PTR
#undef VAL_OFFSETS
#undef VAL_BUFFER_PTR
#undef ACTIVE_MASK
#undef COUNTER

#undef AggregateDataBuffer


#include "evalbc_ionheader.h"

// bcunboxblob adjusts Z30 and Z31 to match the contents of blob.
//
// If the input contains any non-blob Ion value, it fails the
// execution of the current program.
TEXT bcstrictunboxblob(SB), NOSPLIT|NOFRAME, $0

    KTESTW K1, K1
    JZ skip

#define HEAD_BYTES      Z10
#define T_FIELD         Z11
#define L_FIELD         Z12
#define HEADER_LENGTH   Z13
#define OBJECT_SIZE     Z14
#define CONST_0x01      Z15
#define CONST_0x0e      Z16
#define CONST_0x0f      Z17
#define CONST_0x7f      Z18
#define CONST_0x80      Z19

#define TMP             Z20
#define TMP2            Z21
#define TMP3            Z22

    VPBROADCASTD        CONSTD_1(),     CONST_0x01
    VPBROADCASTD        CONSTD_0x0E(),  CONST_0x0e
    VPBROADCASTD        CONSTD_0x0F(),  CONST_0x0f
    VPBROADCASTD        CONSTD_0x7F(),  CONST_0x7f
    VPBROADCASTD        CONSTD_0x80(),  CONST_0x80

    LOAD_OBJECT_HEADER(K1)
    VPCMPD.BCST $VPCMP_IMM_NE, CONSTD_0x0A(), T_FIELD, K1, K2 /* blob */
    KTESTW K2, K2
    JNZ trap // wrong input

    CALCULATE_OBJECT_SIZE(K1, no_uvint, uvint_done)

    // update the slices
    VPADDD      HEADER_LENGTH, Z30, Z30
    VMOVDQA32   OBJECT_SIZE, Z31

#undef HEAD_BYTES
#undef T_FIELD
#undef L_FIELD
#undef HEADER_LENGTH
#undef OBJECT_SIZE
#undef CONST_0x01
#undef CONST_0x0e
#undef CONST_0x0f
#undef CONST_0x7f
#undef CONST_0x80

#undef TMP
#undef TMP2
#undef TMP3

skip:
    NEXT()

trap:
    FAIL()
