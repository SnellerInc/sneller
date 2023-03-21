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

// _ = aggapproxcount(a[0], h[1], u16@imm[2]).k[3]
TEXT bcaggapproxcount(SB), NOSPLIT|NOFRAME, $0
    MOVQ R12, bytecode_spillArea(VIRT_BCPTR)

    BC_UNPACK_RU32(0, OUT(R15))
    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE, OUT(R8))
    // unpacked out-of-order due to the R12 clobber:
    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE + BC_SLOT_SIZE + 2, OUT(BX))
    BC_LOAD_RU16_FROM_SLOT(OUT(R14), IN(BX)) // BX = valid lanes

    ADDQ VIRT_VALUES, R8
    BC_UNPACK_RU16(BC_AGGSLOT_SIZE + BC_SLOT_SIZE, OUT(R12))

    // Note: The virtual hash registers are 128-bit ones, we use the higher 64 bits of each.
    ADDQ    VIRT_AGG_BUFFER, R15        // R15 -> aggregate buffer of size 1 << precision bytes
    MOVQ    $64, R13
    SUBQ    R12, R13                    // R13 = 64 - R12 - hash bits

scalar_loop:
    TESTL   $1, R14
    JZ      loop_tail
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

loop_tail:
    ADDQ    $16, R8
    SHRL    $1, R14
    JNZ     scalar_loop

next:
    MOVQ bytecode_spillArea(VIRT_BCPTR), R12

    NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE + 2)

// bcaggapproxcountmerge implements buckets filled by bcaggapproxcount opcode.
//
// The merge operation is merely a max operation - please see
// aggApproxCountDistinctUpdateBuckets function from aggcountdistinct.go.
//
// _ = aggapproxcountmerge(a[0], s[1], u16@imm[2]).k[3]
TEXT bcaggapproxcountmerge(SB), NOSPLIT|NOFRAME, $0
#define BUFFER_SIZE             BX
#define AGG_BUFFER_PTR_ORIG     CX
#define AGG_BUFFER_PTR          DX
#define VAL_OFFSETS             R8
#define VAL_BUFFER_PTR          R13
#define ACTIVE_MASK             R14
#define COUNTER                 R15

    // bcAggSlot, bcReadS, bcImmU16, bcPredicate
    BC_UNPACK_RU32(0, OUT(AGG_BUFFER_PTR_ORIG))
    BC_UNPACK_SLOT_RU16_SLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(DX), OUT(R8))

    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
    BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1))

    /* BUFFER_SIZE = 1 << precision - the expected size of input buffers */
    XORQ    BUFFER_SIZE, BUFFER_SIZE
    BTSQ    DX, BUFFER_SIZE             // 1 << precision

    /* Check if all lengths equal to 1 << precision */
    VPBROADCASTQ    BUFFER_SIZE, Z29
    VPCMPQ          $VPCMP_IMM_NE, Z29, Z3, K1, K2
    KTESTQ          K2, K2
    JNZ wrong_input

    // Note: the minimum precision of APPROX_COUNT_DISTINCT is 4 (ApproxCountDistinctMinPrecision),
    //       thus we can safely process the buffers in 16-byte chunks.
    SHRQ        $4, BUFFER_SIZE

    /* Input buffers offsets (we already validated all have the correct size) */
    LEAQ        bytecode_spillArea(VIRT_BCPTR), VAL_OFFSETS
    VMOVDQU32   Z2, (VAL_OFFSETS)

    /* Aggregate buffer pointer */
    ADDQ    VIRT_AGG_BUFFER, AGG_BUFFER_PTR_ORIG

    KMOVW   K1, ACTIVE_MASK

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
    NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE + 2)

wrong_input:
    FAIL()

#undef BUFFER_SIZE
#undef AGG_BUFFER_PTR
#undef VAL_OFFSETS
#undef VAL_BUFFER_PTR
#undef ACTIVE_MASK
#undef COUNTER


#define CURRENT_MASK        R9
#define HASHMEM_PTR         R8
#define BYTEBUCKET_PTR      R12
#define BITS_PER_HLL_BUCKET R14
#define BITS_PER_HLL_HASH   R13

// bcaggslotapproxcount implements update of HLL state for
// aggregates executed in GROUP BY
//
// The main algorithm is exactly the same as in bcaggapproxcount.
//
// _ = aggslotapproxcount(a[0], l[1], h[2], u16@imm[3]).k[4]
TEXT bcaggslotapproxcount(SB), NOSPLIT|NOFRAME, $0
    VMOVQ R9, X9
    VMOVQ R12, X12

    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE + 2*BC_SLOT_SIZE + 2, OUT(BX))
    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(BX))

    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE, OUT(DX)) // regL slot
    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE + BC_SLOT_SIZE, OUT(HASHMEM_PTR)) // regH slot
    BC_UNPACK_RU16(BC_AGGSLOT_SIZE + 2*BC_SLOT_SIZE, OUT(BITS_PER_HLL_BUCKET))

    KTESTW  K1, K1
    JZ      next

    // Get the current mask
    KMOVW   K1, CURRENT_MASK

    // Get the offset in hashmem
    ADDQ    VIRT_VALUES, HASHMEM_PTR

    // Get parameters for the HLL algorithm
    MOVQ    $64, BITS_PER_HLL_HASH
    SUBQ    BITS_PER_HLL_BUCKET, BITS_PER_HLL_HASH    // BITS_PER_HLL_HASH = 64 - BITS_PER_HLL_BUCKET

    // Get bucket base pointer
    LEAQ    0(VIRT_VALUES)(DX*1), BYTEBUCKET_PTR

iter_rows:
    TESTQ   $1, CURRENT_MASK
    JZ      skip

    // update i-th radix tree bucket
#define     AGG_BUFFER_PTR    R15

    // AGG_BUFFER_PTR_ORIG = radixtree[k].values[8 + bucket[i]]
    BC_UNPACK_RU32(0, OUT(DX))
    MOVL    (BYTEBUCKET_PTR), AGG_BUFFER_PTR
    ADDQ    DX, AGG_BUFFER_PTR
    ADDQ    $const_aggregateTagSize, AGG_BUFFER_PTR
    ADDQ    radixTree64_values(VIRT_AGG_BUFFER), AGG_BUFFER_PTR

    // Calculate HLL hash and its bucket ID
    // HLL_HASH is lower BITS_PER_HLL_HASH of 64 higher bits of the 128-bit hash

#define HASH_HI64   DX
#define HLL_HASH    CX
#define HLL_BUCKET  DX  // alised with HASH_HI64
#define HLL_VAL     CX  // alised with HLL_HASH
#define HLL_OLD_VAL BX

    MOVQ    (HASHMEM_PTR), HASH_HI64
    SHLXQ   BITS_PER_HLL_BUCKET, HASH_HI64, HLL_HASH
    LZCNTQ  HLL_HASH, HLL_VAL
    INCQ    HLL_VAL           // HLL_VAL = lzcnt(HLL_HASH) + 1
    SHRXQ   BITS_PER_HLL_HASH, HASH_HI64, HLL_BUCKET

    // update HLL register
    MOVBQZX (AGG_BUFFER_PTR)(HLL_BUCKET*1), HLL_OLD_VAL
    CMPQ    HLL_OLD_VAL, HLL_VAL
    CMOVQLT HLL_VAL, HLL_OLD_VAL      // max(HLL_OLD_VAL, HLL_VAL)
    MOVB    HLL_OLD_VAL, (AGG_BUFFER_PTR)(HLL_BUCKET*1)

#undef HASH_HI64
#undef HLL_HASH
#undef HLL_BUCKET
#undef HLL_VAL
#undef HLL_OLD_VAL
#undef AGG_BUFFER_PTR

skip:
    ADDQ    $16, HASHMEM_PTR // next 128-bit hash
    ADDQ    $4, BYTEBUCKET_PTR // next bucket
    SHRQ    $1, CURRENT_MASK
    JNZ     iter_rows

#undef CURRENT_MASK
#undef HASHMEM_PTR
#undef BYTEBUCKET_PTR
#undef BITS_PER_HLL_BUCKET
#undef BITS_PER_HLL_HASH
#undef AGG_BUFFER_PTR_ORIG

next:
    VMOVQ X12, R12
    VMOVQ X9, R9
    NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE + 2)

// bcaggslotapproxcountmerge implements update of HLL state for
// aggregates executed in GROUP BY
//
// The main algorithm is exactly the same as in bcaggapproxcountmerge.
//
// _ = aggslotapproxcountmerge(a[0], l[1], s[2], u16@imm[3]).k[4]
TEXT bcaggslotapproxcountmerge(SB), NOSPLIT|NOFRAME, $0
#define CURRENT_MASK        R8
#define BUFFER_SIZE         R13
#define BYTEBUCKET_PTR      R14
#define AGG_BUFFER_PTR      R15
#define AGG_BUFFER_PTR_ORIG bytecode_spillArea+64(VIRT_BCPTR)
#define COUNTER             CX
#define VAL_OFFSETS         BX
#define VAL_BUFFER_PTR      DX

    // bcL
    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE, OUT(DX))
    // bcReadS, bcImmU16, bcPredicate
    BC_UNPACK_SLOT_RU16_SLOT(BC_AGGSLOT_SIZE + BC_SLOT_SIZE, OUT(BX), OUT(CX), OUT(R8))

    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
    BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1))

    KTESTW  K1, K1
    JZ      next

    // Get the current mask
    KMOVW   K1, CURRENT_MASK

    BC_UNPACK_RU32(0, OUT(BX))
    ADDQ $const_aggregateTagSize, BX
    ADDQ radixTree64_values(VIRT_AGG_BUFFER), BX
    MOVQ BX, AGG_BUFFER_PTR_ORIG

    // Get parameters for the HLL algorithm
    XORQ    BUFFER_SIZE, BUFFER_SIZE
    BTSQ    CX, BUFFER_SIZE         // SIZE = 1 << precision
    SHRQ    $4, BUFFER_SIZE         // SIZE in 16-byte chunks (precision is never less than 4)

    // Get bucket base pointer
    LEAQ        0(VIRT_VALUES)(DX*1), BYTEBUCKET_PTR

    /* Input buffers offsets (we already validated all have the correct size) */
    LEAQ        bytecode_spillArea(VIRT_BCPTR), VAL_OFFSETS
    VMOVDQU32   Z2, (VAL_OFFSETS)

iter_rows:
    TESTQ   $1, CURRENT_MASK
    JZ      skip

    // update i-th radix tree bucket
    MOVQ    BUFFER_SIZE, COUNTER

    // AGG_BUFFER_PTR_ORIG = radixtree[k].values[8 + bucket[i]]
    MOVL    (BYTEBUCKET_PTR), AGG_BUFFER_PTR
    MOVL    (VAL_OFFSETS), VAL_BUFFER_PTR
    ADDQ    AGG_BUFFER_PTR_ORIG, AGG_BUFFER_PTR
    ADDQ    VIRT_BASE, VAL_BUFFER_PTR

    update:
        // agg_buffer[j] := max(agg_buffer[j], val_buffer[k])
        VMOVDQU (AGG_BUFFER_PTR), X5
        VPMAXUB (VAL_BUFFER_PTR), X5, X5
        VMOVDQU X5, (AGG_BUFFER_PTR)

        // j++; k++
        ADDQ    $16, AGG_BUFFER_PTR
        ADDQ    $16, VAL_BUFFER_PTR
        SUBQ    $1, COUNTER
        JNZ     update

skip:
    ADDQ    $4, BYTEBUCKET_PTR  // next bucket
    ADDQ    $4, VAL_OFFSETS     // next value
    SHRQ    $1, CURRENT_MASK
    JNZ     iter_rows

#undef CURRENT_MASK
#undef BUFFER_SIZE
#undef BYTEBUCKET_PTR
#undef AGG_BUFFER_PTR_ORIG
#undef AGG_BUFFER_PTR
#undef COUNTER
#undef VAL_OFFSETS
#undef VAL_BUFFER_PTR

next:
    NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE + 2)
