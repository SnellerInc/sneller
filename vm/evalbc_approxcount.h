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

// This file contains the implementation of APPROX_COUNT_DISTINCT opcode

// _ = aggapproxcount(a[0], h[1], u16@imm[2]).k[3]
TEXT bcaggapproxcount(SB), NOSPLIT|NOFRAME, $0
    BC_UNPACK_RU32(0, OUT(R15))
    BC_UNPACK_SLOT_RU16_SLOT(BC_AGGSLOT_SIZE, OUT(R8), OUT(R11), OUT(BX))
    BC_LOAD_RU16_FROM_SLOT(OUT(R14), IN(BX))

    ADDQ VIRT_VALUES, R8

    // Note: The virtual hash registers are 128-bit ones, we use the higher 64 bits of each.
    ADDQ    VIRT_AGG_BUFFER, R15        // R15 -> aggregate buffer of size 1 << precision bytes
    MOVQ    $64, R13
    SUBQ    R11, R13                    // R13 = 64 - R11 - hash bits

scalar_loop:
    TESTL   $1, R14
    JZ      loop_tail
    MOVQ    (R8), DX    // DX = higher 64-bit of the 128-bit hash
    SHLXQ   R11, DX, CX // CX - hash
    LZCNTQ  CX, CX
    INCQ    CX          // CX = lzcnt(hash) + 1
    SHRXQ   R13, DX, DX // DX - bucket id

    // update HLL register
    MOVBQZX (R15)(DX*1), BX
    CMPQ    BX, CX
    CMOVQLT CX, BX      // BX = max(DX, BX)
    MOVB    BL, (R15)(DX*1)

loop_tail:
    ADDQ    $8, R8
    SHRL    $1, R14
    JNZ     scalar_loop

next:
    NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE + BC_IMM16_SIZE)


#define CURRENT_MASK        R9
#define HASHMEM_PTR         R8
#define BYTEBUCKET_PTR      R11
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

    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE + 2*BC_SLOT_SIZE + BC_IMM16_SIZE, OUT(BX))
    BC_LOAD_RU16_FROM_SLOT(OUT(R9), IN(BX))

    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE, OUT(DX)) // regL slot
    BC_UNPACK_SLOT(BC_AGGSLOT_SIZE + BC_SLOT_SIZE, OUT(HASHMEM_PTR)) // regH slot
    BC_UNPACK_RU16(BC_AGGSLOT_SIZE + 2*BC_SLOT_SIZE, OUT(BITS_PER_HLL_BUCKET))

    TESTQ   CURRENT_MASK, CURRENT_MASK
    JZ      next

    // Get the offset in hashmem
    ADDQ    VIRT_VALUES, HASHMEM_PTR

    // Get parameters for the HLL algorithm
    MOVQ    $64, BITS_PER_HLL_HASH
    SUBQ    BITS_PER_HLL_BUCKET, BITS_PER_HLL_HASH    // BITS_PER_HLL_HASH = 64 - BITS_PER_HLL_BUCKET

    // Get bucket base pointer
    LEAQ    BC_VSTACK_PTR(DX, 0), BYTEBUCKET_PTR

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
    // HLL_HASH is lower BITS_PER_HLL_HASH of the 64 higher bits of the 128-bit hash

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
    ADDQ    $8, HASHMEM_PTR    // next 64-bit hash word
    ADDQ    $4, BYTEBUCKET_PTR // next bucket
    SHRQ    $1, CURRENT_MASK
    JNZ     iter_rows

#undef CURRENT_MASK
#undef HASHMEM_PTR
#undef BYTEBUCKET_PTR
#undef BITS_PER_HLL_BUCKET
#undef BITS_PER_HLL_HASH

next:
    VMOVQ X9, R9
    NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE + BC_IMM16_SIZE)
