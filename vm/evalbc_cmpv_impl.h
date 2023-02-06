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

// This file provides an implementation of 'bc[sort]cmpv' operations.
//
// It uses the following macros:
//   - BC_CMP_NAME    - name of the bc instruction
//   - BC_CMP_IS_SORT - define for a function with sorting semantics, which performs additional
//                      type comparison and expects 'ION type to Internal Type' predicate in Z7

// Polymorphic comparison instruction that works at a value level and results in [-1, 0, 1] outputs.
// It can be used to compare multiple data types in different lanes. In general the implementation
// does the following comparisons, in the respective order:
//
//   - NULL/BOOL values
//   - NUMBER values (both I64 and F64)
//   - STRING and TIMESTAMP values (comparison treat these the same as only bytes need to be compared)
//
// To perform the comparison, an ION data type is translated to an Internal type ID, where 0xFF means
// that the type is not comparable and the comparison result for such type would be always NULL:
//
//   ION | Data Type   | Internal ID
//   ----+-------------+---------------------
//   0x0 | null        | 0
//   0x1 | bool        | 1
//   0x2 | posint      | 2
//   0x3 | negint      | 2
//   0x4 | float       | 2
//   0x5 | decimal     | 0xFF (don't compare)
//   0x6 | timestamp   | 3
//   0x7 | symbol      | 4
//   0x8 | string      | 4
//   0x9 | clob        | 0xFF (don't compare)
//   0xA | blob        | 0xFF (don't compare)
//   0xB | list        | 0xFF (don't compare)
//   0xC | sexp        | 0xFF (don't compare)
//   0xD | struct      | 0xFF (don't compare)
//   0xE | annotations | 0xFF (don't compare)
//   0xF | reserved    | 0xFF (don't compare)
//
// In general, both fast-path and slow-path use the following register layout:
//
//   Z0:Z1   <- left  - unpacked ION value slice [offset|len] (not unsymbolized, later unsymbolized)
//   Z2:Z3   <- right - unpacked ION value slice [offset|len] (not unsymbolized, later unsymbolized)
//   Z4      <- left  - ION type
//   Z5      <- right - ION type
//   Z6      <- left  - ION type converted to an internal type
//   Z7      <- right - ION type converted to an internal type
//   Z8      <- results of the comparison
//   Z14:Z15 <- left  - initial 8 content bytes
//   Z16:Z17 <- right - initial 8 content bytes
//   Z30     <- left  - ION TLV byte
//   Z31     <- right - ION TLV byte
//
//   K1      <- lanes having compatible types, which were compared (output)
//   K2      <- keeps lanes that still need to be compared, used during the dispatch
//
// Implementation Notes:
//
//   - The default predicate for value comparison without sorting semantics is
//     ion_value_cmp_predicate. Sorting supports two options (NULLS FIRST and
//     NULLS LAST), which is implemented by using a different predicate
//
//   - If this is not a sorting comparison, nulls_first and nulls_last predicate
//     behave the same, because non-comparable types are excluded from output K1
//
//   - Initial 8 content bytes of both left and right values are gathered before
//     jumping to value-specialized compare implementations. The reason is to hide
//     the latency of VPGATHERDQ as much as possible and to basically have the data
//     ready when needed.
TEXT BC_CMP_NAME(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))

  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z0), IN(BX))       // Z0 <- left value header length (TLV + [Length] size in bytes)
  VMOVDQU32 BC_VSTACK_PTR(BX, 64), Z1                 // Z1 <- left value length

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z2), IN(CX))       // Z2 <- right value header length (TLV + [Length] size in bytes)
  VMOVDQU32 BC_VSTACK_PTR(CX, 64), Z3                 // Z3 <- right value length

  KMOVB K1, K3
  KSHIFTRW $8, K1, K2

  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z30), IN(BX))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z31), IN(CX))

  VPSUBD.Z Z0, Z1, K1, Z1                             // Z1 <- left value content length (length - hLen)
  VPADDD.Z BC_VSTACK_PTR(BX, 0), Z0, K1, Z0           // Z0 <- left value content offset (offset + hLen)
  VPXORD X14, X14, X14

  VPGATHERDQ 0(SI)(Y0*1), K3, Z14                     // Z14 <- left value 8 content bytes (low)
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  VPXORD X16, X16, X16
  KMOVB K1, K3
  KMOVB K2, K4
  VPSUBD.Z Z2, Z3, K1, Z3                             // Z3 <- right value content length (length - hLen)
  VPADDD.Z BC_VSTACK_PTR(CX, 0), Z2, K1, Z2           // Z2 <- right value content offset (offset + hLen)
  VEXTRACTI32X8 $1, Z0, Y10
  VEXTRACTI32X8 $1, Z2, Y9

  VPGATHERDQ 0(SI)(Y2*1), K3, Z16                     // Z16 <- right value 8 content bytes (low)
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  VPTERNLOGD $0xFF, Z29, Z29, Z29                     // Z29 <- dword(0xFFFFFFFF)
  VPXORD X17, X17, X17
#ifndef BC_CMP_IS_SORT
  // If BC_CMP_IS_SORT is defined it would be a tail call where Z7 would already be set
  VBROADCASTI32X4 CONST_GET_PTR(ion_value_cmp_predicate_nulls_first, 0), Z7
#endif
  VPSRLD $4, Z30, Z4                                  // Z4 <- left ION type
  VPSRLD $4, Z31, Z5                                  // Z5 <- right ION type
  VPABSD Z29, Z28                                     // Z28 <- dword(1)
  VPXORD X15, X15, X15
  VPSHUFB Z4, Z7, Z6                                  // Z6 <- left ION type converted to an internal type
  VPSHUFB Z5, Z7, Z7                                  // Z7 <- right ION type converted to an internal type

  VPGATHERDQ 0(SI)(Y10*1), K2, Z15                    // Z15 <- left value 8 content bytes (high)
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#ifdef BC_CMP_IS_SORT
  VPORD Z7, Z6, Z9                                    // Z9 <- left and right internal types combined (for K1 calculation)
  VPSUBD.Z Z7, Z6, K1, Z8                             // Z8 <- comparison results, initially only for non-comparable values
  VPCMPEQD Z7, Z6, K1, K2                             // K2 <- comparison predicate (raw, still having types we cannot compare)
  VPCMPD $VPCMP_IMM_LE, Z9, Z6, K2, K1                // K1 <- comparison predicate output (all comparable lanes)
  VPMINSD.Z Z28, Z8, K1, Z8                           // Z8 <- constrained type comparison to [?, 1]
  VPMAXSD.Z Z29, Z8, K1, Z8                           // Z8 <- constrained type comparison to [-1, 1]
#else
  VPXORD X8, X8, X8                                   // Z8 <- comparison results, initially all zeros
  VPCMPEQD Z7, Z6, K1, K2                             // K2 <- comparison predicate (raw, still having types we cannot compare)
  VPCMPD.BCST $VPCMP_IMM_LE, CONSTD_15(), Z6, K2, K1  // K1 <- comparison predicate output (all compatible and comparable lanes)
#endif

  // we compare with original ION value type, because we use a different VPSHUFB predicate
  // to support NULLS FIRST and NULLS LAST sorting compare. When NULL is LAST its internal
  // type would be higher than all comparable types, which would not match InternalType <= 1

  VPCMPD $VPCMP_IMM_LE, Z28, Z4, K1, K5               // K5 <- null/bool comparison predicate
  VPCMPEQD.BCST CONSTD_2(), Z6, K1, K3                // K3 <- number comparison predicate

  KTESTW K3, K3
  VPSUBD Z31, Z30, K5, Z8                             // Z8 <- merged comparison results from NULL/BOOL comparison
  KANDNW K1, K5, K2                                   // K2 <- comparison predicate, without nulls/bools

  VPGATHERDQ 0(SI)(Y9*1), K4, Z17                     // Z17 <- right value 8 content bytes (high)
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  JZ compare_string_or_timestamp                      // skip number comparison if there are no numbers

  // Number Comparison - I64/F64
  // ---------------------------

dispatch_compare_number:
  VPBROADCASTD CONSTD_8(), Z22

  // make K2 contain only lanes without number comparison, will be used later to decide whether
  // we are done or whether there are more lanes (of different value types) to be compared
  KANDNW K2, K3, K2

  // let's test K2 here, so the branch predictor sees the flag early
  KTESTW K2, K2

  // byteswap each value and shift right in case of signed/unsigned int
  VPSUBD Z1, Z22, Z12
  VPSUBD Z3, Z22, Z18
  VPSLLD $3, Z12, Z12
  VPSLLD $3, Z18, Z18
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z22

  VEXTRACTI32X8 $1, Z12, Y13
  VPMOVZXDQ Y12, Z12
  VEXTRACTI32X8 $1, Z18, Y19
  VPMOVZXDQ Y18, Z18
  VPMOVZXDQ Y13, Z13
  VPMOVZXDQ Y19, Z19

  VPSHUFB Z22, Z14, Z10
  VPSHUFB Z22, Z15, Z11
  VPSRLVQ Z12, Z10, Z10
  VPSHUFB Z22, Z16, Z12
  VPSRLVQ Z13, Z11, Z11
  VPSHUFB Z22, Z17, Z13
  VPSRLVQ Z18, Z12, Z12
  VPSRLVQ Z19, Z13, Z13

  VPBROADCASTD CONSTD_3(), Z25
  VPBROADCASTD CONSTD_2(), Z24

  // apply nagation to negative integers, which are stored as positive integers in ION
  VPCMPEQD Z25, Z4, K3, K4
  VPCMPEQD Z25, Z5, K3, K5

  VPXORQ X25, X25, X25
  KSHIFTRW $8, K4, K6
  VPSUBQ Z10, Z25, K4, Z10
  VPSUBQ Z11, Z25, K6, Z11

  KSHIFTRW $8, K5, K6
  VPSUBQ Z12, Z25, K5, Z12
  VPSUBQ Z13, Z25, K6, Z13

  // also, make the ION type int (instead of negative int) so we can actually compare the values
  VMOVDQA32 Z24, K4, Z4
  VMOVDQA32 Z24, K5, Z5

  // Now we have either a double precision floating point or int64 (per lane) in Z10|Z11 (left) and Z12|Z13 (right).
  // What we want is to compare floats with floats and integers with integers. Our canonical format is designed in
  // a way that we only use floating point in case that integer is not representable. This means that if a value is
  // floating point, but without a fraction, it's beyond a 64-bit integer range. This leads to a conclusion that if
  // there is an integer vs floating point, we convert the integer to floating point and compare floats.

  VPCMPEQD Z24, Z4, K3, K4                            // K4 <- integer values on the left side
  VPCMPEQD Z24, Z5, K3, K5                            // K5 <- integer values on the right side
  KANDW K4, K5, K6                                    // K6 <- integer values on both sides
  KANDNW K4, K6, K4                                   // K4 <- integer values on the left side to convert to floats
  KANDNW K5, K6, K5                                   // K5 <- integer values on the right side to convert to floats

  // Convert mixed integer/floating point values on both lanes to floating point
  VCVTQQ2PD Z10, K4, Z10
  KSHIFTRW $8, K4, K4
  VCVTQQ2PD Z12, K5, Z12
  KSHIFTRW $8, K5, K5
  VCVTQQ2PD Z11, K4, Z11
  VCVTQQ2PD Z13, K5, Z13

  KANDNW K3, K6, K5                                   // K5 <- floating point values on both sides
  KSHIFTRW $8, K3, K4                                 // K4 <- number predicate (high)
  KSHIFTRW $8, K5, K6                                 // K6 <- floating point values on both sides (high)

  VPANDQ.Z Z12, Z10, K5, Z18
  VPANDQ.Z Z13, Z11, K6, Z19
  VPMOVQ2M Z18, K5                                    // K5 <- floating point negative values (low)
  VPMOVQ2M Z19, K6                                    // K6 <- floating point negative values (high)

  VPXORD X18, X18, X18
  KUNPCKBW K5, K6, K5                                 // K5 <- floating point negative values (all)

  VPCMPQ $VPCMP_IMM_LT, Z12, Z10, K3, K0              // K0 <- less than (low)
  VPCMPQ $VPCMP_IMM_LT, Z13, Z11, K4, K6              // K6 <- less than (high)
  KUNPCKBW K0, K6, K6                                 // K6 <- less than (all)
  VMOVDQA32 Z29, K6, Z8                               // Z8 <- merge less than results (-1)

  VPCMPQ $VPCMP_IMM_GT, Z12, Z10, K3, K0              // K0 <- greater than (low)
  VPCMPQ $VPCMP_IMM_GT, Z13, Z11, K4, K6              // K6 <- greater than (high)
  KUNPCKBW K0, K6, K6                                 // K6 <- greater than (all)
  VMOVDQA32 Z28, K6, Z8                               // Z8 <- merge greater than results (1)

  VPSUBD Z8, Z18, K5, Z8                              // Z8 <- results with corrected floating point comparison
  JZ next

  // String | Timestamp Comparison - Unsymbolize
  // -------------------------------------------

compare_string_or_timestamp:
  // To continue comparing string and timestamp values, we have to first "unsymbolize".
  VPBROADCASTD CONSTD_7(), Z20
  VPBROADCASTD CONSTD_4(), Z21

  VPCMPEQD Z20, Z4, K2, K3                            // K3 <- left lanes that are symbols
  VPCMPEQD Z20, Z5, K2, K4                            // K4 <- right lanes that are symbols

  KORTESTW K3, K4
  JZ skip_unsymbolize                                 // don't unsymbolize if there are no symbols

  VPMOVQD Z14, Y10
  VPMOVQD Z15, Y11
  VPMOVQD Z16, Y12
  VPMOVQD Z17, Y13
  VINSERTI32X8 $1, Y11, Z10, Z10                      // Z10 <- left 4 bytes
  VINSERTI32X8 $1, Y13, Z12, Z11                      // Z11 <- right 4 bytes
  VBROADCASTI32X4 bswap32<>+0(SB), Z12

  MOVQ bytecode_symtab+0(VIRT_BCPTR), R8              // R8 <- Symbol table
  VPBROADCASTD bytecode_symtab+8(VIRT_BCPTR), Z13     // Z13 <- Number of symbols in symbol table

  VPSUBD Z1, Z21, Z18
  VPSUBD Z3, Z21, Z19
  VPSHUFB Z12, Z10, Z10
  VPSHUFB Z12, Z11, Z11
  VPSLLD $3, Z18, Z18
  VPSLLD $3, Z19, Z19
  VPSRLVD Z18, Z10, Z18                               // Z18 <- left SymbolIDs
  VPSRLVD Z19, Z11, Z19                               // Z19 <- right SymbolIDs

  // only unsymbolize lanes where id < len(symtab)
  VPCMPUD $VPCMP_IMM_LT, Z13, Z18, K3, K3             // K3 <- left symbols that are in symtab
  KMOVB K3, K5
  VPGATHERDQ 0(R8)(Y18*8), K5, Z10                    // Z10 <- left vmrefs of symbols (low)

  KSHIFTRW $8, K3, K6
  VEXTRACTI32X8 $1, Z18, Y18
  VPGATHERDQ 0(R8)(Y18*8), K6, Z11                    // Z11 <- left vmrefs of symbols (high)

  VPCMPUD $VPCMP_IMM_LT, Z13, Z19, K4, K4             // K4 <- right symbols that are in symtab
  KMOVB K4, K5
  KSHIFTRW $8, K4, K6
  VPGATHERDQ 0(R8)(Y19*8), K5, Z12                    // Z12 <- right vmrefs of symbols (low)

  VEXTRACTI32X8 $1, Z19, Y19
  BC_MERGE_VMREFS_TO_VALUE(IN_OUT(Z0), IN_OUT(Z1), IN(Z10), IN(Z11), IN(K3), Z18, Y18, Y20)
  VPGATHERDQ 0(R8)(Y19*8), K6, Z13                    // Z13 <- right vmrefs of symbols (high)

  VPBROADCASTD CONSTD_14(), Z21
  BC_CALC_VALUE_HLEN(OUT(Z9), IN(Z1), IN(K3), IN(Z28), IN(Z21), Z19, K5)

  VPADDD Z9, Z0, K3, Z0
  VPSUBD Z9, Z1, K3, Z1
  VEXTRACTI32X8 $1, Z0, Y9
  KMOVB K3, K5
  KSHIFTRW $8, K3, K6
  VPGATHERDQ 0(SI)(Y0*1), K5, Z14

  BC_MERGE_VMREFS_TO_VALUE(IN_OUT(Z2), IN_OUT(Z3), IN(Z12), IN(Z13), IN(K4), Z18, Y18, Y19)
  VPGATHERDQ 0(SI)(Y9*1), K6, Z15

  BC_CALC_VALUE_HLEN(OUT(Z9), IN(Z3), IN(K4), IN(Z28), IN(Z21), Z19, K5)
  VPADDD Z9, Z2, K4, Z2
  VPSUBD Z9, Z3, K4, Z3

  KMOVB K4, K5
  KSHIFTRW $8, K4, K6
  VPGATHERDQ 0(SI)(Y2*1), K5, Z16

  VEXTRACTI32X8 $1, Z2, Y9
  VPGATHERDQ 0(SI)(Y9*1), K6, Z17

skip_unsymbolize:

  // String | Timestamp Comparison - Prepare
  // ---------------------------------------

  LEAQ bytecode_spillArea+0(VIRT_BCPTR), R8           // R8 <- pointer to spill area
  VPSUBD Z3, Z1, K2, Z8                               // Z8 <- merged length comparison
  VMOVDQU32.Z Z0, K2, Z18                             // Z18 <- left content iterator (offset) (increasing)
  VMOVDQU32.Z Z2, K2, Z19                             // Z19 <- right content iterator (offset) (increasing)
  VPMINUD Z3, Z1, Z20                                 // Z20 <- length iterator (min(left, right) length) (decreasing)

  // Store slices into spillArea so we can read them in scalar loops
  VMOVDQU32 Z8, 192(R8)                               // [] <- Comparison results, initially `left - right` length

  // String | Timestamp Comparison - Vector
  // --------------------------------------

  // We keep K2 alive - it's not really necessary in the current implementation, but it's
  // likely we would want to extend this to support lists and structs in the future.
  // Additionally - to prevent bugs triggered by empty strings that have arbitrary offsets,
  // but zero lengths, we filter them here. Any string that has zero length would be already
  // compared before entering vector or scalar loop.

  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z22
  VPMINSD Z28, Z8, K2, Z8                             // Z8 <- constrained length comparison to [?, 1]
  VPTESTMD Z20, Z20, K2, K3                           // K3 <- comparison predicate (values having non-zero length)
  VPBROADCASTD CONSTD_8(), Z23
  VPMAXSD Z29, Z8, K2, Z8                             // Z8 <- constrained length comparison to [-1, 1]

  // Avoid gathering bytes that we have already gathered. The idea is to use the 8
  // bytes of each lane that we have already gathered, and to do some computation
  // that we do meanwhile gathering inside the loop here (as otherwise we would
  // have to avoid doing any computations meanwhile gathering).

  VMOVDQA32 Z14, Z10
  VMOVDQA32 Z15, Z11

  // 1.
  VPMINUD Z23, Z20, Z21                               // Z21 <- number of bytes to compare (max 8).
  VPADDD Z21, Z18, K3, Z18                            // Z18 <- adjusted left slice offset
  VPSUBD Z21, Z23, Z24                                // Z24 <- number of bytes to discard (8 - Z21).

  // 2.
  VPSUBD Z21, Z20, K3, Z20                            // Z20 <- adjusted length to compare
  VPSLLD $3, Z24, Z24                                 // Z24 <- number of bits to discard

  // 3.
  VPADDD Z21, Z19, K3, Z19                            // Z19 <- adjusted right slice offset
  VEXTRACTI32X8 $1, Z24, Y25
  VPMOVZXDQ Y24, Z24
  VPMOVZXDQ Y25, Z25

  VMOVDQA32 Z16, Z12
  VMOVDQA32 Z17, Z13

  JMP compare_string_vector_after_gather

  // The idea is to keep using vector loop unless the number of lanes gets too low.
  // The initial eight bytes are always compared in this vector loop to prevent
  // going scalar in case that those eight bytes determine the results of all lanes.

compare_string_vector:
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VEXTRACTI32X8 $1, Z18, Y9

  VPXORQ X10, X10, X10
  VPGATHERDQ 0(SI)(Y18*1), K4, Z10

  // 1.
  VPMINUD Z23, Z20, Z21                               // Z21 <- number of bytes to compare (max 8).
  VPADDD Z21, Z18, K3, Z18                            // Z18 <- adjusted left slice offset
  VPSUBD Z21, Z23, Z24                                // Z24 <- number of bytes to discard (8 - Z21).

  VPXORQ X11, X11, X11
  VPGATHERDQ 0(SI)(Y9*1), K5, Z11

  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VEXTRACTI32X8 $1, Z19, Y9

  // 2.
  VPSUBD Z21, Z20, K3, Z20                            // Z20 <- adjusted length to compare
  VPSLLD $3, Z24, Z24                                 // Z24 <- number of bits to discard

  VPXORQ X12, X12, X12
  VPGATHERDQ 0(SI)(Y19*1), K4, Z12

  VPADDD Z21, Z19, K3, Z19                            // Z19 <- adjusted right slice offset
  VEXTRACTI32X8 $1, Z24, Y25
  VPMOVZXDQ Y24, Z24
  VPMOVZXDQ Y25, Z25

  VPXORQ X13, X13, X13
  VPGATHERDQ 0(SI)(Y9*1), K5, Z13

compare_string_vector_after_gather:

  VPSLLVQ Z24, Z10, Z10                               // Z10 <- left bytes to compare (low)
  VPSLLVQ Z25, Z11, Z11                               // Z11 <- left bytes to compare (high)
  VPSHUFB Z22, Z10, Z10                               // Z10 <- left byteswapped quadword to compare (low)
  VPSHUFB Z22, Z11, Z11                               // Z11 <- left byteswapped quadword to compare (high)

  VPSLLVQ Z24, Z12, Z12                               // Z12 <- right bytes to compare (low)
  VPSLLVQ Z25, Z13, Z13                               // Z13 <- right bytes to compare (high)
  VPSHUFB Z22, Z12, Z12                               // Z12 <- right byteswapped quadword to compare (low)
  VPSHUFB Z22, Z13, Z13                               // Z13 <- right byteswapped quadword to compare (high)

  KSHIFTRW $8, K3, K4
  VPCMPQ $VPCMP_IMM_NE, Z12, Z10, K3, K5              // K5 <- lanes having values that aren't equal (low)
  VPCMPQ $VPCMP_IMM_NE, Z13, Z11, K4, K4              // K4 <- lanes having values that aren't equal (high)
  KUNPCKBW K5, K4, K5                                 // K5 <- lanes having values that aren't equal (all lanes)
  KANDNW K3, K5, K3                                   // K3 <- lanes to continue being compared

  VPCMPUQ $VPCMP_IMM_LT, Z12, Z10, K5, K0             // K0 <- lanes where the comparison is less than (low)
  VPCMPUQ $VPCMP_IMM_LT, Z13, Z11, K4, K6             // K6 <- lanes where the comparison is less than (high)
  VPCMPUD $VPCMP_IMM_GE, Z28, Z20, K3, K3             // K3 <- lanes to continue being compared (where length is non-zero)
  KUNPCKBW K0, K6, K6                                 // K6 <- lanes where the comparison is less than (all lanes)
  VMOVDQA32 Z29, K6, Z8                               // Z8 <- merge less than results (-1)
  KMOVW K3, BX

  VPCMPUQ $VPCMP_IMM_GT, Z12, Z10, K5, K5             // K5 <- lanes where the comparison is greater than (low)
  VPCMPUQ $VPCMP_IMM_GT, Z13, Z11, K4, K4             // K6 <- lanes where the comparison is greater than (high)
  POPCNTL BX, DX                                      // DX <- number of remaining lanes to process
  KUNPCKBW K5, K4, K5                                 // K5 <- lanes where the comparison is greater than (all lanes)
  VMOVDQA32 Z28, K5, Z8                               // Z8 <- merge greater than results (1)

  TESTL BX, BX
  JZ next

  // Go to scalar loop if the number of lanes to compare gets low
  CMPL DX, $BC_SCALAR_PROCESSING_LANE_COUNT
  JHI compare_string_vector

  VMOVDQU32 Z18, 0(R8)                                // left content iterator
  VMOVDQU32 Z19, 64(R8)                               // right content iterator
  VMOVDQU32 Z20, 128(R8)                              // min(left, right) length
  VMOVDQU32 Z8, 192(R8)                               // comparison results

  MOVQ $-1, R13
  JMP compare_string_scalar_lane

  // String | Timestamp Comparison (Scalar)
  // --------------------------------------

compare_string_scalar_diff:
  KMOVQ K4, CX
  TZCNTQ CX, CX
  MOVBLZX 0(R14)(CX*1), R14
  MOVBLZX 0(R15)(CX*1), R15
  SUBL R15, R14
  MOVL R14, 192(R8)(DX * 4)

  TESTL BX, BX
  JE compare_string_scalar_done

compare_string_scalar_lane:
  TZCNTL BX, DX                                       // DX - Index of the lane to process
  BLSRL BX, BX                                        // clear the index of the iterator

  MOVL 128(R8)(DX * 4), CX                            // min(left, right) length
  MOVL 0(R8)(DX * 4), R14                             // left slice offset
  ADDQ SI, R14                                        // make left offset an absolute VM address
  MOVL 64(R8)(DX * 4), R15                            // right slice offset
  ADDQ SI, R15                                        // make right offset an absolute VM address

  SUBL $64, CX
  JCS compare_string_tail

compare_string_scalar_iter:                           // main compare loop that processes 64 bytes at once
  VMOVDQU8 0(R14), Z10
  VMOVDQU8 0(R15), Z11
  VPCMPB $VPCMP_IMM_NE, Z11, Z10, K4
  KTESTQ K4, K4
  JNE compare_string_scalar_diff

  ADDQ $64, R14
  ADDQ $64, R15
  SUBL $64, CX
  JA compare_string_scalar_iter

compare_string_tail:                                  // tail loop that processes up to 64 bytes at once
  SHLXQ CX, R13, CX
  NOTQ CX
  KMOVQ CX, K4                                        // K4 <- LSB mask of bits to process (valid characters)

  VMOVDQU8.Z 0(R14), K4, Z10
  VMOVDQU8.Z 0(R15), K4, Z11

  VPCMPB $VPCMP_IMM_NE, Z11, Z10, K4
  KTESTQ K4, K4
  JNE compare_string_scalar_diff

  // Comparable slices have the same content, which means that `leftLen-rightLen` is the result
  // This result was already precalculated before entering the scalar loop, so we don't have to
  // calculate and store it again.

  TESTL BX, BX
  JNE compare_string_scalar_lane

compare_string_scalar_done:
  VPMINSD 192(R8), Z28, Z8
  VPMAXSD Z29, Z8, Z8

next:
  VEXTRACTI32X8 $1, Z8, Y9
  VPMOVSXDQ Y8, Z8                                    // Z8 <- merged comparison results of all types (low)
  VPMOVSXDQ Y9, Z9                                    // Z9 <- merged comparison results of all types (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z8), IN(Z9), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)
