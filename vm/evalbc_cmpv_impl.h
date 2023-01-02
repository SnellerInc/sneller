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

// TODO (Petr): There shouldn't be unsymbolize / unboxing, but this needs other planned changes first

// This file provides an implementation of 'bc[sort]cmpv' operations.
//
// It uses the following macros:
//   - BC_CMP_NAME    - name of the bc instruction
//   - BC_CMP_IS_SORT - define for a function with sorting semantics, which performs additional
//                      type comparison and expects 'ION type to Internal Type' predicate in Z11

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
//   Z30|Z31 <- left  - boxed ION value [start|len] (not unsymbolized, later unsymbolized)
//   Z4|Z5   <- right - boxed ION value [start|len] (not unsymbolized, later unsymbolized)
//   Z6      <- left  - first 4 bytes of each ION value
//   Z7      <- right - first 4 bytes of each ION value
//   Z8      <- left  - ION type
//   Z9      <- right - ION type
//   Z10     <- left  - ION type converted to an internal type
//   Z11     <- right - ION type converted to an internal type
//   Z12     <- left  - L field or length of unboxed slice in case of slice
//   Z13     <- right - L field or length of unboxed slice in case of slice
//   Z14     <- left  - unboxed slice start
//   Z15     <- right - unboxed slice start
//
//   K1      <- lanes having compatible types, which were compared (output)
//   K2      <- keeps lanes that still need to be compared, used during the dispatch

TEXT BC_CMP_NAME(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_VALUE_FROM_SLOT(OUT(Z30), OUT(Z31), IN(BX))
  KMOVW K1, K2
  VPXORQ X6, X6, X6
  VPGATHERDD 0(SI)(Z30*1), K2, Z6                     // Z6 <- first 4 bytes of the left value

  BC_LOAD_VALUE_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))
  KMOVW K1, K3
  VPXORQ X7, X7, X7
  VPGATHERDD 0(SI)(Z4*1), K3, Z7                      // Z7 <- first 4 bytes of the right value

  // NOTE 1: The default predicate for value comparison without sorting semantics is
  //         ion_value_cmp_predicate. Sorting supports two options (NULLS FIRST and
  //         NULLS LAST), which is implemented by using a different predicate
  //
  // NOTE 2: If this is not a sorting comparison, nulls_first and nulls_last predicate
  //         behave the same, because non-comparable types are excluded from output K1
#ifndef BC_CMP_IS_SORT
  VBROADCASTI32X4 CONST_GET_PTR(ion_value_cmp_predicate_nulls_first, 0), Z11
#endif

  VPBROADCASTD CONSTD_0xFF(), Z10
  VPBROADCASTD CONSTD_0x0F(), Z13
  VPBROADCASTD CONSTD_1(), Z28
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z29

  VPANDD Z10, Z6, Z8
  VPANDD Z10, Z7, Z9
  VPSRLD $4, Z8, Z8                                   // Z8 <- left ION type
  VPSRLD $4, Z9, Z9                                   // Z9 <- right ION type

  VPSHUFB Z8, Z11, Z10                                // Z10 <- left ION type converted to an internal type
  VPSHUFB Z9, Z11, Z11                                // Z11 <- right ION type converted to an internal type

#ifdef BC_CMP_IS_SORT
  VPORD Z11, Z10, Z14                                 // Z14 <- Left and right internal types combined (for K1 calculation)
  VPSUBD.Z Z11, Z10, K1, Z2                           // Z2 <- Comparison results, initially only for non-comparable values
  VPCMPEQD Z11, Z10, K1, K2                           // K2 <- comparison predicate (raw, still having types we cannot compare)
  VPCMPD $VPCMP_IMM_LE, Z14, Z10, K1, K1              // K1 <- comparison predicate output (all comparable lanes)
  VPMINSD.Z Z28, Z2, K1, Z2                           // Z2 <- constrained type comparison to [?, 1]
  VPMAXSD.Z Z29, Z2, K1, Z2                           // Z2 <- constrained type comparison to [-1, 1]
#else
  VPXORD X2, X2, X2                                   // Z2 <- Comparison results, initially all zeros
  VPCMPEQD Z11, Z10, K1, K2                           // K2 <- comparison predicate (raw, still having types we cannot compare)
  VPCMPD $VPCMP_IMM_LE, Z13, Z10, K2, K1              // K1 <- comparison predicate output (all compatible and comparable lanes)
#endif

  VPANDD Z13, Z6, Z12                                 // Z12 <- left L field
  VPANDD Z13, Z7, Z13                                 // Z13 <- right L field

#ifdef BC_CMP_IS_SORT
  VPMINSD.Z Z28, Z2, K1, Z2                           // Z2 <- constrained type comparison to [?, 1]
  VPMAXSD.Z Z29, Z2, K1, Z2                           // Z2 <- constrained type comparison to [-1, 1]
#endif

  // NOTE: We compare with original ION value type, because we use a different VPSHUFB predicate
  // to support NULLS FIRST and NULLS LAST sorting compare. When NULL is LAST its internal type
  // would be higher than all comparable types, which would not match `internal type <= 1`.
  VPCMPD $VPCMP_IMM_LE, Z28, Z8, K1, K4               // K4 <- null/bool comparison predicate

  VPCMPEQD.BCST CONSTD_2(), Z10, K1, K3               // K3 <- number comparison predicate
  VPSUBD Z13, Z12, K4, Z2                             // Z2 <- merged comparison results from NULL/BOOL comparison
  KANDNW K1, K4, K2                                   // K2 <- comparison predicate, without nulls/bools

  KTESTW K3, K3
  JZ compare_string_or_timestamp                      // Skip number comparison if there are no numbers

  // Number Comparison (Int64/Double)
  // --------------------------------

dispatch_compare_number:
  // Make K2 contain only lanes without number comparison, will be
  // used later to decide whether we are done or whether there are
  // more lanes (of different value types) to be compared.
  KANDNW K2, K3, K2
  LEAQ 1(SI), R15

  // Let's test K2 here, so the branch predictor sees the flag early.
  KTESTW K2, K2

  // Gather 8 bytes of each left value
  VEXTRACTI32X8 $1, Z30, Y22
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X14, X14, X14
  VPXORQ X15, X15, X15
  VPGATHERDQ 0(R15)(Y30*1), K4, Z14
  VPGATHERDQ 0(R15)(Y22*1), K5, Z15

  // Gather 8 bytes of each right value
  VEXTRACTI32X8 $1, Z4, Y22
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X16, X16, X16
  VPXORQ X17, X17, X17
  VPGATHERDQ 0(R15)(Y4*1), K4, Z16
  VPGATHERDQ 0(R15)(Y22*1), K5, Z17

  // Byteswap each value and shift right in case of signed/unsigned int
  VPBROADCASTD CONSTD_8(), Z23
  VPSUBD Z12, Z23, Z18
  VPSUBD Z13, Z23, Z20
  VPSLLD $3, Z18, Z18
  VPSLLD $3, Z20, Z20

  VEXTRACTI32X8 $1, Z18, Y19
  VEXTRACTI32X8 $1, Z20, Y21
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z22
  VPMOVZXDQ Y18, Z18
  VPMOVZXDQ Y19, Z19
  VPMOVZXDQ Y20, Z20
  VPMOVZXDQ Y21, Z21

  VPSHUFB Z22, Z14, Z14
  VPSHUFB Z22, Z15, Z15
  VPSHUFB Z22, Z16, Z16
  VPSHUFB Z22, Z17, Z17

  VPBROADCASTD CONSTD_2(), Z24
  VPBROADCASTD CONSTD_3(), Z25
  VPSRLVQ Z18, Z14, Z14
  VPSRLVQ Z19, Z15, Z15
  VPSRLVQ Z20, Z16, Z16
  VPSRLVQ Z21, Z17, Z17

  // Apply nagation to negative integers, which are stored as positive integers in ION
  VPCMPEQD Z25, Z8, K3, K4
  VPCMPEQD Z25, Z9, K3, K5

  VPXORQ X25, X25, X25
  KSHIFTRW $8, K4, K6
  VPSUBQ Z14, Z25, K4, Z14
  VPSUBQ Z15, Z25, K6, Z15

  KSHIFTRW $8, K5, K6
  VPSUBQ Z16, Z25, K5, Z16
  VPSUBQ Z17, Z25, K6, Z17

  // Also, make the ION type int (instead of negative int) so we can actually compare the values
  VMOVDQA32 Z24, K4, Z8
  VMOVDQA32 Z24, K5, Z9

  // Now we have either a double precision floating point or int64 (per lane) in Z14|Z15 (left) and Z16|Z17 (right).
  // What we want is to compare floats with floats and integers with integers. Our canonical format is designed in
  // a way that we only use floating point in case that integer is not representable. This means that if a value is
  // floating point, but without a fraction, it's beyond a 64-bit integer range. This leads to a conclusion that if
  // there is an integer vs floating point, we convert the integer to floating point and compare floats.

  VPCMPEQD Z24, Z8, K3, K4                            // K4 <- integer values on the left side
  VPCMPEQD Z24, Z9, K3, K5                            // K5 <- integer values on the right side
  KANDW K4, K5, K6                                    // K6 <- integer values on both sides
  KANDNW K4, K6, K4                                   // K4 <- integer values on the left side to convert to floats
  KANDNW K5, K6, K5                                   // K5 <- integer values on the right side to convert to floats

  // Convert mixed integer/floating point values on both lanes to floating point
  VCVTQQ2PD Z14, K4, Z14
  KSHIFTRW $8, K4, K4
  VCVTQQ2PD Z16, K5, Z16
  KSHIFTRW $8, K5, K5
  VCVTQQ2PD Z15, K4, Z15
  VCVTQQ2PD Z17, K5, Z17

  KANDNW K3, K6, K5                                   // K5 <- floating point values on both sides
  KSHIFTRW $8, K3, K4                                 // K4 <- number predicate (high)
  KSHIFTRW $8, K5, K6                                 // K6 <- floating point values on both sides (high)

  VPANDQ.Z Z16, Z14, K5, Z18
  VPANDQ.Z Z17, Z15, K6, Z19
  VPMOVQ2M Z18, K5                                    // K5 <- floating point negative values (low)
  VPMOVQ2M Z19, K6                                    // K6 <- floating point negative values (high)

  VPXORD X18, X18, X18
  KUNPCKBW K5, K6, K5                                 // K5 <- floating point negative values (all)

  VPCMPQ $VPCMP_IMM_LT, Z16, Z14, K3, K0              // K0 <- less than (low)
  VPCMPQ $VPCMP_IMM_LT, Z17, Z15, K4, K6              // K6 <- less than (high)
  KUNPCKBW K0, K6, K6                                 // K6 <- less than (all)
  VMOVDQA32 Z29, K6, Z2                               // Z2 <- merge less than results (-1)

  VPCMPQ $VPCMP_IMM_GT, Z16, Z14, K3, K0              // K0 <- greater than (low)
  VPCMPQ $VPCMP_IMM_GT, Z17, Z15, K4, K6              // K6 <- greater than (high)
  KUNPCKBW K0, K6, K6                                 // K6 <- greater than (all)
  VMOVDQA32 Z28, K6, Z2                               // Z2 <- merge greater than results (1)

  VPSUBD Z2, Z18, K5, Z2                              // Z2 <- results with corrected floating point comparison
  JNE compare_string_or_timestamp

  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                    // Z2 <- merged comparison results of all types (low)
  VPMOVSXDQ Y3, Z3                                    // Z3 <- merged comparison results of all types (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

  // Unsymbolize / Unbox
  // -------------------

compare_string_or_timestamp:
  // To continue comparing string and timestamp values, we have to first "unsymbolize".

  VPBROADCASTD CONSTD_7(), Z16
  VPBROADCASTD CONSTD_4(), Z15
  VPCMPEQD Z16, Z8, K2, K3                            // K3 <- left lanes that are symbols
  VPCMPEQD Z16, Z9, K2, K4                            // K4 <- right lanes that are symbols

  KORTESTW K3, K4
  JZ skip_unsymbolize                                 // don't unsymbolize if there are no symbols

  VPCMPD $VPCMP_IMM_LT, Z15, Z12, K3, K5              // only choose left lanes where size < 4
  VPCMPD $VPCMP_IMM_LT, Z15, Z13, K4, K6              // only choose right lanes where size < 4
  VPSUBD.Z Z12, Z15, K5, Z14                          // Z14 <- 4 - left symbol size
  VPSUBD.Z Z13, Z15, K6, Z15                          // Z15 <- 4 - right symbol size
  VPSLLD $3, Z14, Z14                                 // Z14 <- (4 - left symbol size) << 3
  VPSLLD $3, Z15, Z15                                 // Z15 <- (4 - right symbol size) << 3

  VBROADCASTI32X4 bswap32<>+0(SB), Z18
  VPSRLD $8, Z6, Z16                                  // Z16 <- raw left uint value (plus garbage)
  VPSRLD $8, Z7, Z17                                  // Z17 <- raw right uint value (plus garbage)
  VPSHUFB Z18, Z16, Z16                               // Z16 <- bswap32(left uint value)
  VPSHUFB Z18, Z17, Z17                               // Z17 <- bswap32(right uint value)
  VPSRLVD Z14, Z16, Z16                               // Z16 <- left symbol ID
  VPSRLVD Z15, Z17, Z17                               // Z17 <- right symbol ID

  MOVQ bytecode_symtab+0(VIRT_BCPTR), R8              // R8 <- Symbol table
  VPBROADCASTD bytecode_symtab+8(VIRT_BCPTR), Z18     // Z18 <- Number of symbols in symbol table

  // only keep lanes where id < len(symtab)
  VPCMPD $VPCMP_IMM_LT, Z18, Z16, K3, K5
  VPCMPD $VPCMP_IMM_LT, Z18, Z17, K4, K6
  VMOVDQA32.Z Z16, K5, Z16                            // Z16 <- left symbol ID with invalid symbol ID zeroed
  VMOVDQA32.Z Z17, K6, Z17                            // Z17 <- right symbol ID with invalid symbol ID zeroed

  // Gather values of left symbols.
  KMOVB K3, K5
  VPXORQ X20, X20, X20
  VPGATHERDQ 0(R8)(Y16*8), K5, Z20
  KSHIFTRW $8, K3, K6
  VEXTRACTI32X8 $1, Z16, Y18
  VPXORQ X21, X21, X21
  VPGATHERDQ 0(R8)(Y18*8), K6, Z21

  // Gather values of right symbols.
  KMOVB K4, K5
  VPXORQ X22, X22, X22
  VPGATHERDQ 0(R8)(Y17*8), K5, Z22
  KSHIFTRW $8, K4, K6
  VEXTRACTI32X8 $1, Z17, Y18
  VPXORQ X23, X23, X23
  VPGATHERDQ 0(R8)(Y18*8), K6, Z23

  // Merge gathered left values with original values
  VPMOVQD Z20, Y24
  VPMOVQD Z21, Y25
  VPSRLQ $32, Z20, Z20
  VPSRLQ $32, Z21, Z21
  VPMOVQD Z20, Y20
  VPMOVQD Z21, Y21
  VINSERTI32X8 $1, Y21, Z20, Z21
  VINSERTI32X8 $1, Y25, Z24, Z20
  VMOVDQA32 Z20, K3, Z30
  VMOVDQA32 Z21, K3, Z31

  KMOVW K3, K5
  VPGATHERDD 0(SI)(Z30*1), K5, Z6

  // Merge gathered right values with original values
  VPMOVQD Z22, Y24
  VPMOVQD Z23, Y25
  VPSRLQ $32, Z22, Z22
  VPSRLQ $32, Z23, Z23
  VPMOVQD Z22, Y22
  VPMOVQD Z23, Y23
  VINSERTI32X8 $1, Y23, Z22, Z21
  VINSERTI32X8 $1, Y25, Z24, Z22
  VMOVDQA32 Z22, K4, Z4
  VMOVDQA32 Z21, K4, Z5

  VPBROADCASTD CONSTD_0x0F(), Z20
  KMOVW K4, K5
  VPGATHERDD 0(SI)(Z4*1), K5, Z7

  VPANDD Z20, Z6, Z12
  VPANDD Z20, Z7, Z13

skip_unsymbolize:

  // String | Timestamp Comparison (Unbox)
  // -------------------------------------

  VPBROADCASTD CONSTD_0x0E(), Z20

  // To continue comparing string and timestamp values, we have to first "unbox" them to slices.
  VPCMPEQD Z20, Z12, K2, K3                           // K3 <- set when left L is 0xE => decode varuint
  VPCMPEQD Z20, Z13, K2, K4                           // K4 <- set when right L is 0xE => decode varuint

  VPBROADCASTD CONSTD_0x7F(), Z20
  VPBROADCASTD CONSTD_0x80(), Z21
  VPSRLD $8, Z6, Z14                                  // Z14 <- 3 bytes following left Type|L
  VPSRLD $8, Z7, Z15                                  // Z15 <- 3 bytes following right Type|L

  VPANDD Z20, Z14, K3, Z12                            // Z12 <- left length accumulator
  VPANDD Z20, Z15, K4, Z13                            // Z13 <- right length accumulator

  VPTESTNMD Z21, Z14, K3, K3                          // K3 <- left more length? (second byte)
  VPTESTNMD Z21, Z15, K4, K4                          // K4 <- right more length? (second byte)
  VPSLLD $7, Z12, K3, Z12                             // Z12 <- prepare for accumulation
  VPSLLD $7, Z13, K4, Z13                             // Z13 <- prepare for accumulation
  VPSRLD $8, Z14, Z14
  VPSRLD $8, Z15, Z15
  VPTERNLOGD $0xF8, Z20, Z14, K3, Z12                 // Z12 <- accumulate second 7-bit sequence {Z12 = Z12 | (Z14 & Z20)}
  VPTERNLOGD $0xF8, Z20, Z15, K4, Z13                 // Z13 <- accumulate second 7-bit sequence {Z13 = Z13 | (Z15 & Z20)}

  VPTESTNMD Z21, Z14, K3, K3                          // K3 <- left more length? (third byte)
  VPTESTNMD Z21, Z15, K4, K4                          // K4 <- right more length? (third byte)
  VPSLLD $7, Z12, K3, Z12                             // Z12 <- prepare for accumulation
  VPSLLD $7, Z13, K4, Z13                             // Z13 <- prepare for accumulation
  VPSRLD $8, Z14, Z14
  VPSRLD $8, Z15, Z15
  VPTERNLOGD $0xF8, Z20, Z14, K3, Z12                 // Z12 <- accumulate third 7-bit sequence {Z12 = Z12 | (Z14 & Z20)}
  VPTERNLOGD $0xF8, Z20, Z15, K4, Z13                 // Z13 <- accumulate third 7-bit sequence {Z13 = Z13 | (Z15 & Z20)}

  LEAQ bytecode_spillArea+0(VIRT_BCPTR), R8
  VPADDD Z30, Z31, Z14                                // Z14 <- left `start + boxedLength`
  VPADDD Z4, Z5, Z15                                  // Z15 <- right `start + boxedLength`
  VPSUBD Z12, Z14, Z14                                // Z14 <- left slice offset
  VPSUBD Z13, Z15, Z15                                // Z15 <- right slice offset

  // Store slices into spillArea so we can read them in scalar loops
  VPSUBD Z13, Z12, Z21                                // Z21 <- Comparison results in case all the bytes are equal
  VPMINUD Z13, Z12, Z20                               // Z20 <- min(left, right) length
  VMOVDQU32 Z21, 192(R8)                              // [] <- Comparison results, initially `left - right` length

  // String | Timestamp Comparison (Vector)
  // --------------------------------------

  // We keep K2 alive - it's not really necessary in the current implementation, but it's
  // likely we would want to extend this to support lists and structs in the future.
  // Additionally - to prevent bugs triggered by empty strings that have arbitrary offsets,
  // but zero lengths, we filter them here. Any string that is zero would be already compared
  // before entering vector or scalar loop.

  VPSUBD Z13, Z12, K2, Z2                             // Z2 <- merged length comparison
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z22
  VPMINSD Z28, Z2, K2, Z2                             // Z2 <- constrained length comparison to [?, 1]
  VPTESTMD Z20, Z20, K2, K3                           // K3 <- comparison predicate (values having non-zero length)
  VPBROADCASTD CONSTD_8(), Z23
  VPMAXSD Z29, Z2, K2, Z2                             // Z2 <- constrained length comparison to [-1, 1]

  // The idea is to keep using vector loop unless the number of lanes gets too low.
  // The initial eight bytes are always compared in this vector loop to prevent
  // going scalar in case that those eight bytes determine the results of all lanes.

compare_string_vector:
  VEXTRACTI32X8 $1, Z14, Y24
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X16, X16, X16
  VPXORQ X17, X17, X17
  VPGATHERDQ 0(SI)(Y14*1), K4, Z16
  VPGATHERDQ 0(SI)(Y24*1), K5, Z17

  VEXTRACTI32X8 $1, Z15, Y25
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X18, X18, X18
  VPXORQ X19, X19, X19
  VPGATHERDQ 0(SI)(Y15*1), K4, Z18
  VPGATHERDQ 0(SI)(Y25*1), K5, Z19

  VPMINUD Z23, Z20, Z21                               // Z21 <- number of bytes to compare (max 8).

  VPSUBD Z21, Z23, Z24                                // Z24 <- number of bytes to discard (8 - Z21).
  VPSUBD Z21, Z20, K3, Z20                            // Z20 <- adjusted length to compare

  VPSLLD $3, Z24, Z24                                 // Z24 <- number of bits to discard
  VPADDD Z21, Z14, K3, Z14                            // Z14 <- adjusted left slice offset
  VPADDD Z21, Z15, K3, Z15                            // Z15 <- adjusted right slice offset

  VEXTRACTI32X8 $1, Z24, Y25
  VPMOVZXDQ Y24, Z24
  VPMOVZXDQ Y25, Z25

  VPSLLVQ Z24, Z16, Z16                               // Z16 <- left bytes to compare (low)
  VPSLLVQ Z25, Z17, Z17                               // Z17 <- left bytes to compare (high)
  VPSLLVQ Z24, Z18, Z18                               // Z18 <- right bytes to compare (low)
  VPSLLVQ Z25, Z19, Z19                               // Z19 <- right bytes to compare (high)

  VPSHUFB Z22, Z16, Z16                               // Z16 <- left byteswapped quadword to compare (low)
  VPSHUFB Z22, Z17, Z17                               // Z17 <- left byteswapped quadword to compare (high)
  VPSHUFB Z22, Z18, Z18                               // Z18 <- right byteswapped quadword to compare (low)
  VPSHUFB Z22, Z19, Z19                               // Z19 <- right byteswapped quadword to compare (high)

  KSHIFTRW $8, K3, K4
  VPCMPQ $VPCMP_IMM_NE, Z18, Z16, K3, K5              // K5 <- lanes having values that aren't equal (low)
  VPCMPQ $VPCMP_IMM_NE, Z19, Z17, K4, K4              // K4 <- lanes having values that aren't equal (high)
  KUNPCKBW K5, K4, K5                                 // K5 <- lanes having values that aren't equal (all lanes)
  KANDNW K3, K5, K3                                   // K3 <- lanes to continue being compared

  VPCMPUQ $VPCMP_IMM_LT, Z18, Z16, K5, K0             // K0 <- lanes where the comparison is less than (low)
  VPCMPUQ $VPCMP_IMM_LT, Z19, Z17, K4, K6             // K6 <- lanes where the comparison is less than (high)
  VPCMPUD $VPCMP_IMM_GE, Z28, Z20, K3, K3             // K3 <- lanes to continue being compared (where length is non-zero)
  KUNPCKBW K0, K6, K6                                 // K6 <- lanes where the comparison is less than (all lanes)
  VMOVDQA32 Z29, K6, Z2                               // Z2 <- merge less than results (-1)
  KMOVW K3, BX

  VPCMPUQ $VPCMP_IMM_GT, Z18, Z16, K5, K5             // K5 <- lanes where the comparison is greater than (low)
  VPCMPUQ $VPCMP_IMM_GT, Z19, Z17, K4, K4             // K6 <- lanes where the comparison is greater than (high)
  POPCNTL BX, DX                                      // DX <- number of remaining lanes to process
  KUNPCKBW K5, K4, K5                                 // K5 <- lanes where the comparison is greater than (all lanes)
  VMOVDQA32 Z28, K5, Z2                               // Z2 <- merge greater than results (1)

  TESTL BX, BX
  JZ next

  // Go to scalar loop if the number of lanes to compare gets low
  CMPL DX, $BC_SCALAR_PROCESSING_LANE_COUNT
  JHI compare_string_vector

  VMOVDQU32 Z14, 0(R8)                                // left slice offset
  VMOVDQU32 Z15, 64(R8)                               // right slice offset
  VMOVDQU32 Z20, 128(R8)                              // min(left, right) length
  VMOVDQU32 Z2, 192(R8)                               // comparison results

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
  VMOVDQU8 0(R14), Z18
  VMOVDQU8 0(R15), Z19
  VPCMPB $VPCMP_IMM_NE, Z18, Z19, K4
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

  VMOVDQU8.Z 0(R14), K4, Z18
  VMOVDQU8.Z 0(R15), K4, Z19

  VPCMPB $VPCMP_IMM_NE, Z18, Z19, K4
  KTESTQ K4, K4
  JNE compare_string_scalar_diff

  // Comparable slices have the same content, which means that `leftLen-rightLen` is the result
  // This result was already precalculated before entering the scalar loop, so we don't have to
  // calculate and store it again.

  TESTL BX, BX
  JNE compare_string_scalar_lane

compare_string_scalar_done:
  VPMINSD 192(R8), Z28, Z2
  VPMAXSD Z29, Z2, Z2

next:
  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                    // Z2 <- merged comparison results of all types (low)
  VPMOVSXDQ Y3, Z3                                    // Z3 <- merged comparison results of all types (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)
