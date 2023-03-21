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

// This file provides an implementation of 'bccmpxxstr' operations (string vs string compare).
//
// It uses the following macros:
//   - BC_CMP_NAME  - name of the bc instruction
//   - BC_CMP_I_IMM - predicate for integer comparison (VPCMPx instruction)

//TEXT bc...(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K3), IN(R8))

  KTESTW K3, K3
  KXORW K1, K1, K1                                    // K1 <- results of all comparisons, initially all false

  BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z6), OUT(Z7), IN(BX), IN(K3)) // Z6:Z7 <- left string slice offsets
  BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(CX), IN(K3)) // Z4:Z5 <- right string slice offsets

  JZ next

  LEAQ bytecode_spillArea+0(VIRT_BCPTR), R8           // R8 <- Spill area
  VPSUBD Z5, Z7, Z9                                   // Z9 <- Comparison results in case all the bytes are equal
  VPMINUD Z7, Z5, Z8                                  // Z8 <- min(left, right) length
  VMOVDQU32 Z9, 192(R8)                               // [] <- Comparison results, initially `left - right` length
  VPXORD X9, X9, X9                                   // Z9 <- zero

  // Vector Loop
  // -----------

  // The idea is to keep using vector loop unless the number of lanes gets too low.
  // The initial eight bytes are always compared in this vector loop to prevent
  // going scalar in case that those eight bytes determine the results of all lanes.

  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z22
  VPBROADCASTD CONSTD_8(), Z23

vector_loop_iter:
  VEXTRACTI32X8 $1, Z6, Y14
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X10, X10, X10
  VPXORQ X11, X11, X11
  VPGATHERDQ 0(VIRT_BASE)(Y6*1), K4, Z10
  VPGATHERDQ 0(VIRT_BASE)(Y14*1), K5, Z11

  VEXTRACTI32X8 $1, Z4, Y14
  KMOVB K3, K4
  KSHIFTRW $8, K3, K5
  VPXORQ X12, X12, X12
  VPXORQ X13, X13, X13
  VPGATHERDQ 0(VIRT_BASE)(Y4*1), K4, Z12
  VPGATHERDQ 0(VIRT_BASE)(Y14*1), K5, Z13

  VPMINUD Z23, Z8, Z21                                // Z21 <- number of bytes to compare (max 8).
  VPSUBD Z21, Z23, Z24                                // Z24 <- number of bytes to discard (8 - Z21).
  VPSUBD Z21, Z8, K3, Z8                              // Z8 <- adjusted length to compare

  VPSLLD $3, Z24, Z24                                 // Z24 <- number of bits to discard
  VPADDD Z21, Z6, K3, Z6                              // Z6 <- adjusted left slice offset
  VPADDD Z21, Z4, K3, Z4                              // Z4 <- adjusted right slice offset

  VEXTRACTI32X8 $1, Z24, Y25
  VPMOVZXDQ Y24, Z24
  VPMOVZXDQ Y25, Z25

  VPSLLVQ Z24, Z10, Z10                               // Z10 <- low left bytes to compare
  VPSLLVQ Z25, Z11, Z11                               // Z11 <- high left bytes to compare
  VPSLLVQ Z24, Z12, Z12                               // Z12 <- low right bytes to compare
  VPSLLVQ Z25, Z13, Z13                               // Z13 <- high right bytes to compare

  VPSHUFB Z22, Z10, Z10                               // Z10 <- low left bytes to compare (byteswapped)
  VPSHUFB Z22, Z11, Z11                               // Z11 <- high left bytes to compare (byteswapped)
  VPSHUFB Z22, Z12, Z12                               // Z12 <- low right bytes to compare (byteswapped)
  VPSHUFB Z22, Z13, Z13                               // Z13 <- high right bytes to compare (byteswapped)

  KSHIFTRW $8, K3, K4
  VPCMPQ $VPCMP_IMM_NE, Z12, Z10, K3, K5              // K5 <- low lanes having values that aren't equal
  VPCMPQ $VPCMP_IMM_NE, Z13, Z11, K4, K6              // K6 <- high lanes having values that aren't equal
  KUNPCKBW K5, K6, K5                                 // K5 <- lanes having values that aren't equal
  KANDNW K3, K5, K3                                   // K3 <- lanes to continue being compared

  VPCMPUQ $BC_CMP_I_IMM, Z12, Z10, K5, K5             // K5 <- low lanes where the comparison yields TRUE
  VPCMPUQ $BC_CMP_I_IMM, Z13, Z11, K6, K6             // K6 <- high lanes where the comparison yields TRUE
  KUNPCKBW K5, K6, K5                                 // K5 <- lanes where the comparison yields TRUE
  KORW K5, K1, K1                                     // K1 <- merged results of the comparison

  // If the remaining length to compare of a lane is zero and that lane is still in K3 (active lanes) it
  // means that we have compared all its bytes and they were equal. However, strings can have different
  // sizes, so in that case `left - right` length determines the result of the comparison.

  VPCMPEQD Z9, Z8, K3, K4                             // K4 <- active lanes having the remaining length zero
  KANDNW K3, K4, K3                                   // K3 <- lanes to continue being compared
  KMOVW K3, BX
  POPCNTL BX, DX                                      // DX <- number of remaining lanes to process

  VPCMPD $BC_CMP_I_IMM, Z5, Z7, K4, K5                // K5 <- comparison result of lanes specified by K4
  KORW K5, K1, K1                                     // K1 <- merged results of the comparison

  TESTL BX, BX
  JZ next

  // Go to scalar loop if the number of lanes to compare gets low
  CMPL DX, $BC_SCALAR_PROCESSING_LANE_COUNT
  JHI vector_loop_iter

  VMOVDQU32 Z6, 0(R8)                                 // left slice offsets
  VMOVDQU32 Z4, 64(R8)                                // right slice offsets
  VMOVDQU32 Z8, 128(R8)                               // min(left, right) length

  MOVQ $-1, R13
  JMP scalar_lane

  // Scalar Loop
  // -----------

scalar_diff:
  KMOVQ K4, CX
  TZCNTQ CX, CX
  MOVBLZX 0(R14)(CX*1), R14
  MOVBLZX 0(R15)(CX*1), R15
  SUBL R15, R14
  MOVL R14, 192(R8)(DX * 4)

  TESTL BX, BX
  JE scalar_done

scalar_lane:
  TZCNTL BX, DX                                       // DX - Index of the lane to process
  BLSRL BX, BX                                        // clear the index of the iterator

  MOVL 128(R8)(DX * 4), CX                            // min(left, right) length
  MOVL 0(R8)(DX * 4), R14                             // left slice offset
  ADDQ VIRT_BASE, R14                                 // make left offset an absolute VM address
  MOVL 64(R8)(DX * 4), R15                            // right slice offset
  ADDQ VIRT_BASE, R15                                 // make right offset an absolute VM address

  SUBL $64, CX
  JCS scalar_tail

scalar_loop_iter:                                     // main compare loop that processes 64 bytes at once
  VMOVDQU8 0(R14), Z12
  VMOVDQU8 0(R15), Z13
  VPCMPB $VPCMP_IMM_NE, Z12, Z13, K4
  KTESTQ K4, K4
  JNE scalar_diff

  ADDQ $64, R14
  ADDQ $64, R15
  SUBL $64, CX
  JA scalar_loop_iter

scalar_tail:                                          // tail loop that processes up to 64 bytes at once
  SHLXQ CX, R13, CX
  NOTQ CX
  KMOVQ CX, K4                                        // K4 <- LSB mask of bits to process (valid characters)

  VMOVDQU8.Z 0(R14), K4, Z12
  VMOVDQU8.Z 0(R15), K4, Z13

  VPCMPB $VPCMP_IMM_NE, Z12, Z13, K4
  KTESTQ K4, K4
  JNE scalar_diff

  // Comparable slices have the same content, which means that `leftLen-rightLen` is the result
  // This result was already precalculated before entering the scalar loop, so we don't have to
  // calculate and store it again.

  TESTL BX, BX
  JNE scalar_lane

scalar_done:
  VMOVDQU32 192(R8), Z10
  VPCMPD $BC_CMP_I_IMM, Z9, Z10, K3, K4
  KORW K4, K1, K1

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)
