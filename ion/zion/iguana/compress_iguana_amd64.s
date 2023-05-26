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
#include "../../../internal/asmutils/bc_imm_amd64.h"
#include "../../../internal/asmutils/bc_constant.h"


// func pickBestMatchCD(ec *encodingContext, src []byte, candidates []uint32) matchDescriptor
TEXT ·pickBestMatchCD(SB), NOSPLIT | NOFRAME, $0-56
    MOVQ            candidates_len+40(FP), R15                          // R15 := uint64{candidates.Len}
    MOVQ            ec+0(FP), BX                                        // BX  := uint64{ec}
    TESTQ           R15, R15
    JZ              no_candidates

    // There are some candidates

    VPXORD          X0, X0, X0                                          // Z0  := {0*}
    VPXORD          X16, X16, X16                                       // Z16 := uint32{best_start[i]} for i in 15..0
    VPTERNLOGD      $0xff, Z1, Z1, Z1                                   // Z1  := {-1*}
    VPXORD          X17, X17, X17                                       // Z17 := uint32{best_length[i]} for i in 15..0
    XORL            DI, DI                                              // DI  := uint32{0}
    VPSRLD          $1, Z1, Z18                                         // Z18 := uint32{costInfinite times 16}
    MOVL            $(const_lastLongOffset + const_mmLongOffsets), SI   // SI  := uint32{lastLongOffset + mmLongOffsets}
    MOVQ            (encodingContext_pendingLiterals+const_offsSliceHeaderLen)(BX), AX // AX := int64{litLen}
    MOVQ            candidates_base+32(FP), R14                         // R14 := uint64{candidates.Data}
    VPSRLD          $31, Z1, Z26                                        // Z26 := uint32{1 times 16}
    VPBROADCASTD    SI, Z14                                             // Z14 := uint32{(lastLongOffset + mmLongOffsets) times 16}
    MOVL            encodingContext_currentOffset(BX), R13        // R13 := uint32{ec.currentOffset}
    VPSLLD          $2, Z26, Z28                                        // Z28 := uint32{4 times 16}
    MOVL            $1, SI                                              // SI  := uint32{1}
    VPADDD          Z26, Z26, Z27                                       // Z27 := uint32{2 times 16}
    VPBROADCASTD    CONST_GET_PTR(consts_varUIntThreshold1B, 0), Z29    // Z29 := uint32{varUIntThreshold1B times 16}
    VPSRLD          $28, Z1, Z20                                        // Z20 := uint32{maxShortMatchLen times 16}
    VMOVDQA32       Z18, Z24                                            // Z24 := uint32{costInfinite times 16}
    TESTL           AX, AX                                              // EFLAGS.ZF := 1 <=> litLen == 0
    SETGT           DX                                                  // DL  := uint8{1 if litLen > 0; 0 otherwise}
    VPBROADCASTD    CONST_GET_PTR(consts_varUIntThreshold3B, 0), Z30    // Z30 := uint32{varUIntThreshold3B times 16}
    SUBQ            $const_maxShortLitLen, AX                           // AX  := int64{litLen - maxShortLitLen}
    CMOVLGE         SI, DI                                              // DI  := uint32{1 if litLen >= maxShortLitLen; 0 otherwise}
    MOVBLZX         DX, DX                                              // DX  := uint32{1 if litLen > 0; 0 otherwise}
    MOVL            $0x03, SI                                           // SI  := uint32{3}
    CMPL            AX, $const_varUIntThreshold1B
    CMOVLGE         SI, DI                                              // DI  := uint32{3 if litLen >= (varUIntThreshold1B+maxShortLitLen); 1 if litLen >= maxShortLitLen; 0 otherwise}
    MOVL            $0x04, SI                                           // SI  := uint32{4}
    CMPL            AX, $const_varUIntThreshold3B
    CMOVLGE         SI, DI                                              // DI  := uint32{4 if litLen >= (varUIntThreshold3B+maxShortLitLen); 3 if litLen >= (varUIntThreshold1B+maxShortLitLen); 1 if litLen >= maxShortLitLen; 0 otherwise}
    MOVQ            src_base+8(FP), SI                                  // SI  := uint64{src.Data}
    VPBROADCASTD    DX, Z13                                             // Z13 := uint32{(1 if litLen > 0; 0 otherwise) times 16}
    VPSRLD          $16, Z1, Z25                                        // Z25 := uint32{0x0000_ffff times 16}
    VPBROADCASTD    DI, Z19                                             // Z19 := uint32{litLenCorrector times 16}
    MOVQ            src_len+16(FP), R12                                 // R12 := uint64{src.Len}
    VPBROADCASTD    R13, Z22                                            // Z22 := uint32{ec.currentOffset times 16}
    SUBQ            R13, R12                                            // R12 := int64{src.Len - ec.currentOffset}
    ADDQ            SI, R13                                             // R13 := uint64{&src[ec.currentOffset]}
    VPBROADCASTD    2(R13), Z12                                         // Z12 := uint32{(src[ec.currentOffset+5..2]) times 16}
    VPBROADCASTD    CONST_GET_PTR(consts_minOffset, 0), Z23             // Z23 := uint32{minOffset times 16}
    VPBROADCASTD    encodingContext_lastEncodedOffset(BX), Z21    // Z21 := uint32{ec.lastEncodedOffset times 16}
    VPSRLD          $30, Z1, Z15                                        // Z15 := uint32{3 times 16}
    VMOVDQU32       CONST_GET_PTR(consts_shuffle, 0), Z31               // Z31 := uint32{consts_shuffle}
    VPBROADCASTD    R12, Z10                                            // Z10 := uint32{(src.Len - ec.currentOffset) times 16}

    // BX  := uint64{ec}
    // SI  := uint64{src.Data}
    // R12 := int64{src.Len - ec.currentOffset}
    // R13 := uint64{&src[ec.currentOffset]}
    // R14 := uint64{candidates.Data}
    // R15 := uint64{candidates.Len}
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z10 := uint32{(src.Len - ec.currentOffset) times 16}
    // Z12 := uint32{(src[ec.currentOffset+5..2]) times 16}
    // Z13 := uint32{(1 if litLen > 0; 0 otherwise) times 16}
    // Z14 := uint32{(lastLongOffset + mmLongOffsets) times 16}
    // Z15 := uint32{3 times 16}
    // Z16 := uint32{best_start[i]} for i in 15..0
    // Z17 := uint32{best_length[i]} for i in 15..0
    // Z18 := uint32{best_cost[i]} for i in 15..0
    // Z19 := uint32{litLenCorrector times 16}
    // Z20 := uint32{maxShortMatchLen times 16}
    // Z21 := uint32{ec.lastEncodedOffset times 16}
    // Z22 := uint32{ec.currentOffset times 16}
    // Z23 := uint32{minOffset times 16}
    // Z24 := uint32{costInfinite times 16}
    // Z25 := uint32{0x0000_ffff times 16}
    // Z26 := uint32{1 times 16}
    // Z27 := uint32{2 times 16}
    // Z28 := uint32{4 times 16}
    // Z29 := uint32{varUIntThreshold1B times 16}
    // Z30 := uint32{varUIntThreshold3B times 16}
    // Z31 := uint32{consts_shuffle}

loop:
    SUBQ            $16, R15                                        // Indicate the consumption of 16 candidates
    JCS             fetch_candidates_partial                        // There are fewer than 16 candidates, so a correction needs to be applied

    // There are at least 16 candidates available
    KXNORW          K6, K6, K6                                      // K6  := uint16{0xffff}
    VMOVDQU32       (R14)(R15*4), Z8                                // Z8 := uint32{start[i]} for i in 15..0

candidates_fetched:
    KMOVW           K6, K1
    VPXORD          X3, X3, X3
    VPSUBD          Z8, Z22, Z2                                     // Z2  := uint32{offs[i]} for i in 15..0
    VPGATHERDD      2(SI)(Z8*1), K1, Z3                             // Z3  := {the bytes 5..2 of candidate[i]} for i in 15..0
    VPCMPD          $VPCMP_IMM_GE, Z23, Z2, K6, K6                  // K6  := uint16{offs[i] < minOffset} for i in 15..0
    //p0
    VPCMPD          $VPCMP_IMM_NE, Z21, Z2, K6, K2                  // K2  := {offs[i] != ec.lastEncodedOffset} for i in 15..0
    //p0
    VPCMPD          $VPCMP_IMM_GT, Z25, Z2, K2, K3                  // K3  := {(offs[i] != ec.lastEncodedOffset) && (offs[i] > maxUint16)} for i in 15..0
    //p0
    KTESTW          K6, K6                                          // EFLAGS.ZF==1 <=> K6 == 0
    JZ              loop                                            // No more live candidates

    // The candidate[i][1..0] == reference[1..0] relation is ensured by how the hash chains work, so it needs not to be checked.
    // Due to a possibly elusive nature of the implementation, here's how it works:
    //
    // The hash function used in the hash chains is composed of the first two characters of the reference string, so when
    // a list of candidates is presented for matching, these first two characters must by definition be equal and checking
    // them can be disregarded. Secondly, experiments show that most candidates die young, so there is no point in engaging
    // in full-blown length calculations until proven otherwise. Specifically, bytes [5..2] of the reference are replicated
    // in Z12 to avoid continual re-fetching and compared to the bytes 5..2 of every candidate. Two comparisons are executed
    // in parallel to utilize the superscalar capabilities of the processor. The first one checks if all the bytes 5..2 are
    // equal to the bytes 5..2 of the reference, so it executes with a doubleword granularity. The results are stored in K1.
    // If there exists such a candidate, the matching of the subsequent chunks needs to be continued, as the match length is
    // at least 6 bytes. Fortunately, most candidates are sorted out already in this early phase. Independently, a byte-level
    // comparison of the same data is executed with the latency-one instructions VPSUBB and VPMINUB. For each candidate,
    // the result in Z3 is composed of four bytes from the set {0, 1}. A zero means the j-th byte of that candidate
    // is equal to the j-th byte of the reference, whereas 1 indicates a mismatch. We are interested in the position of
    // the first mismatch, as it defines the match length. However, the position will indicate the 1-byte position, not a bit
    // position, so it needs to be corrected by 8. As there is no intrinsic vector BSF capability, it is emulated with
    // VPLZCNTD, as described here: https://stackoverflow.com/questions/73930875/trying-to-write-a-vectorized-implementation-of-gerd-isenbergs-bit-scan-forward

    VPCMPD          $VPCMP_IMM_EQ, Z12, Z3, K6, K1                  // K1  := {candidate[i][5..2] == reference[5..2]} for i in 15..0
    VPSUBB          Z12, Z3, Z3                                     // Z3  := uint8{0 <=> candidate[i][j]-reference[j]} for i in 15..0, j in 5..2
    //p05
    VPSUBD          Z3, Z0, Z4                                      // Z4  := uint32{-(candidate[i][5..2])} for i in 15..0
    //p05
    VPANDD          Z3, Z4, Z3                                      // Z3  := uint32{candidate[i][5..2] & -(candidate[i][5..2])} for i in 15..0
    KTESTW          K1, K1                                          // EFLAGS.ZF == 1 <=> there exists a candidate[i][5..2] where all the four bytes equal to reference[j]
    VPLZCNTD        Z3, Z3                                          // Z3  := uint32{bsr(candidate[i][5..2] & -(candidate[i][5..2]))} for i in 15..0
    //p5
    KANDNW          K6, K3, K5                                      // K5  := {(offs[i] == ec.lastEncodedOffset) || (offs[i] <= maxUint16)} for i in 15..0
    //p5
    //p05
    //p05
    VPSRLD          $3, Z3, Z3                                      // Z3  := uint32{bsr(candidate[i][5..2] & -(candidate[i][5..2])) >> 3} for i in 15..0
    //p5
    VPXORD          Z15, Z3, Z3                                     // Z3  := uint32{(bsr(candidate[i][5..2] & -(candidate[i][5..2])) >> 3) ^ 0x03} for i in 15..0
    //p05
    VPMINUD         Z3, Z28, Z3                                     // Z3  := uint32{the length of the matching 5..2 part for candidate[i]} for i in 15..0
    //p05
    VPADDD          Z27, Z3, Z9                                     // Z9  := uint32{length[i] := Z3[i] + 2}: the 2 is to account for bytes 1..0 being equal by design
    JNZ             long_match                                      // There still exists a candidate[i][5..0]==reference[5..0], so the follow-up chunks need to be inspected

length_computed:    // The length vector in Z9 has been populated
    VPMINUD         Z10, Z9, Z9                                     // Z9  := uint32{min(length[i], src.Len - ec.currentOffset)} for i in 15..0
    //p5
    VPSUBD          Z20, Z9, Z4                                     // Z4  := int32{length[i] - maxShortMatchLen} for i in 15..0
    VPCMPUD         $VPCMP_IMM_LE, Z20, Z9, K3, K4                  // K4  := {length[i] <= maxShortMatchLen} for i in 15..0
    VPSUBD          Z14, Z9, K3, Z4                                 // Z4  := int32{if K3[i] (length[i] - (lastLongOffset + mmLongOffsets)); (length[i] - maxShortMatchLen) otherwise} for i in 15..0
    VPSUBD          Z9, Z26, Z3                                     // Z3  := int32{c[i] := costToken - int32(length[i])} for i in 15..0
    VPCMPD          $VPCMP_IMM_LT, Z30, Z4, K1                      // K1  := {Z4[i] < varUIntThreshold3B} for i in 15..0
    VPADDD          Z13, Z3, K3, Z3                                 // Z3  := int32{if K3[i] c += costToken} for i in 15..0
    KANDNW          K6, K4, K6                                      // K6  := {not costInfinite on lane i} for i in 15..0
    VPCMPD          $VPCMP_IMM_LT, Z29, Z4, K4                      // K4  := {Z4[i] < varUIntThreshold1B} for i in 15..0
    VPADDD          Z15, Z3, K3, Z3                                 // Z3  := int32{if K3[i] c += costOffs24} for i in 15..0
    VPSRLD          $31, Z4, Z4                                     // Z4  := uint32{1 if Z4[i] < 0; 0 otherwise} for i in 15..0
    VPBLENDMD       Z15, Z28, K1, Z5                                // Z5  := uint32{3 if K1[i]; 4 otherwise} for i in 15..0
    VPXORD          Z4, Z26, Z4                                     // Z4  := uint32{1 if Z4[i] >= 0; 0 otherwise} for i in 15..0
    KANDW           K5, K2, K2                                      // K2  := {offs[i] <= maxUint16} for i in 15..0
    VPBLENDMD       Z4, Z5, K4, Z4                                  // Z4  := uint32{costVarUInt(matchLen)} for i in 15..0
    VMOVDQA32       Z24, Z5                                         // Z5  := uint32{costInfinite times 16}
    VPADDD          Z27, Z3, K2, Z3                                 // Z3  := int32{if K2[i] c += costOffs16} for i in 15..0
    VPADDD          Z19, Z4, Z4                                     // Z4  := uint32{costVarUInt(litLen) + costVarUInt(matchLen[i])} for i in 15..0
    VPADDD          Z3, Z4, K6, Z5                                  // Z5  := int32{cost[i]} for i in 15..0

    // Z5 contains the costs. Check if there is a better candidate by computing and broadcasting the just calculated minimum.
    // Z5 := int32{cost[15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]}

    VPCMPD          $VPCMP_IMM_LT, Z18, Z5, K1                      // K1  := {cost[i] < best_cost[i]} for i in 15..0
    VPROLQ          $32, Z5, Z2

    // Z2  := int32{cost[14, 15, 12, 13, 10, 11, 8, 9, 6, 7, 4, 5, 2, 3, 0, 1]}

    VPMINSD         Z5, Z2, Z2
    //p5

    // Z2  := int32{min[15, 14], min[15, 14], min[13, 12], min[13, 12],
    //              min[11, 10], min[11, 10], min[9, 8], min[9, 8],
    //              min[7, 6], min[7, 6], min[5, 4], min[5, 4],
    //              min[3, 2], min[3, 2], min[1, 0], min[1, 0]}

    VPSHUFD         $0b001010, Z2, Z3
    //p0

    // Z3  := int32{min[13, 12], min[13, 12], min[15, 14], min[15, 14],
    //              min[9, 8], min[9, 8], min[11, 10], min[11, 10],
    //              min[5, 4], min[5, 4], min[7, 6], min[7, 6],
    //              min[1, 0], min[1, 0], min[3, 2], min[3, 2]}

    VPMINSD         Z3, Z2, Z2
    //p5

    // Z2  := int32{min[15..12] times 4, min[11..8] times 4, min[7..4] times 4, min[3..0] times 4}

    VPERMD          Z2, Z31, Z2
    KTESTW          K1, K1
    JZ              loop
    //p0+p5
    //p0+p5

    // Z2  := int32{(min[15..12], min[11..8], min[7..4], min[3..0]) times 4}

    VPROLQ          $32, Z2, Z3
    //p5

    // Z3  := int32{(min[11..8], min[15..12], min[3..0], min[7..4]) times 4}

    VPMINSD         Z3, Z2, Z2
    //p5

    // Z2  := int32{(min[15..8], min[15..8], min[7..0], min[7..0]) times 4}

    VPSHUFD         $0b001010, Z2, Z3
    //p0

    // Z3  := int32{(min[7..0], min[7..0], min[15..8], min[15..8]) times 4}

    VPMINSD         Z3, Z2, Z18
    //p5

    // Z18  := int32{min[15..0] times 16}
    VPCMPD          $VPCMP_IMM_EQ, Z18, Z5, K1                      // K1  := {the indices of all the places where best_cost[i] == min[15..0]} for i in 15..0
    KMOVW           K1, AX                                          // AX  := {the indices of all the places where best_cost[i] == min[15..0]} for i in 15..0
    BSRL            AX, AX                                          // AX  := uint32{the min_index of the rightmost minimal candidate}: select the winner with the smallest offset
    VPBROADCASTD    AX, Z2                                          // Z2  := uint32{min_index times 16}
    VPERMD          Z8, Z2, Z16                                     // Z16 := uint32{best_start[min_index] times 16}
    VPERMD          Z9, Z2, Z17                                     // Z17 := uint32{best_length[min_index] times 16}
    JMP             loop


long_match:
    KMOVW           K1, K7                                          // K7  := {the candidates that need to have the succeeding chunks inspected}
    LEAQ            6(SI), AX                                       // AX  := uint64{&src[6]}: mutable reference cursor; the first 6 bytes have already been checked.
    MOVL            $6, CX                                          // CX  := uint32{6}: mutable candidate cursor; ditto

long_match_loop:
    VPBROADCASTD    (R13)(CX*1), Z2                                 // Z2  := uint8{(src[CX+3..CX] times 16)}
    VPXORD          X3, X3, X3                                      // for gather
    CMPQ            R12, CX                                         // The Go "for ec.currentOffset+length < n"
    JBE             length_computed                                 // No more chunks to inspect
    VPGATHERDD      (AX)(Z8*1), K1, Z3                              // Z3  := uint32{src[start[i]+CX+3..start[i]+CX} for i in 15..0
    VPCMPD          $VPCMP_IMM_EQ, Z2, Z3, K7, K1                   // K1  := {candidate[i][start[i]+CX+3..start[i]+CX] == reference[CX+3..CX]} for i in 15..0
    VPSUBB          Z2, Z3, Z3                                      // Z3  := uint8{0 <=> candidate[i][start[i]+CX+3..start[i]+CX]==reference[CX+3..CX]} for i in 15..0
    ADDQ            $4, AX                                          // Adjust the reference cursor
    VPSUBD          Z3, Z0, Z2                                      // Z2  := uint32{-(candidate[i][start[i]+CX+3..start[i]+CX])} for i in 15..0
    ADDL            $4, CX                                          // Adjust the candidates' cursor
    VPANDD          Z2, Z3, Z2                                      // Z2  := uint32{(candidate[i][start[i]+CX+3..start[i]+CX]) & -(candidate[i][start[i]+CX+3..start[i]+CX])} for i in 15..0
    KTESTW          K1, K1                                          // EFLAGS.ZF == 1 <=> there exists a candidate[i][CX+3..CX] where all the four bytes equal to reference[CX+3..CX]
    VPLZCNTD        Z2, Z2                                          // Z2  := uint32{bsr(candidate[i][start[i]+CX+3..start[i]+CX] & -(candidate[i][start[i]+CX+3..start[i]+CX]))} for i in 15..0
    //p5
    //p05
    //p05
    //p05
    VPSRLD          $3, Z2, Z2                                      // Z2  := uint32{bsf(candidate[i][start[i]+CX+3..start[i]+CX]==reference[start[i]+CX+3..start[i]+CX]) >> 3}: the length of the matching CX+3..CX part for candidate[i]
    //p5
    VPXORD          Z15, Z2, Z2                                     // Z2  := uint32{(bsr(candidate[i][start[i]+CX+3..start[i]+CX] & -(candidate[i][start[i]+CX+3..start[i]+CX])) >> 3) ^ 0x03} for i in 15..0
    //p05
    VPMINUD         Z2, Z28, Z2                                     // Z2  := uint32{the length of the matching start[i]+CX+3..start[i]+CX part for candidate[i]} for i in 15..0
    //p5
    VPADDD          Z2, Z9, K7, Z9                                  // Z9  := uint32{length[i] += Z2[i]}: adjust the currently calculated length
    KMOVW           K1, K7                                          // K7  := {the candidates that need to have the succeeding chunks inspected}
    JZ              length_computed                                 // No more candidates require further inspection
    JMP             long_match_loop                                 // Inspect the next chunk


fetch_candidates_partial:
    ADDQ            $16, R15                                        // Undo the too large stride
    JZ              out_of_candidates                               // There are no more candidates to evaluate
    MOVL            $-1, DX                                         // DX  := uint32{0xffff_ffff}
    SHLXL           R15, DX, DX                                     // DX  := uint32{^fetch_mask}
    XORL            R15, R15                                        // R15 := uint32{0}: no more candidates left
    NOTL            DX                                              // DX  := uint32{fetch_mask}
    KMOVW           DX, K6                                          // K6  := uint16{fetch_mask}
    VMOVDQU32.Z     (R14), K6, Z8                                   // Z8  := uint32{the remaining candidates or 0}
    JMP             candidates_fetched                              // Process the remainder


out_of_candidates:
    VMOVSS          X18, ret_cost+64(FP)                            // ret.cost := best_cost
    VMOVSS          X17, ret_length+60(FP)                          // ret.length := best_length
    VMOVSS          X16, ret_start+56(FP)                           // ret.start := best_start
    RET


no_candidates:
    MOVL            $const_costInfinite, ret_cost+64(FP)            // ret.cost := best_cost
    MOVL            R15, ret_length+60(FP)                          // ret.length := best_length
    MOVL            R15, ret_start+56(FP)                           // ret.start := best_start
    RET


CONST_DATA_U32(consts_varUIntThreshold1B, 0, $const_varUIntThreshold1B)
CONST_GLOBAL(consts_varUIntThreshold1B, $4)

CONST_DATA_U32(consts_varUIntThreshold3B, 0, $const_varUIntThreshold3B)
CONST_GLOBAL(consts_varUIntThreshold3B, $4)

CONST_DATA_U32(consts_minOffset, 0, $const_minOffset)
CONST_GLOBAL(consts_minOffset, $4)

CONST_DATA_U32(consts_shuffle,  (0*4), $0)
CONST_DATA_U32(consts_shuffle,  (1*4), $4)
CONST_DATA_U32(consts_shuffle,  (2*4), $8)
CONST_DATA_U32(consts_shuffle,  (3*4), $12)
CONST_DATA_U32(consts_shuffle,  (4*4), $0)
CONST_DATA_U32(consts_shuffle,  (5*4), $4)
CONST_DATA_U32(consts_shuffle,  (6*4), $8)
CONST_DATA_U32(consts_shuffle,  (7*4), $12)
CONST_DATA_U32(consts_shuffle,  (8*4), $0)
CONST_DATA_U32(consts_shuffle,  (9*4), $4)
CONST_DATA_U32(consts_shuffle, (10*4), $8)
CONST_DATA_U32(consts_shuffle, (11*4), $12)
CONST_DATA_U32(consts_shuffle, (12*4), $0)
CONST_DATA_U32(consts_shuffle, (13*4), $4)
CONST_DATA_U32(consts_shuffle, (14*4), $8)
CONST_DATA_U32(consts_shuffle, (15*4), $12)
CONST_GLOBAL(consts_shuffle, $64)
