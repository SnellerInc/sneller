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

#include "../internal/percentile/sinf32.h"
#include "../internal/percentile/asinf32.h"

//; swaps {0,5}{1,4}{2,12}{3,13}{6,7}{8,9}{10,15}{11,14}
//; perm [5 4 12 13 1 0 7 6 9 8 15 14 2 3 11 10 ]
DATA x3_swapdata16_0+0(SB)/8, $0x0000000400000005
DATA x3_swapdata16_0+8(SB)/8, $0x0000000D0000000C
DATA x3_swapdata16_0+16(SB)/8, $0x0000000000000001
DATA x3_swapdata16_0+24(SB)/8, $0x0000000600000007
DATA x3_swapdata16_0+32(SB)/8, $0x0000000800000009
DATA x3_swapdata16_0+40(SB)/8, $0x0000000E0000000F
DATA x3_swapdata16_0+48(SB)/8, $0x0000000300000002
DATA x3_swapdata16_0+56(SB)/8, $0x0000000A0000000B
GLOBL x3_swapdata16_0(SB), RODATA|NOPTR, $64

//; swaps {0,2}{1,10}{3,6}{4,7}{5,14}{8,11}{9,12}{13,15}
//; perm [2 10 0 6 7 14 3 4 11 12 1 8 9 15 5 13 ]
DATA x3_swapdata16_1+0(SB)/8, $0x0000000A00000002
DATA x3_swapdata16_1+8(SB)/8, $0x0000000600000000
DATA x3_swapdata16_1+16(SB)/8, $0x0000000E00000007
DATA x3_swapdata16_1+24(SB)/8, $0x0000000400000003
DATA x3_swapdata16_1+32(SB)/8, $0x0000000C0000000B
DATA x3_swapdata16_1+40(SB)/8, $0x0000000800000001
DATA x3_swapdata16_1+48(SB)/8, $0x0000000F00000009
DATA x3_swapdata16_1+56(SB)/8, $0x0000000D00000005
GLOBL x3_swapdata16_1(SB), RODATA|NOPTR, $64

//; swaps {0,8}{1,3}{2,11}{4,13}{5,9}{6,10}{7,15}{12,14}
//; perm [8 3 11 1 13 9 10 15 0 5 6 2 14 4 12 7 ]
DATA x3_swapdata16_2+0(SB)/8, $0x0000000300000008
DATA x3_swapdata16_2+8(SB)/8, $0x000000010000000B
DATA x3_swapdata16_2+16(SB)/8, $0x000000090000000D
DATA x3_swapdata16_2+24(SB)/8, $0x0000000F0000000A
DATA x3_swapdata16_2+32(SB)/8, $0x0000000500000000
DATA x3_swapdata16_2+40(SB)/8, $0x0000000200000006
DATA x3_swapdata16_2+48(SB)/8, $0x000000040000000E
DATA x3_swapdata16_2+56(SB)/8, $0x000000070000000C
GLOBL x3_swapdata16_2(SB), RODATA|NOPTR, $64

//; swaps {0,1}{2,4}{3,8}{5,6}{7,12}{9,10}{11,13}{14,15}
//; perm [1 0 4 8 2 6 5 12 3 10 9 13 7 11 15 14 ]
DATA x3_swapdata16_3+0(SB)/8, $0x0000000000000001
DATA x3_swapdata16_3+8(SB)/8, $0x0000000800000004
DATA x3_swapdata16_3+16(SB)/8, $0x0000000600000002
DATA x3_swapdata16_3+24(SB)/8, $0x0000000C00000005
DATA x3_swapdata16_3+32(SB)/8, $0x0000000A00000003
DATA x3_swapdata16_3+40(SB)/8, $0x0000000D00000009
DATA x3_swapdata16_3+48(SB)/8, $0x0000000B00000007
DATA x3_swapdata16_3+56(SB)/8, $0x0000000E0000000F
GLOBL x3_swapdata16_3(SB), RODATA|NOPTR, $64

//; swaps {1,3}{2,5}{4,8}{6,9}{7,11}{10,13}{12,14}
//; perm [0 3 5 1 8 2 9 11 4 6 13 7 14 10 12 15 ]
DATA x3_swapdata16_4+0(SB)/8, $0x0000000300000000
DATA x3_swapdata16_4+8(SB)/8, $0x0000000100000005
DATA x3_swapdata16_4+16(SB)/8, $0x0000000200000008
DATA x3_swapdata16_4+24(SB)/8, $0x0000000B00000009
DATA x3_swapdata16_4+32(SB)/8, $0x0000000600000004
DATA x3_swapdata16_4+40(SB)/8, $0x000000070000000D
DATA x3_swapdata16_4+48(SB)/8, $0x0000000A0000000E
DATA x3_swapdata16_4+56(SB)/8, $0x0000000F0000000C
GLOBL x3_swapdata16_4(SB), RODATA|NOPTR, $64

//; swaps {1,2}{3,5}{4,11}{6,8}{7,9}{10,12}{13,14}
//; perm [0 2 1 5 11 3 8 9 6 7 12 4 10 14 13 15 ]
DATA x3_swapdata16_5+0(SB)/8, $0x0000000200000000
DATA x3_swapdata16_5+8(SB)/8, $0x0000000500000001
DATA x3_swapdata16_5+16(SB)/8, $0x000000030000000B
DATA x3_swapdata16_5+24(SB)/8, $0x0000000900000008
DATA x3_swapdata16_5+32(SB)/8, $0x0000000700000006
DATA x3_swapdata16_5+40(SB)/8, $0x000000040000000C
DATA x3_swapdata16_5+48(SB)/8, $0x0000000E0000000A
DATA x3_swapdata16_5+56(SB)/8, $0x0000000F0000000D
GLOBL x3_swapdata16_5(SB), RODATA|NOPTR, $64

//; swaps {2,3}{4,5}{6,7}{8,9}{10,11}{12,13}
//; perm [0 1 3 2 5 4 7 6 9 8 11 10 13 12 14 15 ]
DATA x3_swapdata16_6+0(SB)/8, $0x0000000100000000
DATA x3_swapdata16_6+8(SB)/8, $0x0000000200000003
DATA x3_swapdata16_6+16(SB)/8, $0x0000000400000005
DATA x3_swapdata16_6+24(SB)/8, $0x0000000600000007
DATA x3_swapdata16_6+32(SB)/8, $0x0000000800000009
DATA x3_swapdata16_6+40(SB)/8, $0x0000000A0000000B
DATA x3_swapdata16_6+48(SB)/8, $0x0000000C0000000D
DATA x3_swapdata16_6+56(SB)/8, $0x0000000F0000000E
GLOBL x3_swapdata16_6(SB), RODATA|NOPTR, $64

//; swaps {4,6}{5,7}{8,10}{9,11}
//; perm [0 1 2 3 6 7 4 5 10 11 8 9 12 13 14 15 ]
DATA x3_swapdata16_7+0(SB)/8, $0x0000000100000000
DATA x3_swapdata16_7+8(SB)/8, $0x0000000300000002
DATA x3_swapdata16_7+16(SB)/8, $0x0000000700000006
DATA x3_swapdata16_7+24(SB)/8, $0x0000000500000004
DATA x3_swapdata16_7+32(SB)/8, $0x0000000B0000000A
DATA x3_swapdata16_7+40(SB)/8, $0x0000000900000008
DATA x3_swapdata16_7+48(SB)/8, $0x0000000D0000000C
DATA x3_swapdata16_7+56(SB)/8, $0x0000000F0000000E
GLOBL x3_swapdata16_7(SB), RODATA|NOPTR, $64

//; swaps {3,4}{5,6}{7,8}{9,10}{11,12}
//; perm [0 1 2 4 3 6 5 8 7 10 9 12 11 13 14 15 ]
DATA x3_swapdata16_8+0(SB)/8, $0x0000000100000000
DATA x3_swapdata16_8+8(SB)/8, $0x0000000400000002
DATA x3_swapdata16_8+16(SB)/8, $0x0000000600000003
DATA x3_swapdata16_8+24(SB)/8, $0x0000000800000005
DATA x3_swapdata16_8+32(SB)/8, $0x0000000A00000007
DATA x3_swapdata16_8+40(SB)/8, $0x0000000C00000009
DATA x3_swapdata16_8+48(SB)/8, $0x0000000D0000000B
DATA x3_swapdata16_8+56(SB)/8, $0x0000000F0000000E
GLOBL x3_swapdata16_8(SB), RODATA|NOPTR, $64

//; swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
//; perm [8 9 10 11 12 13 14 15 0 1 2 3 4 5 6 7 ]
DATA x3_swapdata16_w8x8_0+0(SB)/8, $0x0000000900000008
DATA x3_swapdata16_w8x8_0+8(SB)/8, $0x0000000B0000000A
DATA x3_swapdata16_w8x8_0+16(SB)/8, $0x0000000D0000000C
DATA x3_swapdata16_w8x8_0+24(SB)/8, $0x0000000F0000000E
DATA x3_swapdata16_w8x8_0+32(SB)/8, $0x0000000100000000
DATA x3_swapdata16_w8x8_0+40(SB)/8, $0x0000000300000002
DATA x3_swapdata16_w8x8_0+48(SB)/8, $0x0000000500000004
DATA x3_swapdata16_w8x8_0+56(SB)/8, $0x0000000700000006
GLOBL x3_swapdata16_w8x8_0(SB), RODATA|NOPTR, $64

//; swaps {6,10}{7,11}{5,9}{4,8}
//; perm [0 1 2 3 8 9 10 11 4 5 6 7 12 13 14 15 ]
DATA x3_swapdata16_w8x8_1+0(SB)/8, $0x0000000100000000
DATA x3_swapdata16_w8x8_1+8(SB)/8, $0x0000000300000002
DATA x3_swapdata16_w8x8_1+16(SB)/8, $0x0000000900000008
DATA x3_swapdata16_w8x8_1+24(SB)/8, $0x0000000B0000000A
DATA x3_swapdata16_w8x8_1+32(SB)/8, $0x0000000500000004
DATA x3_swapdata16_w8x8_1+40(SB)/8, $0x0000000700000006
DATA x3_swapdata16_w8x8_1+48(SB)/8, $0x0000000D0000000C
DATA x3_swapdata16_w8x8_1+56(SB)/8, $0x0000000F0000000E
GLOBL x3_swapdata16_w8x8_1(SB), RODATA|NOPTR, $64

//; swaps {2,4}{7,9}{10,12}{11,13}{6,8}{3,5}
//; perm [0 1 4 5 2 3 8 9 6 7 12 13 10 11 14 15 ]
DATA x3_swapdata16_w8x8_2+0(SB)/8, $0x0000000100000000
DATA x3_swapdata16_w8x8_2+8(SB)/8, $0x0000000500000004
DATA x3_swapdata16_w8x8_2+16(SB)/8, $0x0000000300000002
DATA x3_swapdata16_w8x8_2+24(SB)/8, $0x0000000900000008
DATA x3_swapdata16_w8x8_2+32(SB)/8, $0x0000000700000006
DATA x3_swapdata16_w8x8_2+40(SB)/8, $0x0000000D0000000C
DATA x3_swapdata16_w8x8_2+48(SB)/8, $0x0000000B0000000A
DATA x3_swapdata16_w8x8_2+56(SB)/8, $0x0000000F0000000E
GLOBL x3_swapdata16_w8x8_2(SB), RODATA|NOPTR, $64

//; swaps {9,10}{7,8}{5,6}{11,12}{13,14}{3,4}{1,2}
//; perm [0 2 1 4 3 6 5 8 7 10 9 12 11 14 13 15 ]
DATA x3_swapdata16_w8x8_3+0(SB)/8, $0x0000000200000000
DATA x3_swapdata16_w8x8_3+8(SB)/8, $0x0000000400000001
DATA x3_swapdata16_w8x8_3+16(SB)/8, $0x0000000600000003
DATA x3_swapdata16_w8x8_3+24(SB)/8, $0x0000000800000005
DATA x3_swapdata16_w8x8_3+32(SB)/8, $0x0000000A00000007
DATA x3_swapdata16_w8x8_3+40(SB)/8, $0x0000000C00000009
DATA x3_swapdata16_w8x8_3+48(SB)/8, $0x0000000E0000000B
DATA x3_swapdata16_w8x8_3+56(SB)/8, $0x0000000F0000000D
GLOBL x3_swapdata16_w8x8_3(SB), RODATA|NOPTR, $64

DATA rol_32+0(SB)/8, $0x000000000000000F
DATA rol_32+8(SB)/8, $0x0000000200000001
DATA rol_32+16(SB)/8, $0x0000000400000003
DATA rol_32+24(SB)/8, $0x0000000600000005
DATA rol_32+32(SB)/8, $0x0000000800000007
DATA rol_32+40(SB)/8, $0x0000000A00000009
DATA rol_32+48(SB)/8, $0x0000000C0000000B
DATA rol_32+56(SB)/8, $0x0000000E0000000D
GLOBL rol_32(SB), RODATA|NOPTR, $64
#define CONST_ROL_32() rol_32(SB)

//; 32bit floating point common constants
//; CONSTF32_NEGATIVE_INF() = uint32(0xFF800000)
//; CONSTF32_POSITIVE_INF() = uint32(0x7F800000)
//; CONSTF32_HALF_PI() = uint32(0x3fc90fdb)
//; CONSTF32_PI_RECI() = uint32(0x3ea2f983)
//; CONSTF32_2_RECI() = uint32(0x3f000000)
//; CONSTF32_16_RECI() = uint32(0x3d800000)
//; CONSTF32_16_TIMES_PI_RECI() = uint32(0x40a2f983)
//; CONSTF32_PI_TIMES_16_RECI() = uint32(0x3e490fdb)

//; #region bcAggTDigest
// _ = aggtdigest.f64(a[0], s[1]).k[2]
TEXT bcAggTDigest(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z6), OUT(Z7), IN(BX), IN(K1), IN(K2))
  BC_UNPACK_RU32(0, OUT(R13))
  KTESTW        K1,  K1                   //;39DACE73 ZF := (K1==0); CF := 1          ;K1=lane_active;
  JZ            next                      //;302CA6F9 jump if zero (ZF = 1)           ;

  ADDQ          R10, R13                  //;D53166C7 agg_local_ptr += agg_data_ptr   ;R13=agg_local_ptr; R10=agg_data_ptr;

//; convert the 16 float64 input values to float32
  VCVTPD2PS     Z6,  Y6                   //;9B75E776 8x float64 -> 8x float32        ;Z6=scratch1;
  VCVTPD2PS     Z7,  Y7                   //;13365A29 8x float64 -> 8x float32        ;Z7=scratch2;
  VINSERTF32X8  $1,  Y7,  Z6,  Z15        //;AE2329B5 assemble 16x float32            ;Z15=mean2; Z6=scratch1; Z7=scratch2;

//; calculate mean maximum
//; reduce_max_ps: IN Z15, K1
  VPBROADCASTD  CONSTF32_NEGATIVE_INF(),Z1 //;DDC16052 bcst constant -inf             ;Z1=inter_result;
  VMOVDQA32     Z15, K1,  Z1              //;DDBA46F2 set dead lanes to -inf          ;Z1=inter_result; K1=lane_active; Z15=mean2;
  VEXTRACTF32X8 $1,  Z1,  Y6              //;C77F6E74 extract top 256 bits            ;Z6=scratch; Z1=inter_result;
  VMAXPS        Y1,  Y6,  Y1              //;B03AEB40 inter_result := max(scratch, inter_result);Z1=inter_result; Z6=scratch;
  VEXTRACTF128  $1,  Y1,  X6              //;6D58D964 extract top 128 bits            ;Z6=scratch; Z1=inter_result;
  VMAXPS        X1,  X6,  X1              //;F39D265F inter_result := max(scratch, inter_result);Z1=inter_result; Z6=scratch;
  VPSRLDQ       $8,  X1,  X6              //;FCBB2777 scratch := inter_result>>8      ;Z6=scratch; Z1=inter_result;
  VMAXPS        X1,  X6,  X1              //;AA319DE0 inter_result := max(scratch, inter_result);Z1=inter_result; Z6=scratch;
  VPSRLDQ       $4,  X1,  X6              //;7B4CEA19 scratch := inter_result>>4      ;Z6=scratch; Z1=inter_result;
  VMAXSS        X1,  X6,  X1              //;F3CF7517 mean_max := max(scratch, inter_result);Z1=mean_max; Z6=scratch; Z1=inter_result;
//; reduce_max_ps: OUT Z1

//; calculate mean minimum
//; reduce_min_ps: IN Z15, K1
  VPBROADCASTD  CONSTF32_POSITIVE_INF(),Z2 //;69727A96 bcst constant +inf             ;Z2=inter_result;
  VMOVDQA32     Z15, K1,  Z2              //;9F647579 set dead lanes to +inf          ;Z2=inter_result; K1=lane_active; Z15=mean2;
  VEXTRACTF32X8 $1,  Z2,  Y6              //;EA38B7F6 extract top 256 bits            ;Z6=scratch; Z2=inter_result;
  VMINPS        Y2,  Y6,  Y2              //;7DC194FB inter_result := min(scratch, inter_result);Z2=inter_result; Z6=scratch;
  VEXTRACTF128  $1,  Y2,  X6              //;E58D41C2 extract top 128 bits            ;Z6=scratch; Z2=inter_result;
  VMINPS        X2,  X6,  X2              //;8060AC48 inter_result := min(scratch, inter_result);Z2=inter_result; Z6=scratch;
  VPSRLDQ       $8,  X2,  X6              //;76E59F7C scratch := inter_result>>8      ;Z6=scratch; Z2=inter_result;
  VMINPS        X2,  X6,  X2              //;1918F621 inter_result := min(scratch, inter_result);Z2=inter_result; Z6=scratch;
  VPSRLDQ       $4,  X2,  X6              //;99C18E05 scratch := inter_result>>4      ;Z6=scratch; Z2=inter_result;
  VMINSS        X2,  X6,  X2              //;6B279DC9 mean_min := min(scratch, inter_result);Z2=mean_min; Z6=scratch; Z2=inter_result;
//; reduce_min_ps: OUT Z2

//; set dead lanes to +inf
  KNOTW         K1,  K3                   //;B8D96637                                 ;K3=tmp_mask; K1=lane_active;
  VPBROADCASTD  CONSTF32_POSITIVE_INF(),K3,  Z15 //;4D75868C                          ;Z15=mean2; K3=tmp_mask;

//; sortnet16_f32: IN Z15
//; swaps {0,5}{1,4}{2,12}{3,13}{6,7}{8,9}{10,15}{11,14}
  VMOVDQU32     x3_swapdata16_0+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_0+0(SB)];Z6=swap_data;
  MOVW          $3407,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {0,2}{1,10}{3,6}{4,7}{5,14}{8,11}{9,12}{13,15}
  VMOVDQU32     x3_swapdata16_1+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_1+0(SB)];Z6=swap_data;
  MOVW          $9019,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {0,8}{1,3}{2,11}{4,13}{5,9}{6,10}{7,15}{12,14}
  VMOVDQU32     x3_swapdata16_2+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_2+0(SB)];Z6=swap_data;
  MOVW          $4343,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {0,1}{2,4}{3,8}{5,6}{7,12}{9,10}{11,13}{14,15}
  VMOVDQU32     x3_swapdata16_3+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_3+0(SB)];Z6=swap_data;
  MOVW          $19117,R11                //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {1,3}{2,5}{4,8}{6,9}{7,11}{10,13}{12,14}
  VMOVDQU32     x3_swapdata16_4+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_4+0(SB)];Z6=swap_data;
  MOVW          $5334,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {1,2}{3,5}{4,11}{6,8}{7,9}{10,12}{13,14}
  VMOVDQU32     x3_swapdata16_5+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_5+0(SB)];Z6=swap_data;
  MOVW          $9434,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {2,3}{4,5}{6,7}{8,9}{10,11}{12,13}
  VMOVDQU32     x3_swapdata16_6+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_6+0(SB)];Z6=swap_data;
  MOVW          $5460,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {4,6}{5,7}{8,10}{9,11}
  VMOVDQU32     x3_swapdata16_7+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_7+0(SB)];Z6=swap_data;
  MOVW          $816,R11                  //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; swaps {3,4}{5,6}{7,8}{9,10}{11,12}
  VMOVDQU32     x3_swapdata16_8+0(SB),Z6  //;41E2065E swap_data := [x3_swapdata16_8+0(SB)];Z6=swap_data;
  MOVW          $2728,R11                 //;556B87C0                                 ;R11=scratch;
  KMOVW         R11, K3                   //;1AEB66EA                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z15, Z6,  Z7              //;BAEF0C4D                                 ;Z7=shuffled_data; Z6=swap_data; Z15=mean2;
  VMINPS        Z15, Z7,  Z6              //;A434C84D min := min(shuffled_data, mean2);Z6=min; Z7=shuffled_data; Z15=mean2;
  VMAXPS        Z15, Z7,  Z7              //;6973CAB2 max := max(shuffled_data, mean2);Z7=max; Z7=shuffled_data; Z15=mean2;
  VPBLENDMD     Z6,  Z7,  K3,  Z15        //;2E2367FF                                 ;Z15=mean2; K3=merge_mask; Z7=max; Z6=min;

//; sortnet16_f32: OUT Z15

  VPBROADCASTD  CONSTF32_1(),Z12          //;A8CF560D bcst constant 1.0f              ;Z12=weight2;
//; calculate weight sum
  KMOVW         K1,  DX                   //;4035BC0A copy mask to gpr                ;DX=len_out; K1=lane_active;
  POPCNTL       DX,  DX                   //;C9096838 count active lanes              ;DX=len_out;
  CVTSL2SS      DX,  X0                   //;730C6124 convert int32 to float32; INTEL CVTSI2SSL;Z0=weigth_sum; DX=len_out;

//; test if data-structure is initialized
  MOVL          12(R13),CX                //;DBDA8EDD load len_in                     ;CX=len_in; R13=agg_local_ptr;
  TESTL         CX,  CX                   //;E4065BE0 at least one value?             ;CX=len_in;
  JZ            init                      //;392E8540 no, then not initialized; jump if zero (ZF = 1);

//; data structure is initialized
  ADDL          DX,  CX                   //;BF6B654A total number of centeroids is existing number of centroids (CX) plus newly added number (DX);CX=len_in; DX=len_out;

  MOVL          (R13),X7                  //;479DBA73 load current total weight from data-structure;Z7=scratch_Z7; R13=agg_local_ptr;
  VADDSS        X0,  X7,  X0              //;C60D1361 weigth_sum += scratch_Z7        ;Z0=weigth_sum; Z7=scratch_Z7;
  MOVL          X0,  (R13)                //;2937EC6B store the new total weight in data-structure;R13=agg_local_ptr; Z0=weigth_sum;
  VPBROADCASTD  X0,  Z28                  //;1F013204 bcst weight_total               ;Z28=weight_total; Z0=weigth_sum;

  MOVL          4(R13),X7                 //;633AF2BF load current mean max from data-structure;Z7=scratch_Z7; R13=agg_local_ptr;
  VMAXSS        X1,  X7,  X1              //;C60D1361 mean_max := max(scratch_Z7, mean_max);Z1=mean_max; Z7=scratch_Z7;
  MOVL          X1,  4(R13)               //;AB3B53D2 store the new mean max in data-structure;R13=agg_local_ptr; Z1=mean_max;

  MOVL          8(R13),X7                 //;3237121F load current mean min from data-structure;Z7=scratch_Z7; R13=agg_local_ptr;
  VMINSS        X2,  X7,  X2              //;C60D1361 mean_min := min(scratch_Z7, mean_min);Z2=mean_min; Z7=scratch_Z7;
  MOVL          X2,  8(R13)               //;2AC2B957 store the new mean min in data-structure;R13=agg_local_ptr; Z2=mean_min;

//; load the mean and weight data
  VMOVDQU32     16+0*64(R13),Z10          //;22F54620 read first 16 weights           ;Z10=weight0; R13=agg_local_ptr;
  VMOVDQU32     16+1*64(R13),Z11          //;6F0A0D1D read next 16 weights            ;Z11=weight1; R13=agg_local_ptr;
  VMOVDQU32     16+2*64(R13),Z13          //;8B7D355A read first 16 means             ;Z13=mean0; R13=agg_local_ptr;
  VMOVDQU32     16+3*64(R13),Z14          //;5EB0A20B read next 16 means              ;Z14=mean1; R13=agg_local_ptr;

//; ------------------------------
//; 2] sort means mean0 (Z13), mean1 (Z14) which are already sorted, and mean2 (Z15) which is already sorted

//; layer1: {{0, 4}, {3, 5}}, // 01-23-45
  VSHUFI64X2    $0b01000100,Z15, Z13, Z6  //;6E073F61 select 0, 4                     ;Z6=dataA0; Z13=mean0; Z15=mean2;
  VSHUFI64X2    $0b11101110,Z15, Z14, Z7  //;5669375D select 3, 5                     ;Z7=dataA1; Z14=mean1; Z15=mean2;
  VSHUFI64X2    $0b01000100,Z12, Z10, Z8  //;924C9B92 select 0, 4                     ;Z8=dataB0; Z10=weight0; Z12=weight2;
  VSHUFI64X2    $0b11101110,Z12, Z11, Z9  //;347E6795 select 3, 5                     ;Z9=dataB1; Z11=weight1; Z12=weight2;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x3_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {6,10}{7,11}{5,9}{4,8}
  VMOVDQU32     x3_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {2,4}{7,9}{10,12}{11,13}{6,8}{3,5}
  VMOVDQU32     x3_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R11                 //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {9,10}{7,8}{5,6}{11,12}{13,14}{3,4}{1,2}
  VMOVDQU32     x3_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R11                //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; sortnet16_w8x8_f32_f32_i2: OUT Z6, Z8, and Z7, Z9

//; layer2: {{2, 4}, {1, 3}}, // 01-23-45
  VMOVDQA32     Z6,  Z25                  //;58897869 store 0                         ;Z25=tmpA1; Z6=dataA0;
  VMOVDQA32     Z7,  Z26                  //;64FDEC5A store 5                         ;Z26=tmpA2; Z7=dataA1;
  VMOVDQA32     Z8,  Z0                   //;BCCF793B store 0                         ;Z0=tmpB1; Z8=dataB0;
  VMOVDQA32     Z9,  Z1                   //;82C82D98 store 5                         ;Z1=tmpB2; Z9=dataB1;
  VSHUFI64X2    $0b11100100,Z6,  Z14, Z6  //;6EA5D5BE overwrite 0 with 2              ;Z6=dataA0; Z14=mean1;
  VSHUFI64X2    $0b01001110,Z7,  Z13, Z7  //;A3B98F72 overwrite 5 with 1              ;Z7=dataA1; Z13=mean0;
  VSHUFI64X2    $0b11100100,Z8,  Z11, Z8  //;68B5268F overwrite 0 with 2              ;Z8=dataB0; Z11=weight1;
  VSHUFI64X2    $0b01001110,Z9,  Z10, Z9  //;61E783CE overwrite 5 with 1              ;Z9=dataB1; Z10=weight0;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x3_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {6,10}{7,11}{5,9}{4,8}
  VMOVDQU32     x3_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {2,4}{7,9}{10,12}{11,13}{6,8}{3,5}
  VMOVDQU32     x3_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R11                 //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {9,10}{7,8}{5,6}{11,12}{13,14}{3,4}{1,2}
  VMOVDQU32     x3_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R11                //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; sortnet16_w8x8_f32_f32_i2: OUT Z6, Z8, and Z7, Z9

//; layer3: {{3, 4}, {1, 2}}, // 01-23-45
  VMOVDQA32     Z6,  Z27                  //;6E5FC32F store 2                         ;Z27=tmpA3; Z6=dataA0;
  VMOVDQA32     Z8,  Z2                   //;467FD394 store 2                         ;Z2=tmpB3; Z8=dataB0;
  VSHUFI64X2    $0b11101110,Z6,  Z7,  Z6  //;9D4C7E0B overwrite 2 with 3              ;Z6=dataA0; Z7=dataA1;
  VSHUFI64X2    $0b01000100,Z27, Z7,  Z7  //;C0A7D6C1 overwrite 3 with 2              ;Z7=dataA1; Z27=tmpA3;
  VSHUFI64X2    $0b11101110,Z8,  Z9,  Z8  //;584A3DEA overwrite 2 with 3              ;Z8=dataB0; Z9=dataB1;
  VSHUFI64X2    $0b01000100,Z2,  Z9,  Z9  //;1108D78A overwrite 3 with 2              ;Z9=dataB1; Z2=tmpB3;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x3_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {6,10}{7,11}{5,9}{4,8}
  VMOVDQU32     x3_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R11                  //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {2,4}{7,9}{10,12}{11,13}{6,8}{3,5}
  VMOVDQU32     x3_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R11                 //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; swaps swaps {9,10}{7,8}{5,6}{11,12}{13,14}{3,4}{1,2}
  VMOVDQU32     x3_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x3_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R11                //;37A1777F                                 ;R11=scratch;
  KMOVW         R11, K3                   //;AFFDB892                                 ;K3=merge_mask; R11=scratch;
  VPERMD        Z6,  Z18, Z17             //;9FE4AA28                                 ;Z17=shuffled_data0; Z18=swap_data; Z6=dataA0;
  VPERMD        Z7,  Z18, Z21             //;7BD6443C                                 ;Z21=shuffled_data1; Z18=swap_data; Z7=dataA1;
  VMINPS        Z6,  Z17, Z16             //;74704523 min0 := min(shuffled_data0, dataA0);Z16=min0; Z17=shuffled_data0; Z6=dataA0;
  VMAXPS        Z6,  Z17, Z17             //;444DF775 max0 := max(shuffled_data0, dataA0);Z17=max0; Z17=shuffled_data0; Z6=dataA0;
  VMINPS        Z7,  Z21, Z19             //;601C3028 min1 := min(shuffled_data1, dataA1);Z19=min1; Z21=shuffled_data1; Z7=dataA1;
  VMAXPS        Z7,  Z21, Z21             //;DA8668B5 max1 := max(shuffled_data1, dataA1);Z21=max1; Z21=shuffled_data1; Z7=dataA1;
  VPBLENDMD     Z16, Z17, K3,  Z16        //;F53A5D90                                 ;Z16=tmp0; K3=merge_mask; Z17=max0; Z16=min0;
  VPBLENDMD     Z19, Z21, K3,  Z19        //;24C6F126                                 ;Z19=tmp1; K3=merge_mask; Z21=max1; Z19=min1;
  VPCMPD        $4,  Z6,  Z16, K4         //;45C64347 K4 := (tmp0!=dataA0)            ;K4=change0_mask; Z16=tmp0; Z6=dataA0;
  VPCMPD        $4,  Z7,  Z19, K3         //;9DE200D0 K3 := (tmp1!=dataA1)            ;K3=change1_mask; Z19=tmp1; Z7=dataA1;
  VMOVDQA32     Z16, Z6                   //;2596ACCF dataA0 := tmp0                  ;Z6=dataA0; Z16=tmp0;
  VMOVDQA32     Z19, Z7                   //;F509A685 dataA1 := tmp1                  ;Z7=dataA1; Z19=tmp1;
  VPERMD        Z8,  Z18, K4,  Z8         //;784213AC                                 ;Z8=dataB0; K4=change0_mask; Z18=swap_data;
  VPERMD        Z9,  Z18, K3,  Z9         //;731E40B9                                 ;Z9=dataB1; K3=change1_mask; Z18=swap_data;

//; sortnet16_w8x8_f32_f32_i2: OUT Z6, Z8, and Z7, Z9

//; restore 01-23-45
  VSHUFI64X2    $0b01000100,Z7,  Z25, Z13 //;21D6DD81 select 0 and 1                  ;Z13=mean0; Z25=tmpA1; Z7=dataA1;
  VSHUFI64X2    $0b01001110,Z6,  Z7,  Z14 //;92F3D2CE select 2 and 3                  ;Z14=mean1; Z7=dataA1; Z6=dataA0;
  VSHUFI64X2    $0b11101110,Z26, Z6,  Z15 //;53BD3F30 select 4 and 5                  ;Z15=mean2; Z6=dataA0; Z26=tmpA2;
  VSHUFI64X2    $0b01000100,Z9,  Z0,  Z10 //;5B62B939 select 0 and 1                  ;Z10=weight0; Z0=tmpB1; Z9=dataB1;
  VSHUFI64X2    $0b01001110,Z8,  Z9,  Z11 //;8E4E731E select 2 and 3                  ;Z11=weight1; Z9=dataB1; Z8=dataB0;
  VSHUFI64X2    $0b11101110,Z1,  Z8,  Z12 //;AB8240FC select 4 and 5                  ;Z12=weight2; Z8=dataB0; Z1=tmpB2;

  LEAQ          16(R13),R14               //;A2FF8F99 ptr_in := agg_local_ptr +  + 16 ;R14=ptr_in; R13=agg_local_ptr;

  VMOVDQU32     Z10, 0*64(R14)            //;91E42A99                                 ;R14=ptr_in; Z10=weight0;
  VMOVDQU32     Z11, 1*64(R14)            //;4B8FF537                                 ;R14=ptr_in; Z11=weight1;
  VMOVDQU32     Z12, 2*64(R14)            //;F5F1C962                                 ;R14=ptr_in; Z12=weight2;
  VMOVDQU32     Z13, 3*64(R14)            //;2BD44D12                                 ;R14=ptr_in; Z13=mean0;
  VMOVDQU32     Z14, 4*64(R14)            //;CF09CFFB                                 ;R14=ptr_in; Z14=mean1;
  VMOVDQU32     Z15, 5*64(R14)            //;BC377191                                 ;R14=ptr_in; Z15=mean2;

//; ------------------------------
//; 3a] calculate the weight sums
//; calc b.f32[0] = a.f32[0]; b.f32[1] = a.f32[0]+a.f32[1]; ... b.f32[15]=a.f32[0] + ... + a.f32[15]

//; reduce_add_ps: IN Z10
  VMOVDQA32     Z10, Z3                   //;2CB57FC6 inter_result := weight0         ;Z3=inter_result; Z10=weight0;
  VEXTRACTF32X8 $1,  Z3,  Y5              //;5E6EFC1D extract top 256 bits            ;Z5=scratch; Z3=inter_result;
  VADDPS        Y3,  Y5,  Y3              //;FE7F9E2F inter_result += scratch         ;Z3=inter_result; Z5=scratch;
  VEXTRACTF128  $1,  Y3,  X5              //;A6AE6FE7 extract top 128 bits            ;Z5=scratch; Z3=inter_result;
  VADDPS        X3,  X5,  X3              //;5B7260C6 inter_result += scratch         ;Z3=inter_result; Z5=scratch;
  VPSRLDQ       $8,  X3,  X5              //;86560EBC scratch := inter_result>>8      ;Z5=scratch; Z3=inter_result;
  VADDPS        X3,  X5,  X3              //;89CDE6C1 inter_result += scratch         ;Z3=inter_result; Z5=scratch;
  VPSRLDQ       $4,  X3,  X5              //;5DE1F1F0 scratch := inter_result>>4      ;Z5=scratch; Z3=inter_result;
  VADDSS        X3,  X5,  X3              //;4F3C27FB tmp0 := scratch + inter_result  ;Z3=tmp0; Z5=scratch; Z3=inter_result;
//; reduce_max_ps: OUT Z3

//; reduce_add_ps: IN Z11
  VMOVDQA32     Z11, Z4                   //;2CB57FC6 inter_result := weight1         ;Z4=inter_result; Z11=weight1;
  VEXTRACTF32X8 $1,  Z4,  Y5              //;5E6EFC1D extract top 256 bits            ;Z5=scratch; Z4=inter_result;
  VADDPS        Y4,  Y5,  Y4              //;FE7F9E2F inter_result += scratch         ;Z4=inter_result; Z5=scratch;
  VEXTRACTF128  $1,  Y4,  X5              //;A6AE6FE7 extract top 128 bits            ;Z5=scratch; Z4=inter_result;
  VADDPS        X4,  X5,  X4              //;5B7260C6 inter_result += scratch         ;Z4=inter_result; Z5=scratch;
  VPSRLDQ       $8,  X4,  X5              //;86560EBC scratch := inter_result>>8      ;Z5=scratch; Z4=inter_result;
  VADDPS        X4,  X5,  X4              //;89CDE6C1 inter_result += scratch         ;Z4=inter_result; Z5=scratch;
  VPSRLDQ       $4,  X4,  X5              //;5DE1F1F0 scratch := inter_result>>4      ;Z5=scratch; Z4=inter_result;
  VADDSS        X4,  X5,  X4              //;4F3C27FB tmp0 := scratch + inter_result  ;Z4=tmp0; Z5=scratch; Z4=inter_result;
//; reduce_max_ps: OUT Z4

//; initialize loop counters
  VMOVDQU32     CONST_ROL_32(),Z5         //;B23CF5F3 rol := [CONST_ROL_32            ;Z5=const_rol;
  VMOVDQA32     Z10, Z0                   //;635F4FFD tmp0 := weight0                 ;Z0=tmp0; Z10=weight0;
  VMOVDQA32     Z11, Z1                   //;49843238 tmp1 := weight1                 ;Z1=tmp1; Z11=weight1;
  VMOVDQA32     Z12, Z2                   //;660B035A tmp2 := weight2                 ;Z2=tmp2; Z12=weight2;
  VPXORD        Z25, Z25, Z25             //;DD339FDA init hsum0 to zero              ;Z25=hsum_weight0;
  VPBROADCASTD  X3,  Z26                  //;4FA8FB48 init                            ;Z26=hsum_weight1; Z3=tmp0;
  VPBROADCASTD  X4,  Z27                  //;AD41396A init                            ;Z27=hsum_weight2; Z4=tmp0;
  VADDPS        Z27, Z26, Z27             //;4E0FC4A6 hsum_weight2 += hsum_weight1    ;Z27=hsum_weight2; Z26=hsum_weight1;
  KXNORW        K3,  K3,  K3              //;34F3DD2D set 0xFFFF                      ;K3=tmp_mask;
  MOVL          $16, DX                   //;B9E49693 init loop counter               ;DX=counter;

loop:
  VADDPS        Z25, Z0,  K3,  Z25        //;2DDBEBEA hsum_weight0 += tmp0            ;Z25=hsum_weight0; K3=tmp_mask; Z0=tmp0;
  VADDPS        Z26, Z1,  K3,  Z26        //;51BCC680 hsum_weight1 += tmp1            ;Z26=hsum_weight1; K3=tmp_mask; Z1=tmp1;
  VADDPS        Z27, Z2,  K3,  Z27        //;9369D5C0 hsum_weight2 += tmp2            ;Z27=hsum_weight2; K3=tmp_mask; Z2=tmp2;
  KSHIFTLW      $1,  K3,  K3              //;B9D1EF40                                 ;K3=tmp_mask;
  VPERMD        Z0,  Z5,  Z0              //;3CCC71A7 Z0 := ROL 32                    ;Z0=tmp0; Z5=const_rol;
  VPERMD        Z1,  Z5,  Z1              //;8DB38C3F Z1 <<= 32                       ;Z1=tmp1; Z5=const_rol;
  VPERMD        Z2,  Z5,  Z2              //;41891DCF Z2 <<= 32                       ;Z2=tmp2; Z5=const_rol;
  DECL          DX                        //;245FFC3A counter--                       ;DX=counter;
  JNZ           loop                      //;30E184E3 jump if not zero (ZF = 0)       ;

//; shift combined Z21, Z22 and Z23 32bits to left
  VPERMD        Z25, Z5,  Z25             //;3CC441DE                                 ;Z25=hsum_weight0; Z5=const_rol;
  VPERMD        Z26, Z5,  Z26             //;CF7D0288                                 ;Z26=hsum_weight1; Z5=const_rol;
  VPERMD        Z27, Z5,  Z27             //;B59C2F36                                 ;Z27=hsum_weight2; Z5=const_rol;
  MOVL          $1,  R11                  //;E9735195                                 ;R11=scratch;
  KMOVW         R11, K3                   //;E867D490                                 ;K3=tmp_mask; R11=scratch;
  VMOVDQA32     Z26, K3,  Z27             //;B211E4BC hsum_weight2 := hsum_weight1    ;Z27=hsum_weight2; K3=tmp_mask; Z26=hsum_weight1;
  VMOVDQA32     Z25, K3,  Z26             //;3D7A13AA hsum_weight1 := hsum_weight0    ;Z26=hsum_weight1; K3=tmp_mask; Z25=hsum_weight0;
  VPXORD        Z25, Z25, K3,  Z25        //;F3F2A805 hsum_weight0 := 0               ;Z25=hsum_weight0; K3=tmp_mask;

//; ------------------------------
//; 3b] calculate the weight limits
  KXNORW        K1,  K1,  K1              //;7E34F70F sin is calculated for 16 lanes  ;K1=lane_active;

  VDIVPS        Z28, Z25, Z0              //;D3256B7C interm := hsum_weight0 / weight_total;Z0=interm; Z25=hsum_weight0; Z28=weight_total;
  VMULPS.BCST   CONSTF32_2(),Z0,  Z0      //;2CC7F539 interm := interm * 2            ;Z0=interm;
  VSUBPS.BCST   CONSTF32_1(),Z0,  Z0      //;2E3C326F interm--                        ;Z0=interm;
  CALL          asinf32(SB)               //;AF60F98E INOUT Z0; destroyed: Z2..Z25    ;
  VADDPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;4DD2F908 interm += HALF_PI              ;Z0=interm;
  VMULPS.BCST   CONSTF32_16_TIMES_PI_RECI(),Z0,  Z0 //;BD2CABC9 interm := interm * 16_TIMES_PI_RECI;Z0=interm;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;BF419CC0 interm++                        ;Z0=interm;
  VMINPS.BCST   CONSTF32_16(),Z0,  Z0     //;F1B7194C interm := min(interm, 16)       ;Z0=interm;
  VMULPS.BCST   CONSTF32_PI_TIMES_16_RECI(),Z0,  Z0 //;D4246F72 interm := interm * PI_TIMES_16_RECI;Z0=interm;
  VSUBPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;7AE6857D interm -= HALF_PI              ;Z0=interm;
  CALL          sinf32(SB)                //;C8A18194 INOUT Z0; destroyed: R8, R15, Z2..Z25;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;29F1DE62 interm++                        ;Z0=interm;
  VMULPS.BCST   CONSTF32_2_RECI(),Z0,  Z0 //;652B20D3 interm := interm * 2_RECI       ;Z0=interm;
  VMULPS        Z28, Z0,  Z0              //;3E91F7DD interm := interm * weight_total ;Z0=interm; Z28=weight_total;
  VMOVDQU32     Z0,  6*64(R14)            //;851EFAE0 store weight limits 0           ;R14=ptr_in; Z0=interm;

  VDIVPS        Z28, Z26, Z0              //;DC878E2D interm := hsum_weight1 / weight_total;Z0=interm; Z26=hsum_weight1; Z28=weight_total;
  VMULPS.BCST   CONSTF32_2(),Z0,  Z0      //;2CC7F539 interm := interm * 2            ;Z0=interm;
  VSUBPS.BCST   CONSTF32_1(),Z0,  Z0      //;2E3C326F interm--                        ;Z0=interm;
  CALL          asinf32(SB)               //;AF60F98E INOUT Z0; destroyed: Z2..Z25    ;
  VADDPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;4DD2F908 interm += HALF_PI              ;Z0=interm;
  VMULPS.BCST   CONSTF32_16_TIMES_PI_RECI(),Z0,  Z0 //;BD2CABC9 interm := interm * 16_TIMES_PI_RECI;Z0=interm;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;BF419CC0 interm++                        ;Z0=interm;
  VMINPS.BCST   CONSTF32_16(),Z0,  Z0     //;F1B7194C interm := min(interm, 16)       ;Z0=interm;
  VMULPS.BCST   CONSTF32_PI_TIMES_16_RECI(),Z0,  Z0 //;D4246F72 interm := interm * PI_TIMES_16_RECI;Z0=interm;
  VSUBPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;7AE6857D interm -= HALF_PI              ;Z0=interm;
  CALL          sinf32(SB)                //;C8A18194 INOUT Z0; destroyed: R8, R15, Z2..Z25;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;29F1DE62 interm++                        ;Z0=interm;
  VMULPS.BCST   CONSTF32_2_RECI(),Z0,  Z0 //;652B20D3 interm := interm * 2_RECI       ;Z0=interm;
  VMULPS        Z28, Z0,  Z0              //;3DC14449 interm := interm * weight_total ;Z0=interm; Z28=weight_total;
  VMOVDQU32     Z0,  7*64(R14)            //;3D97C714 store weight limits 1           ;R14=ptr_in; Z0=interm;

  VDIVPS        Z28, Z27, Z0              //;6776A70F interm := hsum_weight2 / weight_total;Z0=interm; Z27=hsum_weight2; Z28=weight_total;
  VMULPS.BCST   CONSTF32_2(),Z0,  Z0      //;2CC7F539 interm := interm * 2            ;Z0=interm;
  VSUBPS.BCST   CONSTF32_1(),Z0,  Z0      //;2E3C326F interm--                        ;Z0=interm;
  CALL          asinf32(SB)               //;AF60F98E INOUT Z0; destroyed: Z2..Z25    ;
  VADDPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;4DD2F908 interm += HALF_PI              ;Z0=interm;
  VMULPS.BCST   CONSTF32_16_TIMES_PI_RECI(),Z0,  Z0 //;BD2CABC9 interm := interm * 16_TIMES_PI_RECI;Z0=interm;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;BF419CC0 interm++                        ;Z0=interm;
  VMINPS.BCST   CONSTF32_16(),Z0,  Z0     //;F1B7194C interm := min(interm, 16)       ;Z0=interm;
  VMULPS.BCST   CONSTF32_PI_TIMES_16_RECI(),Z0,  Z0 //;D4246F72 interm := interm * PI_TIMES_16_RECI;Z0=interm;
  VSUBPS.BCST   CONSTF32_HALF_PI(),Z0,  Z0 //;7AE6857D interm -= HALF_PI              ;Z0=interm;
  CALL          sinf32(SB)                //;C8A18194 INOUT Z0; destroyed: R8, R15, Z2..Z25;
  VADDPS.BCST   CONSTF32_1(),Z0,  Z0      //;29F1DE62 interm++                        ;Z0=interm;
  VMULPS.BCST   CONSTF32_2_RECI(),Z0,  Z0 //;652B20D3 interm := interm * 2_RECI       ;Z0=interm;
  VMULPS        Z28, Z0,  Z0              //;78F1A289 interm := interm * weight_total ;Z0=interm; Z28=weight_total;
  VMOVDQU32     Z0,  8*64(R14)            //;3706F680 store weight limits 2           ;R14=ptr_in; Z0=interm;

//; ------------------------------
//; 4] compress the centroids
//; memory layout:  wIn[0..48], mIn[0..48], wLim[0..48], wOut[0..48], mOut[0..48]
//; wIn:  offset 0*64
//; mIn:  offset 3*64
//; wLim: offset 6*64
//; wOut: offset 9*64
//; mOut: offset 12*64

  XORL          DX,  DX                   //;69CECAEC clear counter                   ;DX=len_out;
  LEAQ          (9*64)+16(R13),R11        //;5638AE38 ptr_out := agg_local_ptr +  + (9*64)+16;R11=ptr_out; R13=agg_local_ptr;

  VPXORD        Z0,  Z0,  Z0              //;6BA9B214 scratch := 0                    ;Z0=scratch;
  VMOVDQU32     Z0,  0*64(R11)            //;E1240A74 set weights to zero             ;R11=ptr_out; Z0=scratch;
  VMOVDQU32     Z0,  1*64(R11)            //;494CE8BD set weights to zero             ;R11=ptr_out; Z0=scratch;

  VPBROADCASTD  CONSTF32_POSITIVE_INF(),Z0 //;B5198451                                ;Z0=scratch;
  VMOVDQU32     Z0,  2*64(R11)            //;618F3945 set mean to +inf                ;R11=ptr_out; Z0=scratch;
  VMOVDQU32     Z0,  3*64(R11)            //;3414844D set mean to +inf                ;R11=ptr_out; Z0=scratch;

  MOVSS         0*64(R14),X1              //;8B6342CE load wIn[0]                     ;Z1=weight_sum; R14=ptr_in;
  MOVSS         3*64(R14),X3              //;65E5F41C load mIn[0]                     ;Z3=mean_i; R14=ptr_in;
  MOVSS         6*64(R14),X0              //;6C3162CA load weightLimit[0]             ;Z0=weight_lim; R14=ptr_in;
  ADDQ          $4,  R14                  //;6F1A8661 ptr_in += 4                     ;R14=ptr_in;

  MOVSS         X1,  0*64(R11)            //;7515A465 wOut[0] = wIn[0]                ;R11=ptr_out; Z1=weight_sum;
  MOVSS         X3,  2*64(R11)            //;91212D40 mOut[0] = mIn[0]                ;R11=ptr_out; Z3=mean_i;
  ADDQ          $4,  R11                  //;7C1BC149 ptr_out += 4                    ;R11=ptr_out;
  INCL          DX                        //;DD68CB22 len_out++                       ;DX=len_out;
  DECL          CX                        //;3865377E len_in--                        ;CX=len_in;
  JZ            choice_done_tail          //;165FEDBD jump if zero (ZF = 1)           ;

loop_compress:

  MOVSS         0*64(R14),X2              //;A3546018 load wi                         ;Z2=weight_i; R14=ptr_in;
  MOVSS         3*64(R14),X3              //;9DF131DA load mi                         ;Z3=mean_i; R14=ptr_in;
  VADDSS        X1,  X2,  X1              //;BC899420 wSum += wi                      ;Z1=weight_sum; Z2=weight_i;

  VCMPSS        $2,  X0,  X1,  K3         //;2DAC5790 K3 := (weight_sum<=weight_lim)  ;K3=scratch_mask; Z1=weight_sum; Z0=weight_lim; 2=LessEq;
  KTESTB        K3,  K3                   //;2E211104 ZF := (K3==0); CF := 1          ;K3=scratch_mask;
  JZ            choice                    //;94FD713A jump if zero (ZF = 1)           ;

  MOVSS         (0*64)-4(R11),X4          //;BB2B0F31 y0 := wOut[n-1]                 ;Z4=y0; R11=ptr_out;
  MOVSS         (2*64)-4(R11),X5          //;67718C1F x0 := mOut[n-1]                 ;Z5=x0; R11=ptr_out;

  VADDSS        X2,  X4,  X4              //;6F5EB680 y0 += weight_i                  ;Z4=y0; Z2=weight_i;
  VSUBSS        X5,  X3,  X6              //;D4A09014 tmp := mean_i - x0              ;Z6=tmp; Z3=mean_i; Z5=x0;
  VMULSS        X2,  X6,  X6              //;DDA38CD6 tmp := tmp * weight_i           ;Z6=tmp; Z2=weight_i;
  VDIVSS        X4,  X6,  X6              //;5E181EAE tmp := tmp / y0                 ;Z6=tmp; Z4=y0;
  VADDSS        X5,  X6,  X6              //;AEDC7730 tmp += x0                       ;Z6=tmp; Z5=x0;

  MOVSS         X4,  (0*64)-4(R11)        //;AF6A7C98 wOut[n-1] = y0                  ;R11=ptr_out; Z4=y0;
  MOVSS         X6,  (2*64)-4(R11)        //;F7DA49F2 mOut[n-1] = x4                  ;R11=ptr_out; Z6=tmp;
  JMP           choice_done               //;3F9CD43C                                 ;

choice:
  MOVSS         6*64(R14),X0              //;91D088F6 load weightLimit[i]             ;Z0=weight_lim; R14=ptr_in;
  MOVSS         X2,  0*64(R11)            //;A95068F6 wOut[n] = wi                    ;R11=ptr_out; Z2=weight_i;
  MOVSS         X3,  2*64(R11)            //;F9217450 mOut[n] = mi                    ;R11=ptr_out; Z3=mean_i;
  ADDQ          $4,  R11                  //;C427EC39 ptr_out += 4                    ;R11=ptr_out;
  INCL          DX                        //;31509E20 len_out++                       ;DX=len_out;

choice_done:
  ADDQ          $4,  R14                  //;C22614F0 ptr_in += 4                     ;R14=ptr_in;
  DECL          CX                        //;107A221F len_in--                        ;CX=len_in;
  JNZ           loop_compress             //;76537A6C jump if not zero (ZF = 0)       ;
choice_done_tail:

//; ------------------------------
//; 5] retrieve the results from the tmp data-structure
  VMOVDQU32     (9*64)+16(R13),Z10        //;4566E094 weight0 := [agg_local_ptr+(9*64)+16];Z10=weight0; R13=agg_local_ptr;
  VMOVDQU32     (10*64)+16(R13),Z11       //;FFC297B3 weight1 := [agg_local_ptr+(10*64)+16];Z11=weight1; R13=agg_local_ptr;
  VMOVDQU32     (11*64)+16(R13),Z13       //;9F8F3D73 mean0 := [agg_local_ptr+(11*64)+16];Z13=mean0; R13=agg_local_ptr;
  VMOVDQU32     (12*64)+16(R13),Z14       //;14EBF763 mean1 := [agg_local_ptr+(12*64)+16];Z14=mean1; R13=agg_local_ptr;

  JMP           sync_data                 //;00000000                                 ;

init:
//; store mean and weights data; layout: 2x16 weights; 2x16 means (means are sorted)
  VMOVDQA32     Z15, Z13                  //;42D9629C mean0 := mean2                  ;Z13=mean0; Z15=mean2;
  VPBROADCASTD  CONSTF32_POSITIVE_INF(),Z14 //;C91D3F8E                               ;Z14=mean1;
  VMOVDQA32     Z12, Z10                  //;B848296B weight0 := weight2              ;Z10=weight0; Z12=weight2;
  VPXORD        Z11, Z11, Z11             //;4E4A4410 weight1 := 0                    ;Z11=weight1;

  MOVL          X0,  (R13)                //;A41C6C61 store weight sum                ;R13=agg_local_ptr; Z0=weigth_sum;
  MOVL          X1,  4(R13)               //;A8ECB213 store mean maximum              ;R13=agg_local_ptr; Z1=mean_max;
  MOVL          X2,  8(R13)               //;886588A7 store mean minimum              ;R13=agg_local_ptr; Z2=mean_min;

sync_data:
  MOVL          DX,  12(R13)              //;DAD9877D store len_out                   ;R13=agg_local_ptr; DX=len_out;
  VMOVDQU32     Z10, (0*64)+16(R13)       //;D4F8C992 write weights0                  ;R13=agg_local_ptr; Z10=weight0;
  VMOVDQU32     Z11, (1*64)+16(R13)       //;8AB10EA4 write weights1                  ;R13=agg_local_ptr; Z11=weight1;
  VMOVDQU32     Z13, (2*64)+16(R13)       //;BEC3C605 write sorted mean0              ;R13=agg_local_ptr; Z13=mean0;
  VMOVDQU32     Z14, (3*64)+16(R13)       //;6FE46FEC write sorted mean1              ;R13=agg_local_ptr; Z14=mean1;

next:
  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)
//; #endregion bcAggTDigest
