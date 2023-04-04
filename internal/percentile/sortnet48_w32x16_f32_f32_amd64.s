// Copyright (C) 2023 Sneller, Inc.
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

//; swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
//; perm [8 9 10 11 12 13 14 15 0 1 2 3 4 5 6 7 ]
DATA x2_swapdata16_w8x8_0+0(SB)/8, $0x0000000900000008
DATA x2_swapdata16_w8x8_0+8(SB)/8, $0x0000000B0000000A
DATA x2_swapdata16_w8x8_0+16(SB)/8, $0x0000000D0000000C
DATA x2_swapdata16_w8x8_0+24(SB)/8, $0x0000000F0000000E
DATA x2_swapdata16_w8x8_0+32(SB)/8, $0x0000000100000000
DATA x2_swapdata16_w8x8_0+40(SB)/8, $0x0000000300000002
DATA x2_swapdata16_w8x8_0+48(SB)/8, $0x0000000500000004
DATA x2_swapdata16_w8x8_0+56(SB)/8, $0x0000000700000006
GLOBL x2_swapdata16_w8x8_0(SB), RODATA|NOPTR, $64

//; swaps {6,10}{7,11}{5,9}{4,8}
//; perm [0 1 2 3 8 9 10 11 4 5 6 7 12 13 14 15 ]
DATA x2_swapdata16_w8x8_1+0(SB)/8, $0x0000000100000000
DATA x2_swapdata16_w8x8_1+8(SB)/8, $0x0000000300000002
DATA x2_swapdata16_w8x8_1+16(SB)/8, $0x0000000900000008
DATA x2_swapdata16_w8x8_1+24(SB)/8, $0x0000000B0000000A
DATA x2_swapdata16_w8x8_1+32(SB)/8, $0x0000000500000004
DATA x2_swapdata16_w8x8_1+40(SB)/8, $0x0000000700000006
DATA x2_swapdata16_w8x8_1+48(SB)/8, $0x0000000D0000000C
DATA x2_swapdata16_w8x8_1+56(SB)/8, $0x0000000F0000000E
GLOBL x2_swapdata16_w8x8_1(SB), RODATA|NOPTR, $64

//; swaps {2,4}{7,9}{10,12}{11,13}{6,8}{3,5}
//; perm [0 1 4 5 2 3 8 9 6 7 12 13 10 11 14 15 ]
DATA x2_swapdata16_w8x8_2+0(SB)/8, $0x0000000100000000
DATA x2_swapdata16_w8x8_2+8(SB)/8, $0x0000000500000004
DATA x2_swapdata16_w8x8_2+16(SB)/8, $0x0000000300000002
DATA x2_swapdata16_w8x8_2+24(SB)/8, $0x0000000900000008
DATA x2_swapdata16_w8x8_2+32(SB)/8, $0x0000000700000006
DATA x2_swapdata16_w8x8_2+40(SB)/8, $0x0000000D0000000C
DATA x2_swapdata16_w8x8_2+48(SB)/8, $0x0000000B0000000A
DATA x2_swapdata16_w8x8_2+56(SB)/8, $0x0000000F0000000E
GLOBL x2_swapdata16_w8x8_2(SB), RODATA|NOPTR, $64

//; swaps {9,10}{7,8}{5,6}{11,12}{13,14}{3,4}{1,2}
//; perm [0 2 1 4 3 6 5 8 7 10 9 12 11 14 13 15 ]
DATA x2_swapdata16_w8x8_3+0(SB)/8, $0x0000000200000000
DATA x2_swapdata16_w8x8_3+8(SB)/8, $0x0000000400000001
DATA x2_swapdata16_w8x8_3+16(SB)/8, $0x0000000600000003
DATA x2_swapdata16_w8x8_3+24(SB)/8, $0x0000000800000005
DATA x2_swapdata16_w8x8_3+32(SB)/8, $0x0000000A00000007
DATA x2_swapdata16_w8x8_3+40(SB)/8, $0x0000000C00000009
DATA x2_swapdata16_w8x8_3+48(SB)/8, $0x0000000E0000000B
DATA x2_swapdata16_w8x8_3+56(SB)/8, $0x0000000F0000000D
GLOBL x2_swapdata16_w8x8_3(SB), RODATA|NOPTR, $64

//; #region Sortnet48w32x16f32f32
TEXT ·Sortnet48w32x16f32f32(SB), NOSPLIT|NOFRAME, $0
  MOVQ          data+0(FP),R8             //;398BE831                                 ;R8=scratch;
  VMOVDQU32     0*64(R8),Z0               //;82D25AA4 dataA0 := [scratch+0*64]        ;Z0=dataA0; R8=scratch;
  VMOVDQU32     1*64(R8),Z1               //;92F72918 dataA1 := [scratch+1*64]        ;Z1=dataA1; R8=scratch;
  VMOVDQU32     2*64(R8),Z2               //;98507173 dataA2 := [scratch+2*64]        ;Z2=dataA2; R8=scratch;
  VMOVDQU32     3*64(R8),Z3               //;1CB3C655 dataB0 := [scratch+3*64]        ;Z3=dataB0; R8=scratch;
  VMOVDQU32     4*64(R8),Z4               //;8B61F525 dataB1 := [scratch+4*64]        ;Z4=dataB1; R8=scratch;
  VMOVDQU32     5*64(R8),Z5               //;CEE04CCB dataB2 := [scratch+5*64]        ;Z5=dataB2; R8=scratch;

//; layer1: {{0, 4}, {3, 5}}, // 01-23-45
  VSHUFI64X2    $0b01000100,Z2,  Z0,  Z6  //;6E073F61 select 0, 4                     ;Z6=dataA0; Z0=dataA0; Z2=dataA2;
  VSHUFI64X2    $0b11101110,Z2,  Z1,  Z7  //;5669375D select 3, 5                     ;Z7=dataA1; Z1=dataA1; Z2=dataA2;
  VSHUFI64X2    $0b01000100,Z5,  Z3,  Z8  //;924C9B92 select 0, 4                     ;Z8=dataB0; Z3=dataB0; Z5=dataB2;
  VSHUFI64X2    $0b11101110,Z5,  Z4,  Z9  //;347E6795 select 3, 5                     ;Z9=dataB1; Z4=dataB1; Z5=dataB2;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x2_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R15                 //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R15                //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQA32     Z6,  Z10                  //;58897869 store 0                         ;Z10=tmpA1; Z6=dataA0;
  VMOVDQA32     Z7,  Z11                  //;64FDEC5A store 5                         ;Z11=tmpA2; Z7=dataA1;
  VMOVDQA32     Z8,  Z13                  //;BCCF793B store 0                         ;Z13=tmpB1; Z8=dataB0;
  VMOVDQA32     Z9,  Z14                  //;82C82D98 store 5                         ;Z14=tmpB2; Z9=dataB1;
  VSHUFI64X2    $0b11100100,Z6,  Z1,  Z6  //;6EA5D5BE overwrite 0 with 2              ;Z6=dataA0; Z1=dataA1;
  VSHUFI64X2    $0b01001110,Z7,  Z0,  Z7  //;A3B98F72 overwrite 5 with 1              ;Z7=dataA1; Z0=dataA0;
  VSHUFI64X2    $0b11100100,Z8,  Z4,  Z8  //;68B5268F overwrite 0 with 2              ;Z8=dataB0; Z4=dataB1;
  VSHUFI64X2    $0b01001110,Z9,  Z3,  Z9  //;61E783CE overwrite 5 with 1              ;Z9=dataB1; Z3=dataB0;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x2_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R15                 //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R15                //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQA32     Z6,  Z12                  //;6E5FC32F store 2                         ;Z12=tmpA3; Z6=dataA0;
  VMOVDQA32     Z8,  Z15                  //;467FD394 store 2                         ;Z15=tmpB3; Z8=dataB0;
  VSHUFI64X2    $0b11101110,Z6,  Z7,  Z6  //;9D4C7E0B overwrite 2 with 3              ;Z6=dataA0; Z7=dataA1;
  VSHUFI64X2    $0b01000100,Z12, Z7,  Z7  //;C0A7D6C1 overwrite 3 with 2              ;Z7=dataA1; Z12=tmpA3;
  VSHUFI64X2    $0b11101110,Z8,  Z9,  Z8  //;584A3DEA overwrite 2 with 3              ;Z8=dataB0; Z9=dataB1;
  VSHUFI64X2    $0b01000100,Z15, Z9,  Z9  //;1108D78A overwrite 3 with 2              ;Z9=dataB1; Z15=tmpB3;

//; sortnet16_w8x8_f32_f32_i2: IN Z6, Z8, and Z7, Z9
//; swaps swaps {0,8}{2,10}{7,15}{6,14}{3,11}{1,9}{5,13}{4,12}
  VMOVDQU32     x2_swapdata16_w8x8_0+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_0+0(SB)];Z18=swap_data;
  MOVW          $255,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_1+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_1+0(SB)];Z18=swap_data;
  MOVW          $240,R15                  //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_2+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_2+0(SB)];Z18=swap_data;
  MOVW          $3276,R15                 //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VMOVDQU32     x2_swapdata16_w8x8_3+0(SB),Z18 //;E7F5C275 swap_data := [x2_swapdata16_w8x8_3+0(SB)];Z18=swap_data;
  MOVW          $10922,R15                //;37A1777F                                 ;R15=scratch;
  KMOVW         R15, K3                   //;AFFDB892                                 ;K3=merge_mask; R15=scratch;
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
  VSHUFI64X2    $0b01000100,Z7,  Z10, Z0  //;21D6DD81 select 0 and 1                  ;Z0=dataA0; Z10=tmpA1; Z7=dataA1;
  VSHUFI64X2    $0b01001110,Z6,  Z7,  Z1  //;92F3D2CE select 2 and 3                  ;Z1=dataA1; Z7=dataA1; Z6=dataA0;
  VSHUFI64X2    $0b11101110,Z11, Z6,  Z2  //;53BD3F30 select 4 and 5                  ;Z2=dataA2; Z6=dataA0; Z11=tmpA2;
  VSHUFI64X2    $0b01000100,Z9,  Z13, Z3  //;5B62B939 select 0 and 1                  ;Z3=dataB0; Z13=tmpB1; Z9=dataB1;
  VSHUFI64X2    $0b01001110,Z8,  Z9,  Z4  //;8E4E731E select 2 and 3                  ;Z4=dataB1; Z9=dataB1; Z8=dataB0;
  VSHUFI64X2    $0b11101110,Z14, Z8,  Z5  //;AB8240FC select 4 and 5                  ;Z5=dataB2; Z8=dataB0; Z14=tmpB2;

  VMOVDQU32     Z0,  0*64(R8)             //;DD0D2D34                                 ;R8=scratch; Z0=dataA0;
  VMOVDQU32     Z1,  1*64(R8)             //;E5724E07                                 ;R8=scratch; Z1=dataA1;
  VMOVDQU32     Z2,  2*64(R8)             //;6A433FBC                                 ;R8=scratch; Z2=dataA2;
  VMOVDQU32     Z3,  3*64(R8)             //;21F1348A                                 ;R8=scratch; Z3=dataB0;
  VMOVDQU32     Z4,  4*64(R8)             //;C0B18087                                 ;R8=scratch; Z4=dataB1;
  VMOVDQU32     Z5,  5*64(R8)             //;29CC4A81                                 ;R8=scratch; Z5=dataB2;
  RET                                     //;A2BC0265                                 ;
//; #endregion Sortnet48w32x16f32f32
