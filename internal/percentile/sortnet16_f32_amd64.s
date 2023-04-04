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

//; swaps {0,5}{1,4}{2,12}{3,13}{6,7}{8,9}{10,15}{11,14}
//; perm [5 4 12 13 1 0 7 6 9 8 15 14 2 3 11 10 ]
DATA x_swapdata16_0+0(SB)/8, $0x0000000400000005
DATA x_swapdata16_0+8(SB)/8, $0x0000000D0000000C
DATA x_swapdata16_0+16(SB)/8, $0x0000000000000001
DATA x_swapdata16_0+24(SB)/8, $0x0000000600000007
DATA x_swapdata16_0+32(SB)/8, $0x0000000800000009
DATA x_swapdata16_0+40(SB)/8, $0x0000000E0000000F
DATA x_swapdata16_0+48(SB)/8, $0x0000000300000002
DATA x_swapdata16_0+56(SB)/8, $0x0000000A0000000B
GLOBL x_swapdata16_0(SB), RODATA|NOPTR, $64

//; swaps {0,2}{1,10}{3,6}{4,7}{5,14}{8,11}{9,12}{13,15}
//; perm [2 10 0 6 7 14 3 4 11 12 1 8 9 15 5 13 ]
DATA x_swapdata16_1+0(SB)/8, $0x0000000A00000002
DATA x_swapdata16_1+8(SB)/8, $0x0000000600000000
DATA x_swapdata16_1+16(SB)/8, $0x0000000E00000007
DATA x_swapdata16_1+24(SB)/8, $0x0000000400000003
DATA x_swapdata16_1+32(SB)/8, $0x0000000C0000000B
DATA x_swapdata16_1+40(SB)/8, $0x0000000800000001
DATA x_swapdata16_1+48(SB)/8, $0x0000000F00000009
DATA x_swapdata16_1+56(SB)/8, $0x0000000D00000005
GLOBL x_swapdata16_1(SB), RODATA|NOPTR, $64

//; swaps {0,8}{1,3}{2,11}{4,13}{5,9}{6,10}{7,15}{12,14}
//; perm [8 3 11 1 13 9 10 15 0 5 6 2 14 4 12 7 ]
DATA x_swapdata16_2+0(SB)/8, $0x0000000300000008
DATA x_swapdata16_2+8(SB)/8, $0x000000010000000B
DATA x_swapdata16_2+16(SB)/8, $0x000000090000000D
DATA x_swapdata16_2+24(SB)/8, $0x0000000F0000000A
DATA x_swapdata16_2+32(SB)/8, $0x0000000500000000
DATA x_swapdata16_2+40(SB)/8, $0x0000000200000006
DATA x_swapdata16_2+48(SB)/8, $0x000000040000000E
DATA x_swapdata16_2+56(SB)/8, $0x000000070000000C
GLOBL x_swapdata16_2(SB), RODATA|NOPTR, $64

//; swaps {0,1}{2,4}{3,8}{5,6}{7,12}{9,10}{11,13}{14,15}
//; perm [1 0 4 8 2 6 5 12 3 10 9 13 7 11 15 14 ]
DATA x_swapdata16_3+0(SB)/8, $0x0000000000000001
DATA x_swapdata16_3+8(SB)/8, $0x0000000800000004
DATA x_swapdata16_3+16(SB)/8, $0x0000000600000002
DATA x_swapdata16_3+24(SB)/8, $0x0000000C00000005
DATA x_swapdata16_3+32(SB)/8, $0x0000000A00000003
DATA x_swapdata16_3+40(SB)/8, $0x0000000D00000009
DATA x_swapdata16_3+48(SB)/8, $0x0000000B00000007
DATA x_swapdata16_3+56(SB)/8, $0x0000000E0000000F
GLOBL x_swapdata16_3(SB), RODATA|NOPTR, $64

//; swaps {1,3}{2,5}{4,8}{6,9}{7,11}{10,13}{12,14}
//; perm [0 3 5 1 8 2 9 11 4 6 13 7 14 10 12 15 ]
DATA x_swapdata16_4+0(SB)/8, $0x0000000300000000
DATA x_swapdata16_4+8(SB)/8, $0x0000000100000005
DATA x_swapdata16_4+16(SB)/8, $0x0000000200000008
DATA x_swapdata16_4+24(SB)/8, $0x0000000B00000009
DATA x_swapdata16_4+32(SB)/8, $0x0000000600000004
DATA x_swapdata16_4+40(SB)/8, $0x000000070000000D
DATA x_swapdata16_4+48(SB)/8, $0x0000000A0000000E
DATA x_swapdata16_4+56(SB)/8, $0x0000000F0000000C
GLOBL x_swapdata16_4(SB), RODATA|NOPTR, $64

//; swaps {1,2}{3,5}{4,11}{6,8}{7,9}{10,12}{13,14}
//; perm [0 2 1 5 11 3 8 9 6 7 12 4 10 14 13 15 ]
DATA x_swapdata16_5+0(SB)/8, $0x0000000200000000
DATA x_swapdata16_5+8(SB)/8, $0x0000000500000001
DATA x_swapdata16_5+16(SB)/8, $0x000000030000000B
DATA x_swapdata16_5+24(SB)/8, $0x0000000900000008
DATA x_swapdata16_5+32(SB)/8, $0x0000000700000006
DATA x_swapdata16_5+40(SB)/8, $0x000000040000000C
DATA x_swapdata16_5+48(SB)/8, $0x0000000E0000000A
DATA x_swapdata16_5+56(SB)/8, $0x0000000F0000000D
GLOBL x_swapdata16_5(SB), RODATA|NOPTR, $64

//; swaps {2,3}{4,5}{6,7}{8,9}{10,11}{12,13}
//; perm [0 1 3 2 5 4 7 6 9 8 11 10 13 12 14 15 ]
DATA x_swapdata16_6+0(SB)/8, $0x0000000100000000
DATA x_swapdata16_6+8(SB)/8, $0x0000000200000003
DATA x_swapdata16_6+16(SB)/8, $0x0000000400000005
DATA x_swapdata16_6+24(SB)/8, $0x0000000600000007
DATA x_swapdata16_6+32(SB)/8, $0x0000000800000009
DATA x_swapdata16_6+40(SB)/8, $0x0000000A0000000B
DATA x_swapdata16_6+48(SB)/8, $0x0000000C0000000D
DATA x_swapdata16_6+56(SB)/8, $0x0000000F0000000E
GLOBL x_swapdata16_6(SB), RODATA|NOPTR, $64

//; swaps {4,6}{5,7}{8,10}{9,11}
//; perm [0 1 2 3 6 7 4 5 10 11 8 9 12 13 14 15 ]
DATA x_swapdata16_7+0(SB)/8, $0x0000000100000000
DATA x_swapdata16_7+8(SB)/8, $0x0000000300000002
DATA x_swapdata16_7+16(SB)/8, $0x0000000700000006
DATA x_swapdata16_7+24(SB)/8, $0x0000000500000004
DATA x_swapdata16_7+32(SB)/8, $0x0000000B0000000A
DATA x_swapdata16_7+40(SB)/8, $0x0000000900000008
DATA x_swapdata16_7+48(SB)/8, $0x0000000D0000000C
DATA x_swapdata16_7+56(SB)/8, $0x0000000F0000000E
GLOBL x_swapdata16_7(SB), RODATA|NOPTR, $64

//; swaps {3,4}{5,6}{7,8}{9,10}{11,12}
//; perm [0 1 2 4 3 6 5 8 7 10 9 12 11 13 14 15 ]
DATA x_swapdata16_8+0(SB)/8, $0x0000000100000000
DATA x_swapdata16_8+8(SB)/8, $0x0000000400000002
DATA x_swapdata16_8+16(SB)/8, $0x0000000600000003
DATA x_swapdata16_8+24(SB)/8, $0x0000000800000005
DATA x_swapdata16_8+32(SB)/8, $0x0000000A00000007
DATA x_swapdata16_8+40(SB)/8, $0x0000000C00000009
DATA x_swapdata16_8+48(SB)/8, $0x0000000D0000000B
DATA x_swapdata16_8+56(SB)/8, $0x0000000F0000000E
GLOBL x_swapdata16_8(SB), RODATA|NOPTR, $64

//; #region Sortnet16f32
TEXT ·Sortnet16f32(SB), NOSPLIT|NOFRAME, $0
  MOVQ          data+0(FP),R8             //;398BE831                                 ;R8=scratch;
  VMOVDQU32     (R8),Z0                   //;918EB726 data := [scratch]               ;Z0=data; R8=scratch;

//; sortnet16_f32: IN Z0
//; swaps {0,5}{1,4}{2,12}{3,13}{6,7}{8,9}{10,15}{11,14}
  VMOVDQU32     x_swapdata16_0+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_0+0(SB)];Z1=swap_data;
  MOVW          $3407,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {0,2}{1,10}{3,6}{4,7}{5,14}{8,11}{9,12}{13,15}
  VMOVDQU32     x_swapdata16_1+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_1+0(SB)];Z1=swap_data;
  MOVW          $9019,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {0,8}{1,3}{2,11}{4,13}{5,9}{6,10}{7,15}{12,14}
  VMOVDQU32     x_swapdata16_2+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_2+0(SB)];Z1=swap_data;
  MOVW          $4343,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {0,1}{2,4}{3,8}{5,6}{7,12}{9,10}{11,13}{14,15}
  VMOVDQU32     x_swapdata16_3+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_3+0(SB)];Z1=swap_data;
  MOVW          $19117,R15                //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {1,3}{2,5}{4,8}{6,9}{7,11}{10,13}{12,14}
  VMOVDQU32     x_swapdata16_4+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_4+0(SB)];Z1=swap_data;
  MOVW          $5334,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {1,2}{3,5}{4,11}{6,8}{7,9}{10,12}{13,14}
  VMOVDQU32     x_swapdata16_5+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_5+0(SB)];Z1=swap_data;
  MOVW          $9434,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {2,3}{4,5}{6,7}{8,9}{10,11}{12,13}
  VMOVDQU32     x_swapdata16_6+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_6+0(SB)];Z1=swap_data;
  MOVW          $5460,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {4,6}{5,7}{8,10}{9,11}
  VMOVDQU32     x_swapdata16_7+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_7+0(SB)];Z1=swap_data;
  MOVW          $816,R15                  //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; swaps {3,4}{5,6}{7,8}{9,10}{11,12}
  VMOVDQU32     x_swapdata16_8+0(SB),Z1   //;41E2065E swap_data := [x_swapdata16_8+0(SB)];Z1=swap_data;
  MOVW          $2728,R15                 //;556B87C0                                 ;R15=scratch;
  KMOVW         R15, K3                   //;1AEB66EA                                 ;K3=merge_mask; R15=scratch;
  VPERMD        Z0,  Z1,  Z2              //;BAEF0C4D                                 ;Z2=shuffled_data; Z1=swap_data; Z0=data;
  VMINPS        Z0,  Z2,  Z1              //;A434C84D min := min(shuffled_data, data) ;Z1=min; Z2=shuffled_data; Z0=data;
  VMAXPS        Z0,  Z2,  Z2              //;6973CAB2 max := max(shuffled_data, data) ;Z2=max; Z2=shuffled_data; Z0=data;
  VPBLENDMD     Z1,  Z2,  K3,  Z0         //;2E2367FF                                 ;Z0=data; K3=merge_mask; Z2=max; Z1=min;

//; sortnet16_f32: OUT Z0

  VMOVDQU32     Z0,  (R8)                 //;B9D79A53                                 ;R8=scratch; Z0=data;
  RET                                     //;A2BC0265                                 ;
//; #endregion Sortnet16f32
