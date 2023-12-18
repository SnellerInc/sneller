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

package vm

import "github.com/SnellerInc/sneller/internal/simd"

// constants
var (
	ConstNBytesUtf8 = [16]uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4}
	ConstTailMask   = [16]uint32{0, 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF}
	ConstBswap32    = [16]uint32{0x00010203, 0x04050607, 0x08090A0B, 0x0C0D0E0F, 0x00010203, 0x04050607, 0x08090A0B, 0x0C0D0E0F, 0x00010203, 0x04050607, 0x08090A0B, 0x0C0D0E0F, 0x00010203, 0x04050607, 0x08090A0B, 0x0C0D0E0F}
)

// DFA6TGo is pure go implementation of the DFA6T assembly code
func DFA6TGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {

	var Z2, Z3, Z5, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z15, Z16, Z18, Z20, Z21, Z22, Z23 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(192), &K6)         // Kmovw         192(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(194), &Z5)  // VPBROADCASTD  194(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(198), &Z13) // VPBROADCASTD  198(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;

	//; load constants
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;

	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;

main_loop:
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;

	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skip_wildcard
	}
	//; get char-groups
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K5, &Z8) // Vmovdqu8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;

	// ; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4) // Vpcmpd        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;

	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4) // Vpcmpd        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;6FD26853 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;

	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4) // Vpcmpd        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;BCCB1762 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;

	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4) // Vpcmpd        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;42917E87 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;

	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto main_loop
	}
next:
	return simd.Mask2Uint16(&K1)

skip_wildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4) // Vpcmpd        $5,  Z11, Z3,  K4         //;8DFA55D5 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;A73B0AC3 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;21B4F359 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;1A66952A curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	goto tail                      // JMP           tail                      //;E21E4B3D                                 ;
}

func DFA7TGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z15, Z16, Z18, Z20, Z21, Z22, Z23, Z24 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(192), &Z24)    // VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(256), &K6)         // Kmovw         256(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(258), &Z5)  // VPBROADCASTD  258(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(262), &Z13) // VPBROADCASTD  262(R14),Z13              //;6891DA5E load accept nodeID              ;Z13=accept_node; R14=needle_ptr;
	//; load constants
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skipWildcard
	}
	//; get char-groups
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K5, &Z8) // Vmovdqu8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
	//; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4) // Vpcmpd        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4) // Vpcmpd        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;6FD26853 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4) // Vpcmpd        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;BCCB1762 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4) // Vpcmpd        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;42917E87 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_node==curr_state);K3=scratch; K2=lane_todo; Z13=accept_node; Z7=curr_state; 0=Eq;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}
next:
	return simd.Mask2Uint16(&K1)

skipWildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4) // Vpcmpd        $5,  Z11, Z3,  K4         //;850DE385 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	goto tail                      // JMP           tail                      //;E21E4B3D                                 ;
}

// DFA8TGo go implementation of DfaT8 Deterministic Finite Automaton (DFA) with 8-bits lookup-key
func DFA8TGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z15, Z16, Z17, Z18, Z20, Z21, Z22, Z23, Z24, Z25, Z26 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(192), &Z24)    // VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(256), &Z25)    // VMOVDQU32     256(R14),Z25              //;BE3AEA52 trans_table3 := [needle_ptr+256];Z25=trans_table3; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(320), &Z26)    // VMOVDQU32     320(R14),Z26              //;C346A0C9 trans_table4 := [needle_ptr+320];Z26=trans_table4; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(384), &K6)         // Kmovw         384(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(386), &Z5)  // VPBROADCASTD  386(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(390), &Z13) // VPBROADCASTD  390(R14),Z13              //;E6CE5A10 load accept nodeID              ;Z13=accept_node; R14=needle_ptr;
	//; load constants
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skipWildcard
	}
	//; get char-groups
	simd.Vpmovb2M(&Z8, &K3)        // Vpmovb2M      Z8,  K3                   //;23A1705D extract non-ASCII mask          ;K3=scratch; Z8=data_msg;
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K3, &Z8) // Vmovdqu8      Z11, K3,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K3=scratch; Z11=0;
	//; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4)  // Vpcmpd        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4)  // Vpcmpd        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;565C3FAD extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4)  // Vpcmpd        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;44C2F748 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4)  // Vpcmpd        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;BC53D421 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_node==curr_state);K3=scratch; K2=lane_todo; Z13=accept_node; Z7=curr_state; 0=Eq;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}
next:
	return simd.Mask2Uint16(&K1)

skipWildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4)  // Vpcmpd        $5,  Z11, Z3,  K4         //;BFA0A870 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	goto tail                       // JMP           tail                      //;E21E4B3D                                 ;
}

// DFA6TZGo go implementation of DfaT6Z Deterministic Finite Automaton (DFA) with 6-bits lookup-key and Zero length remaining assertion
func DFA6TZGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, Z16, Z18, Z20, Z21, Z22, Z23 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(192), &K6)         // Kmovw         192(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(194), &Z5)  // VPBROADCASTD  194(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(198), &Z13) // VPBROADCASTD  198(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
	simd.KmovqMem(R14.Ptr(202), &K3)         // KMOVQ         202(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
	simd.Vpmovm2B(&K3, &Z14)                 // Vpmovm2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
	//; load constants
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Vpxord(&Z6, &Z6, &Z6)           // Vpxord        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skipWildcard
	}
	//; get char-groups
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K5, &Z8) // Vmovdqu8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
	//; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4) // Vpcmpd        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpcmpd(0, &Z10, &Z3, &K5) // Vpcmpd        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4) // Vpcmpd        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpcmpd(0, &Z15, &Z3, &K5) // Vpcmpd        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4) // Vpcmpd        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpcmpd(0, &Z16, &Z3, &K5) // Vpcmpd        $0,  Z16, Z3,  K5         //;91BAEA96 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4) // Vpcmpd        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpcmpd(0, &Z20, &Z3, &K5) // Vpcmpd        $0,  Z20, Z3,  K5         //;2154FFD7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
	simd.Vpermb(&Z14, &Z6, &Z12)         // Vpermb        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
	simd.VpslldImmZ(24, &Z12, &K2, &Z12) // VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
	simd.Vpmovd2M(&Z12, &K4)             // Vpmovd2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Korw(&K3, &K4, &K3)             // Korw          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}

next:
	return simd.Mask2Uint16(&K1)

skipWildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4) // Vpcmpd        $5,  Z11, Z3,  K4         //;89485A8A K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpcmpd(0, &Z11, &Z3, &K5) // Vpcmpd        $0,  Z11, Z3,  K5         //;A23A5A84 K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermb(&Z23, &Z9, &Z9)    // Vpermb        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	goto tail                      // JMP           tail                      //;E21E4B3D                                 ;
}

// DFA7TZGo go implementation of DfaT7Z Deterministic Finite Automaton (DFA) with 7-bits lookup-key and Zero length remaining assertion
func DFA7TZGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, Z16, Z18, Z20, Z21, Z22, Z23, Z24 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(192), &Z24)    // VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(256), &K6)         // Kmovw         256(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(258), &Z5)  // VPBROADCASTD  258(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(262), &Z13) // VPBROADCASTD  262(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
	simd.KmovqMem(R14.Ptr(266), &K3)         // KMOVQ         266(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
	simd.Vpmovm2B(&K3, &Z14)                 // Vpmovm2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
	//; load constants
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Vpxord(&Z6, &Z6, &Z6)           // Vpxord        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skipWildcard
	}
	//; get char-groups
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K5, &Z8) // Vmovdqu8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
	//; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4) // Vpcmpd        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpcmpd(0, &Z10, &Z3, &K5) // Vpcmpd        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4) // Vpcmpd        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpcmpd(0, &Z15, &Z3, &K5) // Vpcmpd        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4) // Vpcmpd        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpcmpd(0, &Z16, &Z3, &K5) // Vpcmpd        $0,  Z16, Z3,  K5         //;91BAEA96 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)    // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4) // Vpcmpd        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpcmpd(0, &Z20, &Z3, &K5) // Vpcmpd        $0,  Z20, Z3,  K5         //;2154FFD7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
	simd.Vpermb(&Z14, &Z6, &Z12)         // Vpermb        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
	simd.VpslldImmZ(24, &Z12, &K2, &Z12) // VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
	simd.Vpmovd2M(&Z12, &K4)             // Vpmovd2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Korw(&K3, &K4, &K3)             // Korw          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}
next:
	return simd.Mask2Uint16(&K1)

skipWildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4) // Vpcmpd        $5,  Z11, Z3,  K4         //;7C3C9240 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpcmpd(0, &Z11, &Z3, &K5) // Vpcmpd        $0,  Z11, Z3,  K5         //;6843E9F0 K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)      // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpermi2B(&Z24, &Z23, &Z9) // Vpermi2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqa32K(&Z9, &K4, &Z7) // Vmovdqa32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
	simd.Vmovdqa32K(&Z9, &K5, &Z6) // Vmovdqa32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
	goto tail                      // JMP           tail                      //;E21E4B3D                                 ;
}

// DFA8TZGo go implementation of DfaT8Z Deterministic Finite Automaton 8-bits with Zero length remaining assertion
func DFA8TZGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, Z16, Z17, Z18, Z20, Z21, Z22, Z23, Z24, Z25, Z26 simd.Vec8x64
	var K1, K2, K3, K4, K5, K6 simd.Mask
	var RSI, R14 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.Vmovdqu32Mem(R14.Ptr(0), &Z21)      // VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(64), &Z22)     // VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(128), &Z23)    // VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(192), &Z24)    // VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(256), &Z25)    // VMOVDQU32     256(R14),Z25              //;BE3AEA52 trans_table3 := [needle_ptr+256];Z25=trans_table3; R14=needle_ptr;
	simd.Vmovdqu32Mem(R14.Ptr(320), &Z26)    // VMOVDQU32     320(R14),Z26              //;C346A0C9 trans_table4 := [needle_ptr+320];Z26=trans_table4; R14=needle_ptr;
	simd.KmovwMem(R14.Ptr(384), &K6)         // Kmovw         384(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(386), &Z5)  // VPBROADCASTD  386(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
	simd.VpbroadcastdMem(R14.Ptr(390), &Z13) // VPBROADCASTD  390(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
	simd.KmovqMem(R14.Ptr(394), &K3)         // KMOVQ         394(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
	simd.Vpmovm2B(&K3, &Z14)                 // Vpmovm2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
	//; load constants
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.Vpaddd(&Z10, &Z10, &Z15)          // Vpaddd        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
	simd.Vpaddd(&Z10, &Z15, &Z16)          // Vpaddd        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
	simd.Vpaddd(&Z15, &Z15, &Z20)          // Vpaddd        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z18) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
	//; init variables
	simd.Kmovw(&K1, &K2)      // Kmovw         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
	simd.Kxorw(&K1, &K1, &K1) // Kxorw         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7) // Vmovdqa32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Vpxord(&Z6, &Z6, &Z6)           // Vpxord        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpxord(&Z8, &Z8, &Z8)           // Vpxord        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	simd.Vpmovb2M(&Z8, &K5)              // Vpmovb2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
	//; determine whether a lane has a non-ASCII code-point
	simd.Vpmovm2B(&K5, &Z12)              // Vpmovm2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
	simd.VpcmpdK(4, &Z12, &Z11, &K2, &K3) // Vpcmpd        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
	simd.Ktestw(&K6, &K3, &flags)         // Ktestw        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
	if !flags.Zero {                      // JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
		goto skipWildcard
	}
	//; get char-groups
	simd.Vpermi2B(&Z22, &Z21, &Z8) // Vpermi2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqu8K(&Z11, &K5, &Z8) // Vmovdqu8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
	//; handle 1st ASCII in data
	simd.Vpcmpd(5, &Z10, &Z3, &K4)  // Vpcmpd        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
	simd.Vpcmpd(0, &Z10, &Z3, &K5)  // Vpcmpd        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	simd.Vmovdqa32K(&Z17, &K5, &Z6) // Vmovdqa32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
	//; handle 2nd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z15, &Z3, &K4)  // Vpcmpd        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
	simd.Vpcmpd(0, &Z15, &Z3, &K5)  // Vpcmpd        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	simd.Vmovdqa32K(&Z17, &K5, &Z6) // Vmovdqa32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
	//; handle 3rd ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z16, &Z3, &K4)  // Vpcmpd        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
	simd.Vpcmpd(0, &Z16, &Z3, &K5)  // Vpcmpd        $0,  Z16, Z3,  K5         //;2154FFD7 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	simd.Vmovdqa32K(&Z17, &K5, &Z6) // Vmovdqa32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
	//; handle 4th ASCII in data
	simd.VpsrldImm(8, &Z8, &Z8)     // VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
	simd.Vpcmpd(5, &Z20, &Z3, &K4)  // Vpcmpd        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
	simd.Vpcmpd(0, &Z20, &Z3, &K5)  // Vpcmpd        $0,  Z20, Z3,  K5         //;95E6ECB7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	simd.Vmovdqa32K(&Z17, &K5, &Z6) // Vmovdqa32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
	//; advance 4 bytes (= 4 code-points)
	simd.Vpaddd(&Z20, &Z2, &Z2) // Vpaddd        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
	simd.Vpsubd(&Z20, &Z3, &Z3) // Vpsubd        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
	simd.VpcmpdK(0, &Z7, &Z13, &K2, &K3) // Vpcmpd        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
	simd.Vpermb(&Z14, &Z6, &Z12)         // Vpermb        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
	simd.VpslldImmZ(24, &Z12, &K2, &Z12) // VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
	simd.Vpmovd2M(&Z12, &K4)             // Vpmovd2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
	simd.VpcmpdK(4, &Z7, &Z11, &K2, &K2) // Vpcmpd        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2) // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Korw(&K3, &K4, &K3)             // Korw          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
	simd.Kandnw(&K2, &K3, &K2)           // Kandnw        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)             // Korw          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	simd.Ktestw(&K2, &K2, &flags)        // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                     // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}
next:
	return simd.Mask2Uint16(&K1)

skipWildcard:
	//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
	simd.VpsrldImm(4, &Z8, &Z12) // VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
	simd.Vpermd(&Z18, &Z12, &Z12) // Vpermd        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
	//; get char-groups
	simd.VpcmpdK(4, &Z12, &Z10, &K2, &K3) // Vpcmpd        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
	simd.Vpermi2B(&Z22, &Z21, &Z8)        // Vpermi2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
	simd.Vmovdqa32K(&Z5, &K3, &Z8)        // Vmovdqa32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
	//; advance 1 code-point
	simd.Vpsubd(&Z12, &Z3, &Z3) // Vpsubd        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
	simd.Vpaddd(&Z12, &Z2, &Z2) // Vpaddd        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
	//; handle 1st code-point in data
	simd.Vpcmpd(5, &Z11, &Z3, &K4)  // Vpcmpd        $5,  Z11, Z3,  K4         //;A17DDD33 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
	simd.Vpcmpd(0, &Z11, &Z3, &K5)  // Vpcmpd        $0,  Z11, Z3,  K5         //;9AA6077F K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
	simd.Vpord(&Z8, &Z7, &Z9)       // Vpord         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
	simd.Vpmovb2M(&Z9, &K3)         // Vpmovb2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
	simd.Vmovdqa32(&Z9, &Z17)       // Vmovdqa32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
	simd.Vpermi2B(&Z26, &Z25, &Z9)  // Vpermi2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
	simd.Vpermi2B(&Z24, &Z23, &Z17) // Vpermi2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
	simd.Vmovdqu8K(&Z9, &K3, &Z17)  // Vmovdqu8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
	simd.Vmovdqa32K(&Z17, &K4, &Z7) // Vmovdqa32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
	simd.Vmovdqa32K(&Z17, &K5, &Z6) // Vmovdqa32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
	goto tail                       // JMP           tail                      //;E21E4B3D                                 ;
}

// DFALZGo go implementation of DfaLZ Deterministic Finite Automaton(DFA) with unlimited capacity (Large) and Remaining Length Zero Assertion (RLZA)
func DFALZGo(data []byte, maskIn uint16, offsets, sizes [16]uint32, dsByte []byte) uint16 {
	var Z2, Z3, Z5, Z6, Z7, Z8, Z10, Z11, Z12, Z17, Z18, Z19, Z20, Z21, Z22, Z26 simd.Vec8x64
	var K1, K2, K3, K4, K5 simd.Mask
	var RSI, CX, DX, R8, R13, R14, R15 simd.Gpr
	var flags simd.Flags

	RSI.MakePtr(data)
	R14.MakePtr(dsByte)
	R13.MakePtr(dsByte)

	// copy input parameters into register variables
	K1 = simd.Uint162Mask16(maskIn) // BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
	simd.Ktestw(&K1, &K1, &flags)   // Ktestw        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
	if flags.Zero {                 // JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
		goto next
	}
	simd.Vmovdqu32X(offsets, &Z2) //  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
	simd.Vmovdqu32X(sizes, &Z3)

	//; load parameters
	simd.MovlMem(R14.Ptr(0), &R8) // Movl          (R14),R8                  //;6AD2EA95 load n_states                   ;R8=n_states; R14=needle_ptr;
	simd.AddqImm(4, &R14, &flags) // Addq          $4,  R14                  //;3259F7B2 init state_offset               ;R14=needle_ptr;
	//; test for special situation with DFA ->s0 -$> s1
	simd.Movl(&R8, &R15)    // Movl          R8,  R15                  //;97339D56 scratch := n_states             ;R15=scratch; R8=n_states;
	simd.Incl(&R15, &flags) // Incl          R15                       //;91D62E05 scratch++                       ;R15=scratch;
	if !flags.Zero {        // JNZ           normal_operation          //;19338985 if result==0, then special situation; jump if not zero (ZF = 0);
		goto normalOperation
	}

	simd.VptestnmdK(&Z3, &Z3, &K1, &K1) // VPTESTNMD     Z3,  Z3,  K1,  K1         //;29B38DE0 K1 &= (str_len==0)              ;K1=lane_active; Z3=str_len;
	goto next                           // JMP           next                      //;E5E69BC1                                 ;

normalOperation:
	//; test if start state is accepting
	simd.Testl(&R8, &R8, &flags) // Testl         R8,  R8                   //;637F12FC are there more than 0 states?   ;R8=n_states;
	if flags.Zero { // JLE           next                      //;AEE3942A no, then there is only an accept state; jump if less or equal ((ZF = 1) or (SF neq OF));
		goto next
	}
	//; load constants
	simd.Vmovdqu32X(ConstTailMask, &Z18)   // VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
	simd.Vmovdqu32X(ConstNBytesUtf8, &Z21) // VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
	simd.Vpxord(&Z11, &Z11, &Z11)          // Vpxord        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
	simd.VpbroadcastdImm(1, &Z10)          // VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
	simd.VpbroadcastdImm(4, &Z20)          // VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
	simd.VpbroadcastdImm(0x3FFFFFFF, &Z17) // VPBROADCASTD  CONSTD_0x3FFFFFFF(),Z17   //;EF9E72D4 load flags_mask                 ;Z17=flags_mask;
	simd.Vmovdqu32X(ConstBswap32, &Z12)    // VMOVDQU32     bswap32<>(SB),Z12         //;A0BC360A load constant bswap32           ;Z12=bswap32;
	//; init variables before main loop
	simd.VpcmpdK(1, &Z3, &Z11, &K1, &K2) // Vpcmpd        $1,  Z3,  Z11, K1,  K2    //;95727519 K2 := K1 & (0<str_len)          ;K2=lane_todo; K1=lane_active; Z11=0; Z3=str_len; 1=LessThen;
	simd.Kxorw(&K1, &K1, &K1)            // Kxorw         K1,  K1,  K1              //;C1A15D64 lane_active := 0                ;K1=lane_active;
	simd.Vmovdqa32(&Z10, &Z7)            // Vmovdqa32     Z10, Z7                   //;77B17C9A curr_state := 1                 ;Z7=curr_state; Z10=1;
mainLoop:
	simd.Kmovw(&K2, &K3)                 // Kmovw         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
	simd.Vpgatherdd(&RSI, &Z2, &K3, &Z8) // Vpgatherdd    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
	//; init variables before states loop
	simd.Vpxord(&Z6, &Z6, &Z6) // Vpxord        Z6,  Z6,  Z6              //;E4D2E400 next_state := 0                 ;Z6=next_state;
	simd.Vmovdqa32(&Z10, &Z5)  // Vmovdqa32     Z10, Z5                   //;A30F50D2 state_id := 1                   ;Z5=state_id; Z10=1;
	simd.Movl(&R8, &CX)        // Movl          R8,  CX                   //;B08178D1 init state_counter              ;CX=state_counter; R8=n_states;
	simd.Movq(&R14, &R13)      // Movq          R14, R13                  //;F0D423D2 init state_offset               ;R13=state_offset; R14=needle_ptr;
	//; get number of bytes in code-point
	simd.VpsrldImm(4, &Z8, &Z26)  // VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
	simd.Vpermd(&Z21, &Z26, &Z22) // Vpermd        Z21, Z26, Z22             //;68FECBA0 get n_bytes_data                ;Z22=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
	//; remove tail from data
	simd.Vpermd(&Z18, &Z22, &Z19)     // Vpermd        Z18, Z22, Z19             //;E5886CFE get tail_mask (data)            ;Z19=tail_mask; Z22=n_bytes_data; Z18=tail_mask_data;
	simd.VpanddZ(&Z8, &Z19, &K2, &Z8) // Vpandd.Z      Z8,  Z19, K2,  Z8         //;BF3EB085 mask data                       ;Z8=data_msg; K2=lane_todo; Z19=tail_mask;
	//; transform data such that we can compare
	simd.Vpshufb(&Z12, &Z8, &Z8)  // Vpshufb       Z12, Z8,  Z8              //;964815FF toggle endiannes                ;Z8=data_msg; Z12=bswap32;
	simd.Vpsubd(&Z22, &Z20, &Z26) // Vpsubd        Z22, Z20, Z26             //;43F001E9 scratch_Z26 := 4 - n_bytes_data ;Z26=scratch_Z26; Z20=4; Z22=n_bytes_data;
	simd.VpslldImm(3, &Z26, &Z26) // VpslldImm        $3,  Z26, Z26             //;22D27D9F scratch_Z26 <<= 3               ;Z26=scratch_Z26;
	simd.Vpsrlvd(&Z26, &Z8, &Z8)  // Vpsrlvd       Z26, Z8,  Z8              //;C0B21528 data_msg >>= scratch_Z26        ;Z8=data_msg; Z26=scratch_Z26;
	//; advance one code-point
	simd.Vpsubd(&Z22, &Z3, &Z3) // Vpsubd        Z22, Z3,  Z3              //;CB5D370F str_len -= n_bytes_data         ;Z3=str_len; Z22=n_bytes_data;
	simd.Vpaddd(&Z22, &Z2, &Z2) // Vpaddd        Z22, Z2,  Z2              //;DEE2A990 str_start += n_bytes_data       ;Z2=str_start; Z22=n_bytes_data;
loopStates:
	simd.VpcmpdK(0, &Z7, &Z5, &K2, &K4) // Vpcmpd        $0,  Z7,  Z5,  K2,  K4    //;F998800A K4 := K2 & (state_id==curr_state);K4=state_matched; K2=lane_todo; Z5=state_id; Z7=curr_state; 0=Eq;
	simd.Vpaddd(&Z5, &Z10, &Z5)         // Vpaddd        Z5,  Z10, Z5              //;ED016003 state_id++                      ;Z5=state_id; Z10=1;
	simd.Ktestw(&K4, &K4, &flags)       // Ktestw        K4,  K4                   //;43122CE8 did any states match?           ;K4=state_matched;
	if flags.Zero {                     // JZ            skip_edges                //;6DE8E146 no, skip the loop with edges; jump if zero (ZF = 1);
		goto skipEdges
	}
	simd.MovlMem(R13.Ptr(4), &DX) // Movl          4(R13),DX                 //;CA7C9CE3 load number of edges            ;DX=edge_counter; R13=state_offset;
	simd.AddqImm(8, &R13, &flags) // Addq          $8,  R13                  //;729CC51F state_offset += 8               ;R13=state_offset;
loopEdges:
	simd.VpcmpudKBcst(5, R13.Ptr(0), &Z8, &K4, &K3) // VPCMPUD.BCST  $5,  (R13),Z8,  K4,  K3   //;510F046E K3 := K4 & (data_msg>=[state_offset]);K3=trans_matched; K4=state_matched; Z8=data_msg; R13=state_offset; 5=GreaterEq;
	simd.VpcmpudKBcst(2, R13.Ptr(4), &Z8, &K3, &K3) // VPCMPUD.BCST  $2,  4(R13),Z8,  K3,  K3  //;59D7E2CF K3 &= (data_msg<=[state_offset+4]);K3=scratch; Z8=data_msg; R13=state_offset; 2=LessEq;
	simd.VpbroadcastdMemK(R13.Ptr(8), &K3, &Z6)     // VPBROADCASTD  8(R13),K3,  Z6            //;789252A5 update next_state               ;Z6=next_state; K3=trans_matched; R13=state_offset;
	simd.AddqImm(12, &R13, &flags)                  // Addq          $12, R13                  //;9997E3C9 state_offset += 12              ;R13=state_offset;
	simd.Decl(&DX, &flags)                          // Decl          DX                        //;F5ED8DBE edge_counter--                  ;DX=edge_counter;
	if !flags.Zero {                                // JNZ           loop_edges                //;314C4D30 jump if not zero (ZF = 0)       ;
		goto loopEdges
	}
	goto loopEdgesDone // JMP           loop_edges_done           //;D662BEEB                                 ;
skipEdges:
	simd.MovlMem(R13.Ptr(0), &DX) // Movl          (R13),DX                  //;33839A60 load total bytes of edges       ;DX=edge_counter; R13=state_offset;
	simd.Addq(&DX, &R13, &flags)  // Addq          DX,  R13                  //;2E22DACA state_offset += edge_counter    ;R13=state_offset; DX=edge_counter;
loopEdgesDone:
	simd.Decl(&CX, &flags) // Decl          CX                        //;D33A44D5 state_counter--                 ;CX=state_counter;
	if !flags.Zero {       // JNZ           loop_states               //;CFB42829 jump if not zero (ZF = 0)       ;
		goto loopStates
	}

	//; test the RLZ condition
	simd.Vpmovd2M(&Z6, &K3)              // Vpmovd2M      Z6,  K3                   //;E2246D80 retrieve RLZ bit                ;K3=scratch; Z6=next_state;
	simd.VpcmpdK(0, &Z3, &Z11, &K3, &K5) // Vpcmpd        $0,  Z3,  Z11, K3,  K5    //;CF75D163 K5 := K3 & (0==str_len)         ;K5=rlz_condition; K3=scratch; Z11=0; Z3=str_len; 0=Eq;
	//; test accept condition
	simd.Vpaddd(&Z6, &Z6, &Z26) // Vpaddd        Z6,  Z6,  Z26             //;185F151B shift accept-bit into most sig pos;Z26=scratch_Z26; Z6=next_state;
	simd.Vpmovd2M(&Z26, &K3)    // Vpmovd2M      Z26, K3                   //;38627E18 retrieve accept bit             ;K3=scratch; Z26=scratch_Z26;
	//; update lane_todo and lane_active
	simd.Korw(&K5, &K3, &K3)   // Korw          K5,  K3,  K3              //;D1E8D8B6 scratch |= rlz_condition        ;K3=scratch; K5=rlz_condition;
	simd.Kandnw(&K2, &K3, &K2) // Kandnw        K2,  K3,  K2              //;1C70ECDA lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
	simd.Korw(&K1, &K3, &K1)   // Korw          K1,  K3,  K1              //;E320A3B2 lane_active |= scratch          ;K1=lane_active; K3=scratch;
	//; determine if there is more data to process
	simd.VpcmpudK(4, &Z6, &Z11, &K2, &K2) // VPCMPUD       $4,  Z6,  Z11, K2,  K2    //;7D6781E6 K2 &= (0!=next_state)           ;K2=lane_todo; Z11=0; Z6=next_state; 4=NotEqual;
	simd.Vpandd(&Z6, &Z17, &Z7)           // Vpandd        Z6,  Z17, Z7              //;17DDB755 curr_state := flags_mask & next_state;Z7=curr_state; Z17=flags_mask; Z6=next_state;
	simd.VpcmpdK(1, &Z3, &Z11, &K2, &K2)  // Vpcmpd        $1,  Z3,  Z11, K2,  K2    //;7668811F K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
	simd.Ktestw(&K2, &K2, &flags)         // Ktestw        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
	if !flags.Zero {                      // JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;
		goto mainLoop
	}
next:
	return simd.Mask2Uint16(&K1)
}

func DfaGoImpl(op bcop, data []byte, inputK uint16, offsets, sizes [16]uint32, dsByte []byte) kRegData {
	switch op {
	case opDfaT6:
		return kRegData{DFA6TGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaT7:
		return kRegData{DFA7TGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaT8:
		return kRegData{DFA8TGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaT6Z:
		return kRegData{DFA6TZGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaT7Z:
		return kRegData{DFA7TZGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaT8Z:
		return kRegData{DFA8TZGo(data, inputK, offsets, sizes, dsByte)}
	case opDfaLZ:
		return kRegData{DFALZGo(data, inputK, offsets, sizes, dsByte)}
	default:
		panic("provided op is not a DFA")
	}
}
