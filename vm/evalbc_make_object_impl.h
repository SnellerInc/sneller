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

// This file provides an implementation of 'bcmakelist' and 'bcmakestruct'
// operations. There operations are similar - 'bcmakestruct' stores key+value
// pairs, while 'bcmalelist' only stores values. The extra logic required by
// 'bcmakestruct' is behind BC_GENERATE_MAKE_STRUCT #ifdef.
//
// It uses the following macros:
//   - BC_GENERATE_MAKE_LIST - defined to generate 'bcmakelist' instruction
//   - BC_GENERATE_MAKE_STRUCT - defined to generate 'bcmakestruct' instruction

#ifdef BC_GENERATE_MAKE_LIST
#define BC_INSTRUCTION_NAME bcmakelist
#define BC_ION_OBJECT_TYPE CONSTD_0xB0()
#define BC_VA_TUPLE_SIZE 4
#define BC_VA_VALUE_STACK_SLOT 0
#define BC_VA_PREDICATE_STACK_SLOT 2
#endif

#ifdef BC_GENERATE_MAKE_STRUCT
#define BC_INSTRUCTION_NAME bcmakestruct
#define BC_ION_OBJECT_TYPE CONSTD_0xD0()
#define BC_VA_TUPLE_SIZE 8
#define BC_VA_VALUE_STACK_SLOT 4
#define BC_VA_PREDICATE_STACK_SLOT 6
#endif

// v[0].k[1] = make_list(va...).k[2]
// v[0].k[1] = make_struct(va...).k[2]
//
// Boxes a list/struct composed of boxed values (va)
TEXT BC_INSTRUCTION_NAME(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  BC_UNPACK_RU32(BC_SLOT_SIZE*3, OUT(CX))              // CX <- count of variable arguments

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  ADDQ $(BC_SLOT_SIZE*3 + 4), VIRT_PCREG               // VIRT_PCREG <- VA base

  VPXORQ X4, X4, X4
  VPXORQ X5, X5, X5
  VMOVQ VIRT_PCREG, X20                                // X20 <- Save VIRT_PCREG

  // it's not allowed to have zero arguments here, but check anyway...
  XORL DX, DX
  TESTL CX, CX
  JZ no_va_args

  // calculate the number of bytes needed for each lane in Z4:Z5, which is the
  // sum of each boxed value and the overhead to store the `Type|L + [Length]`
calc_length_iter:
#ifdef BC_GENERATE_MAKE_STRUCT
  LZCNTL 0(VIRT_PCREG)(DX*BC_VA_TUPLE_SIZE), BX
  MOVL $4, R15
  SHRL $3, BX
  SUBL BX, R15
  VPBROADCASTQ R15, Z8
#endif
  MOVWLZX BC_VA_VALUE_STACK_SLOT(VIRT_PCREG)(DX*BC_VA_TUPLE_SIZE), R8
  MOVWLZX BC_VA_PREDICATE_STACK_SLOT(VIRT_PCREG)(DX*BC_VA_TUPLE_SIZE), BX
  ADDL $1, DX

  KMOVW 0(VIRT_VALUES)(BX*1), K3
  VPMOVZXDQ 64(VIRT_VALUES)(R8*1), Z6
  KANDW K1, K3, K3
  VPMOVZXDQ 96(VIRT_VALUES)(R8*1), Z7
  KSHIFTRW $8, K3, K4

  VPADDQ Z6, Z4, K3, Z4
  VPADDQ Z7, Z5, K4, Z5

#ifdef BC_GENERATE_MAKE_STRUCT
  // account encoded symbol ID length
  VPADDQ Z8, Z4, K3, Z4
  VPADDQ Z8, Z5, K4, Z5
#endif

  CMPL CX, DX
  JNE calc_length_iter

  // Z4 <- saturated length in DWORD units
  VPMOVUSQD Z4, Y4
  VPMOVUSQD Z5, Y5

  VPBROADCASTD.Z CONSTD_0x0E(), K1, Z11
  VINSERTI32X8 $1, Y5, Z4, Z4

  // K3 <- lanes that only need Type|L to encode array/struct
  VPCMPUD $VPCMP_IMM_LT, Z11, Z4, K1, K3

  // K1 <- clear all lanes that would cause overflow during horizontal addition (paranoia)
  VPCMPUD.BCST $VPCMP_IMM_LE, CONSTD_134217727(), Z4, K1, K1

  // encode ION [Length] field and then use it to calculate the object header size
  VMOVDQA32.Z Z4, K1, Z5                               // Z5 <- [xxxxxxxx|xxxxxxxx|xxxxxxxx|xAAAAAAA]
  VPSLLD.Z $1, Z4, K1, Z6                              // Z6 <- [xxxxxxxx|xxxxxxxx|xBBBBBBB|xxxxxxxx]
  VPSLLD.Z $2, Z4, K1, Z7                              // Z7 <- [xxxxxxxx|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPSLLD.Z $3, Z4, K1, Z8                              // Z8 <- [xDDDDDDD|xxxxxxxx|xxxxxxxx|xxxxxxxx]
  VMOVDQA32.Z Z4, K1, Z13                              // Z13 <- lengths of each active lane (inactive are set to zeros)

  VPBROADCASTD CONSTD_0x007F007F(), Z10
  VPTERNLOGD $TLOG_BLEND_BA, Z10, Z5, Z6            // Z6 <- [xxxxxxxx|xxxxxxxx|xBBBBBBB|xAAAAAAA]
  VPTERNLOGD $TLOG_BLEND_BA, Z10, Z7, Z8            // Z8 <- [xDDDDDDD|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPTERNLOGD.BCST $TLOG_BLEND_BA, CONSTD_0xFFFF0000(), Z8, Z6 // Z6 <- [xDDDDDDD|xCCCCCCC|xBBBBBBB|xAAAAAAA]
  VPANDD.BCST CONSTD_0x7F7F7F7F(), Z6, Z6              // Z6 <- [0DDDDDDD|0CCCCCCC|0BBBBBBB|0AAAAAAA]

  VPBROADCASTD CONSTD_4(), Z8
  VPLZCNTD Z6, Z7                                      // Z7 <- find the last leading bit set, which will be used to determine the real length
  VPORD.BCST CONSTD_128(), Z6, K1, Z6                  // Z6 <- [0DDDDDDD|0CCCCCCC|0BBBBBBB|1AAAAAAA] where '1' is a run-length termination bit

  VPSRLD $3, Z7, Z7
  VPBROADCASTD CONSTD_32(), Z9
  VPSUBD.Z Z7, Z8, K1, Z7                              // Z7 <- the number of bytes required to store each length
  VPBROADCASTD CONSTD_1(), Z10
  VPSLLD $3, Z7, Z8                                    // Z8 <- the number of bits (aligned to 8) required to store each length
  VPSHUFB CONST_GET_PTR(bswap32, 0), Z6, Z6            // Z6 <- [1AAAAAAA|0BBBBBBB|0CCCCCCC|0DDDDDDD] (ByteSwapped)
  VPSUBD Z8, Z9, Z8                                    // Z8 <- the number of bits to discard in Z6
  VPSRLVD Z8, Z6, Z14                                  // Z14 <- encoded 32-bits of [Length] VarUint of each lane
  VPXORD Z7, Z7, K3, Z7                                // Z7 <- clear [Length] in lanes that represent array/struct having length less than 14 bytes
  VPADDD.Z Z10, Z7, K1, Z5                             // Z5 <- the number of bytes that is required to store both `Type|L + [Length]` of each lane

  // Calculate offsets in the output buffer
  VPADDD.Z Z5, Z13, K1, Z4                             // Z4 <- [15    14    13    12   |11    10    09    08   |07    06    05    04   |03    02    01    00   ]
  VPSLLDQ $4, Z4, Z6                                   // Z6 <- [14    13    12    __   |10    09    08    __   |06    05    04    __   |02    01    00    __   ]
  VPADDD Z6, Z4, Z6                                    // Z6 <- [15+14 14+13 13+12 12   |11+10 10+09 09+08 08   |07+06 06+05 05+04 04   |03+02 02+01 01+00 00   ]
  VPSLLDQ $8, Z6, Z7                                   // Z7 <- [13+12 12    __    __   |09+08 08    __    __   |05+04 04    __    __   |01+00 00    __    __   ]
  VPADDD Z6, Z7, Z6                                    // Z6 <- [15:12 14:12 13:12 12   |11:08 10:08 09:08 08   |07:04 06:04 05:04 04   |03:00 02:00 01:00 00   ]

  MOVL $0xF0F0, R15
  KMOVW R15, K4
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), Z6, Z7        // Z7 <- [15:12 15:12 15:12 15:12|11:08 11:08 11:08 11:08|07:04 07:04 07:04 07:04|03:00 03:00 03:00 03:00]
  VPERMQ $SHUFFLE_IMM_4x2b(1, 1, 1, 1), Z7, Z7         // Z7 <- [11:08 11:08 11:08 11:08|<ign> <ign> <ign> <ign>|03:00 03:00 03:00 03:00|<ign> <ign> <ign> <ign>]
  VPADDD Z7, Z6, K4, Z6                                // Z6 <- [15:08 14:08 13:08 12:08|11:08 10:08 09:08 08   |07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ]

  MOVL $0xFF00, R15
  KMOVW R15, K4
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), Z6, Z7        // Z7 <- [15:08 15:08 15:08 15:08|11:08 11:08 11:08 11:08|07:00 07:00 07:00 07:00|03:00 03:00 03:00 03:00]
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 1, 1, 1), Z7, Z7, Z7 // Z7 <- [07:00 07:00 07:00 07:00|07:00 07:00 07:00 07:00|<ign> <ign> <ign> <ign>|<ign> <ign> <ign> <ign>]
  VPADDD Z7, Z6, K4, Z6                                // Z6 <- [15:00 14:00 13:00 12:00|11:00 10:00 09:00 08:00|07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ]

  VEXTRACTI32X4 $3, Z6, X7
  VPEXTRD $3, X7, R15                                  // R15 <- number of bytes to be written in the destination buffer (sum of required bytes of all lanes)

  // Z7 <- offset of each lane in the destination buffer (offsets starts with zero)
  VPSUBD Z4, Z6, Z7                                    // Z7 <- [14:00 13:00 12:00 11:00|10:00 09:00 08:00 07:00|06:00 05:00 04:00 03:00|02:00 01:00 00    zero ]

  MOVQ bytecode_scratch+8(VIRT_BCPTR), DX              // DX <- output buffer length.
  MOVQ bytecode_scratch+16(VIRT_BCPTR), R8             // R8 <- output buffer capacity.
  LEAQ 8(R15), BX                                      // BX <- capacity required to store the output (let's assume 8 bytes more for 8-byte stores).
  SUBQ DX, R8                                          // R8 <- remaining space in the output buffer.

  // abort if the output buffer is too small
  CMPQ R8, BX
  JLT abort

#ifdef BC_GENERATE_MAKE_STRUCT
  // spill R10 and R11
  VMOVQ R10, X15
  VMOVQ R11, X16
#endif

  VPBROADCASTD DX, Z30                                 // Z30 <- output buffer length (first byte to be written), broadcasted
  VPADDD.BCST bytecode_scratchoff(VIRT_BCPTR), Z30, Z30// Z30 <- output buffer length + scratch offset
  VMOVDQA32 Z13, K3, Z11
  VPADDD.Z Z7, Z30, K1, Z30                            // Z30 <- offsets of each boxed object
  VMOVDQA32.Z Z4, K1, Z31                              // Z31 <- lengths of each boxed object
  ADDQ DX, R15
  MOVQ R15, bytecode_scratch+8(VIRT_BCPTR)             // Update the length of scratch buffer
  VPORD.BCST.Z BC_ION_OBJECT_TYPE, Z11, K1, Z11        // Z11 <- encoded Type|L byte (each byte is encoded in a 32-bit lane)

  VEXTRACTI32X8 $1, Z14, Y9
  VEXTRACTI32X8 $1, Z11, Y12
  VPMOVZXDQ Y14, Z8
  VPMOVZXDQ Y9, Z9
  VPMOVZXDQ Y11, Z13
  VPMOVZXDQ Y12, Z12
  VPSLLQ $8, Z8, Z8
  VPSLLQ $8, Z9, Z9
  VPORQ Z13, Z8, Z8                                    // Z8 <- encoded Type|L + [Length] in each 64-bit lane (low)
  VPORQ Z12, Z9, Z9                                    // Z9 <- encoded Type|L + [Length] in each 64-bit lane (high)

  // The easiest thing to do is to scatter the Type|L + length as 8 byte units as it's much
  // faster than storing these on stack and then reloading and storing again as scalar ops.
  //
  // NOTE: Scatter defines overlapping stores, the last is stored last, which follows our data.
  KMOVB K1, K2
  VPSCATTERDQ Z8, K2, 0(VIRT_BASE)(Y30*1)
  VEXTRACTI32X8 $1, Z30, Y13
  KSHIFTRW $8, K1, K3
  VPSCATTERDQ Z9, K3, 0(VIRT_BASE)(Y13*1)

  VPADDD Z30, Z5, Z17                                  // Z17 <- offsets of each lane incremented by sizeof(Type|L + Length)
  VMOVDQU32 Z17, bytecode_spillArea(VIRT_BCPTR)        // save Z17 offsets so we can use them in a scalar loop

  // Now copy all the bytes into the destination buffer by processing each value at once (per lane).

va_iter:
  KMOVW K1, BX
  MOVWLZX BC_VA_VALUE_STACK_SLOT(VIRT_PCREG), R8       // R8 <- value stack slot, relative to VIRT_VALUES
  MOVWLZX BC_VA_PREDICATE_STACK_SLOT(VIRT_PCREG), DX   // DX <- predicate stack slot, relative to VIRT_VALUES
  ADDQ VIRT_VALUES, R8                                 // R8 <- value stack address
  ANDW 0(VIRT_VALUES)(DX*1), BX                        // BX <- value predicate
  ADDQ $(BC_VA_TUPLE_SIZE), VIRT_PCREG                 // advance VIRT_PCREG, which is our VA pointer as well

  TESTL BX, BX
  JNE lane_iter_init

  SUBL $1, CX
  JNE va_iter

  JMP va_end

lane_iter_init:
#ifdef BC_GENERATE_MAKE_STRUCT
  // R10 <- encoded symbol ID as VarUInt (preencoded by emitMakeStruct)
  // R11 <- the size of the encoded symbol ID of this field
  MOVL -BC_VA_TUPLE_SIZE(VIRT_PCREG), R10
  LZCNTL R10, R15
  MOVL $4, R11
  SHRL $3, R15
  SUBL R15, R11
#endif

lane_iter:
  TZCNTL BX, DX                                        // DX <- index of the lane to process
  BLSRL BX, BX                                         // BX <- remaining lanes to iterate

  MOVL 64(R8)(DX * 4), R13                             // R13 <- input length
  MOVL 0(R8)(DX * 4), R14                              // R14 <- input index
  MOVL bytecode_spillArea(VIRT_BCPTR)(DX * 4), R15     // R15 <- output index

  ADDQ VIRT_BASE, R14                                  // make input address from input index
  ADDQ VIRT_BASE, R15                                  // make output address from output index
  ADDL R13, bytecode_spillArea(VIRT_BCPTR)(DX * 4)     // add input length to the output index

#ifdef BC_GENERATE_MAKE_STRUCT
  // First store two bytes - we know that the smallest possible value that could follow is one byte,
  // which means that we can store two bytes without affecting another lane in case this is the last
  // value to be written in this lane (for example all following values are missing or it's the very
  // last one).
  MOVW R10, 0(R15)
  CMPQ R11, $2
  JBE field_encoding_done

  // If we are here it means the symbol needs at least 3 bytes to be encoded, so store two more bytes.
  // To avoid shifts or the need to have the HI_WORD of R15 in another register we just overwrite the
  // two bytes we have stored previously.
  MOVL R10, 0(R15)

field_encoding_done:
  ADDQ R11, R15
  ADDL R11, bytecode_spillArea(VIRT_BCPTR)(DX * 4)     // add encoded symbol ID length to the output index
#endif

  SUBL $64, R13
  JCS copy_tail

  // Main copy loop that processes 64 bytes at once
copy_iter:
  VMOVDQU8 0(R14), Z7
  ADDQ $64, R14
  VMOVDQU8 Z7, 0(R15)
  ADDQ $64, R15

  SUBL $64, R13
  JCC copy_iter

  // Process 0..63 bytes
copy_tail:
  MOVQ $-1, DX
  SHLXQ R13, DX, DX
  NOTQ DX
  KMOVQ DX, K2

  VMOVDQU8.Z 0(R14), K2, Z7
  VMOVDQU8 Z7, K2, 0(R15)

  TESTL BX, BX
  JNE lane_iter

  SUBL $1, CX
  JNE va_iter

va_end:
  VMOVQ X20, BX                                        // BX <- The original VIRT_PCREG that points to VA base

#ifdef BC_GENERATE_MAKE_STRUCT
  // restore spilled R10 and R11
  VMOVQ X16, R11
  VMOVQ X15, R10
#endif

  BC_MOV_SLOT (-BC_SLOT_SIZE*3 - 4)(BX), DX
  BC_MOV_SLOT (-BC_SLOT_SIZE*2 - 4)(BX), R8

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z11), IN(Z5), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(0)

no_va_args:
  MOVL $const_bcerrCorrupt, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

#undef BC_VA_PREDICATE_STACK_SLOT
#undef BC_VA_VALUE_STACK_SLOT
#undef BC_VA_TUPLE_SIZE
#undef BC_ION_OBJECT_TYPE
#undef BC_INSTRUCTION_NAME
