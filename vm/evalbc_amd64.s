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

// the opaddrs global is produced
// by parsing this file and emitting
// a table entry for every function
// declared as /^TEXT bc.*/
#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"
#include "bc_amd64.h"
#include "bc_imm_amd64.h"
#include "bc_constant.h"
#include "bc_constant_gen.h"
#include "bc_constant_rempi.h"
#include "bc_macros_amd64.h"

// decodes the next instruction from the virtual pc
// register, advances virtual pc register, and jumps
// into the next bytecode instruction.
#define _NEXT(vm_pc, advance) \
  ADDQ $(advance + 8), vm_pc  \
  JMP  -8(vm_pc)

// every bytecode instruction
// other than 'ret' should end in
// NEXT(), which will branch into
// the next pseudo-instruction
#define NEXT() _NEXT(VIRT_PCREG, 0)

#define NEXT_ADVANCE(advance) _NEXT(VIRT_PCREG, advance)

#define BC_ADVANCE_REG(SrcReg)        \
  ADDQ SrcReg, VIRT_PCREG             \
  JMP -8(VIRT_PCREG)

#define BC_CALC_ADVANCE(ArgSize, Dst) \
  MOVL $(8 + ArgSize), Dst

// RET_ABORT returns early
// with the carry flag set to
// indicate an aborted bytecode program
#define RET_ABORT() \
  STC \
  RET

// use FAIL() when you encounter
// an unrecoverable error
#define FAIL()                                       \
  SUBQ bytecode_compiled+0(VIRT_BCPTR), VIRT_PCREG   \
  MOVL VIRT_PCREG, bytecode_errpc(VIRT_BCPTR)        \
  MOVL $const_bcerrCorrupt, bytecode_err(VIRT_BCPTR) \
  RET_ABORT()

// this is the 'unimplemented!' op
//
// The opcode has to be the first one, as its default
// opcode for no-op SSA nodes.
//
// _ = trap()
TEXT bctrap(SB), NOSPLIT|NOFRAME, $0
  BYTE $0xCC
  RET

// Integer Math Instructions
// -------------------------

#include "bc_eval_math_i64_amd64.h"

// Floating Point Math Instructions
// --------------------------------

#include "bc_eval_math_f64_amd64.h"

// Control Flow Instructions
// -------------------------

#define BC_RETURN_SUCCESS()               \
  /* bytecode.auxpos += popcnt(lanes) */  \
  KMOVW   K7, R8                          \
  POPCNTL R8, R8                          \
  ADDQ    R8, bytecode_auxpos(VIRT_BCPTR) \
  CLC                                     \
  RET

// the 'return' instruction
//
// _ = ret()
//
TEXT bcret(SB), NOSPLIT|NOFRAME, $0
  BC_RETURN_SUCCESS()

// return k[0] in K1
//
// _ = ret.k(k[0])
//
TEXT bcretk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(BX))

  BC_RETURN_SUCCESS()

// return b[0] in Z0:Z1 and k[1] in K1
//
// _ = ret.b.k(b[0], k[1])
//
TEXT bcretbk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(BX), OUT(CX))

  BC_LOAD_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(CX))

  BC_RETURN_SUCCESS()

// return s[0] in Z2:Z3 and k[1] in K1
//
// _ = ret.s.k(s[0], k[1])
//
TEXT bcretsk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(BX), OUT(CX))

  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(CX))

  BC_RETURN_SUCCESS()

// return b[0] in Z0:Z1 and k[2] in K1 (h[1] is ignored atm, it's just for SSA)
//
// _ = ret.b.h.k(b[0], h[1], k[2])
//
TEXT bcretbhk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(BX))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(CX))

  BC_LOAD_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(CX))

  BC_RETURN_SUCCESS()

// Temporary instruction to save registers alive at entry to stack slots.
//
// b[0].k[1] = init()
//
TEXT bcinit(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, DX, R8)
  MOVQ bytecode_symtab+0(VIRT_BCPTR), BX // BC <- symbol table base

  BC_STORE_SLICE_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K7), IN(R8))

  // Verify that a symbol table exists and fail if not. The test here prevents
  // asserting the presence of symtab every time a symbol is unsymbolized.
  TESTQ BX, BX
  JZ error_null_symtab

  NEXT_ADVANCE(BC_SLOT_SIZE*2)

  _BC_ERROR_HANDLER_NULL_SYMTAB()

// Mask Instructions
// -----------------

// k[0] = 0
//
// k[0] = broadcast0.k()
//
TEXT bcbroadcast0k(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(DX))
  MOVW $0, 0(VIRT_VALUES)(DX*1)
  NEXT_ADVANCE(BC_SLOT_SIZE*1)

// k[0] = 1 & ValidLanes
//
// k[0] = broadcast1.k()
//
TEXT bcbroadcast1k(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K7), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*1)

// v[0].k[1] = 0
//
// v[0].k[1] = false.k()
//
TEXT bcfalse(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(CX))
  VPXORD X0, X0, X0
  XORL BX, BX
  BC_STORE_VALUE_TO_SLOT(IN(Z0), IN(Z0), IN(Z0), IN(Z0), IN(DX))
  BC_STORE_RU16_TO_SLOT(IN(BX), IN(CX))

  NEXT_ADVANCE(BC_SLOT_SIZE*2)

// k[0] = !k[1] & ValidLanes
//
// k[0] = not.k(k[1])
//
TEXT bcnotk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(BX))

  KMOVW K7, CX
  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  ANDNL CX, BX, BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2)

// k[0] = k[1] & k[2] (never sets invalid lanes)
//
// k[0] = and.k(k[1], k[2])
//
TEXT bcandk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX))
  MOVWLZX 0(VIRT_VALUES)(BX*1), BX

  BC_UNPACK_SLOT(0, OUT(DX))
  ANDW 0(VIRT_VALUES)(CX*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = !k[1] & k[2] (never sets invalid lanes)
//
// k[0] = andn.k(k[1], k[2])
//
TEXT bcandnk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX))
  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))
  BC_LOAD_RU16_FROM_SLOT(OUT(CX), IN(CX))

  BC_UNPACK_SLOT(0, OUT(DX))
  ANDNL CX, BX, BX

  BC_STORE_RU16_TO_SLOT(IN(BX), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = k[1] | k[2] (never sets invalid lanes)
//
// k[0] = or.k(k[1], k[2])
//
TEXT bcork(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX))
  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))

  BC_UNPACK_SLOT(0, OUT(DX))
  ORW 0(VIRT_VALUES)(CX*1), BX

  BC_STORE_RU16_TO_SLOT(IN(BX), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = k[1] ^ k[2] (never sets invalid lanes)
//
// k[0] = xor.k(k[1], k[2])
//
TEXT bcxork(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX))
  MOVWLZX 0(VIRT_VALUES)(BX*1), BX

  BC_UNPACK_SLOT(0, OUT(DX))
  XORW 0(VIRT_VALUES)(CX*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = (!(k[1] ^ k[2])) & ValidLanes
//
// k[0] = xnor.k(k[1], k[2])
//
TEXT bcxnork(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX))
  KMOVW 0(VIRT_VALUES)(BX*1), K1
  KMOVW 0(VIRT_VALUES)(CX*1), K2

  BC_UNPACK_SLOT(0, OUT(DX))
  KXNORW K1, K2, K1
  KANDW K1, K7, K1
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// Conversion Instructions
// -----------------------

// f64[0] = k[1] ? 1.0 : 0.0
//
// f64[0] = cvt.ktof64(k[1])
//
TEXT bccvtktof64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(BX))

  VBROADCASTSD CONSTF64_1(), Z3
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(BX))

  VMOVAPD.Z Z3, K1, Z2
  VMOVAPD.Z Z3, K2, Z3

  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*2)

// i64[0] = k[1] ? 1 : 0
//
// i64[0] = cvt.ktoi64(k[1])
//
TEXT bccvtktoi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(BX))

  VPBROADCASTQ CONSTQ_1(), Z3
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(BX))

  VMOVDQA64.Z Z3, K1, Z2
  VMOVDQA64.Z Z3, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*2)

// k[0] = (i64[1] ? 1 : 0).k[2]
//
// k[0] = cvt.i64tok(i64[1]).k[2]
//
TEXT bccvti64tok(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))

  VPXORQ X4, X4, X4
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPCMPQ $VPCMP_IMM_NE, 0(VIRT_VALUES)(BX*1), Z4, K1, K1
  VPCMPQ $VPCMP_IMM_NE, 64(VIRT_VALUES)(BX*1), Z4, K2, K2

  KUNPCKBW K1, K2, K1
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = (f64[1] ? 1 : 0).k[2]
//
// k[0] = cvt.f64tok(i64[1]).k[2]
//
TEXT bccvtf64tok(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  VPXORQ X4, X4, X4
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VCMPPD $VCMP_IMM_NEQ_OQ, 0(VIRT_VALUES)(BX*1), Z4, K1, K1
  VCMPPD $VCMP_IMM_NEQ_OQ, 64(VIRT_VALUES)(BX*1), Z4, K2, K2
  KUNPCKBW K1, K2, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// f64[0].k[1] = cvt.i64tof64(i64[2]).k[3]
TEXT bccvti64tof64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VCVTQQ2PD.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VCVTQQ2PD.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = cvttrunc.f64toi64(f64[2]).k[3]
TEXT bccvttruncf64toi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VCVTTPD2QQ.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VCVTTPD2QQ.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = cvtfloor.f64toi64(f64[2]).k[3]
TEXT bccvtfloorf64toi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VCVTPD2QQ.RD_SAE.Z Z2, K1, Z2
  VCVTPD2QQ.RD_SAE.Z Z3, K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = cvtceil.f64toi64(f64[2]).k[3]
TEXT bccvtceilf64toi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VCVTPD2QQ.RU_SAE.Z Z2, K1, Z2
  VCVTPD2QQ.RU_SAE.Z Z3, K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// s[0].k[1] = cvt.i64tostr(i64[2]).k[3]
//
// scratch: 20 * 16
//
// Converts a signed 64-bit integer to a string slice.
//
// Implementation notes:
//   - maximum length of the output is 20 bytes, including '-' sign.
//   - we split the string into 3 parts (two 8-char parts, and one 4-char part) forming [4-8-8] string.
//   - the integer is converted to string by subdividing it, by 10000000000000000, 100000000, 10000, 100, and 10.
//   - then after we have 0-9 numbers in each byte representing a character, we just add '48' to make it ASCII.
//   - the length of each string in each lane is found at the end, by counting leading zeros.
//   - we always insert a '-' sign, the string length is incremented if the integer is negative, so it will only
//     appear when the input is negative. This simplifies the code a bit.
TEXT bccvti64tostr(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Get the signs so we can prepend '-' sign at the end.
  VPMOVQ2M Z2, K2
  VPMOVQ2M Z3, K3
  KUNPCKBW K2, K3, K2

  // Make the inputs unsigned - since we know which lanes are negative, it's destructive.
  VPABSQ Z2, Z2
  VPABSQ Z3, Z3

  // Step A:
  //
  // Split the input into 3 parts:
  //   Z2:Z3   <- 8-char low part lanes
  //   Z12:Z13 <- 8-char high part lanes
  //   Z20     <- 4-char high part lanes

  // NOTE: We don't rely on high-precision integer division here. We can just shift
  // right by 16 bits, which is the maximum we can to divide by `10000000000000000`
  // to get the 4-char high part lanes.
  VPSRLQ $16, Z2, Z12
  VPSRLQ $16, Z3, Z13

  // 152587890625 == 10000000000000000 >> 16
  VBROADCASTSD CONSTF64_152587890625(), Z19

  VCVTUQQ2PD Z12, Z12
  VCVTUQQ2PD Z13, Z13

  VDIVPD.RD_SAE Z19, Z12, Z8
  VDIVPD.RD_SAE Z19, Z13, Z9

  VPBROADCASTQ CONSTQ_0xFFFF(), Z20
  VBROADCASTSD CONSTF64_65536(), Z18

  VPANDQ Z20, Z3, Z21
  VPANDQ Z20, Z2, Z20

  VCVTUQQ2PD Z21, Z21
  VCVTUQQ2PD Z20, Z20

  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z8, Z8
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z9, Z9

  VFNMADD231PD Z19, Z8, Z12
  VFNMADD231PD Z19, Z9, Z13

  VFMADD132PD Z18, Z20, Z12
  VFMADD132PD Z18, Z21, Z13

  // Required for splitting to 8-char parts, where each part is between 0 to 99999999.
  VBROADCASTSD CONSTF64_100000000(), Z18

  VDIVPD.RD_SAE Z18, Z12, Z2
  VDIVPD.RD_SAE Z18, Z13, Z3

  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z2, Z2
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z3, Z3

  VFNMADD231PD Z18, Z2, Z12
  VFNMADD231PD Z18, Z3, Z13

  // Z20 <- 4-char high part lanes
  VCVTPD2UDQ Z8, Y20
  VCVTPD2UDQ Z9, Y21
  VINSERTI32X8 $1, Y21, Z20, Z20

  // Z2:Z3 <- 8-char high part lanes
  VCVTPD2UQQ Z2, Z2
  VCVTPD2UQQ Z3, Z3

  // Z12:Z13 <- 8-char low part lanes
  VCVTPD2UQQ Z12, Z12
  VCVTPD2UQQ Z13, Z13

  // Step B:
  //
  // Stringify the input parts:
  //   - the output would be 20 characters.
  //   - the stringification happens in 3 steps:
  //     - Step X - tens of thousands [X / 10000, X % 10000]
  //     - Step Y - hundreds          [Y / 100  , Y % 100  ]
  //     - Step Z - tens              [Z / 10   , Z % 10   ]
  //   - the output is bytes having numbers from 0-9 (decimals).

  // Constants for X step.
  VPBROADCASTQ CONSTQ_3518437209(), Z18
  VPBROADCASTQ CONSTQ_10000(), Z19

  // Z4:Z5, Z14:Z15 <- X / 10000
  VPMULUDQ Z18, Z2, Z4
  VPMULUDQ Z18, Z3, Z5
  VPMULUDQ Z18, Z12, Z14
  VPMULUDQ Z18, Z13, Z15

  VPSRLQ $45, Z4, Z4
  VPSRLQ $45, Z5, Z5
  VPSRLQ $45, Z14, Z14
  VPSRLQ $45, Z15, Z15

  // Z6:Z7, Z16:Z17 <- X % 10000
  VPMULUDQ Z19, Z4, Z6
  VPMULUDQ Z19, Z5, Z7
  VPMULUDQ Z19, Z14, Z16
  VPMULUDQ Z19, Z15, Z17

  VPSUBD Z6, Z2, Z6
  VPSUBD Z7, Z3, Z7
  VPSUBD Z16, Z12, Z16
  VPSUBD Z17, Z13, Z17

  // Constants for Y step.
  VPBROADCASTD CONSTD_5243(), Z18
  VPBROADCASTD CONSTD_100(), Z19

  // Z4:Z5, Z14:Z15 <- Y == [X / 10000, X % 10000]
  VPSLLQ $32, Z6, Z6
  VPSLLQ $32, Z7, Z7
  VPSLLQ $32, Z16, Z16
  VPSLLQ $32, Z17, Z17

  VPORD Z6, Z4, Z4
  VPORD Z7, Z5, Z5
  VPORD Z16, Z14, Z14
  VPORD Z17, Z15, Z15

  // Z6:Z7, Z16:Z17, Z21 <- Y / 100
  VPMULHUW Z18, Z20, Z21
  VPMULHUW Z18, Z4, Z6
  VPMULHUW Z18, Z5, Z7
  VPMULHUW Z18, Z14, Z16
  VPMULHUW Z18, Z15, Z17

  VPSRLW $3, Z21, Z21
  VPSRLW $3, Z6, Z6
  VPSRLW $3, Z7, Z7
  VPSRLW $3, Z16, Z16
  VPSRLW $3, Z17, Z17

  // Z4:Z5, Z14:Z15, Z20 <- Y % 100
  VPMULLW Z19, Z21, Z22
  VPMULLW Z19, Z6, Z8
  VPMULLW Z19, Z7, Z9
  VPMULLW Z19, Z16, Z18
  VPMULLW Z19, Z17, Z19

  VPSUBW Z22, Z20, Z20
  VPSUBW Z8, Z4, Z4
  VPSUBW Z9, Z5, Z5
  VPSUBW Z18, Z14, Z14
  VPSUBW Z19, Z15, Z15

  // Z4:Z5, Z14:Z15, Z20 <- Z == [Y / 100, Y % 100]
  VPSLLD $16, Z20, Z20
  VPSLLD $16, Z4, Z4
  VPSLLD $16, Z5, Z5
  VPSLLD $16, Z14, Z14
  VPSLLD $16, Z15, Z15

  VPORD Z21, Z20, Z20
  VPORD Z6, Z4, Z4
  VPORD Z7, Z5, Z5
  VPORD Z16, Z14, Z14
  VPORD Z17, Z15, Z15

  // Constants for Z step.
  VPBROADCASTW CONSTD_6554(), Z18
  VPBROADCASTW CONSTD_10(), Z19

  // Z4:Z5, Z14:Z15, Z21 <- Z / 10
  VPMULHUW Z18, Z20, Z21
  VPMULHUW Z18, Z4, Z6
  VPMULHUW Z18, Z5, Z7
  VPMULHUW Z18, Z14, Z16
  VPMULHUW Z18, Z15, Z17

  // Z4:Z5, Z14:Z15, Z20 <- Z % 10
  VPMULLW Z19, Z21, Z22
  VPMULLW Z19, Z6, Z8
  VPMULLW Z19, Z7, Z9
  VPMULLW Z19, Z16, Z18
  VPMULLW Z19, Z17, Z19

  VPSUBW Z22, Z20, Z20
  VPSUBW Z8, Z4, Z4
  VPSUBW Z9, Z5, Z5
  VPSUBW Z18, Z14, Z14
  VPSUBW Z19, Z15, Z15

  // Z4:Z5, Z14:Z15, Z20 <- [Z / 10, Z % 10]
  VPSLLW $8, Z20, Z20
  VPSLLW $8, Z4, Z4
  VPSLLW $8, Z5, Z5
  VPSLLW $8, Z14, Z14
  VPSLLW $8, Z15, Z15

  VPORD Z21, Z20, Z20
  VPORD Z6, Z4, Z4
  VPORD Z7, Z5, Z5
  VPORD Z16, Z14, Z14
  VPORD Z17, Z15, Z15

  // Step C:
  //
  // Find the length of the output string of each lane and insert a '-' sign
  // before the first non-zero character. This is not really trivial as the
  // string is split across three registers. So, we start at the highest
  // character and use VPLZCNT[D|Q] to advance.

  // This temporarily reverses the strings as we would not be able to
  // use VPLZCNT[D|Q] otherwise. There are in general two options, generate
  // reversed string, or reverse the string before the counting. It doesn't
  // matter, as either way we would have to reverse it (either for storing
  // or for zero counting).
  VBROADCASTI32X4 CONST_GET_PTR(bswap32, 0), Z10
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z9

  VPSHUFB Z10, Z20, Z10
  VPSHUFB Z9, Z4, Z6
  VPSHUFB Z9, Z5, Z7
  VPSHUFB Z9, Z14, Z8
  VPSHUFB Z9, Z15, Z9

  // Stringified number must have at least 1 character, so make it nonzero in tmp Z8/Z9.
  VPORQ.BCST CONSTD_0x7F(), Z8, Z8
  VPORQ.BCST CONSTD_0x7F(), Z9, Z9

  VPLZCNTD Z10, Z10
  VPLZCNTQ Z6, Z6
  VPLZCNTQ Z7, Z7
  VPLZCNTQ Z8, Z8
  VPLZCNTQ Z9, Z9

  // VPLZCNT[D|Q] gives us bits, but we need shifts of 8-bit quantities.
  //
  // NOTE: We keep the quantities in bits - so 2 characters are 16, etc... The reason
  // is that this makes the code simpler as shift operation needs bits, and we have to
  // insert a sign, which is shifted by bits and not bytes.
  VPBROADCASTD CONSTD_7(), Z11
  VPANDND Z10, Z11, Z10
  VPANDNQ Z6, Z11, Z6
  VPANDNQ Z7, Z11, Z7
  VPANDNQ Z8, Z11, Z8
  VPANDNQ Z9, Z11, Z9

  // Number of characters * 8 of the output string (will be advanced).
  VPSLLD.BCST $3, CONSTD_20(), Z3

  // Advance high 4 chars.
  VPSUBD Z10, Z3, K1, Z3
  VPSUBD.BCST CONSTD_8(), Z10, Z10
  VPBROADCASTD CONSTD_3(), Z11
  VPSLLVD Z10, Z11, Z11
  VPSUBB Z11, Z20, Z20

  // Advance high 8-chars.
  VPCMPEQD.BCST CONSTD_128(), Z3, K3
  KSHIFTRW $8, K3, K4

  VPMOVQD Z6, Y10
  VPMOVQD Z7, Y11
  VPSUBQ.BCST CONSTQ_8(), Z6, Z6
  VPSUBQ.BCST CONSTQ_8(), Z7, Z7

  VINSERTI32X8 $1, Y11, Z10, Z10
  VPSUBD Z10, Z3, K3, Z3

  VPBROADCASTQ.Z CONSTQ_3(), K3, Z12
  VPBROADCASTQ.Z CONSTQ_3(), K4, Z13
  VPSLLVQ Z6, Z12, Z12
  VPSLLVQ Z7, Z13, Z13

  VPSUBB Z12, Z4, Z4
  VPSUBB Z13, Z5, Z5

  // Advance low 8-chars.
  VPCMPEQD.BCST CONSTD_64(), Z3, K3
  KSHIFTRW $8, K3, K4

  VPMOVQD Z8, Y10
  VPMOVQD Z9, Y11
  VPSUBQ.BCST CONSTQ_8(), Z8, Z8
  VPSUBQ.BCST CONSTQ_8(), Z9, Z9

  VINSERTI32X8 $1, Y11, Z10, Z10
  VPSUBD Z10, Z3, K3, Z3

  VPBROADCASTQ.Z CONSTQ_3(), K3, Z12
  VPBROADCASTQ.Z CONSTQ_3(), K4, Z13
  VPSLLVQ Z8, Z12, Z12
  VPSLLVQ Z9, Z13, Z13

  VPSUBB Z12, Z14, Z14
  VPSUBB Z13, Z15, Z15

  // Z3 contains the number of characters * 8 (in bit units) - convert it back to bytes.
  VPSRLD $3, Z3, Z3

  // Step D:
  //
  // Shuffle in a way so we get low 16 character part for each lane. The rest 4
  // characters are kept in Z20 (4 character high lanes). Then store the characters
  // to consecutive memory.

  VPUNPCKLQDQ Z14, Z4, Z6 // Lane [06] [04] [02] [00]
  VPUNPCKHQDQ Z14, Z4, Z7 // Lane [07] [05] [03] [01]
  VPUNPCKLQDQ Z15, Z5, Z8 // Lane [14] [12] [10] [08]
  VPUNPCKHQDQ Z15, Z5, Z9 // Lane [15] [13] [11] [09]

  // Constants for converting the number to ASCII.
  VPBROADCASTB CONSTD_48(), Z18

  VPADDB Z18, Z20, Z20
  VPADDB Z18, Z6, Z6
  VPADDB Z18, Z7, Z7
  VPADDB Z18, Z8, Z8
  VPADDB Z18, Z9, Z9

  // Z3 now contains the length of the output string of each lane including '-' sign when negative.
  VPADDD.BCST CONSTD_1(), Z3, K2, Z3

  // Make sure we have at least 20 bytes for each lane, we always overallocate to make the conversion easier.
  BC_CHECK_SCRATCH_CAPACITY($(20 * 16), R8, abort)

  BC_GET_SCRATCH_BASE_GP(R8)

  // Update the length of the output buffer.
  ADDQ $(20 * 16), bytecode_scratch+8(VIRT_BCPTR)

  // Broadcast scratch base to all lanes in Z2, which becomes string slice offset.
  VPBROADCASTD.Z R8, K1, Z2

  // Make R8 the first address where the output will be stored.
  ADDQ VIRT_BASE, R8

  VPADDD CONST_GET_PTR(consts_offsets_d_20, 4), Z2, Z2
  VPSUBD.Z Z3, Z2, K1, Z2

  VEXTRACTI32X4 $1, Z20, X21
  VEXTRACTI32X4 $2, Z20, X22
  VEXTRACTI32X4 $3, Z20, X23

  // Store output strings (low lanes).
  VPEXTRD $0, X20, 0(R8)
  VEXTRACTI32X4 $0, Z6, 4(R8)
  VPEXTRD $1, X20, 20(R8)
  VEXTRACTI32X4 $0, Z7, 24(R8)
  VPEXTRD $2, X20, 40(R8)
  VEXTRACTI32X4 $1, Z6, 44(R8)
  VPEXTRD $3, X20, 60(R8)
  VEXTRACTI32X4 $1, Z7, 64(R8)

  VPEXTRD $0, X21, 80(R8)
  VEXTRACTI32X4 $2, Z6, 84(R8)
  VPEXTRD $1, X21, 100(R8)
  VEXTRACTI32X4 $2, Z7, 104(R8)
  VPEXTRD $2, X21, 120(R8)
  VEXTRACTI32X4 $3, Z6, 124(R8)
  VPEXTRD $3, X21, 140(R8)
  VEXTRACTI32X4 $3, Z7, 144(R8)

  // Store output strings (high lanes).
  VPEXTRD $0, X22, 160(R8)
  VEXTRACTI32X4 $0, Z8, 164(R8)
  VPEXTRD $1, X22, 180(R8)
  VEXTRACTI32X4 $0, Z9, 184(R8)
  VPEXTRD $2, X22, 200(R8)
  VEXTRACTI32X4 $1, Z8, 204(R8)
  VPEXTRD $3, X22, 220(R8)
  VEXTRACTI32X4 $1, Z9, 224(R8)

  VPEXTRD $0, X23, 240(R8)
  VEXTRACTI32X4 $2, Z8, 244(R8)
  VPEXTRD $1, X23, 260(R8)
  VEXTRACTI32X4 $2, Z9, 264(R8)
  VPEXTRD $2, X23, 280(R8)
  VEXTRACTI32X4 $3, Z8, 284(R8)
  VPEXTRD $3, X23, 300(R8)
  VEXTRACTI32X4 $3, Z9, 304(R8)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()


// Comparison Instructions - [Sort]Cmp(Value, Value)
// -------------------------------------------------

#include "evalbc_cmpv_impl.h"

// s[0].k[1] = cmpv(v[2], v[3], k[4])
TEXT bccmpv(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTI32X4 CONST_GET_PTR(cmpv_predicate_matching_type, 0), Z16
  JMP cmpv_tail(SB)

// s[0].k[1] = sortcmpv@nf(v[2], v[3], k[4])
TEXT bcsortcmpvnf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTI32X4 CONST_GET_PTR(cmpv_predicate_sort_nulls_first, 0), Z16
  JMP cmpv_tail(SB)

// s[0].k[1] = sortcmpv@nl(v[2], v[3], k[4])
TEXT bcsortcmpvnl(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTI32X4 CONST_GET_PTR(cmpv_predicate_sort_nulls_last, 0), Z16
  JMP cmpv_tail(SB)

TEXT cmpv_tail(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))

  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z10), IN(BX))       // Z10 <- left value header length (TLV + [Length] size in bytes)
  VMOVDQU32 BC_VSTACK_PTR(BX, 64), Z11                 // Z11 <- left value length

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z12), IN(CX))       // Z12 <- right value header length (TLV + [Length] size in bytes)
  VMOVDQU32 BC_VSTACK_PTR(CX, 64), Z13                 // Z13 <- right value length

  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z14), IN(BX))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z15), IN(CX))

  VPSUBD.Z Z10, Z11, K1, Z11                           // Z11 <- left value content length (length - hLen)
  VPADDD.Z BC_VSTACK_PTR(BX, 0), Z10, K1, Z10          // Z10 <- left value content offset (offset + hLen)
  VPSUBD.Z Z12, Z13, K1, Z13                           // Z13 <- right value content length (length - hLen)
  VPADDD.Z BC_VSTACK_PTR(CX, 0), Z12, K1, Z12          // Z12 <- right value content offset (offset + hLen)

  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z30       // Z30 <- predicate(bswap64)
  VPTERNLOGD $0xFF, Z31, Z31, Z31                      // Z31 <- dword(0xFFFFFFFF)

  MOVQ VIRT_BASE, R8                                   // R8 <- base of the right value (the same as left)
  CALL fncmpv(SB)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VEXTRACTI32X8 $1, Z16, Y17
  VPMOVSXDQ Y16, Z16                                   // Z16 <- merged comparison results of all types (low)
  VPMOVSXDQ Y17, Z17                                   // Z17 <- merged comparison results of all types (high)

  BC_STORE_I64_TO_SLOT(IN(Z16), IN(Z17), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// Comparison Instructions - Cmp(Value, Bool)
// ------------------------------------------

// compares the content of a boxed value and bool
//
// i64[0].k[1] = cmpv.k(v[2], k[3]).k[4]
//
TEXT bccmpvk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, BX, CX, R8)

  KMOVW 0(VIRT_VALUES)(R8*1), K1
  VMOVDQU32 0(VIRT_VALUES)(BX*1), Z2

  KMOVW K1, K3
  VPXORD X4, X4, X4
  VPGATHERDD 0(VIRT_BASE)(Z2*1), K3, Z4                // Z4 <- first 4 bytes of the left value

  VPBROADCASTD CONSTD_0x0F(), Z6                       // Z6 <- Constant(0xF)
  KMOVW 0(VIRT_VALUES)(CX*1), K2                       // K2 <- right boolean value
  VPSRLD.Z $3, Z6, Z7                                  // Z7 <- Constant(1)

  VPSRLD $4, Z4, Z5
  VPANDD Z6, Z5, Z5                                    // Z5 <- left ION type
  VPANDD Z6, Z4, Z4                                    // Z4 <- left boolean value (0 or 1)
  VMOVDQA32.Z Z7, K2, Z2                               // Z2 <- right boolean value (0 or 1)
  VPCMPEQD Z7, Z5, K1, K1                              // K1 <- bool comparison predicate (the output predicate)

  VPSUBD.Z Z2, Z4, K1, Z2                              // Z2 <- comparison results {-1, 0, 1} (all)
  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                     // Z2 <- comparison results (low)
  VPMOVSXDQ Y3, Z3                                     // Z3 <- comparison results (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// compares the content of a boxed value and bool (imm)
//
// i64[0].k[1] = cmpv.k@imm(v[2], k@imm[3]).k[4]
//
TEXT bccmpvkimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3 + BC_IMM16_SIZE, OUT(R8))

  VMOVDQU32 0(VIRT_VALUES)(BX*1), Z2
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  KMOVW K1, K2
  VPXORQ X4, X4, X4
  VPGATHERDD 0(VIRT_BASE)(Z2*1), K2, Z4                // Z4 <- first 4 bytes of the left value

  VPBROADCASTD CONSTD_0x0F(), Z6                       // Z6 <- Constant(0xF)
  VPSRLD $3, Z6, Z7                                    // Z7 <- Constant(1)
  VPANDD.BCST (BC_SLOT_SIZE*3)(VIRT_PCREG), Z7, Z2     // Z2 <- right boolean value (0 or 1)

  VPSRLD $4, Z4, Z5
  VPANDD Z6, Z5, Z5                                    // Z5 <- left ION type
  VPANDD Z6, Z4, Z4                                    // Z4 <- left boolean value (0 or 1)
  VPCMPEQD Z7, Z5, K1, K1                              // K1 <- bool comparison predicate (the output predicate)

  VPSUBD.Z Z2, Z4, K1, Z2                              // Z2 <- comparison results {-1, 0, 1} (all)
  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                     // Z2 <- comparison results (low)
  VPMOVSXDQ Y3, Z3                                     // Z3 <- comparison results (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_IMM16_SIZE)


// Comparison Instructions - Cmp(Value, Int64)
// -------------------------------------------

// Compares the content of a boxed value and i64
//
// i64[0].k[1] = cmpv.i64(v[2], i64[3]).k[4]
//
TEXT bccmpvi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_CALC_ADVANCE(BC_SLOT_SIZE*5, OUT(R15))
  BC_LOAD_I64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(CX))
  JMP cmpvi64_tail(SB)

// Compares the content of a boxed value and i64 immediate
//
// i64[0].k[1] = cmpv.i64@imm(v[2], i64@imm[3]).k[4]
//
TEXT bccmpvi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z6), OUT(R8))
  BC_CALC_ADVANCE(BC_SLOT_SIZE*4+8, OUT(R15))
  VMOVDQA64 Z6, Z7
  JMP cmpvi64_tail(SB)

TEXT cmpvi64_tail(SB), NOSPLIT|NOFRAME, $0
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_VALUE_SLICE_FROM_SLOT(OUT(Z30), OUT(Z31), IN(BX))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z9), IN(BX))

  VPXORD X2, X2, X2                                   // Z2 <- Comparison results, initially all zeros (low)
  VPBROADCASTD CONSTD_0x0F(), Z10

  VPXORD X3, X3, X3                                   // Z3 <- Comparison results, initially all zeros (high)
  VBROADCASTI32X4 CONST_GET_PTR(cmpv_predicate_matching_type, 0), Z11

  VPSRLD $4, Z9, Z8                                   // Z8 <- left ION type
  VPSHUFB Z8, Z11, Z11                                // Z11 <- left ION type converted to an internal type
  VPANDD Z10, Z9, Z10                                 // Z10 <- left L field
  VPCMPEQD.BCST CONSTD_2(), Z11, K1, K1               // K1 <- number comparison predicate (the output predicate)

  KTESTW K1, K1
  JZ skip                                             // Skip number comparison if there are no numbers (K1 == 0)

  // Gather 8 bytes of each left value
  VEXTRACTI32X8 $1, Z30, Y11
  KMOVB K1, K2
  VPXORQ X4, X4, X4
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K2, Z4
  KSHIFTRW $8, K1, K3
  VPXORQ X5, X5, X5
  VPGATHERDQ 1(VIRT_BASE)(Y11*1), K3, Z5

  // Byteswap each gathered value and shift right in case of signed/unsigned int
  VPBROADCASTD CONSTD_8(), Z11
  VPSUBD Z10, Z11, Z10
  VPSLLD $3, Z10, Z10

  VEXTRACTI32X8 $1, Z10, Y11
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z12
  VPMOVZXDQ Y10, Z10
  VPMOVZXDQ Y11, Z11

  VPSHUFB Z12, Z4, Z4
  VPSHUFB Z12, Z5, Z5

  VPBROADCASTD CONSTD_1(), Z12
  VPBROADCASTD CONSTD_3(), Z13
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z14

  VPSRLVQ Z10, Z4, Z4
  VPSRLVQ Z11, Z5, Z5

  // Convert negative uint64 to int64 (ION stores negative integers as positive ones, but having a different tag)
  VPCMPEQD Z13, Z8, K1, K2
  KSHIFTRW $8, K2, K3

  VPSUBQ Z4, Z2, K2, Z4
  VPSUBQ Z5, Z3, K3, Z5

  VPCMPGTD Z13, Z8, K1, K2                             // K2 <- mask of floating point values (low / all)
  KSHIFTRW $8, K2, K3                                  // K3 <- mask of floating point values (high)

  VCVTQQ2PD Z6, K2, Z6                                 // Z6 <- mixed i64|f64 values depending on left value type (low)
  VCVTQQ2PD Z7, K3, Z7                                 // Z7 <- mixed i64|f64 values depending on left value type (high)

  VPANDQ.Z Z6, Z4, K2, Z10                             // Z10 <- MSB bits of left & right negative floats (low)
  VPANDQ.Z Z7, Z5, K3, Z11                             // Z11 <- MSB bits of left & right negative floats (high)
  VPMOVQ2M Z10, K5                                     // K4 <- floating point negative values (low)
  VPMOVQ2M Z11, K6                                     // K5 <- floating point negative values (high)

  VPXORQ X10, X10, X10
  KUNPCKBW K5, K6, K5                                  // K5 <- floating point negative values (all)

  KSHIFTRW $8, K1, K2
  VPCMPQ $VPCMP_IMM_LT, Z6, Z4, K1, K3                 // K3 <- less than (low)
  VPCMPQ $VPCMP_IMM_LT, Z7, Z5, K2, K4                 // K4 <- less than (high)
  KUNPCKBW K3, K4, K3                                  // K3 <- less than (all)
  VMOVDQA32 Z14, K3, Z2                                // Z2 <- merge less than results (-1)

  VPCMPQ $VPCMP_IMM_GT, Z6, Z4, K1, K3                 // K3 <- greater than (low)
  VPCMPQ $VPCMP_IMM_GT, Z7, Z5, K2, K4                 // K4 <- greater than (high)
  KUNPCKBW K3, K4, K3                                  // K3 <- greater than (all)
  VMOVDQA32 Z12, K3, Z2                                // Z2 <- merge greater than results (1)
  VPSUBD Z2, Z10, K5, Z2                               // Z2 <- results with corrected floating point comparison

  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                     // Z2 <- comparison results (low)
  VPMOVSXDQ Y3, Z3                                     // Z3 <- comparison results (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  BC_ADVANCE_REG(R15)

skip:
  VPXORQ X2, X2, X2

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z2), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  BC_ADVANCE_REG(R15)

// Comparison Instructions - Cmp(Value, Float64)
// ---------------------------------------------

// Compares the content of a boxed value and f64
//
// i64[0].k[1] = cmpv.f64(value[2], f64[3]).k[4]
//
TEXT bccmpvf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_CALC_ADVANCE(BC_SLOT_SIZE*5, OUT(R15))
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(CX))
  JMP cmpvf64_tail(SB)

// Compares the content of a boxed value and f64 immediate
//
// i64[0].k[1] = cmpv.f64@imm(value[2], f64@imm[3]).k[4]
//
TEXT bccmpvf64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZF64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z6), OUT(R8))
  BC_CALC_ADVANCE(BC_SLOT_SIZE*4+8, OUT(R15))
  VMOVDQA64 Z6, Z7
  JMP cmpvf64_tail(SB)

TEXT cmpvf64_tail(SB), NOSPLIT|NOFRAME, $0
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_VALUE_SLICE_FROM_SLOT(OUT(Z30), OUT(Z31), IN(BX))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z9), IN(BX))

  VPXORD X2, X2, X2                                   // Z2 <- Comparison results, initially all zeros (low)
  VPBROADCASTD CONSTD_0x0F(), Z10

  VPXORD X3, X3, X3                                   // Z3 <- Comparison results, initially all zeros (high)
  VBROADCASTI32X4 CONST_GET_PTR(cmpv_predicate_matching_type, 0), Z11

  VPSRLD $4, Z9, Z8                                   // Z8 <- left ION type
  VPSHUFB Z8, Z11, Z11                                // Z11 <- left ION type converted to an internal type
  VPANDD Z10, Z9, Z10                                 // Z10 <- left L field
  VPCMPEQD.BCST CONSTD_2(), Z11, K1, K1               // K1 <- number comparison predicate (the output predicate)

  KTESTW K1, K1
  JZ skip                                             // Skip number comparison if there are no numbers (K1 == 0)

  // Gather 8 bytes of each left value
  VEXTRACTI32X8 $1, Z30, Y11
  KMOVB K1, K2
  VPXORQ X4, X4, X4
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K2, Z4
  KSHIFTRW $8, K1, K3
  VPXORQ X5, X5, X5
  VPGATHERDQ 1(VIRT_BASE)(Y11*1), K3, Z5

  // Byteswap each gathered value and shift right in case of signed/unsigned int
  VPBROADCASTD CONSTD_8(), Z11
  VPSUBD Z10, Z11, Z10
  VPSLLD $3, Z10, Z10

  VEXTRACTI32X8 $1, Z10, Y11
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z12
  VPMOVZXDQ Y10, Z10
  VPMOVZXDQ Y11, Z11

  VPSHUFB Z12, Z4, Z4
  VPSHUFB Z12, Z5, Z5

  VPBROADCASTD CONSTD_1(), Z12
  VPBROADCASTD CONSTD_3(), Z13
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z14

  VPSRLVQ Z10, Z4, Z4
  VPSRLVQ Z11, Z5, Z5

  // Convert negative uint64 to int64 (ION stores negative integers as positive ones, but having a different tag)
  VPCMPEQD Z13, Z8, K1, K2
  KSHIFTRW $8, K2, K3

  VPSUBQ Z4, Z2, K2, Z4
  VPSUBQ Z5, Z3, K3, Z5

  VPCMPD $VPCMP_IMM_LE, Z13, Z8, K1, K2                // K2 <- mask of integer values (all)
  KSHIFTRW $8, K2, K3                                  // K3 <- mask of integer values (high)

  VCVTQQ2PD Z4, K2, Z4                                 // Z4 <- left numbers converted to float64 (low)
  KSHIFTRW $8, K1, K2                                  // K1 <- active lanes (high)
  VCVTQQ2PD Z5, K3, Z5                                 // Z5 <- left numbers converted to float64 (high)

  VPANDQ.Z Z6, Z4, K1, Z10                             // Z10 <- MSB bits of left & right negative floats (low)
  VPANDQ.Z Z7, Z5, K2, Z11                             // Z11 <- MSB bits of left & right negative floats (high)
  VPMOVQ2M Z10, K5                                     // K4 <- floating point negative values (low)
  VPMOVQ2M Z11, K6                                     // K5 <- floating point negative values (high)

  VPXORQ X10, X10, X10
  KUNPCKBW K5, K6, K5                                  // K5 <- floating point negative values (all)

  VPCMPQ $VPCMP_IMM_LT, Z6, Z4, K1, K3                 // K3 <- less than (low)
  VPCMPQ $VPCMP_IMM_LT, Z7, Z5, K2, K4                 // K4 <- less than (high)
  KUNPCKBW K3, K4, K3                                  // K3 <- less than (all)
  VMOVDQA32 Z14, K3, Z2                                // Z2 <- merge less than results (-1)

  VPCMPQ $VPCMP_IMM_GT, Z6, Z4, K1, K3                 // K3 <- greater than (low)
  VPCMPQ $VPCMP_IMM_GT, Z7, Z5, K2, K4                 // K4 <- greater than (high)
  KUNPCKBW K3, K4, K3                                  // K3 <- greater than (all)
  VMOVDQA32 Z12, K3, Z2                                // Z2 <- merge greater than results (1)
  VPSUBD Z2, Z10, K5, Z2                               // Z2 <- results with corrected floating point comparison

  VEXTRACTI32X8 $1, Z2, Y3
  VPMOVSXDQ Y2, Z2                                     // Z2 <- comparison results (low)
  VPMOVSXDQ Y3, Z3                                     // Z3 <- comparison results (high)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  BC_ADVANCE_REG(R15)

skip:
  VPXORQ X2, X2, X2

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z2), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  BC_ADVANCE_REG(R15)

// Comparison Instructions - String
// --------------------------------

// k[0] = cmplt.str(str[1], str[2]).k[3]
TEXT bccmpltstr(SB), NOSPLIT|NOFRAME, $0
#define BC_CMP_I_IMM VPCMP_IMM_LT
#include "evalbc_cmpxxstr_impl.h"
#undef BC_CMP_I_IMM
    NEXT()

// k[0] = cmple.str(str[1], str[2]).k[3]
TEXT bccmplestr(SB), NOSPLIT|NOFRAME, $0
#define BC_CMP_I_IMM VPCMP_IMM_LE
#include "evalbc_cmpxxstr_impl.h"
#undef BC_CMP_I_IMM
    NEXT()

// k[0] = cmpgt.str(str[1], str[2]).k[3]
TEXT bccmpgtstr(SB), NOSPLIT|NOFRAME, $0
#define BC_CMP_I_IMM VPCMP_IMM_GT
#include "evalbc_cmpxxstr_impl.h"
#undef BC_CMP_I_IMM
    NEXT()

// k[0] = cmpge.str(str[1], str[2]).k[3]
TEXT bccmpgestr(SB), NOSPLIT|NOFRAME, $0
#define BC_CMP_I_IMM VPCMP_IMM_GE
#include "evalbc_cmpxxstr_impl.h"
#undef BC_CMP_I_IMM
    NEXT()


// Comparison Instructions - Bool
// ------------------------------

// `A < B` => C (simplify to C = !A & B)
// ------------
// `0 < 0` => 0
// `0 < 1` => 1
// `1 < 0` => 0
// `1 < 1` => 0

// k[0] = cmplt.k(k[1], k[2]).k[3]
TEXT bccmpltk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  MOVWLZX 0(VIRT_VALUES)(CX*1), CX
  ANDNL CX, BX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// k[0] = cmplt.k@imm(k[1], k@imm[2]).k[3]
TEXT bccmpltkimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  ANDNL CX, BX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)

// `A <= B` => C (simplify to C = !A | B)
// -------------
// `0 <= 0` => 1
// `0 <= 1` => 1
// `1 <= 0` => 0
// `1 <= 1` => 1

// k[0] = cmple.k(k[1], k[2]).k[3]
TEXT bccmplek(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  MOVWLZX 0(VIRT_VALUES)(CX*1), CX
  ANDNL R8, BX, BX
  ORL CX, BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// k[0] = cmple.k@imm(k[1], k@imm[2]).k[3]
TEXT bccmplekimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  NOTL BX
  ORL CX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)

// `A > B` => C (simplify to C = A & !B)
// ------------
// `0 > 0` => 0
// `0 > 1` => 0
// `1 > 0` => 1
// `1 > 1` => 0

// k[0] = cmpgt.k(k[1], k[2]).k[3]
TEXT bccmpgtk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  MOVWLZX 0(VIRT_VALUES)(CX*1), CX
  ANDNL BX, CX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// k[0] = cmpgt.k@imm(k[1], k@imm[2]).k[3]
TEXT bccmpgtkimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  ANDNL BX, CX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)

// `A >= B` => C (simplify to C = A | !B)
// -------------
// `0 >= 0` => 1
// `0 >= 1` => 0
// `1 >= 0` => 1
// `1 >= 1` => 1

// k[0] = cmpge.k(k[1], k[2]).k[3]
TEXT bccmpgek(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  MOVWLZX 0(VIRT_VALUES)(CX*1), CX
  ANDNL R8, CX, CX
  ORL CX, BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// k[0] = cmpge.k@imm(k[1], k@imm[2]).k[3]
TEXT bccmpgekimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))

  MOVWLZX 0(VIRT_VALUES)(BX*1), BX
  NOTL CX
  ORL CX, BX
  ANDW 0(VIRT_VALUES)(R8*1), BX
  MOVW BX, 0(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)


// Comparison Instructions - Number
// --------------------------------

// k[0] = cmp_xx(f64[1], f64@imm[2]).k[3]
#define BC_CMP_OP_F64_IMM(Predicate)                                   \
  BC_UNPACK_2xSLOT_ZF64_SLOT(0, OUT(DX), OUT(BX), OUT(Z4), OUT(R8))    \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                    \
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                  \
                                                                       \
  VCMPPD Predicate, Z4, Z2, K1, K1                                     \
  VCMPPD Predicate, Z4, Z3, K2, K2                                     \
                                                                       \
  KUNPCKBW K1, K2, K1                                                  \
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))

// k[0] = cmp_xx(i64[1], i64[2]).k[3]
#define BC_CMP_OP_I64(Predicate)                                       \
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))              \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                    \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                  \
                                                                       \
  VPCMPQ Predicate, 0(VIRT_VALUES)(CX*1), Z2, K1, K1                   \
  VPCMPQ Predicate, 64(VIRT_VALUES)(CX*1), Z3, K2, K2                  \
                                                                       \
  KUNPCKBW K1, K2, K1                                                  \
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))

// k[0] = cmp_xx(i64[1], i64@imm[2]).k[3]
#define BC_CMP_OP_I64_IMM(Predicate)                                   \
  BC_UNPACK_2xSLOT_ZI64_SLOT(0, OUT(DX), OUT(BX), OUT(Z4), OUT(R8))    \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                    \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                  \
                                                                       \
  VPCMPQ Predicate, Z4, Z2, K1, K1                                     \
  VPCMPQ Predicate, Z4, Z3, K2, K2                                     \
                                                                       \
  KUNPCKBW K1, K2, K1                                                  \
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))

// k[0] = cmpeq.f64(f64[1], f64[2]).k[3]
//
// Floating point equality in the sense that
// `-0 == 0`, `NaN == NaN`, and `-NaN != NaN`
TEXT bccmpeqf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))

  // Floating point comparison handles `-0 == 0` properly
  VCMPPD $VCMP_IMM_EQ_OQ, Z4, Z2, K1, K3
  VCMPPD $VCMP_IMM_EQ_OQ, Z5, Z3, K2, K4

  // Integer comparison handles `NaN == NaN` properly
  VPCMPEQQ Z4, Z2, K1, K1
  VPCMPEQQ Z5, Z3, K2, K2

  KORW K3, K1, K1
  KORW K4, K2, K2
  KUNPCKBW K1, K2, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// current scalar float == f64(imm)
//
// k[0] = cmpeq.f64@imm(f64[1], f64@imm[2]).k[3]
//
TEXT bccmpeqf64imm(SB), NOSPLIT|NOFRAME, $0
  // We expect that the immediate is not NaN or -0 here. If the immediate
  // value is NaN 'bcisnanf' should be used instead. This gives us the
  // opportunity to make this function smaller as we know what the second
  // value isn't.
  BC_CMP_OP_F64_IMM($VCMP_IMM_EQ_OQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

// Implements `cmp_unordered_lt(a, b) ^ isnan(a)`:
//   - `val < val` -> result
//   - `NaN < val` -> false
//   - `val < NaN` -> true
//   - `NaN < NaN` -> false
//
// k[0] = cmplt.f64(f64[1], f64[2]).k[3]
TEXT bccmpltf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  VCMPPD $VCMP_IMM_UNORD_Q, Z2, Z2, K1, K3
  VCMPPD $VCMP_IMM_UNORD_Q, Z3, Z3, K2, K4
  VCMPPD $VCMP_IMM_NGE_UQ, 0(VIRT_VALUES)(CX*1), Z2, K1, K1
  VCMPPD $VCMP_IMM_NGE_UQ, 64(VIRT_VALUES)(CX*1), Z3, K2, K2

  KUNPCKBW K3, K4, K3
  KUNPCKBW K1, K2, K1
  KXORW K3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// `val < imm` -> result
// `NaN < imm` -> false
//
// k[0] = cmplt.f64@imm(f64[1], f64@imm[2]).k[3]
//
TEXT bccmpltf64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_F64_IMM($VCMP_IMM_LT_OQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

// Implements `cmp_ordered_le(a, b) | isnan(b)`:
//   - `val <= val` -> result
//   - `NaN <= val` -> false
//   - `val <= NaN` -> true
//   - `NaN <= NaN` -> true
//
// k[0] = cmple.f64(f64[1], f64[2]).k[3]
//
TEXT bccmplef64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))

  VCMPPD $VCMP_IMM_UNORD_Q, Z4, Z4, K1, K3
  VCMPPD $VCMP_IMM_UNORD_Q, Z5, Z5, K2, K4
  VCMPPD $VCMP_IMM_LE_OQ, Z4, Z2, K1, K1
  VCMPPD $VCMP_IMM_LE_OQ, Z5, Z3, K2, K2

  KUNPCKBW K3, K4, K3
  KUNPCKBW K1, K2, K1
  KORW K3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// `val <= imm` -> result
// `NaN <= imm` -> false
//
// k[0] = cmple.f64@imm(f64[1], f64@imm[2]).k[3]
//
TEXT bccmplef64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_F64_IMM($VCMP_IMM_LE_OQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

// Implements `cmp_unordered_gt(a, b) ^ isnan(b)`
//   - `val > val` -> result
//   - `NaN > val` -> true
//   - `val > NaN` -> false
//   - `NaN > NaN` -> false
//
// k[0] = cmpgt.f64(f64[1], f64[2]).k[3]
//
TEXT bccmpgtf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))

  VCMPPD $VCMP_IMM_UNORD_Q, Z4, Z4, K1, K3
  VCMPPD $VCMP_IMM_UNORD_Q, Z5, Z5, K2, K4
  VCMPPD $VCMP_IMM_NLE_UQ, Z4, Z2, K1, K1
  VCMPPD $VCMP_IMM_NLE_UQ, Z5, Z3, K2, K2

  KUNPCKBW K3, K4, K3
  KUNPCKBW K1, K2, K1
  KXORW K3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// `val > imm` -> result
// `NaN > imm` -> true
//
// k[0] = cmpgt.f64@imm(f64[1], f64@imm[2]).k[3]
//
TEXT bccmpgtf64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_F64_IMM($VCMP_IMM_NLE_UQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

// Implements `cmp_ordered_ge(a, b) | isnan(a)`
//   - `val >= val` -> result
//   - `NaN >= val` -> true
//   - `val >= NaN` -> false
//   - `NaN >= NaN` -> true
//
// k[0] = cmpge.f64(f64[1], f64[2]).k[3]
//
TEXT bccmpgef64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  VCMPPD $VCMP_IMM_UNORD_Q, Z2, Z2, K1, K3
  VCMPPD $VCMP_IMM_UNORD_Q, Z3, Z3, K2, K4
  VCMPPD $VCMP_IMM_GE_OQ, 0(VIRT_VALUES)(CX*1), Z2, K1, K1
  VCMPPD $VCMP_IMM_GE_OQ, 64(VIRT_VALUES)(CX*1), Z3, K2, K2

  KUNPCKBW K3, K4, K3
  KUNPCKBW K1, K2, K1
  KXORW K3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// `val >= imm` -> result
// `NaN >= imm` -> true
//
// k[0] = cmpge.f64@imm(f64[1], f64@imm[2]).k[3]
//
TEXT bccmpgef64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_F64_IMM($VCMP_IMM_NLT_UQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

//
// k[0] = cmpeq.i64(i64[1], i64[2]).k[3]
//
TEXT bccmpeqi64(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64($VPCMP_IMM_EQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

//
// k[0] = cmpeq.i64@imm(i64[1], i64@imm[2]).k[3]
//
TEXT bccmpeqi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64_IMM($VPCMP_IMM_EQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

//
// k[0] = cmplt.i64(i64[1], i64[2]).k[3]
//
TEXT bccmplti64(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64($VPCMP_IMM_LT)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

//
// k[0] = cmplt.i64@imm(i64[1], i64@imm[2]).k[3]
//
TEXT bccmplti64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64_IMM($VPCMP_IMM_LT)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

//
// k[0] = cmple.i64(i64[1], i64[2]).k[3]
//
TEXT bccmplei64(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64($VPCMP_IMM_LE)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

//
// k[0] = cmple.i64@imm(i64[1], i64@imm[2]).k[3]
//
TEXT bccmplei64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64_IMM($VPCMP_IMM_LE)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

//
// k[0] = cmpgt.i64(i64[1], i64[2]).k[3]
//
TEXT bccmpgti64(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64($VPCMP_IMM_GT)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

//
// k[0] = cmpgt.i64@imm(i64[1], i64@imm[2]).k[3]
//
TEXT bccmpgti64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64_IMM($VPCMP_IMM_GT)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

//
// k[0] = cmpge.i64(i64[1], i64[2]).k[3]
//
TEXT bccmpgei64(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64($VPCMP_IMM_GE)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

//
// k[0] = cmpge.i64@imm(i64[1], i64@imm[2]).k[3]
//
TEXT bccmpgei64imm(SB), NOSPLIT|NOFRAME, $0
  BC_CMP_OP_I64_IMM($VPCMP_IMM_GE)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

#undef BC_CMP_OP_F64_IMM
#undef BC_CMP_OP_I64_IMM
#undef BC_CMP_OP_I64


// Test Instructions
// -----------------

// k[0] = isnan.f(f64[1]).k[2]
//
// isnan(x) is the same as x != x
TEXT bcisnanf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  VCMPPD $VCMP_IMM_NEQ_UQ, Z2, Z2, K1, K1
  VCMPPD $VCMP_IMM_NEQ_UQ, Z3, Z3, K2, K2
  KUNPCKBW K1, K2, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// v[0].k[1] = checktag(v[2], imm16[3]).k[4]
//
// Take the tag pointed to in v[1] and determine if it
// contains _any_ of the immediate bits passed in imm16.
TEXT bcchecktag(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3+2, OUT(R8))

  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD CONSTD_1(), Z11                     // Z11 <- constant(1)
  VPSRLD $4, Z4, Z5                                // Z5 <- Type
  VPBROADCASTW (BC_SLOT_SIZE*3)(VIRT_PCREG), Z10   // Z10 <- tag bits (twice per 32-bit lane, but it's ok)
  VPSLLVD Z5, Z11, Z5                              // Z5 <- 1 << Type
  VPTESTMD Z10, Z5, K1, K1                         // K1 <- values matching the required tag (tag&Z5 != 0)

  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1))
  VMOVDQA32.Z Z4, K1, Z4
  BC_LOAD_VALUE_HLEN_FROM_SLOT_MASKED(OUT(Z5), IN(BX), IN(K1))

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  BC_STORE_VALUE_TO_SLOT(IN(Z2), IN(Z3), IN(Z4), IN(Z5), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_IMM16_SIZE)

// LUT for ion type bits -> json "type" bits
CONST_DATA_U8(consts_typebits_shuf, 0, $(1 << 0))  // null -> null
CONST_DATA_U8(consts_typebits_shuf, 1, $(1 << 1))  // bool -> bool
CONST_DATA_U8(consts_typebits_shuf, 2, $(1 << 2))  // uint -> number
CONST_DATA_U8(consts_typebits_shuf, 3, $(1 << 2))  // int -> number
CONST_DATA_U8(consts_typebits_shuf, 4, $(1 << 2))  // float -> number
CONST_DATA_U8(consts_typebits_shuf, 5, $(1 << 2))  // decimal -> number
CONST_DATA_U8(consts_typebits_shuf, 6, $(1 << 3))  // timestamp -> timestamp
CONST_DATA_U8(consts_typebits_shuf, 7, $(1 << 4))  // symbol -> string
CONST_DATA_U8(consts_typebits_shuf, 8, $(1 << 4))  // string -> string
CONST_DATA_U8(consts_typebits_shuf, 9, $0)         // clob is unused
CONST_DATA_U8(consts_typebits_shuf, 10, $0)        // blob is unused
CONST_DATA_U8(consts_typebits_shuf, 11, $(1 << 5)) // list -> list
CONST_DATA_U8(consts_typebits_shuf, 12, $0)        // sexp is unused
CONST_DATA_U8(consts_typebits_shuf, 13, $(1 << 6)) // struct -> struct
CONST_DATA_U8(consts_typebits_shuf, 14, $0)        // annotation is unused
CONST_DATA_U8(consts_typebits_shuf, 15, $0)        // reserved is unused
CONST_GLOBAL(consts_typebits_shuf, $16)

// i64[0] = typebits(v[1]).k[2]
//
// turn the tag bits in v[1] into an integer
TEXT bctypebits(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z2), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPMOVZXBD CONST_GET_PTR(consts_typebits_shuf, 0), Z5
  VPSRLD $4, Z2, Z2                                // Z2 <- value tag
  VPERMD.Z Z5, Z2, K1, Z2                          // Z2 <- type bits

  VEXTRACTI32X8 $1, Z2, Y3
  BC_UNPACK_SLOT(0, OUT(DX))

  VPMOVZXDQ Y2, Z2
  VPMOVZXDQ Y3, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = isnull.v(v[1]).k[2]
//
// calculated as `(tag & 0xF) == 0xF`
TEXT bcisnullv(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(BX))
  VPBROADCASTB CONSTD_0x0F(), X3

  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  VPAND BC_VSTACK_PTR(BX, vRegData_typeL), X3, X2

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_UNPACK_SLOT(0, OUT(DX))

  VPCMPEQB X3, X2, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = isnotnull.v(v[1]).k[2]
//
// calculated as `(tag & 0xF) != 0xF`
TEXT bcisnotnullv(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(BX))
  VPBROADCASTB CONSTD_0x0F(), X3

  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  VPAND BC_VSTACK_PTR(BX, vRegData_typeL), X3, X2

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_UNPACK_SLOT(0, OUT(DX))

  VPCMPUB $VPCMP_IMM_NE, X3, X2, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// CONSTD_TRUE_BYTE() = 0x11
// CONSTD_FALSE_BYTE() = 0x10

// k[0] = istrue.v(v[1]).k[2]
//
// calculated as `tag == 0x11`
TEXT bcistruev(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  VPBROADCASTB CONSTD_TRUE_BYTE(), X3

  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_UNPACK_SLOT(0, OUT(DX))
  VPCMPEQB BC_VSTACK_PTR(BX, vRegData_typeL), X3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = isfalse.v(v[1]).k[2]
//
// calculated as `tag[0] == 0x10`
TEXT bcisfalsev(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  VPBROADCASTB CONSTD_FALSE_BYTE(), X3

  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_UNPACK_SLOT(0, OUT(DX))
  VPCMPEQB BC_VSTACK_PTR(BX, vRegData_typeL), X3, K1, K1

  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// k[0] = cmpeq.slice(s[1], s[2]).k[3]
TEXT bccmpeqslice(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1))
  BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z6), OUT(Z7), IN(CX), IN(K1))

  // restrict the comparison to lanes that have equal lengths
  VPCMPEQD Z7, Z5, K1, K1

  JMP cmpeq_tail(SB)

// k[0] = cmpeq.v(v[1], v[2]).k[3]
TEXT bccmpeqv(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1))
  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z6), OUT(Z7), IN(CX), IN(K1))

  // restrict the comparison to lanes that have equal lengths
  VPCMPEQD Z7, Z5, K1, K1

  JMP cmpeq_tail(SB)

// k[0] = cmpeq.v@imm(v[1], litref[2]).k[3]
TEXT bccmpeqvimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2 + 10, OUT(R8))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(DX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  MOVL bytecode_scratchoff(VIRT_BCPTR), R15 // R15 <- scratch offset
  MOVL (BC_SLOT_SIZE*2+0)(VIRT_PCREG), BX
  MOVL (BC_SLOT_SIZE*2+4)(VIRT_PCREG), CX  // CX <- value length
  ADDQ R15, BX                             // BX <- value offset

  VPBROADCASTD CX, Z7
  VMOVDQU32.Z 0(VIRT_VALUES)(DX*1), K1, Z4

  // restrict the comparison to lanes that have equal lengths
  VPCMPEQD 64(VIRT_VALUES)(DX*1), Z7, K1, K1

  JMP cmpeqimm_tail(SB)

// compares slices [Z4:Z5].K1 with [Z6:Z7].K1 and stores the resulting mask in a DX slot
TEXT cmpeq_tail(SB), NOSPLIT|NOFRAME, $0
  KTESTW       K1, K1
  JZ           next

  VPBROADCASTD CONSTD_4(), Z24
  VPXORD       Z10, Z10, Z10    // default behavior is 0 = 0 (matching)
  VPXORD       Z11, Z11, Z11
  JMP          loop4tail

loop4:
  KMOVW        K2, K3
  KMOVW        K2, K4
  VPGATHERDD   0(VIRT_BASE)(Z4*1), K3, Z11
  VPGATHERDD   0(VIRT_BASE)(Z6*1), K2, Z10
  VPCMPEQD     Z10, Z11, K1, K1 // matching &= words are equal
  KANDW        K1, K4, K4
  VPADDD       Z24, Z4, K4, Z4  // offsets += 4
  VPADDD       Z24, Z6, K4, Z6
  VPSUBD       Z24, Z7, K4, Z7  // lengths -= 4
  VPSUBD       Z24, Z5, K4, Z5

loop4tail:
  VPCMPD          $VPCMP_IMM_GE, Z24, Z7, K1, K2 // K2 = matching lanes w/ length >= 4
  KTESTW          K2, K2
  JNZ             loop4

  // test final 4 bytes w/ mask
  VPTESTMD        Z7, Z7, K1, K2          // only load lanes w/ length > 0
  VBROADCASTI64X2 tail_mask_map<>(SB), Z9
  VPERMD          Z9, Z7, Z9
  KMOVW           K2, K3
  VPGATHERDD      0(VIRT_BASE)(Z4*1), K3, Z11
  VPGATHERDD      0(VIRT_BASE)(Z6*1), K2, Z10
  VPANDD          Z9, Z10, Z10
  VPANDD          Z9, Z11, Z11
  VPCMPEQD        Z10, Z11, K1, K1

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// compares slices in [Z4] with literal values and stores the result in DX slot
TEXT cmpeqimm_tail(SB), NOSPLIT|NOFRAME, $0
  XORL R15, R15
  KTESTW K1, K1

  VPBROADCASTD CONSTD_4(), Z24
  CMOVQEQ R15, CX

  SUBQ $4, CX
  JCS tail

loop:
  KMOVW K1, K2
  VPXORD X10, X10, X10
  VPGATHERDD 0(VIRT_BASE)(Z4*1), K2, Z10         // gather 4 bytes
  VPADDD Z24, Z4, K1, Z4                         // increment offsets by 4
  VPCMPEQD.BCST 0(VIRT_BASE)(BX*1), Z10, K1, K1  // matching &= words are equal

  ADDQ $4, BX
  KTESTW K1, K1
  CMOVQEQ R15, CX
  SUBQ $4, CX
  JCC loop

tail:
  ADDQ $4, CX
  JZ next

  VPBROADCASTD CX, Z5
  VBROADCASTI64X2 tail_mask_map<>(SB), Z9 // byte mask to filter out bytes above the slice

  KTESTW K1, K1
  JZ next

  // test remaining 1-3 bytes (if we are here it's never 0 or 4 bytes actually)
  VPERMD Z9, Z5, Z9
  KMOVW K1, K2
  VPXORD X10, X10, X10
  VPGATHERDD 0(VIRT_BASE)(Z4*1), K2, Z10
  VPANDD.BCST.Z 0(VIRT_BASE)(BX*1), Z9, K1, Z11
  VPANDD Z9, Z10, Z10
  VPCMPEQD Z10, Z11, K1, K1

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_LITREF_SIZE)

// Timestamp Boxing, Unboxing, and Manipulation
// ============================================
//
// First some constants:
//
//   - [0x0000000000000E10] 3600            <- 60 * 60                    (number of seconds per 1 hour)
//   - [0x00000000D693A400] 3600000000      <- 60 * 60 * 1e6              (number of microseconds per 1 hour)
//
//   - [0x0000000000015180] 86400           <- 60 * 60 * 24               (number of seconds per 1 day)
//   - [0x000000141DD76000] 86400000000     <- 60 * 60 * 24 * 1e6         (number of microseconds per 1 day)
//
//   - [0x00000000000005B5] 1461            <- 356 * 4   + 1              (number of days per 4 years cycle)
//   - [0x0000000000008EAC] 36524           <- 356 * 100 + 24             (number of days per 100 years cycle)
//   - [0x0000000000023AB1] 146097          <- 356 * 400 + 97             (number of days per 400 years cycle)
//
//   - [0x0000000000002B09] 11017           <- 10957 + 31 + 29            (number of days between 1970-01-01 and 2000-03-01)
//   - [0x0000000038BC5D80] 951868800       <- 11017 * 60 * 60 * 24       (number of seconds between 1970-01-01 and 2000-03-01)
//   - [0x0D35B7A160C70000] 951868800000000 <- 11017 * 60 * 60 * 24 * 1e6 (number of microseconds between 1970-01-01 and 2000-03-01)
//
// Divide/Modulo with a number that has N zero least significant bits can rewritten in the following way:
//
//   - Division:
//       C = A / B
//       C = (A >> N) / (B >> N)
//
//   - Modulo:
//       C = A % B
//       C = (((A >> N) % (B >> N)) << N) + (A & (N - 1))
//
// Which means that we don't need a 64-bit division with full precision (like the one that we implemented
// for integer pipeline) to decompose a timestamp value, because we can always cut the bits we are not
// interested in and use them later. In addition, unix time with microseconds precision has an interesting
// property - after we truncate the timestamp into day, it's guaranteed that the rest (Year/Month/Day
// combined) fits into a 32-bit integer, because the number of microseconds per day exceeds a 32-bit integer
// range, so there is less bits for representing the rest of the timestamp, which we later decompose to year,
// month, and day of month.
//
// Resources
// ---------
//
//  - https://howardhinnant.github.io/date_algorithms.html - The best resource for composing / decomposing.

#define BC_DIV_U32_RCP_2X(OUT1, OUT2, IN1, IN2, RCP, N_SHR)                      \
  VPMULUDQ RCP, IN1, OUT1                                                        \
  VPMULUDQ RCP, IN2, OUT2                                                        \
  VPSRLQ $(N_SHR), OUT1, OUT1                                                    \
  VPSRLQ $(N_SHR), OUT2, OUT2

#define BC_DIV_U32_RCP_2X_MASKED(OUT1, OUT2, IN1, IN2, RCP, N_SHR, MASK1, MASK2) \
  VPMULUDQ RCP, IN1, MASK1, OUT1                                                 \
  VPMULUDQ RCP, IN2, MASK2, OUT2                                                 \
  VPSRLQ $(N_SHR), OUT1, MASK1, OUT1                                             \
  VPSRLQ $(N_SHR), OUT2, MASK2, OUT2

// calculates `OUT = IN % DIVISOR` as `OUT = IN - ((IN * RCP) / N_SHR) * DIVISOR`
#define BC_MOD_U32_RCP_2X(OUT1, OUT2, IN1, IN2, DIVISOR, RCP, N_SHR, TMP1, TMP2) \
  VPMULUDQ RCP, IN1, TMP1                                                        \
  VPMULUDQ RCP, IN2, TMP2                                                        \
  VPSRLQ $(N_SHR), TMP1, TMP1                                                    \
  VPSRLQ $(N_SHR), TMP2, TMP2                                                    \
  VPMULUDQ DIVISOR, TMP1, TMP1                                                   \
  VPMULUDQ DIVISOR, TMP2, TMP2                                                   \
  VPSUBQ TMP1, IN1, OUT1                                                         \
  VPSUBQ TMP2, IN2, OUT2

#define BC_MOD_U32_RCP_2X_MASKED(OUT1, OUT2, IN1, IN2, DIVISOR, RCP, N_SHR, MASK1, MASK2, TMP1, TMP2) \
  VPMULUDQ RCP, IN1, TMP1                                                                             \
  VPMULUDQ RCP, IN2, TMP2                                                                             \
  VPSRLQ $(N_SHR), TMP1, TMP1                                                                         \
  VPSRLQ $(N_SHR), TMP2, TMP2                                                                         \
  VPMULUDQ DIVISOR, TMP1, TMP1                                                                        \
  VPMULUDQ DIVISOR, TMP2, TMP2                                                                        \
  VPSUBQ TMP1, IN1, MASK1, OUT1                                                                       \
  VPSUBQ TMP2, IN2, MASK2, OUT2

#define BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(DST_A, DST_B, SRC_A, SRC_B, RECIP, N_SHR) \
  VPMULLQ.BCST RECIP, SRC_A, DST_A \
  VPMULLQ.BCST RECIP, SRC_B, DST_B \
                                   \
  VPSRLQ $(N_SHR), DST_A, DST_A    \
  VPSRLQ $(N_SHR), DST_B, DST_B

#define BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST_MASKED(DST_A, DST_B, SRC_A, SRC_B, MASK_A, MASK_B, RECIP, N_SHR) \
  VPMULLQ.BCST RECIP, SRC_A, MASK_A, DST_A \
  VPMULLQ.BCST RECIP, SRC_B, MASK_B, DST_B \
                                           \
  VPSRLQ $(N_SHR), DST_A, MASK_A, DST_A    \
  VPSRLQ $(N_SHR), DST_B, MASK_B, DST_B

// Inputs
//   Z2/Z3   - Input timestamp.
//   K1/K2   - Input mask.
//
// Outputs:
//   Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
//   Z8/Z9   - Year index.
//   Z10/Z11 - Month index - starting from zero, where zero represents March.
//   Z14/Z15 - Day of month - starting from zero.
//
// Clobbers:
//   Z4...Z19
//   K Regs (TODO: Specify)
#define BC_DECOMPOSE_TIMESTAMP_PARTS(INPUT1, INPUT2)                                        \
  /* First cut off some bits that we don't need to calculate Year/Month/Day. */             \
  VPSRAQ.Z $13, INPUT1, K1, Z4                                                              \
  VPSRAQ.Z $13, INPUT2, K2, Z5                                                              \
                                                                                            \
  VPBROADCASTQ CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET_SHR_13(), Z14                      \
  VBROADCASTSD CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13(), Z15                                 \
                                                                                            \
  /* Adjust the value so we always end up with unsigned days count, we want to have */      \
  /* positive 400 years cycles. */                                                          \
  VPADDQ Z14, Z4, Z4                                                                        \
  VPADDQ Z14, Z5, Z5                                                                        \
                                                                                            \
  /* Convert to double precision so we can divide. */                                       \
  VCVTUQQ2PD Z4, Z6                                                                         \
  VCVTUQQ2PD Z5, Z7                                                                         \
                                                                                            \
  /* Z8/Z9 <- Get the number of days: */                                                    \
  /*       <- floor(float64(input >> 13) / float64((60 * 60 * 24 * 1000000) >> 13)). */     \
  VDIVPD.RD_SAE Z15, Z6, Z8                                                                 \
  VDIVPD.RD_SAE Z15, Z7, Z9                                                                 \
                                                                                            \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z8, Z8                                                  \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z9, Z9                                                  \
                                                                                            \
  /* Z12/Z13 - Number of days as integers (adjusted to be unsigned). */                     \
  /*           In this case, always less than 2^32. */                                      \
  VCVTPD2UQQ Z8, Z12                                                                        \
  VCVTPD2UQQ Z9, Z13                                                                        \
                                                                                            \
  /* Z6/Z7 <- Number of (hours, minutes, seconds, and microseconds) >> 13. */               \
  VMULPD Z15, Z8, Z16                                                                       \
  VMULPD Z15, Z9, Z17                                                                       \
  VSUBPD Z16, Z6, Z6                                                                        \
  VSUBPD Z17, Z7, Z7                                                                        \
  VCVTPD2UQQ Z6, Z6                                                                         \
  VCVTPD2UQQ Z7, Z7                                                                         \
                                                                                            \
  /* Z4/Z5 <- Number of hours, minutes, seconds, and microseconds. */                       \
  VPSLLQ $13, Z6, Z4                                                                        \
  VPSLLQ $13, Z7, Z5                                                                        \
  VPTERNLOGQ.BCST $TLOG_BLEND_BA, CONSTQ_0x1FFF(), INPUT1, Z4                            \
  VPTERNLOGQ.BCST $TLOG_BLEND_BA, CONSTQ_0x1FFF(), INPUT2, Z5                            \
                                                                                            \
  /* Z8/Z9 <- Number of 400Y cycles. */                                                     \
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z8, Z9, Z12, Z13, CONSTQ_963315389(), 47)           \
                                                                                            \
  /* Z14/Z15 <- Remaining days [0, 146096]. */                                              \
  VPMULLQ.BCST CONSTQ_146097(), Z8, Z14                                                     \
  VPMULLQ.BCST CONSTQ_146097(), Z9, Z15                                                     \
  VPSUBQ Z14, Z12, Z14                                                                      \
  VPSUBQ Z15, Z13, Z15                                                                      \
                                                                                            \
  /* Z10/Z11 <- Number of 100Y cycles [0, 3]. */                                            \
  VPSRLQ $2, Z14, Z10                                                                       \
  VPSRLQ $2, Z15, Z11                                                                       \
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z10, Z11, Z10, Z11, CONSTQ_963321983(), 43)         \
  VPMINUQ.BCST CONSTQ_3(), Z10, Z10                                                         \
  VPMINUQ.BCST CONSTQ_3(), Z11, Z11                                                         \
                                                                                            \
  /* Z14/Z15 <- Remaining days. */                                                          \
  VPMULLQ.BCST CONSTQ_36524(), Z10, Z16                                                     \
  VPMULLQ.BCST CONSTQ_36524(), Z11, Z17                                                     \
  VPSUBQ Z16, Z14, Z14                                                                      \
  VPSUBQ Z17, Z15, Z15                                                                      \
                                                                                            \
  /* K3/K4 <- 100YCycles != 0. */                                                           \
  VPTESTMQ Z10, Z10, K1, K3                                                                 \
  VPTESTMQ Z11, Z11, K2, K4                                                                 \
                                                                                            \
  /* Z8/Z9 <- 400Y_Cycles * 400. */                                                         \
  VPMULLQ.BCST CONSTQ_400(), Z8, Z8                                                         \
  VPMULLQ.BCST CONSTQ_400(), Z9, Z9                                                         \
                                                                                            \
  /* Z12/Z13 <- Number of 4Y cycles [0, 24]. */                                             \
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z12, Z13, Z14, Z15, CONSTQ_376287347(), 39)         \
  VPMINUQ.BCST CONSTQ_24(), Z12, Z12                                                        \
  VPMINUQ.BCST CONSTQ_24(), Z13, Z13                                                        \
                                                                                            \
  /* Z10/Z11 <- 100Y_Cycles * 100. */                                                       \
  VPMULLQ.BCST CONSTQ_100(), Z10, Z10                                                       \
  VPMULLQ.BCST CONSTQ_100(), Z11, Z11                                                       \
                                                                                            \
  /* Z14/Z15 <- Remaining days. */                                                          \
  VPMULLQ.BCST CONSTQ_1461(), Z12, Z16                                                      \
  VPMULLQ.BCST CONSTQ_1461(), Z13, Z17                                                      \
  VPSUBQ Z16, Z14, Z14                                                                      \
  VPSUBQ Z17, Z15, Z15                                                                      \
                                                                                            \
  /* Z8/Z9 <- 400Y_Cycles * 400 + 100Y_Cycles * 100. */                                     \
  VPADDQ Z10, Z8, Z8                                                                        \
  VPADDQ Z11, Z9, Z9                                                                        \
                                                                                            \
  /* K3/K4 <- 100YCycles != 0 && 4YCycles == 0. */                                          \
  VPTESTNMQ Z12, Z12, K3, K3                                                                \
  VPTESTNMQ Z13, Z13, K4, K4                                                                \
                                                                                            \
  /* Z12/Z13 <- 4YCycles * 4. */                                                            \
  VPSLLQ $2, Z12, Z12                                                                       \
  VPSLLQ $2, Z13, Z13                                                                       \
                                                                                            \
  /* Z8/Z9 <- 400Y_Cycles * 400 + 100Y_Cycles * 100 + 4YCycles * 4. */                      \
  VPADDQ Z12, Z8, Z8                                                                        \
  VPADDQ Z13, Z9, Z9                                                                        \
                                                                                            \
  /* Z16/Z17 <- Remaining years of the 4Y cycle [0, 3]. */                                  \
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z16, Z17, Z14, Z15, CONSTQ_45965(), 24)             \
  VPMINUQ.BCST CONSTQ_3(), Z16, Z16                                                         \
  VPMINUQ.BCST CONSTQ_3(), Z17, Z17                                                         \
                                                                                            \
  /* K3/K4 <- !(100YCycles != 0 && 4YCycles == 0). */                                       \
  KNOTW K3, K3                                                                              \
  KNOTW K4, K4                                                                              \
                                                                                            \
  /* Z8/Z9 <- 400Y_Cycles * 400 + 100Y_Cycles * 100 + 4YCycles * 4 + Remaining_Years. */    \
  VPADDQ Z16, Z8, Z8                                                                        \
  VPADDQ Z17, Z9, Z9                                                                        \
                                                                                            \
  /* K3/K4 - !(100YCycles != 0 && 4YCycles == 0) && RemainingYearsInLast4YCycle == 0. */    \
  VPTESTNMQ Z16, Z16, K3, K3                                                                \
  VPTESTNMQ Z17, Z17, K4, K4                                                                \
                                                                                            \
  /* Z14/Z15 <- Remaining days [0, 366]. */                                                 \
  VPMULLQ.BCST CONSTQ_365(), Z16, Z18                                                       \
  VPMULLQ.BCST CONSTQ_365(), Z17, Z19                                                       \
  VPSUBQ Z18, Z14, Z14                                                                      \
  VPSUBQ Z19, Z15, Z15                                                                      \
                                                                                            \
  /* Z10/Z11 <- Months (starting from 0, where 0 represents March at this point). */        \
  /* The following equation is used to calculate months: `5 * RemainingDays + 2) / 153` */  \
  VPSLLQ $2, Z14, Z10                                                                       \
  VPADDQ.BCST CONSTQ_2(), Z14, Z12                                                          \
  VPSLLQ $2, Z15, Z11                                                                       \
  VPADDQ.BCST CONSTQ_2(), Z15, Z13                                                          \
  VPADDQ Z10, Z12, Z12                                                                      \
  VPADDQ Z11, Z13, Z13                                                                      \
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z10, Z11, Z12, Z13, CONSTQ_3593175255(), 39)        \
                                                                                            \
  /* Z14/Z15 <- Remaining days respecting the month in Z10/Z11. */                          \
  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13                       \
  VPERMD Z13, Z10, Z12                                                                      \
  VPERMD Z13, Z11, Z13                                                                      \
  VPSUBQ Z12, Z14, Z14                                                                      \
  VPSUBQ Z13, Z15, Z15

// Years are ADDED to DST_DAYS_A and DST_DAYS_B.
//
// The input year is a year that uses March as its first month (as used in other functions).
#define BC_COMPOSE_YEAR_TO_DAYS(DST_DAYS_A, DST_DAYS_B, YEAR_A, YEAR_B, TMP_A1, TMP_B1, TMP_A2, TMP_B2, TMP_A3, TMP_B3) \
  /* TMP_A1/B1 <- Number of 400Y cycles (era). */                                           \
  VPMULLQ.BCST CONSTQ_1374389535(), YEAR_A, TMP_A1                                          \
  VPMULLQ.BCST CONSTQ_1374389535(), YEAR_B, TMP_B1                                          \
  VPSRAQ $39, TMP_A1, TMP_A1                                                                \
  VPSRAQ $39, TMP_B1, TMP_B1                                                                \
                                                                                            \
  /* TMP_A2/B2 <- Number of years in the last 400Y era [0, 399]. */                         \
  VPMULLQ.BCST CONSTQ_400(), TMP_A1, TMP_A2                                                 \
  VPMULLQ.BCST CONSTQ_400(), TMP_B1, TMP_B2                                                 \
  VPSUBQ TMP_A2, YEAR_A, TMP_A2                                                             \
  VPSUBQ TMP_B2, YEAR_B, TMP_B2                                                             \
                                                                                            \
  /* DST_DAYS_A/B - Increment full 400Y cycles converted to days. */                        \
  VPMULLQ.BCST CONSTQ_146097(), TMP_A1, TMP_A3                                              \
  VPMULLQ.BCST CONSTQ_146097(), TMP_B1, TMP_B3                                              \
  VPADDQ TMP_A3, DST_DAYS_A, DST_DAYS_A                                                     \
  VPADDQ TMP_B3, DST_DAYS_B, DST_DAYS_B                                                     \
                                                                                            \
  /* DST_DAYS_A/B - Increment days of the last era: YOE * 365 + YOE / 4 - YOE / 100. */     \
  VPMULLQ.BCST CONSTQ_365(), TMP_A2, TMP_A1                                                 \
  VPMULLQ.BCST CONSTQ_365(), TMP_B2, TMP_B1                                                 \
  VPMULLQ.BCST CONSTQ_1374389535(), TMP_A2, TMP_A3                                          \
  VPMULLQ.BCST CONSTQ_1374389535(), TMP_B2, TMP_B3                                          \
                                                                                            \
  VPSRLQ $2, TMP_A2, TMP_A2                                                                 \
  VPSRLQ $2, TMP_B2, TMP_B2                                                                 \
  VPSRLQ $37, TMP_A3, TMP_A3                                                                \
  VPSRLQ $37, TMP_B3, TMP_B3                                                                \
                                                                                            \
  VPADDQ TMP_A1, DST_DAYS_A, DST_DAYS_A                                                     \
  VPADDQ TMP_B1, DST_DAYS_B, DST_DAYS_B                                                     \
  VPADDQ TMP_A2, DST_DAYS_A, DST_DAYS_A                                                     \
  VPADDQ TMP_B2, DST_DAYS_B, DST_DAYS_B                                                     \
                                                                                            \
  VPSUBQ TMP_A3, DST_DAYS_A, DST_DAYS_A                                                     \
  VPSUBQ TMP_B3, DST_DAYS_B, DST_DAYS_B

// DATE_ADD(MONTH|YEAR, interval, timestamp)
//
// If the datepart is less than month we don't have to decompose. In that case we just
// reuse the existing `bcaddi64` and `bcaddi64imm` instructions, which are timestamp agnostic.
//
// We don't really need a specific code for adding years, as `year == month * 12`. This
// means that we can just convert years to months and add `year * 12` months and be done.

// ts[0].k[1] = dateaddmonth(ts[2], i64[3]).k[4]
TEXT bcdateaddmonth(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(0, OUT(DX), OUT(R15), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z20), OUT(Z21), IN(CX))

  ADDQ $(BC_SLOT_SIZE*5), VIRT_PCREG
  JMP dateaddmonth_tail(SB)

// ts[0].k[1] = dateaddmonth.imm(ts[2], i64@imm[3]).k[4]
TEXT bcdateaddmonthimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT_ZI64_SLOT(0, OUT(DX), OUT(R15), OUT(BX), OUT(Z20), OUT(R8))
  VMOVDQA64 Z20, Z21

  ADDQ $(BC_SLOT_SIZE*4+8), VIRT_PCREG
  JMP dateaddmonth_tail(SB)

// ts[0].k[1] = dateaddyear(ts[2], i64[3]).k[4]
TEXT bcdateaddyear(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(0, OUT(DX), OUT(R15), OUT(BX), OUT(CX), OUT(R8))

  // multiply years by 12
  VPSLLQ $3, 0(VIRT_VALUES)(CX*1), Z20
  VPSLLQ $3, 64(VIRT_VALUES)(CX*1), Z21
  VPSRLQ $1, Z20, Z4
  VPSRLQ $1, Z21, Z5
  VPADDQ Z4, Z20, Z20
  VPADDQ Z5, Z21, Z21

  ADDQ $(BC_SLOT_SIZE*5), VIRT_PCREG
  JMP dateaddmonth_tail(SB)

// ts[0].k[1] = dateaddquarter(ts[2], i64[3]).k[4]
TEXT bcdateaddquarter(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(0, OUT(DX), OUT(R15), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z20), OUT(Z21), IN(CX))

  // multiply quarters by 3
  VPSLLQ $1, Z20, Z4
  VPSLLQ $1, Z21, Z5
  VPADDQ Z4, Z20, Z20
  VPADDQ Z5, Z21, Z21

  ADDQ $(BC_SLOT_SIZE*5), VIRT_PCREG
  JMP dateaddmonth_tail(SB)

/*
    Constants

    Unix microseconds from 1st January 1970 to 1st March 0000:
    ((146097 * 5 - 11017) * 86400000000) == 62162035200000000

    CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET() = 62162035200000000

    ((146097 * 5 - 11017) * 86400000000) >> 13
    CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET_SHR_13() = 7588139062500

    float64((60 * 60 * 24 * 1000000) >> 13) == float64(10546875)
    CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13() = 10546875
*/

// Tail instruction implementing DATE_ADD(MONTH, interval, timestamp).
//
// Inputs:
//   BX      - timestamp value slot
//   R8      - predicate slot
//   Z20/Z21 - number of months to add
TEXT dateaddmonth_tail(SB), NOSPLIT|NOFRAME, $0
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // --- Decompose the timestamp ---

  // Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
  // Z8/Z9   - Year index.
  // Z10/Z11 - Month index - starting from zero, where zero represents March.
  // Z14/Z15 - Day of month - starting from zero.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // -- Perform the addition ---

  // Z10/Z11 <- months combined (could be within a range, negative, or greater than 11).
  VPADDQ Z20, Z10, Z10
  VPADDQ Z21, Z11, Z11

  // Load some constants.
  VBROADCASTSD CONSTF64_12(), Z20
  VPXORQ X21, X21, X21

  // Z12/Z13 <- Years difference (int).
  VCVTQQ2PD Z10, Z12
  VCVTQQ2PD Z11, Z13

  VCMPPD $VCMP_IMM_LT_OQ, Z21, Z10, K3
  VCMPPD $VCMP_IMM_LT_OQ, Z21, Z11, K4

  VPBROADCASTQ CONSTF64_11(), Z25
  VPBROADCASTQ CONSTQ_12(), Z26

  VSUBPD Z25, Z12, K3, Z12
  VSUBPD Z25, Z13, K4, Z13

  VDIVPD.RD_SAE Z20, Z12, Z12
  VDIVPD.RD_SAE Z20, Z13, Z13

  VCVTPD2QQ.RD_SAE Z12, Z12
  VCVTPD2QQ.RD_SAE Z13, Z13

  // Z8/Z9 <- Final years (int).
  VPADDQ Z12, Z8, Z8
  VPADDQ Z13, Z9, Z9

  // Z10/Z11 <- Corrected month index [0, 11] (where 0 represents March).
  VPMULLQ Z26, Z12, Z12
  VPMULLQ Z26, Z13, Z13

  VPSUBQ Z12, Z10, Z10
  VPSUBQ Z13, Z11, Z11

  // --- Compose the timestamp ---

  // Z6/Z7 <- Number of days of the last year (months + day of month).
  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13
  VPERMD Z13, Z10, Z12
  VPERMD Z13, Z11, Z13
  VPADDQ Z12, Z14, Z6
  VPADDQ Z13, Z15, Z7

  // Z6/Z7 <- Final number of days.
  BC_COMPOSE_YEAR_TO_DAYS(Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  VPBROADCASTQ CONSTQ_86400000000(), Z25
  VPBROADCASTQ CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z26

  // Z6/Z7 <- Final number of days converted to microseconds.
  VPMULLQ Z25, Z6, Z6
  VPMULLQ Z25, Z7, Z7

  // Z6/Z7 <- Combined microseconds of all days and microseconds of the remaining day.
  VPADDQ Z4, Z6, Z6
  VPADDQ Z5, Z7, Z7

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ.Z Z26, Z6, K1, Z2
  VPSUBQ.Z Z26, Z7, K2, Z3

  BC_STORE_K_TO_SLOT(IN(K1), IN(R15))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

  NEXT_ADVANCE(0)

// ts[0].k[1] = datebin(i64@imm[2], ts[3], ts[4]).k[5]
//
// DATE_BIN(stride, source, origin)
TEXT bcdatebin(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2+8, BX, CX, R8)
  BC_UNPACK_ZI64(BC_SLOT_SIZE*2, OUT(Z10))        // Z10 <- Stride

  BC_FILL_ONES(Z31)

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX)) // Z0:Z1 <- Timestamp
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(CX)) // Z2:Z3 <- Origin

  // Timestamp - Origin
  VPSUBQ Z2, Z0, Z4
  VPSUBQ Z3, Z1, Z5

  // (Timestamp - Origin) / Stride
  BC_DIV_FLOOR_I64VEC_BY_U64IMM(OUT(Z6), OUT(Z7), IN(Z4), IN(Z5), IN(Z10), IN(Z31), IN(K1), IN(K2), Z8, Z9, Z12, Z13, Z14, Z15, Z16, K3, K4, K5, K6)
  VPMULLQ Z10, Z6, Z6
  VPMULLQ Z10, Z7, Z7

  // (Timestamp - Origin) / Stride + Origin
  BC_UNPACK_2xSLOT(0, DX, R8)
  VPADDQ.Z Z6, Z2, K1, Z0
  VPADDQ.Z Z7, Z3, K2, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5 + 8)

// s[0].k[1] = datediffmicrosecond(s[2], s[3]).k[4]
//
TEXT bcdatediffmicrosecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(CX))

  VPSUBQ.Z 0(VIRT_VALUES)(BX*1), Z2, K1, Z2
  VPSUBQ.Z 64(VIRT_VALUES)(BX*1), Z3, K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = datediffparam(ts[2], ts[3], u64@imm[4]).k[5]
//
// DATE_DIFF(DAY|HOUR|MINUTE|SECOND|MILLISECOND, t1, t2)
TEXT bcdatediffparam(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(Z6), OUT(R8))

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_I64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))

  VPSUBQ Z2, Z4, Z4
  VPSUBQ Z3, Z5, Z5

  // We never need the last 3 bits of the value, so cut it off to increase precision.
  VPSRAQ $3, Z6, Z6
  VPSRAQ $3, Z4, Z4
  VPSRAQ $3, Z5, Z5

  VCVTQQ2PD.RD_SAE Z6, Z6
  VCVTQQ2PD.RD_SAE Z4, Z4
  VCVTQQ2PD.RD_SAE Z5, Z5

  VDIVPD.RZ_SAE Z6, Z4, Z4
  VDIVPD.RZ_SAE Z6, Z5, Z5

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VCVTPD2QQ.RZ_SAE Z4, K1, Z2
  VCVTPD2QQ.RZ_SAE Z5, K2, Z3

  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*5 + BC_IMM64_SIZE)

// i64[0].k[1] = datediffmqy(ts[2], ts[3], i16@imm[4]).k[5]
//
// DATE_DIFF(MONTH|QUARTER|YEAR, interval, timestamp)
TEXT bcdatediffmqy(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_RU16_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(DX), OUT(R8))

  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_I64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  LEAQ CONST_GET_PTR(consts_datediff_month_year_div_rcp, 0), R15

  // First make the first timestamp lesser and the second greater. This would give us always
  // a positive difference, which we would negate at the end, where required. This makes it
  // a bit easier to implement months difference as specified in PartiQL SQL reference.
  VPCMPQ $VPCMP_IMM_GT, Z4, Z2, K1, K5
  VPCMPQ $VPCMP_IMM_GT, Z5, Z3, K2, K6

  // Z20/Z21 <- Greater timestamp.
  VPMAXSQ Z2, Z4, Z20
  VPMAXSQ Z3, Z5, Z21

  // Z2/Z3 <- Lesser timestamp.
  VPMINSQ Z2, Z4, K1, Z2
  VPMINSQ Z3, Z5, K2, Z3

  // Decomposed lesser timestamp:
  //   Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
  //   Z8/Z9   - Year index.
  //   Z10/Z11 - Month index - starting from zero, where zero represents March.
  //   Z14/Z15 - Day of month - starting from zero.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ CONSTQ_12(), Z25
  VPBROADCASTQ CONSTQ_1(), Z26

  // Z22/Z23 <- Lesser timestamp's 'Year * 12 + MonthIndex'.
  VPMULLQ Z25, Z8, Z8
  VPMULLQ Z25, Z9, Z9
  VPADDQ Z8, Z10, Z22
  VPADDQ Z9, Z11, Z23

  // Z4/Z5 <- Greater timestamp's value decremented by hours/minutes/... from the lesser timestamp.
  VPSUBQ Z4, Z20, Z4
  VPSUBQ Z5, Z21, Z5

  // Z20/Z21 <- Saved lesser timestamp's day of month, so we can use it later.
  VMOVDQA64 Z14, Z20
  VMOVDQA64 Z15, Z21

  // Decomposed greater timestamp:
  //   Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
  //   Z8/Z9   - Year index.
  //   Z10/Z11 - Month index - starting from zero, where zero represents March.
  //   Z14/Z15 - Day of month - starting from zero.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z4, Z5)

  // Z10/Z11 <- Greater timestamp's 'Year * 12 + MonthIndex'.
  VPMULLQ Z25, Z8, Z8
  VPMULLQ Z25, Z9, Z9
  VPADDQ Z8, Z10, Z10
  VPADDQ Z9, Z11, Z11

  // Z4/Z5 <- Rough months difference (greater timestamp - lesser timestamp).
  VPSUBQ Z22, Z10, Z4
  VPSUBQ Z23, Z11, Z5

  // Z4/Z5 <- Rough months difference - 1.
  VPSUBQ Z26, Z4, Z4
  VPSUBQ Z26, Z5, Z5

  // Z10 <- Zeros
  // Z11 <- Multiplier used to implement the same bytecode for MONTH and YEAR difference.
  VPXORQ X10, X10, X10
  VPBROADCASTQ 0(R15)(DX * 8), Z11

  // Increment one month if the lesser timestamp's day of month <= greater timestamp's day of month.
  VPCMPQ $VPCMP_IMM_GE, Z20, Z14, K3
  VPCMPQ $VPCMP_IMM_GE, Z21, Z15, K4

  VPADDQ Z26, Z4, K3, Z4
  VPADDQ Z26, Z5, K4, Z5

  // Z4/Z5 <- Final months difference - always positive at this point.
  VPMAXSQ Z10, Z4, Z4
  VPMAXSQ Z10, Z5, Z5

  // Z2/Z3 <- Final months/years difference - depending on the bytecode instruction's predicate.
  VPMULLQ Z11, Z4, Z4
  VPMULLQ Z11, Z5, Z5
  VPSRLQ $35, Z4, K1, Z2
  VPSRLQ $35, Z5, K2, Z3

  // Z2/Z3 <- Final months/years difference - positive or negative depending on which timestamp was greater.
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VPSUBQ Z2, Z10, K5, Z2
  VPSUBQ Z3, Z10, K6, Z3

  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*5 + BC_IMM16_SIZE)

#define BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT1, OUT2, IN1, IN2, TMP1, TMP2, TMP3, TMP4, TMP5) \
  /* First cut off some bits and convert to float64 without losing the precision. */      \
  VBROADCASTSD CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13(), TMP5                              \
  VPSRAQ $13, IN1, TMP1                                                                   \
  VPSRAQ $13, IN2, TMP2                                                                   \
  VCVTQQ2PD TMP1, TMP1                                                                    \
  VCVTQQ2PD TMP2, TMP2                                                                    \
                                                                                          \
  /* Z8/Z9 <- floor(float64(input >> 13) / float64((60 * 60 * 24 * 1000000) >> 13)). */   \
  VDIVPD.RD_SAE TMP5, TMP1, TMP3                                                          \
  VDIVPD.RD_SAE TMP5, TMP2, TMP4                                                          \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, TMP3, TMP3                                            \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, TMP4, TMP4                                            \
                                                                                          \
  /* TMP1/TMP2 <- Number of (hours, minutes, seconds, and microseconds) >> 13. */         \
  VMULPD TMP5, TMP3, TMP3                                                                 \
  VMULPD TMP5, TMP4, TMP4                                                                 \
  VSUBPD TMP3, TMP1, TMP1                                                                 \
  VSUBPD TMP4, TMP2, TMP2                                                                 \
  VCVTPD2UQQ TMP1, TMP1                                                                   \
  VCVTPD2UQQ TMP2, TMP2                                                                   \
                                                                                          \
  /* OUT1/OUT2 <- Number of hours, minutes, seconds, and microseconds combined. */        \
  VPBROADCASTQ CONSTQ_0x1FFF(), TMP3                                                      \
  VPSLLQ $13, TMP1, OUT1                                                                  \
  VPSLLQ $13, TMP2, OUT2                                                                  \
  VPTERNLOGQ $TLOG_BLEND_BA, TMP3, IN1, OUT1 /* (A & ~C) | (B & C) */                  \
  VPTERNLOGQ $TLOG_BLEND_BA, TMP3, IN2, OUT2 /* (A & ~C) | (B & C) */

// i64[0] = dateextractmicrosecond(ts[1]).k[2]
//
// EXTRACT(MICROSECOND FROM timestamp) - the result includes seconds
TEXT bcdateextractmicrosecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z4:Z5 <- hours, minutes, seconds, and microseconds combined
  BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT(Z4), OUT(Z5), IN(Z2), IN(Z3), Z6, Z7, Z8, Z9, Z10)

  // discard hours and minutes, keep microseconds that include also seconds
  VPBROADCASTQ CONSTQ_600479951(), Z8
  VPBROADCASTQ CONSTQ_60000000(), Z9
  VPSRLQ $8, Z4, Z6
  VPSRLQ $8, Z5, Z7
  BC_DIV_U32_RCP_2X(OUT(Z6), OUT(Z7), IN(Z6), IN(Z7), IN(Z8), 47)
  VPMULUDQ Z9, Z6, Z6
  VPMULUDQ Z9, Z7, Z7
  VPSUBQ Z6, Z4, K1, Z2
  VPSUBQ Z7, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractmillisecond(ts[1]).k[2]
//
// EXTRACT(MILLISECOND FROM timestamp) - the result includes seconds
TEXT bcdateextractmillisecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z4:Z5 <- hours, minutes, seconds, and microseconds combined
  BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT(Z4), OUT(Z5), IN(Z2), IN(Z3), Z6, Z7, Z8, Z9, Z10)

  // discard hours and minutes, keep milliseconds that include also seconds
  VPBROADCASTQ CONSTQ_600479951(), Z8
  VPBROADCASTQ CONSTQ_60000000(), Z9
  VPSRLQ $8, Z4, Z6
  VPSRLQ $8, Z5, Z7
  BC_DIV_U32_RCP_2X(OUT(Z6), OUT(Z7), IN(Z6), IN(Z7), IN(Z8), 47)
  VPMULUDQ Z9, Z6, Z6
  VPMULUDQ Z9, Z7, Z7
  VPBROADCASTQ CONSTQ_274877907(), Z8
  VPSUBQ Z6, Z4, Z4
  VPSUBQ Z7, Z5, Z5
  BC_DIV_U32_RCP_2X_MASKED(OUT(Z2), OUT(Z3), IN(Z4), IN(Z5), IN(Z8), 38, IN(K1), IN(K2))

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractsecond(ts[1]).k[2]
//
// EXTRACT(SECOND FROM timestamp)
TEXT bcdateextractsecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z4:Z5 <- hours, minutes, seconds, and microseconds combined
  BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT(Z4), OUT(Z5), IN(Z2), IN(Z3), Z6, Z7, Z8, Z9, Z10)

  // discard hours and minutes, keep seconds
  VPBROADCASTQ CONSTQ_600479951(), Z8
  VPBROADCASTQ CONSTQ_60000000(), Z9
  VPSRLQ $8, Z4, Z6
  VPSRLQ $8, Z5, Z7
  BC_DIV_U32_RCP_2X(OUT(Z6), OUT(Z7), IN(Z6), IN(Z7), IN(Z8), 47)
  VPMULUDQ Z9, Z6, Z6
  VPMULUDQ Z9, Z7, Z7
  VPBROADCASTQ CONSTQ_1125899907(), Z8
  VPSUBQ Z6, Z4, Z4
  VPSUBQ Z7, Z5, Z5
  BC_DIV_U32_RCP_2X_MASKED(OUT(Z2), OUT(Z3), IN(Z4), IN(Z5), IN(Z8), 50, IN(K1), IN(K2))

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractminute(ts[1]).k[2]
//
// EXTRACT(MINUTE FROM timestamp)
TEXT bcdateextractminute(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z4:Z5 <- hours, minutes, seconds, and microseconds combined
  BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT(Z4), OUT(Z5), IN(Z2), IN(Z3), Z6, Z7, Z8, Z9, Z10)

  // discard seconds
  VPBROADCASTQ CONSTQ_600479951(), Z8
  VPBROADCASTQ CONSTQ_60000000(), Z9
  VPSRLQ $8, Z4, Z6
  VPSRLQ $8, Z5, Z7
  BC_DIV_U32_RCP_2X(OUT(Z4), OUT(Z5), IN(Z6), IN(Z7), IN(Z8), 47)

  // now keep only minutes (% 60)
  VPBROADCASTQ CONSTQ_2290649225(), Z6
  VPBROADCASTQ CONSTQ_60(), Z7
  BC_MOD_U32_RCP_2X_MASKED(OUT(Z2), OUT(Z3), IN(Z4), IN(Z5), IN(Z7), IN(Z6), 37, IN(K1), IN(K2), Z8, Z9)

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextracthour(ts[1]).k[2]
//
// EXTRACT(HOUR FROM timestamp)
TEXT bcdateextracthour(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z4:Z5 <- hours, minutes, seconds, and microseconds combined
  BC_EXTRACT_HMS_FROM_TIMESTAMP(OUT(Z4), OUT(Z5), IN(Z2), IN(Z3), Z6, Z7, Z8, Z9, Z10)

  // discard minutes and seconds
  VPBROADCASTQ CONSTQ_1281023895(), Z8
  VPSRLQ $10, Z4, Z4
  VPSRLQ $10, Z5, Z5
  BC_DIV_U32_RCP_2X_MASKED(OUT(Z2), OUT(Z3), IN(Z4), IN(Z5), IN(Z8), 52, IN(K1), IN(K2))

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractday(ts[1]).k[2]
//
// EXTRACT(DAY FROM timestamp)
TEXT bcdateextractday(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)
  VPBROADCASTQ CONSTQ_1(), Z4
  VPADDQ Z4, Z14, K1, Z2
  VPADDQ Z4, Z15, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractdow(ts[1]).k[2]
//
// EXTRACT(DOW FROM timestamp)
TEXT bcdateextractdow(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // divide Z2:Z3 by (60 * 60 * 24 * 1000000) (microseconds per day)
  // to get days from the start of unix time
  VBROADCASTSD CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13(), Z6
  VPSRAQ $13, Z2, Z4
  VPSRAQ $13, Z3, Z5
  VCVTQQ2PD.Z Z4, K1, Z4
  VCVTQQ2PD.Z Z5, K2, Z5

  VDIVPD.RD_SAE Z6, Z4, Z4
  VDIVPD.RD_SAE Z6, Z5, Z5

  // The internal [unboxed] representation is a unix microtime, which
  // starts at 1970-01-01, which is Thursday - so add 4 and then modulo
  // it by 7 to get a value in [0, 6] range, where 0 is Sunday.
  VBROADCASTSD CONSTF64_4(), Z6
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z4, Z4
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z5, Z5

  VBROADCASTSD CONSTF64_7(), Z8
  VADDPD Z6, Z4, Z4
  VADDPD Z6, Z5, Z5

  VDIVPD.RD_SAE Z8, Z4, Z6
  VDIVPD.RD_SAE Z8, Z5, Z7

  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z6, Z6
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z7, Z7

  VMULPD Z8, Z6, Z6
  VMULPD Z8, Z7, Z7

  VSUBPD Z6, Z4, Z4
  VSUBPD Z7, Z5, Z5

  VCVTPD2QQ Z4, K1, Z2
  VCVTPD2QQ Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// A DWORD table designed for VPERMD that can be used to map month of the year into the number
// of days preceeding the month + 1, excluding leap day.
CONST_DATA_U32(extract_doy_predicate,  0, $0)         // Zero index is unused
CONST_DATA_U32(extract_doy_predicate,  4, $(59 + 1))  // March
CONST_DATA_U32(extract_doy_predicate,  8, $(90 + 1))  // April
CONST_DATA_U32(extract_doy_predicate, 12, $(120 + 1)) // May
CONST_DATA_U32(extract_doy_predicate, 16, $(151 + 1)) // June
CONST_DATA_U32(extract_doy_predicate, 20, $(181 + 1)) // July
CONST_DATA_U32(extract_doy_predicate, 24, $(212 + 1)) // August
CONST_DATA_U32(extract_doy_predicate, 28, $(243 + 1)) // September
CONST_DATA_U32(extract_doy_predicate, 32, $(273 + 1)) // October
CONST_DATA_U32(extract_doy_predicate, 36, $(304 + 1)) // November
CONST_DATA_U32(extract_doy_predicate, 40, $(334 + 1)) // December
CONST_DATA_U32(extract_doy_predicate, 44, $(0 + 1))   // January
CONST_DATA_U32(extract_doy_predicate, 48, $(31 + 1))  // February
CONST_DATA_U32(extract_doy_predicate, 52, $0)
CONST_DATA_U32(extract_doy_predicate, 56, $0)
CONST_DATA_U32(extract_doy_predicate, 60, $0)
CONST_GLOBAL(extract_doy_predicate, $64)

// i64[0] = dateextractdoy(ts[1]).k[2]
//
// EXTRACT(DOY FROM timestamp)
//
// Extacting DOY is implemented as (x - DATE_TRUNC(x)) / MICROSECONDS_PER_DAY + 1
TEXT bcdateextractdoy(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z8/Z9 <- Year index
  // Z10/Z11 <- Month index - starting from zero, where zero represents March
  // Z14/Z15 <- Day of month
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ CONSTQ_10(), Z12
  VPBROADCASTQ CONSTQ_1(), Z13

  VPCMPUQ $VPCMP_IMM_LT, Z12, Z10, K3, K3
  VPCMPUQ $VPCMP_IMM_LT, Z12, Z11, K4, K4

  VMOVDQU64 CONST_GET_PTR(extract_doy_predicate, 0), Z12
  VPADDQ Z13, Z10, Z10
  VPADDQ Z13, Z11, Z11

  VPERMD Z12, Z10, Z4
  VPERMD Z12, Z11, Z5

  VPADDQ Z13, Z4, K3, Z4
  VPADDQ Z13, Z5, K4, Z5

  VPADDQ Z14, Z4, K1, Z2
  VPADDQ Z15, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractmonth(ts[1]).k[2]
//
// EXTRACT(MONTH FROM timestamp)
TEXT bcdateextractmonth(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ  CONSTQ_3(), Z20
  VPBROADCASTQ  CONSTQ_12(), Z21

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ Z20, Z10, Z10
  VPADDQ Z20, Z11, Z11
  VPCMPUQ $VPCMP_IMM_GT, Z21, Z10, K5
  VPCMPUQ $VPCMP_IMM_GT, Z21, Z11, K6

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ Z21, Z10, K5, Z10
  VPSUBQ Z21, Z11, K6, Z11

  VMOVDQA64 Z10, K1, Z2
  VMOVDQA64 Z11, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractquarter(ts[1]).k[2]
//
// EXTRACT(QUARTER FROM timestamp)
TEXT bcdateextractquarter(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ CONSTQ_1(), Z4
  VBROADCASTI32X4 CONST_GET_PTR(consts_quarter_from_month_1_is_march, 0), Z5

  // convert a resulting month index into [1, 12] range, where 1 represents March
  VPADDQ Z4, Z10, Z10
  VPADDQ Z4, Z11, Z11

  // use VPSHUFB to convert the month index into a quarter
  VPSHUFB Z10, Z5, Z10
  VPSHUFB Z11, Z5, Z11

  VMOVDQA64 Z10, K1, Z2
  VMOVDQA64 Z11, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = dateextractyear(ts[1]).k[2]
//
// EXTRACT(YEAR FROM timestamp)
TEXT bcdateextractyear(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ  CONSTQ_3(), Z20
  VPBROADCASTQ  CONSTQ_12(), Z21
  VPBROADCASTQ  CONSTQ_1(), Z22

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ Z20, Z10, Z10
  VPADDQ Z20, Z11, Z11
  VPCMPUQ $VPCMP_IMM_GT, Z21, Z10, K5
  VPCMPUQ $VPCMP_IMM_GT, Z21, Z11, K6

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ Z21, Z10, K5, Z10
  VPSUBQ Z21, Z11, K6, Z11

  // Increment one year if required to adjust for the month greater than 12 after adding the final offset.
  VPADDQ Z22, Z8, K5, Z8
  VPADDQ Z22, Z9, K6, Z9

  VMOVDQA64 Z8, K1, Z2
  VMOVDQA64 Z9, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

//
// i64[0] = datetounixepoch(ts[1]).k[2]
//
TEXT bcdatetounixepoch(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)

  // Discard some bits so we can prepare the timestamp value for division.
  VPSRAQ $6, Z2, Z2
  VPSRAQ $6, Z3, Z3

  // 15625 == 1000000 >> 6
  VPBROADCASTQ CONSTQ_15625(), Z4
  BC_DIV_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4, K5, K6)

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = datetounixmicro(ts[1]).k[2]
TEXT bcdatetounixmicro(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncmillisecond(ts[1]).k[2]
TEXT bcdatetruncmillisecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)
  VPBROADCASTQ CONSTQ_1000(), Z4

  BC_MOD_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4)
  VPSUBQ Z0, Z2, Z0
  VPSUBQ Z1, Z3, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncsecond(ts[1]).k[2]
TEXT bcdatetruncsecond(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)
  VPBROADCASTQ CONSTQ_1000000(), Z4

  BC_MOD_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4)
  VPSUBQ Z0, Z2, Z0
  VPSUBQ Z1, Z3, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncminute(ts[1]).k[2]
TEXT bcdatetruncminute(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)
  VPBROADCASTQ CONSTQ_60000000(), Z4

  BC_MOD_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4)
  VPSUBQ Z0, Z2, Z0
  VPSUBQ Z1, Z3, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetrunchour(ts[1]).k[2]
TEXT bcdatetrunchour(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)
  VPBROADCASTQ CONSTQ_3600000000(), Z4

  BC_MOD_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4)
  VPSUBQ Z0, Z2, Z0
  VPSUBQ Z1, Z3, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncday(ts[1]).k[2]
TEXT bcdatetruncday(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  BC_FILL_ONES(Z31)
  VPBROADCASTQ CONSTQ_86400000000(), Z4

  BC_MOD_FLOOR_I64VEC_BY_U64IMM(OUT(Z0), OUT(Z1), IN(Z2), IN(Z3), IN(Z4), IN(Z31), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, K3, K4)
  VPSUBQ Z0, Z2, Z0
  VPSUBQ Z1, Z3, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncdow(ts[1], i16@imm[2]).k[3]
//
// Truncating a timestamp to a DOW can be implemented the following way:
//   days = unix_time_in_days(ts)
//   off = days + 4 - dow
//   adj = floor(off / 7) * 7
//   truncated_ts = days_to_microseconds(adj - 4 + dow)
TEXT bcdatetruncdow(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(BX))
  BC_UNPACK_ZI16(BC_SLOT_SIZE*2, Z7)
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2 + 2, OUT(R8))

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // divide Z2:Z3 by (60 * 60 * 24 * 1000000) (microseconds per day)
  // to get days from the start of unix time. Calculate also Z7 to
  // be `4 - DOW`.
  VBROADCASTSD CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13(), Z6
  VPBROADCASTQ CONSTQ_4(), Z8
  VPSRAQ $13, Z2, Z4
  VPSRAQ $13, Z3, Z5
  VCVTQQ2PD.Z Z4, K1, Z4
  VCVTQQ2PD.Z Z5, K2, Z5
  VPSRLQ $48, Z7, Z7
  VDIVPD.RD_SAE Z6, Z4, Z4
  VDIVPD.RD_SAE Z6, Z5, Z5
  VPSUBQ Z7, Z8, Z7

  // The internal [unboxed] representation is a unix microtime, which
  // starts at 1970-01-01, which is Thursday - so add 4 - the requested
  // DayOfWeek so we can calculate the adjustment.
  VBROADCASTSD CONSTF64_7(), Z10
  VCVTQQ2PD Z7, Z7

  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z4, Z4 // Z4 <- Days since 1970-01-01 (low)
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z5, Z5 // Z5 <- Days since 1970-01-01 (high)

  VADDPD Z7, Z4, Z4                        // Z4 <- days + 4 - dow (low)
  VADDPD Z7, Z5, Z5                        // Z5 <- days + 4 - dow (high)
  VDIVPD.RD_SAE Z10, Z4, Z4                // Z4 <- (days + 4 - dow) / 7 (low)
  VDIVPD.RD_SAE Z10, Z5, Z5                // Z5 <- (days + 4 - dow) / 7 (high)
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z4, Z4 // Z4 <- floor((days + 4 - dow) / 7) (low)
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, Z5, Z5 // Z5 <- floor((days + 4 - dow) / 7) (high)
  VMULPD Z10, Z4, Z4                       // Z4 <- floor((days + 4 - dow) / 7) * 7 (low)
  VMULPD Z10, Z5, Z5                       // Z5 <- floor((days + 4 - dow) / 7) * 7 (high)

  VSUBPD Z7, Z4, Z4                        // Z8 <- days since 1970-01-01 truncated to DOW (low) (low)
  VSUBPD Z7, Z5, Z5                        // Z9 <- days since 1970-01-01 truncated to DOW (low) (high)

  VPBROADCASTQ CONSTQ_86400000000(), Z6
  VCVTPD2QQ Z4, Z4
  VCVTPD2QQ Z5, Z5
  VPMULLQ Z6, Z4, K1, Z2                   // Z2 <- Truncated timestamp in unix microseconds (low)
  VPMULLQ Z6, Z5, K2, Z3                   // Z3 <- Truncated timestamp in unix microseconds (high)

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)

// ts[0] = datetruncmonth(ts[1]).k[2]
TEXT bcdatetruncmonth(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z8/Z9 <- Year index.
  // Z10/Z11 <- Month index - starting from zero, where zero represents March.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // Z4/Z5 <- Number of days in a year [0, 365] got from MonthIndex.
  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13
  VPERMD Z13, Z10, Z4
  VPERMD Z13, Z11, Z5

  // Z4/Z5 <- Number of days of all years, including days in the last month.
  BC_COMPOSE_YEAR_TO_DAYS(Z4, Z5, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  VPBROADCASTQ  CONSTQ_86400000000(), Z20
  VPBROADCASTQ  CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z21

  // Z4/Z5 <- Final number of days converted to microseconds.
  VPMULLQ Z20, Z4, Z4
  VPMULLQ Z20, Z5, Z5

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ Z21, Z4, K1, Z2
  VPSUBQ Z21, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// VPSHUFB predicate that aligns a month in [1, 12] range (where 1 is March) to a quarter
// month in [0, 11] range (our processing range that can be then easily composed). This
// is a special predicate designed only for 'bcdatetruncquarter' operation.
CONST_DATA_U64(consts_align_month_to_quarter_1_is_march, 0, $0x0404040101010A00)
CONST_DATA_U64(consts_align_month_to_quarter_1_is_march, 8, $0x0000000A0A070707)
CONST_GLOBAL(consts_align_month_to_quarter_1_is_march, $16)

// ts[0] = datetruncquarter(ts[1]).k[2]
TEXT bcdatetruncquarter(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z8/Z9 <- Year index.
  // Z10/Z11 <- Month index - starting from zero, where zero represents March.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // make month index from [1, 12] so we can safely use VPSHUFB (so zero can stay zero)
  VPBROADCASTQ CONSTQ_1(), Z4
  VBROADCASTI32X4 CONST_GET_PTR(consts_align_month_to_quarter_1_is_march, 0), Z5

  VPADDQ Z4, Z10, Z10
  VPADDQ Z4, Z11, Z11

  // since month index starts in March, we have to decrease one year if the month is March
  VPCMPEQQ Z4, Z10, K1, K3
  VPCMPEQQ Z4, Z11, K2, K4
  VPSUBQ Z4, Z8, K3, Z8
  VPSUBQ Z4, Z9, K4, Z9

  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13
  VPSHUFB Z10, Z5, Z10
  VPSHUFB Z11, Z5, Z11

  // Z4/Z5 <- Number of days in a year [0, 365] got from MonthIndex.
  VPERMD Z13, Z10, Z4
  VPERMD Z13, Z11, Z5

  // Z4/Z5 <- Number of days of all years, including days in the last month.
  BC_COMPOSE_YEAR_TO_DAYS(Z4, Z5, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  VPBROADCASTQ CONSTQ_86400000000(), Z20
  VPBROADCASTQ CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z21

  // Z4/Z5 <- Final number of days converted to microseconds.
  VPMULLQ Z20, Z4, Z4
  VPMULLQ Z20, Z5, Z5

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ Z21, Z4, K1, Z2
  VPSUBQ Z21, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0] = datetruncyear(ts[1]).k[2]
TEXT bcdatetruncyear(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  // Z8/Z9 <- Year index.
  // Z10/Z11 <- Month index - starting from zero, where zero represents March.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ CONSTQ_10(), Z20
  VPBROADCASTQ CONSTQ_1(), Z21

  // Since the month starts from March, we have to check whether the truncation doesn't
  // need to decrement one year (January/February have 10/11 indexes, respectively)
  VPCMPUQ $VPCMP_IMM_LT, Z20, Z10, K3
  VPCMPUQ $VPCMP_IMM_LT, Z20, Z11, K4

  // Decrement one year if required.
  VPSUBQ Z21, Z8, K3, Z8
  VPSUBQ Z21, Z9, K4, Z9

  VPBROADCASTQ CONSTQ_306(), Z4
  VMOVDQA64 Z4, Z5

  BC_COMPOSE_YEAR_TO_DAYS(Z4, Z5, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  VPBROADCASTQ CONSTQ_86400000000(), Z20
  VPBROADCASTQ CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z21

  // Z4/Z5 <- Final number of days converted to microseconds.
  VPMULLQ Z20, Z4, Z4
  VPMULLQ Z20, Z5, Z5

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ Z21, Z4, K1, Z2
  VPSUBQ Z21, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// ts[0].k[1] = unboxts(v[2]).k[3]
TEXT bcunboxts(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z16), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z15), IN(BX))

  VPADDD.Z BC_VSTACK_PTR(BX, 0), Z16, K1, Z2       // Z2 <- value offset that follows Type|L + Length
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 64), K1, Z3        // Z3 <- value length including Type|L + Length
  VEXTRACTI32X8 $1, Z2, Y21                        // Y21 <- 32-bit high offsets for 64-bit gathers
  VPSUBD.Z Z16, Z3, K1, Z3                         // Z3 <- value length excluding Type|L + Length

  VPBROADCASTD CONSTD_6(), Z20
  VPSRLD $4, Z15, Z16                              // Z16 = object tag
  VPCMPEQD Z20, Z16, K1, K1                        // K1 <- Lanes that contain timestamp values
  KSHIFTRW $8, K1, K2

  // Z4:Z5 <- First 8 bytes of the timestamp to process ignoring the timezone offset byte.
  KMOVB K1, K3
  VPXORQ X4, X4, X4
  VPGATHERDQ 1(VIRT_BASE)(Y2*1), K3, Z4

  KMOVB K2, K4
  VPXORQ X5, X5, X5
  VPGATHERDQ 1(VIRT_BASE)(Y21*1), K4, Z5

  // Z20/Z21 <- Frequently used constants to avoid broadcasts.
  VPBROADCASTQ CONSTQ_0x7F(), Z20
  VPBROADCASTQ CONSTQ_0x80(), Z21
  VPBROADCASTQ CONSTQ_1(), Z22
  VPBROADCASTD CONSTD_8(), Z23

  // Z4/Z5 <- First 8 bytes of the timestamp cleared so only bytes that
  //          are within the length are non-zero, other bytes cleared.
  VPMINUD.Z Z23, Z3, K1, Z16
  VPSUBD.Z Z16, Z23, K1, Z16
  VPSLLD $3, Z16, Z16
  VEXTRACTI32X8 $1, Z16, Y17
  VPMOVZXDQ Y16, Z16
  VPMOVZXDQ Y17, Z17
  VPSLLVQ Z16, Z4, Z4
  VPSLLVQ Z17, Z5, Z5
  VPSRLVQ Z16, Z4, Z4
  VPSRLVQ Z17, Z5, Z5

  // Z6/Z7 <- Year (1 to 3 bytes).
  //
  // We assume year to be one to three bytes, month and day must be one bytes each.
  VPTESTNMQ Z21, Z4, K3
  VPTESTNMQ Z21, Z5, K4
  VPANDQ Z20, Z4, Z6
  VPANDQ Z20, Z5, Z7
  VPSRLQ $8, Z4, Z4
  VPSRLQ $8, Z5, Z5

  // KUNPCKBW K3, K4, K5
  VPSLLQ $7, Z6, K3, Z6
  VPSLLQ $7, Z7, K4, Z7
  VPTERNLOGQ $TLOG_BLEND_BA, Z20, Z4, K3, Z6
  VPTERNLOGQ $TLOG_BLEND_BA, Z20, Z5, K4, Z7
  VPSRLQ $8, Z4, K3, Z4
  VPSRLQ $8, Z5, K4, Z5

  VPTESTNMQ Z21, Z4, K3, K3
  VPTESTNMQ Z21, Z5, K4, K4
  VPSLLQ $7, Z6, K3, Z6
  VPSLLQ $7, Z7, K4, Z7
  VPTERNLOGQ $TLOG_BLEND_BA, Z20, Z4, K3, Z6
  VPTERNLOGQ $TLOG_BLEND_BA, Z20, Z5, K4, Z7
  VPSRLQ $8, Z4, K3, Z4
  VPSRLQ $8, Z5, K4, Z5

  // Z4/Z5 <- [?|?|?|Second|Minute|Hour|Day|Month] with 0x80 bit cleared in each value.
  VPBROADCASTQ CONSTQ_0x0000007F7F7F7F7F(), Z25
  VPANDQ Z25, Z4, Z4
  VPANDQ Z25, Z5, Z5

  // Z8/Z9 <- Month (always 1 byte), indexed from 1.
  VPANDQ Z20, Z4, Z8
  VPANDQ Z20, Z5, Z9
  VPSRLQ $8, Z4, Z4
  VPSRLQ $8, Z5, Z5
  VPMAXUQ Z22, Z8, Z8
  VPMAXUQ Z22, Z9, Z9

  // Z10/Z11 <- Day of month (always 1 byte), indexed from 1.
  VPANDQ Z20, Z4, Z10
  VPANDQ Z20, Z5, Z11
  VPSRLQ $8, Z4, Z4
  VPSRLQ $8, Z5, Z5
  VPMAXUQ Z22, Z10, Z10
  VPMAXUQ Z22, Z11, Z11

  // Z4/Z5 <- Hour/Minute/Second converted to Seconds.
  VPBROADCASTQ CONSTQ_0x0001013C(), Z18
  VPBROADCASTQ CONSTQ_0x0001003C(), Z19
  // [0 + Second | Minute + Hour*60] <- [0 | Second | Minute | Hour].
  VPMADDUBSW Z18, Z4, Z4
  VPMADDUBSW Z18, Z5, Z5
  // [Second + Minute*60 + Hour*60*60] <- [0 + Second | Minute + Hour*60].
  VPMADDWD Z19, Z4, Z4
  VPMADDWD Z19, Z5, Z5

  // Z18 <- Load last 4 bytes of the timestamp if it contains microseconds.
  VPCMPD.BCST $VPCMP_IMM_GT, CONSTD_10(), Z3, K1, K3
  VPADDD Z2, Z3, Z19
  VPXORD X18, X18, X18
  VPGATHERDD (-4)(VIRT_BASE)(Z19*1), K3, Z18

  // Z8/Z9 <- Month - 3.
  VPSUBQ.BCST CONSTQ_3(), Z8, Z8
  VPSUBQ.BCST CONSTQ_3(), Z9, Z9

  // NOTE: Z21 is 0x80 - this is enough to check for a negative month in this case.
  VPTESTMQ Z21, Z8, K3
  VPTESTMQ Z21, Z9, K4

  // Z6/Z7 <- Corrected year in case that the month is January/February.
  VPSUBQ Z22, Z6, K3, Z6
  VPSUBQ Z22, Z7, K4, Z7

  // Z8/Z9 <- Corrected month index in range [0, 11] where 0 is March.
  VPADDQ.BCST CONSTQ_12(), Z8, K3, Z8
  VPADDQ.BCST CONSTQ_12(), Z9, K4, Z9

  // --- Compose the timestamp ---

  // Z8/Z9 <- Number of days in a year [0, 365].
  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13
  VPERMD Z13, Z8, Z12
  VPERMD Z13, Z9, Z13
  VPADDQ Z12, Z10, Z8
  VPADDQ Z13, Z11, Z9
  VPSUBQ Z22, Z8, Z8
  VPSUBQ Z22, Z9, Z9

  // Z8/Z9 <- Number of days of all years, including days in the last month.
  BC_COMPOSE_YEAR_TO_DAYS(Z8, Z9, Z6, Z7, Z10, Z11, Z12, Z13, Z14, Z15)

  // Z18 <- Convert last 4 bytes of the timestamp to microseconds (it's either a value or zero).
  VPSHUFB CONST_GET_PTR(bswap24_zero_last_byte, 0), Z18, Z18

  VPBROADCASTQ CONSTQ_86400000000(), Z25
  VPBROADCASTQ CONSTQ_1000000(), Z26
  VPBROADCASTQ CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z27

  // Z8/Z9 <- Final number of days converted to microseconds.
  VPMULLQ Z25, Z8, Z8
  VPMULLQ Z25, Z9, Z9

  // Z8/Z9 <- Combined microseconds of all days and microseconds of the remaining day.
  VEXTRACTI32X8 $1, Z18, Y19
  VPMULLQ Z26, Z4, Z4
  VPMULLQ Z26, Z5, Z5
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))

  VPMOVZXDQ Y18, Z18
  VPMOVZXDQ Y19, Z19
  VPADDQ Z4, Z8, Z8
  VPADDQ Z5, Z9, Z9
  VPADDQ Z18, Z8, Z8
  VPADDQ Z19, Z9, Z9

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ Z27, Z8, K1, Z2
  VPSUBQ Z27, Z9, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// v[0] = boxts(ts[1]).k[2]
//
// scratch: 16 * 16
TEXT bcboxts(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  // Make sure we have at least 16 bytes for each lane, we always overallocate to make the boxing simpler.
  BC_CHECK_SCRATCH_CAPACITY($(16 * 16), R8, abort)

  // set zmm30.k1 to the current scratch base
  BC_GET_SCRATCH_BASE_ZMM(Z30, K1)

  // Update the length of the output buffer.
  ADDQ $(16 * 16), bytecode_scratch+8(VIRT_BCPTR)

  // Decompose the timestamp value into Year/Month/DayOfMonth and microseconds of the day.
  //
  // Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
  // Z8/Z9   - Year index.
  // Z10/Z11 - Month index - starting from zero, where zero represents March.
  // Z14/Z15 - Day of month - starting from zero.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  VPBROADCASTQ CONSTQ_3(), Z20
  VPBROADCASTQ CONSTQ_1(), Z21
  VPBROADCASTQ CONSTQ_12(), Z22

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ Z20, Z10, Z10
  VPADDQ Z20, Z11, Z11
  VPCMPUQ $VPCMP_IMM_GT, Z22, Z10, K5
  VPCMPUQ $VPCMP_IMM_GT, Z22, Z11, K6

  // Increment one year if required to adjust for the month greater than 12 after adding the final offset.
  VPADDQ Z21, Z8, K5, Z8
  VPADDQ Z21, Z9, K6, Z9

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ Z22, Z10, K5, Z10
  VPSUBQ Z22, Z11, K6, Z11

  // Increment one day to make the day of the month start from 1.
  VPADDQ Z21, Z14, Z14
  VPADDQ Z21, Z15, Z15

  // Construct Type|L, Offset, Year, Month, and DayOfMonth data, where:
  //   - Type|L is  (one byte).
  //   - Offset [0] (one byte).
  //   - Year (1 to 3 bytes).
  //   - Month [1, 12] (one byte)
  //   - DayOfMonth [1, 31] (one byte)

  // Z10/Z11 <- [DayOfMonth, Month, 0].
  VPSLLQ $16, Z14, Z14
  VPSLLQ $16, Z15, Z15
  VPSLLQ $8, Z10, Z10
  VPSLLQ $8, Z11, Z11
  VPBROADCASTQ CONSTQ_0x7F(), Z16
  VPORQ Z14, Z10, Z10
  VPORQ Z15, Z11, Z11

  // Z14/Z15 <- Initial L field (length) is 7 bytes - Offset, Year (1 byte), Month, DayOfMonth, Hour, Minute, Second).
  //   - Modified by the algorithm depending on the year's length.
  //   - Used later to calculate the offset to the higher value (representing Hour/Minute/Second/Microsecond).
  VPBROADCASTQ CONSTQ_7(), Z14
  VMOVDQA32 Z14, Z15

  // Z10/Z11 <- [DayOfMonth, Month, Year (1 byte)].
  VPBROADCASTQ CONSTQ_0x0000000000808080(), Z24
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z8, Z10
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z9, Z11
  VPORQ Z24, Z10, Z10
  VPORQ Z24, Z11, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-2 bytes)].
  VPCMPQ $VPCMP_IMM_GT, Z16, Z8, K5
  VPCMPQ $VPCMP_IMM_GT, Z16, Z9, K6
  VPSRLQ $7, Z8, Z8
  VPSRLQ $7, Z9, Z9
  VPADDQ Z21, Z14, K5, Z14
  VPADDQ Z21, Z15, K6, Z15
  VPSLLQ $8, Z10, K5, Z10
  VPSLLQ $8, Z11, K6, Z11
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z8, K5, Z10
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z9, K6, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-3 bytes)].
  VPCMPQ $VPCMP_IMM_GT, Z16, Z8, K5
  VPCMPQ $VPCMP_IMM_GT, Z16, Z9, K6
  VPSRLQ $7, Z8, Z8
  VPSRLQ $7, Z9, Z9
  VPADDQ Z21, Z14, K5, Z14
  VPADDQ Z21, Z15, K6, Z15
  VPSLLQ $8, Z10, K5, Z10
  VPSLLQ $8, Z11, K6, Z11
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z8, K5, Z10
  VPTERNLOGQ $TLOG_BLEND_BA, Z16, Z9, K6, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-3 bytes), Offset (always zero), Type|L (without a possible microsecond encoding length)].
  VPBROADCASTQ CONSTQ_0x0000000000008060(), Z20
  VPSLLQ $16, Z10, Z10
  VPSLLQ $16, Z11, Z11
  VPTERNLOGQ $0xFE, Z20, Z14, Z10
  VPTERNLOGQ $0xFE, Z20, Z15, Z11

  // Z14/Z15 - The size of the lower value of the encoded timestamp, in bytes, including Type|L field.
  VPBROADCASTQ CONSTQ_2(), Z25
  VPSUBQ Z25, Z14, Z14
  VPSUBQ Z25, Z15, Z15

  // Construct Hour, Minute, Second, and an optional Microsecond
  //   - Hour [0, 23] (one byte)
  //   - Minute [0, 59] (one byte)
  //   - Second [0, 59] (one byte)
  //   - Microsecond [0, 999999] (1 byte for fraction_exponent 0xC6, 3 bytes for coefficient - UInt)

  // Z8/Z9 - Hour [0, 23].
  VPSRLQ $12, Z4, Z8
  VPSRLQ $12, Z5, Z9
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z8, Z9, Z8, Z9, CONSTQ_2562048517(), 51)

  // Z4/Z5 - (Minutes * 60000000) + (Second * 1000000) + Microseconds.
  VPMULLQ.BCST CONSTQ_3600000000(), Z8, Z12
  VPMULLQ.BCST CONSTQ_3600000000(), Z9, Z13
  VPSUBQ Z12, Z4, Z4
  VPSUBQ Z13, Z5, Z5

  // Z6/Z7 - Minute [0, 59].
  VPSRLQ $8, Z4, Z6
  VPSRLQ $8, Z5, Z7
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z6, Z7, Z6, Z7, CONSTQ_18764999(), 42)

  // Z4/Z5 - (Seconds * 1000000) + Microseconds.
  VPMULLQ.BCST CONSTQ_60000000(), Z6, Z12
  VPMULLQ.BCST CONSTQ_60000000(), Z7, Z13
  VPSUBQ Z12, Z4, Z4
  VPSUBQ Z13, Z5, Z5

  // Z12/Z13 - Second [0, 59].
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST(Z12, Z13, Z4, Z5, CONSTQ_1125899907(), 50)

  // Z4/Z5 - Microsecond [0, 999999].
  VPBROADCASTQ CONSTQ_1000000(), Z20
  VPMULLQ Z20, Z12, Z16
  VPMULLQ Z20, Z13, Z17
  VPSUBQ Z16, Z4, Z4
  VPSUBQ Z17, Z5, Z5

  // K3/K4 - Non-zero if the lane has a non-zero microsecond.
  VPTESTMQ Z4, Z4, K3
  VPTESTMQ Z5, Z5, K4

  // Z8/Z9 - [Second, Minute, Hour] (3 bytes).
  VPSLLQ $8, Z6, Z6
  VPSLLQ $8, Z7, Z7
  VPSLLQ $16, Z12, Z12
  VPSLLQ $16, Z13, Z13
  VPTERNLOGQ $0xFE, Z12, Z6, Z8
  VPTERNLOGQ $0xFE, Z13, Z7, Z9

  // Z4/Z5 - [Microsecond (3 bytes), 0xC6, Second, Minute, Hour].
  VBROADCASTI64X2 CONST_GET_PTR(consts_boxts_microsecond_swap, 0), Z16
  VPBROADCASTQ CONSTQ_0x00000000C6808080(), Z17
  VPSHUFB Z16, Z4, Z4
  VPSHUFB Z16, Z5, Z5
  VPTERNLOGQ $0xFE, Z17, Z8, Z4
  VPTERNLOGQ $0xFE, Z17, Z9, Z5

  // Z10/Z11 -  [DayOfMonth, Month, Year (1-3 bytes), Offset (always zero), Type|L (final length)].
  VPADDQ.BCST CONSTQ_4(), Z10, K3, Z10
  VPADDQ.BCST CONSTQ_4(), Z11, K4, Z11

  // Z30 - offsets relative to vmm (where each timestamp value starts, overallocated).
  VMOVDQA32 byteidx<>+0(SB), X28 // X28 = [0, 1, 2, 3 ...]
  VPMOVZXBD X28, Z28
  VPSLLD    $4, Z28, Z28
  VPADDD    Z28, Z30, K1, Z30    // Z30 += [0, 16, 32, 48, ...]

  // turn (zmm14 || zmm15) -> zmm14 by truncating
  VPMOVQD      Z14, Y14
  VPMOVQD      Z15, Y15
  VINSERTI32X8 $1, Y15, Z14, Z14

  KMOVB         K1, K3
  KSHIFTRW      $8, K1, K4
  VPADDD        Z14, Z30, Z29                // Z29 = high positions
  VEXTRACTI32X8 $1, Z30, Y21                 // Y21 = hi 8 base positions
  VPSCATTERDQ   Z10, K3, 0(VIRT_BASE)(Y30*1) // write leading bits, lo 8 lanes
  VPSCATTERDQ   Z11, K4, 0(VIRT_BASE)(Y21*1) // write leading bits, hi 8 lanes
  KMOVB         K1, K3
  KSHIFTRW      $8, K1, K4
  VEXTRACTI32X8 $1, Z29, Y21                 // Y21 = hi 8 upper positions
  VPSCATTERDQ   Z4, K3, 0(VIRT_BASE)(Y29*1)  // write trailing bits, lo 8 lanes
  VPSCATTERDQ   Z5, K4, 0(VIRT_BASE)(Y21*1)  // write trailing bits, hi 8 lanes

  VPMOVQD Z10, Y10
  VPMOVQD Z11, Y11

  VPBROADCASTD CONSTD_1(), Z12        // Header Length (always 1)
  VINSERTI32X8 $1, Y11, Z10, Z10      // Type|L bytes

  VPADDD Z12, Z10, Z31
  VPANDD.BCST.Z CONSTD_0x0F(), Z31, K1, Z31

  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z10), IN(Z12), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// Bucket Instructions
// -------------------

// f64[0] = widthbucket.f64(f64[1], f64[2], f64[3], f64[4]).k[5]
//
// WIDTH_BUCKET semantics is as follows:
//   - When the input is less than MIN, the output is 0
//   - When the input is greater than or equal to MAX, the output is BucketCount+1
//
// Some references that I have found that explicitly state that MAX is outside:
//   - https://www.oreilly.com/library/view/sql-in-a/9780596155322/re91.html
//   - https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions214.htm
//   - https://docs.snowflake.com/en/sql-reference/functions/width_bucket.html
TEXT bcwidthbucketf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(DX), OUT(R15), OUT(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX)) // Value
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX)) // MinValue
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(DX)) // MaxValue
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))   // Predicate

  // Value = Input - MinValue
  VSUBPD.RD_SAE Z4, Z2, Z2
  VSUBPD.RD_SAE Z5, Z3, Z3

  // ValueRange = MaxValue - MinValue
  VSUBPD.RD_SAE Z4, Z6, Z6
  VSUBPD.RD_SAE Z5, Z7, Z7

  // Value = (Input - MinValue) / (MaxValue - MinValue)
  VDIVPD.RD_SAE Z6, Z2, Z2
  VDIVPD.RD_SAE Z7, Z3, Z3

  // BucketCount
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(R15))

  // Value = ((Input - MinValue) / (MaxValue - MinValue)) * BucketCount
  VMULPD.RD_SAE Z4, Z2, Z2
  VMULPD.RD_SAE Z5, Z3, Z3

  // Round to integer - this operation would preserve special numbers (Inf/NaN).
  VRNDSCALEPD   $VROUND_IMM_DOWN_SAE, Z2, Z2
  VRNDSCALEPD   $VROUND_IMM_DOWN_SAE, Z3, Z3

  // Restrict output values to [0, BucketCount + 1] range
  VBROADCASTSD  CONSTF64_1(), Z6
  VMINPD        Z4, Z2, Z2
  VMINPD        Z5, Z3, Z3
  VADDPD        Z6, Z2, Z2
  VADDPD        Z6, Z3, Z3
  VXORPD        X6, X6, X6
  VMAXPD.Z      Z6, Z2, K1, Z2
  VMAXPD.Z      Z6, Z3, K2, Z3

  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*6)

// i64[0] = widthbucket.i64(i64[1], i64[2], i64[3], i64[4]).k[5]
//
// NOTE: This function has some precision loss when the arithmetic exceeds 2^53.
TEXT bcwidthbucketi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(DX), OUT(R15), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX)) // Value
  BC_LOAD_I64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX)) // MinValue
  BC_LOAD_I64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(DX)) // MaxValue
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))   // Predicate

  // K3/K4 = Value < MinValue
  VPCMPQ $VPCMP_IMM_LT, Z4, Z2, K1, K3
  VPCMPQ $VPCMP_IMM_LT, Z5, Z3, K2, K4

  // Value.U64 = Input - MinValue
  VPSUBQ Z4, Z2, Z2
  VPSUBQ Z5, Z3, Z3

  // ValueRange.U64 = MaxValue - MinValue
  VPSUBQ Z4, Z6, Z6
  VPSUBQ Z5, Z7, Z7

  // Value.F64 = (F64)Value.U64
  VCVTUQQ2PD Z2, Z2
  VCVTUQQ2PD Z3, Z3

  // ValueRange.F64 = (F64)ValueRange.U64
  VCVTUQQ2PD Z6, Z6
  VCVTUQQ2PD Z7, Z7

  // Value.F64 = (Input - MinValue) / (MaxValue - MinValue)
  VDIVPD.RD_SAE Z6, Z2, Z2
  VDIVPD.RD_SAE Z7, Z3, Z3

  // BucketCount.U64
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_LOAD_I64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(R15))

  // BucketCount.F64 = (F64)BucketCount.U64
  VCVTQQ2PD Z4, Z6
  VCVTQQ2PD Z5, Z7

  // Value.F64 = ((Input - MinValue) / (MaxValue - MinValue)) * BucketCount
  VMULPD.RD_SAE Z6, Z2, Z2
  VMULPD.RD_SAE Z7, Z3, Z3

  // Value.I64 = (I64)Value.F64
  VCVTTPD2QQ Z2, Z2
  VCVTTPD2QQ Z3, Z3

  // Restrict output values to [0, BucketCount + 1] range
  VPBROADCASTQ CONSTQ_1(), Z10
  VPMINSQ Z4, Z2, Z2
  VPMINSQ Z5, Z3, Z3
  VPADDQ.Z Z10, Z2, K1, Z2
  VPADDQ.Z Z10, Z3, K2, Z3
  VPXORQ Z2, Z2, K3, Z2
  VPXORQ Z3, Z3, K4, Z3

  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*6)

// i64[0] = timebucket.ts(i64[1], i64[2]).k[3]
TEXT bctimebucketts(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX)) // Timestamp
  BC_LOAD_I64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(CX)) // Interval
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))   // Predicate

  BC_MODI64_IMPL(Z16, Z17, Z2, Z3, Z4, Z5, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  // subtract modulo value from source in order
  // to get the start value of the bucket
  BC_UNPACK_SLOT(0, OUT(DX))
  VPSUBQ.Z Z16, Z2, K1, Z2
  VPSUBQ.Z Z17, Z3, K2, Z3

  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// GEO Functions
// -------------

/*
    CONSTF64_PI_DIV_180() = uint64(0x3f91df46a2529d39)
    CONSTF64_0p9999() = 0.9999
    CONSTF64_MINUS_0p9999() = -0.9999

    // (1 << 48) / (3.1415926535897931 * 4);
    CONSTF64_281474976710656_DIV_4PI() = uint64(0x42b45f306dc9c883)

    // (1 << 48) / 360.0
    CONSTF64_281474976710656_DIV_360() = uint64(0x4266c16c16c16c17)
*/

#define CONST_GEO_TILE_MAX_PRECISION() CONSTQ_32()
// Calculates GEO HASH bits with full precision.
//
// The output can contain many bits, so it's necessary to BIT-AND the results to get the designated precision.
#define BC_SCALE_GEO_COORDINATES(DST_LAT_A, DST_LAT_B, DST_LON_A, DST_LON_B, SRC_LAT_A, SRC_LAT_B, SRC_LON_A, SRC_LON_B, TMP_0) \
  /* Scale latitude values. */                                 \
  VBROADCASTSD CONSTQ_0x3D86800000000000(), TMP_0              \
  VDIVPD.RD_SAE TMP_0, SRC_LAT_A, DST_LAT_A                    \
  VDIVPD.RD_SAE TMP_0, SRC_LAT_B, DST_LAT_B                    \
                                                               \
  /* Scale longitude values. */                                \
  VBROADCASTSD CONSTQ_0x3D96800000000000(), TMP_0              \
  VDIVPD.RD_SAE TMP_0, SRC_LON_A, DST_LON_A                    \
  VDIVPD.RD_SAE TMP_0, SRC_LON_B, DST_LON_B                    \
                                                               \
  /* Convert to integers. */                                   \
  VPBROADCASTQ CONSTQ_35184372088832(), TMP_0                  \
  VCVTPD2QQ.RD_SAE DST_LAT_A, DST_LAT_A                        \
  VCVTPD2QQ.RD_SAE DST_LAT_B, DST_LAT_B                        \
                                                               \
  VCVTPD2QQ.RD_SAE DST_LON_A, DST_LON_A                        \
  VCVTPD2QQ.RD_SAE DST_LON_B, DST_LON_B                        \
                                                               \
  /* Scaled latitude values to integers of full precision. */  \
  VPADDQ TMP_0, DST_LAT_A, DST_LAT_A                           \
  VPADDQ TMP_0, DST_LAT_B, DST_LAT_B                           \
                                                               \
  /* Scaled longitute values to integers of full precision. */ \
  VPADDQ TMP_0, DST_LON_A, DST_LON_A                           \
  VPADDQ TMP_0, DST_LON_B, DST_LON_B

// s[0] = geohash(f64[1], f64[2], i64[3]).k[4]
//
// scratch: 16 * 16
//
// GEO_HASH is a string representing longitude, latitude, and precision as "HASH" where each
// 5 bits of interleaved latitude and longitude data are encoded by a single ASCII character.
TEXT bcgeohash(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  ADDQ $(BC_SLOT_SIZE*5), VIRT_PCREG

  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(BX))  // Latitude
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(CX))  // Longitude
  BC_LOAD_I64_FROM_SLOT(OUT(Z8), OUT(Z9), IN(R15)) // Precision

  JMP geohash_tail(SB)

// s[0] = geohashimm(f64[1], f64[2], u16@imm[3]).k[4]
//
// scratch: 16 * 16
TEXT bcgeohashimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  ADDQ $(BC_SLOT_SIZE*4 + 2), VIRT_PCREG

  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(BX))  // Latitude
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(CX))  // Longitude
  VPBROADCASTQ R15, Z8                                 // Precision (low)
  VPBROADCASTQ R15, Z9                                 // Precision (high)

  JMP geohash_tail(SB)

TEXT geohash_tail(SB), NOSPLIT|NOFRAME, $0
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPMOVSQD Z8, Y8
  VPMOVSQD Z9, Y9
  VINSERTI32X8 $1, Y9, Z8, Z3

  // Z4/Z5/Z6/Z7 <- Scaled latitude and longitude bits with full precision.
  BC_SCALE_GEO_COORDINATES(Z4, Z5, Z6, Z7, Z4, Z5, Z6, Z7, Z10)

  // Restrict precision to [1, 12] characters (12 characters is 60 bits, which is the maximum).
  VPMAXSD.BCST CONSTD_1(), Z3, Z3
  VPMINSD.BCST.Z CONSTD_12(), Z3, K1, Z3

  // At the moment the output bits contain 46 bits representing latitude and longitude.
  // The maximum precision of geohash we support is 12 (30 bits for each coordinate),
  // so cut off the extra bits from both latitude and longitude.
  VPSRLQ $16, Z4, Z4
  VPSRLQ $16, Z5, Z5
  VPSRLQ $16, Z6, Z6
  VPSRLQ $16, Z7, Z7

  // Usually, GEO_HASH implementations first interleave all the bits of latitude
  // and logitude and then a lookup is used for each 5 bits chunk to get the
  // character representing it. However, this is unnecessary as we just need 5
  // bits of each chunk in any order to apply our own VPSHUFB lookups.
  //
  // We have this (latitude / longitude) (5 bits are used to compose a single character):
  //
  //   [__AABBBC|CDDDEEFF|FGGHHHII|JJJKKLLL] (Z4/Z5) (uppercased are latitudes)
  //   [__aaabbc|ccddeeef|fggghhii|ijjkkkll] (Z6/Z7) (lowercased are longitudes)
  //
  // But we want this so we would be able to encode the bits as GEO_HASH:
  //
  //   [________][________][___AAaaa][___CCccc][___EEeee][___GGggg][___IIiii][___KKkkk] {longitude has 3 bits}
  //   [________][________][___bbBBB][___ddDDD][___ffFFF][___hhHHH][___jjJJJ][___llLLL] {latitude has 3 bits}
  //
  // After this it's easy to use VPUNPCKLBW to interleave the bytes to get the final string.

  // NOTE: This is basically a dumb approach to shuffle bits via shifting and masking.
  VPBROADCASTD CONSTD_0b11001110_01110011_10011100_11100111(), Z14

  VPSRLQ $2, Z6, Z10                            // [________|________|________|________|____aaab|bcccddee|effggghh|iiijjkkk] {lo}
  VPSRLQ $2, Z7, Z11                            // [________|________|________|________|____aaab|bcccddee|effggghh|iiijjkkk] {hi}
  VPSLLQ $3, Z6, Z6                             // [________|________|________|________|__bbcccd|deeeffgg|ghhiiijj|kkkll___] {lo}
  VPSLLQ $3, Z7, Z7                             // [________|________|________|________|__bbcccd|deeeffgg|ghhiiijj|kkkll___] {hi}

  VPTERNLOGD $TLOG_BLEND_BA, Z14, Z4, Z6        // [________|________|________|________|__bbBBBd|dDDDffFF|FhhHHHjj|JJJllLLL] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z14, Z5, Z7        // [________|________|________|________|__bbBBBd|dDDDffFF|FhhHHHjj|JJJllLLL] {hi}
  VPTERNLOGD $TLOG_BLEND_BA, Z14, Z10, Z4       // [________|________|________|________|__AAaaaC|CcccEEee|eGGgggII|iiiKKkkk] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z14, Z11, Z5       // [________|________|________|________|__AAaaaC|CcccEEee|eGGgggII|iiiKKkkk] {hi}

  VPBROADCASTQ CONSTQ_0xFFFFFF(), Z14
  VPSLLQ $9, Z4, Z10                            // [________|________|________|_AAaaaCC|cccEEeee|________|________|________] {lo}
  VPSLLQ $9, Z5, Z11                            // [________|________|________|_AAaaaCC|cccEEeee|________|________|________] {hi}
  VPSLLQ $9, Z6, Z12                            // [________|________|________|_bbBBBdd|DDDffFFF|________|________|________] {lo}
  VPSLLQ $9, Z7, Z13                            // [________|________|________|_bbBBBdd|DDDffFFF|________|________|________] {hi}

  VPTERNLOGQ $TLOG_BLEND_AB, Z14, Z10, Z4       // [________|________|________|_AAaaaCC|cccEEeee|________|eGGgggII|iiiKKkkk] {lo}
  VPTERNLOGQ $TLOG_BLEND_AB, Z14, Z11, Z5       // [________|________|________|_AAaaaCC|cccEEeee|________|eGGgggII|iiiKKkkk] {hi}
  VPTERNLOGQ $TLOG_BLEND_AB, Z14, Z12, Z6       // [________|________|________|_bbBBBdd|DDDffFFF|________|FhhHHHjj|JJJllLLL] {lo}
  VPTERNLOGQ $TLOG_BLEND_AB, Z14, Z13, Z7       // [________|________|________|_bbBBBdd|DDDffFFF|________|FhhHHHjj|JJJllLLL] {hi}

  VPSLLQ $3, Z4, Z10                            // [________|________|________|___CCccc|________|________|___IIiii|________] {lo}
  VPSLLQ $3, Z5, Z11                            // [________|________|________|___CCccc|________|________|___IIiii|________] {hi}
  VPSLLQ $3, Z6, Z12                            // [________|________|________|___ddDDD|________|________|___jjJJJ|________] {lo}
  VPSLLQ $3, Z7, Z13                            // [________|________|________|___ddDDD|________|________|___jjJJJ|________] {hi}

  VPSLLQ $6, Z4, Z14                            // [________|________|___AAaaa|________|________|___GGggg|________|________] {lo}
  VPSLLQ $6, Z5, Z15                            // [________|________|___AAaaa|________|________|___GGggg|________|________] {hi}
  VPSLLQ $6, Z6, Z16                            // [________|________|___bbBBB|________|________|___hhHHH|________|________] {lo}
  VPSLLQ $6, Z7, Z17                            // [________|________|___bbBBB|________|________|___hhHHH|________|________] {hi}

  VPBROADCASTQ CONSTQ_0b00000000_00000000_00000000_00000000_00011111_00000000_00000000_00011111(), Z18
  VPANDD Z18, Z4, Z4                            // [00000000|00000000|00000000|00000000|000EEeee|00000000|00000000|000KKkkk] {lo}
  VPANDD Z18, Z5, Z5                            // [00000000|00000000|00000000|00000000|000EEeee|00000000|00000000|000KKkkk] {hi}
  VPSLLQ $8, Z18, Z19
  VPANDD Z18, Z6, Z6                            // [00000000|00000000|00000000|00000000|000ffFFF|00000000|00000000|000llLLL] {lo}
  VPANDD Z18, Z7, Z7                            // [00000000|00000000|00000000|00000000|000ffFFF|00000000|00000000|000llLLL] {hi}

  VPSLLQ $16, Z18, Z18
  VPTERNLOGD $TLOG_BLEND_BA, Z19, Z10, Z4       // [00000000|00000000|00000000|000CCccc|000EEeee|00000000|000IIiii|000KKkkk] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z19, Z11, Z5       // [00000000|00000000|00000000|000CCccc|000EEeee|00000000|000IIiii|000KKkkk] {hi}
  VPTERNLOGD $TLOG_BLEND_BA, Z19, Z12, Z6       // [00000000|00000000|00000000|000ddDDD|000ffFFF|00000000|000jjJJJ|000llLLL] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z19, Z13, Z7       // [00000000|00000000|00000000|000ddDDD|000ffFFF|00000000|000jjJJJ|000llLLL] {hi}

  VPTERNLOGD $TLOG_BLEND_BA, Z18, Z14, Z4       // [00000000|00000000|000AAaaa|000CCccc|000EEeee|000GGggg|000IIiii|000KKkkk] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z18, Z15, Z5       // [00000000|00000000|000AAaaa|000CCccc|000EEeee|000GGggg|000IIiii|000KKkkk] {hi}
  VPTERNLOGD $TLOG_BLEND_BA, Z18, Z16, Z6       // [00000000|00000000|000bbBBB|000ddDDD|000ffFFF|000hhHHH|000jjJJJ|000llLLL] {lo}
  VPTERNLOGD $TLOG_BLEND_BA, Z18, Z17, Z7       // [00000000|00000000|000bbBBB|000ddDDD|000ffFFF|000hhHHH|000jjJJJ|000llLLL] {hi}

  // Encode the bits into characters.
  //
  // NOTE: Since we need 32 entry LUT, we apply VPSHUFB twice, the second time with a mask that's only valid when `index > 15`.
  VPBROADCASTB CONSTD_15(), Z12
  VBROADCASTI32X4 CONST_GET_PTR(geohash_chars_lut,  0), Z10
  VBROADCASTI32X4 CONST_GET_PTR(geohash_chars_lut, 16), Z11

  VPCMPGTB Z12, Z4, K3
  VPCMPGTB Z12, Z5, K4
  VPSHUFB Z4, Z10, Z14
  VPSHUFB Z5, Z10, Z15
  VPSHUFB Z4, Z11, K3, Z14
  VPSHUFB Z5, Z11, K4, Z15

  VPCMPGTB Z12, Z6, K3
  VPCMPGTB Z12, Z7, K4
  VPSHUFB Z6, Z10, Z16
  VPSHUFB Z7, Z10, Z17
  VPSHUFB Z6, Z11, K3, Z16
  VPSHUFB Z7, Z11, K4, Z17

  // Make sure we have at least 16 bytes for each lane, we always overallocate to make the encoding easier.
  // The encoded hash per lane is 12 bytes, however, we store 16 byte quantities, so we need 16 bytes.
  BC_CHECK_SCRATCH_CAPACITY($(16 * 16), R8, abort)

  BC_GET_SCRATCH_BASE_GP(R8)

  // Update the length of the output buffer.
  ADDQ $(16 * 16), bytecode_scratch+8(VIRT_BCPTR)

  // Broadcast scratch base to all lanes in Z2, which becomes string slice offset.
  VPBROADCASTD.Z R8, K1, Z2

  VPADDD CONST_GET_PTR(consts_offsets_interleaved_d_16, 0), Z2, Z2

  // Make R8 the first address where the output will be stored.
  ADDQ VIRT_BASE, R8

  // Unpack so we will get 16 characters in each 128-bit part of the register.
  VPUNPCKLBW Z14, Z16, Z4  // Lane: [06][04][02][00]
  VPUNPCKHBW Z14, Z16, Z5  // Lane: [07][05][03][01]
  VPUNPCKLBW Z15, Z17, Z6  // Lane: [13][12][10][08]
  VPUNPCKHBW Z15, Z17, Z7  // Lane: [15][13][11][09]

  // Byteswap the characters, as we have the most significant last, at the moment.
  VBROADCASTI32X4 CONST_GET_PTR(geohash_chars_swap, 0), Z10
  VPSHUFB Z10, Z4, Z4
  VPSHUFB Z10, Z5, Z5
  VPSHUFB Z10, Z6, Z6
  VPSHUFB Z10, Z7, Z7

  // Store directly (avoiding scatter).
  VMOVDQU32 Z4, 0(R8)
  VMOVDQU32 Z5, 64(R8)
  VMOVDQU32 Z6, 128(R8)
  VMOVDQU32 Z7, 192(R8)

  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(0)

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// GEO_TILE_X and GEO_TILE_Y functions project latitude and logitude by using Mercator.

// i64[0] = geotilex(f64[1], i64[2]).k[3]
//
// X = FLOOR( (longitude + 180.0) / 360.0 * (1 << zoom) )
//   = FLOOR( [(1 << 48) / 2] + FMA(longitude * [(1 << 48) / 360]) >> (48 - precision)
TEXT bcgeotilex(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VBROADCASTSD CONSTF64_281474976710656_DIV_360(), Z6
  VBROADCASTSD CONSTF64_140737488355328(), Z7

  VFMADD132PD.RZ_SAE Z6, Z7, K1, Z2
  VFMADD132PD.RZ_SAE Z6, Z7, K2, Z3

  VCVTPD2UQQ.RZ_SAE Z2, Z4
  VCVTPD2UQQ.RZ_SAE Z3, Z5

  VPXORQ X8, X8, X8
  VPBROADCASTQ CONST_GEO_TILE_MAX_PRECISION(), Z9
  VPMAXSQ 0(VIRT_VALUES)(CX*1), Z8, Z6
  VPMAXSQ 64(VIRT_VALUES)(CX*1), Z8, Z7

  VPBROADCASTQ CONSTQ_0x0000FFFFFFFFFFFF(), Z11
  VPMINSQ Z9, Z6, Z6
  VPMINSQ Z9, Z7, Z7

  VPBROADCASTQ CONSTQ_48(), Z9
  VPMINSQ Z11, Z4, Z4
  VPMINSQ Z11, Z5, Z5

  VPSUBQ Z6, Z9, Z6
  VPSUBQ Z7, Z9, Z7

  VPSRLVQ.Z Z6, Z4, K1, Z2
  VPSRLVQ.Z Z7, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = geotiley(f64[1], i64[2]).k[3]
//
// Y = FLOOR( {0.5 - [LN((1 + SIN(lat)) / (1 - SIN(lat))] / (4*PI)} * (1 << precision) );
//   = FLOOR( [1 << 48) / 2] - [LN((1 + SIN(lat)) / (1 - SIN(lat)) * (1 << 48) / (4*PI)] ) >> (48 - precision));
TEXT bcgeotiley(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VBROADCASTSD CONSTF64_PI_DIV_180(), Z11
  VBROADCASTSD CONSTF64_1(), Z10

  VMULPD Z11, Z2, K1, Z2
  VMULPD Z11, Z3, K2, Z3
  BC_FAST_SIN_4ULP(Z4, Z5, Z2, Z3)

  // Truncate to [-0.9999, 0.9999] to avoid infinity in border cases.
  VBROADCASTSD CONSTF64_0p9999(), Z11
  VBROADCASTSD CONSTF64_MINUS_0p9999(), Z12
  VMINPD Z11, Z4, Z4
  VMINPD Z11, Z5, Z5
  VMAXPD Z12, Z4, Z4
  VMAXPD Z12, Z5, Z5

  // Z6/Z7 <- 1 - SIN(lat)
  VSUBPD Z4, Z10, Z6
  VSUBPD Z5, Z10, Z7

  // Z4/Z5 <- 1 + SIN(lat)
  VADDPD Z4, Z10, Z4
  VADDPD Z5, Z10, Z5

  // Z4/Z5 <- LN((1 + SIN(lat)) / (1 - SIN(lat)))
  VDIVPD Z6, Z4, Z6
  VDIVPD Z7, Z5, Z7
  BC_FAST_LN_4ULP(Z4, Z5, Z6, Z7)

  VBROADCASTSD CONSTF64_281474976710656_DIV_4PI(), Z10
  VBROADCASTSD CONSTF64_140737488355328(), Z11

  // Z6/Z7 <- [(1 << 48) / 2] - (LN((1 + SIN(lat)) / (1 - SIN(lat))) * [(1 << 48) / 4*PI]
  VFNMADD213PD Z11, Z10, Z4 // Z4 = Z11 - (Z10 * Z4)
  VFNMADD213PD Z11, Z10, Z5 // Z5 = Z11 - (Z10 * Z5)

  VPXORQ X8, X8, X8
  VPBROADCASTQ CONST_GEO_TILE_MAX_PRECISION(), Z9
  VPMAXSQ 0(VIRT_VALUES)(CX*1), Z8, Z6
  VPMAXSQ 64(VIRT_VALUES)(CX*1), Z8, Z7

  VCVTPD2UQQ.RZ_SAE Z4, Z4
  VCVTPD2UQQ.RZ_SAE Z5, Z5

  VPBROADCASTQ CONSTQ_0x0000FFFFFFFFFFFF(), Z11
  VPMINSQ Z9, Z6, Z6
  VPMINSQ Z9, Z7, Z7

  VPBROADCASTQ CONSTQ_48(), Z9
  VPMINSQ Z11, Z4, Z4
  VPMINSQ Z11, Z5, Z5

  VPSUBQ Z6, Z9, Z6
  VPSUBQ Z7, Z9, Z7

  VPSRLVQ.Z Z6, Z4, K1, Z2
  VPSRLVQ.Z Z7, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// GEO_TILE_ES() projects latitude and longitude coordinates by using Mercator function
// and encodes them as "Precision/X/Y" string, which is compatible with Elastic Search.

// Extracts uint16[0|1] of each 64-bit lane and byteswaps it - ((input >> (Index * 16)) & 0xFFFF) << 48
CONST_DATA_U64(const_geotilees_extract_u16, 0, $0x0100FFFFFFFFFFFF)
CONST_DATA_U64(const_geotilees_extract_u16, 8, $0x0908FFFFFFFFFFFF)
CONST_DATA_U64(const_geotilees_extract_u16, 16, $0x0302FFFFFFFFFFFF)
CONST_DATA_U64(const_geotilees_extract_u16, 24, $0x0B0AFFFFFFFFFFFF)
CONST_GLOBAL(const_geotilees_extract_u16, $32)

// Extracts uint16[0|1|2] of each 64-bit lane and byteswaps it - bswap16((input >> (Index * 16)) & 0xFFFF)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 0, $0xFFFFFFFFFFFF0001)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 8, $0xFFFFFFFFFFFF0809)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 16, $0xFFFFFFFFFFFF0203)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 24, $0xFFFFFFFFFFFF0A0B)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 32, $0xFFFFFFFFFFFF0405)
CONST_DATA_U64(const_geotilees_extract_u16_bswap, 40, $0xFFFFFFFFFFFF0C0D)
CONST_GLOBAL(const_geotilees_extract_u16_bswap, $48)

// str[0] = geotilees(f64[1], f64[2], i64[3]).k[4]
//
// scratch: 32 * 16
TEXT bcgeotilees(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z8), OUT(Z9), IN(R15), IN(K1), IN(K2))

  ADDQ $(BC_SLOT_SIZE*5), VIRT_PCREG
  JMP geotilees_tail(SB)

// str[0] = geotilees.imm(f64[1], f64[2], i16@imm[3]).k[4]
//
// scratch: 32 * 16
TEXT bcgeotileesimm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT_RU16_SLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  // Z8/Z9 <- Precision in bits.
  VPBROADCASTQ.Z R15, K1, Z8
  VPBROADCASTQ.Z R15, K2, Z9

  ADDQ $(BC_SLOT_SIZE*4 + 2), VIRT_PCREG
  JMP geotilees_tail(SB)

TEXT geotilees_tail(SB), NOSPLIT|NOFRAME, $0
  // Make sure we have at least 32 bytes for each lane, we always overallocate to make the conversion easier.
  BC_CHECK_SCRATCH_CAPACITY($(32 * 16), R15, abort)

  BC_GET_SCRATCH_BASE_GP(R15)

  // Update the length of the output buffer.
  ADDQ $(32 * 16), bytecode_scratch+8(VIRT_BCPTR)

  // Z4/Z5 <- Projected latitude to Y.
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  VBROADCASTSD CONSTF64_PI_DIV_180(), Z11
  VBROADCASTSD CONSTF64_1(), Z10
  VMULPD.Z Z11, Z2, K1, Z2
  VMULPD.Z Z11, Z3, K2, Z3
  BC_FAST_SIN_4ULP(Z4, Z5, Z2, Z3)

  // Truncate to [-0.9999, 0.9999] to avoid infinity in border cases.
  VBROADCASTSD CONSTF64_0p9999(), Z11
  VBROADCASTSD CONSTF64_MINUS_0p9999(), Z12
  VMINPD Z11, Z4, Z4
  VMINPD Z11, Z5, Z5
  VMAXPD Z12, Z4, Z4
  VMAXPD Z12, Z5, Z5

  // Z6/Z7 <- 1 - SIN(lat)
  VSUBPD Z4, Z10, Z6
  VSUBPD Z5, Z10, Z7

  // Z4/Z5 <- 1 + SIN(lat)
  VADDPD Z4, Z10, Z4
  VADDPD Z5, Z10, Z5

  // Z4/Z5 <- LN((1 + SIN(lat)) / (1 - SIN(lat)))
  VDIVPD Z6, Z4, Z6
  VDIVPD Z7, Z5, Z7
  BC_FAST_LN_4ULP(Z4, Z5, Z6, Z7)

  VBROADCASTSD CONSTF64_281474976710656_DIV_4PI(), Z10
  VBROADCASTSD CONSTF64_140737488355328(), Z11
  VFNMADD213PD Z11, Z10, Z4 // Z4 = Z11 - (Z10 * Z4)
  VFNMADD213PD Z11, Z10, Z5 // Z5 = Z11 - (Z10 * Z5)

  VBROADCASTSD CONSTF64_281474976710656_DIV_360(), Z10
  VCVTPD2UQQ.RZ_SAE Z4, Z4
  VCVTPD2UQQ.RZ_SAE Z5, Z5

  // Z6/Z7 <- Projected longitude to X.
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(CX))
  VFMADD132PD.RZ_SAE Z10, Z11, K1, Z6
  VFMADD132PD.RZ_SAE Z10, Z11, K2, Z7

  VCVTPD2UQQ.RZ_SAE Z6, Z6
  VCVTPD2UQQ.RZ_SAE Z7, Z7

  // Z8/Z9 <- Clamped precision.
  VPXORQ X10, X10, X10
  VPBROADCASTQ CONST_GEO_TILE_MAX_PRECISION(), Z11
  VPMAXSQ Z10, Z8, Z8
  VPMAXSQ Z10, Z9, Z9
  VPMINSQ Z11, Z8, Z8
  VPMINSQ Z11, Z9, Z9

  VPBROADCASTQ CONSTQ_0x0000FFFFFFFFFFFF(), Z10
  VPBROADCASTQ CONSTQ_48(), Z11
  VPMINSQ Z10, Z4, Z4
  VPMINSQ Z10, Z5, Z5

  // Z8/Z9 <- How many bits to shift X and Y to get the desired precision.
  VPSUBQ Z8, Z11, Z10
  VPSUBQ Z9, Z11, Z11

  // Z4/Z5 <- Y bits.
  // Z6/Z7 <- X bits.
  VPSRLVQ Z10, Z4, Z4
  VPSRLVQ Z11, Z5, Z5
  VPSRLVQ Z10, Z6, Z6
  VPSRLVQ Z11, Z7, Z7

  // We have two 32-bit numbers in Z4/Z5 and Z6/Z7 representing Y/X tiles. We can
  // use the same approach as we use in 'i64tostr' instruction, however, it's
  // slightly different as we need to stringify only 32-bit unsigned numbers (so
  // no sign handling, for example) and one value representing the precision,
  // which only requires 1-2 digits.
  //
  // Stringifying a 32-bit number can be split into the following
  //
  //   - stringify low 8 characters
  //   - stringify high 2 characters.
  //
  // This means that we need four registers to strinfigy low 8-char X/Y tiles,
  // and two registers to strinfigy the rest, which represents 2 high characters
  // of latitude and longitude, and 2 characters of precision. We do a bit of
  // shuffling to actually only need one register pair to stringify the remaining
  // 2 high characters of latitude and longitude, and also the whole precision as
  // it's guaranteed to be less than 100.

  VPBROADCASTQ CONSTQ_1441151881(), Z17
  VPBROADCASTQ CONSTQ_100000000(), Z13

  VPMULUDQ Z17, Z4, Z14
  VPMULUDQ Z17, Z5, Z15
  VPMULUDQ Z17, Z6, Z16
  VPMULUDQ Z17, Z7, Z17

  // Z14/Z15 - Y / 100000000 (high 2 chars)
  // Z16/Z17 - X / 100000000 (high 2 chars)
  VPSRLQ $57, Z14, Z14
  VPSRLQ $57, Z15, Z15
  VPSRLQ $57, Z16, Z16
  VPSRLQ $57, Z17, Z17

  VPMULUDQ Z13, Z14, Z10
  VPMULUDQ Z13, Z15, Z11
  VPMULUDQ Z13, Z16, Z12
  VPMULUDQ Z13, Z17, Z13

  // Z4/Z5 - Y % 100000000 (low 8 chars)
  // Z6/Z7 - X % 100000000 (low 8 chars)
  VPSUBQ Z10, Z4, Z4
  VPSUBQ Z11, Z5, Z5
  VPSUBQ Z12, Z6, Z6
  VPSUBQ Z13, Z7, Z7

  // Z14/Z15 <- [0][Z][X][Y]
  VPSLLQ $16, Z16, Z16
  VPSLLQ $16, Z17, Z17
  VPSLLQ $32, Z8, Z8
  VPSLLQ $32, Z9, Z9

  VPTERNLOGQ $0xFE, Z16, Z14, Z8 // Z14 = Z14 | Z16 | Z8
  VPTERNLOGQ $0xFE, Z17, Z15, Z9 // Z15 = Z15 | Z17 | Z9

  // Stringify
  // ---------

  BC_UINT_TO_STR_STEP_10000_PREPARE(OUT(Z26), OUT(Z27))
  BC_UINT_TO_STR_STEP_10000_4X(IN_OUT(Z4), IN_OUT(Z5), IN_OUT(Z6), IN_OUT(Z7), IN(Z26), IN(Z27), CLOBBER(Z22), CLOBBER(Z23), CLOBBER(Z24), CLOBBER(Z25))

  BC_UINT_TO_STR_STEP_100_PREPARE(OUT(Z26), OUT(Z27))
  BC_UINT_TO_STR_STEP_100_4X(IN_OUT(Z4), IN_OUT(Z5), IN_OUT(Z6), IN_OUT(Z7), IN(Z26), IN(Z27), CLOBBER(Z22), CLOBBER(Z23), CLOBBER(Z24), CLOBBER(Z25))

  BC_UINT_TO_STR_STEP_10_PREPARE(OUT(Z26), OUT(Z27))
  BC_UINT_TO_STR_STEP_10_6X(IN_OUT(Z4), IN_OUT(Z5), IN_OUT(Z6), IN_OUT(Z7), IN_OUT(Z8), IN_OUT(Z9), IN(Z26), IN(Z27), CLOBBER(Z22), CLOBBER(Z23), CLOBBER(Z24), CLOBBER(Z25))

  // Prepare Outputs
  // ---------------

  VPBROADCASTQ R15, Z21
  VPBROADCASTQ CONSTQ_64(), Z24
  VPBROADCASTD CONSTD_7(), Z25
  VPSLLQ.BCST $56, CONSTQ_1(), Z26

  VPADDQ.Z CONST_GET_PTR(consts_offsets_q_32, 8), Z21, K1, Z20
  VPADDQ.Z CONST_GET_PTR(consts_offsets_q_32, 8+64), Z21, K2, Z21

  // Prepend "/Y"
  // ------------

  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z27
  VPSHUFB Z27, Z4, Z10
  VPSHUFB Z27, Z5, Z11
  VPORQ Z24, Z10, Z10
  VPORQ Z24, Z11, Z11

  VBROADCASTI32X4 CONST_GET_PTR(const_geotilees_extract_u16_bswap, 0), Z13
  VPSHUFB Z13, Z8, Z12
  VPSHUFB Z13, Z9, Z13

  VPLZCNTQ Z12, Z12
  VPLZCNTQ Z13, Z13

  VPCMPEQQ Z24, Z12, K3
  VPCMPEQQ Z24, Z13, K4

  VPLZCNTQ.Z Z10, K3, Z10
  VPLZCNTQ.Z Z11, K4, Z11

  VPANDNQ Z10, Z25, Z10
  VPANDNQ Z11, Z25, Z11
  VPANDNQ Z12, Z25, Z12
  VPANDNQ Z13, Z25, Z13

  VPSUBQ Z10, Z24, Z14
  VPSUBQ Z11, Z24, Z15
  VPSUBQ Z12, Z24, Z16
  VPSUBQ Z13, Z24, Z17

  VPSRLVQ Z14, Z26, Z10
  VPSRLVQ Z15, Z26, Z11
  VPSRLVQ Z16, Z26, Z12
  VPSRLVQ Z17, Z26, Z13

  VPADDQ Z14, Z16, Z14
  VPADDQ Z15, Z17, Z15
  VPSRLQ $3, Z14, Z14
  VPSRLQ $3, Z15, Z15
  VPADDQ.BCST CONSTQ_1(), Z14, Z22
  VPADDQ.BCST CONSTQ_1(), Z15, Z23

  VBROADCASTI32X4 CONST_GET_PTR(const_geotilees_extract_u16, 0), Z15
  VPSHUFB Z15, Z8, Z14
  VPSHUFB Z15, Z9, Z15

  VPSUBB Z10, Z4, Z4
  VPSUBB Z11, Z5, Z5
  VPSUBB Z12, Z14, Z12
  VPSUBB Z13, Z15, Z13

  VPBROADCASTB CONSTD_48(), Z27
  VPADDB Z27, Z4, Z4
  VPADDB Z27, Z5, Z5
  KMOVB K1, K3
  KMOVB K2, K4
  VPSCATTERQQ Z4, K3, -8(VIRT_BASE)(Z20*1)
  VPSCATTERQQ Z5, K4, -8(VIRT_BASE)(Z21*1)

  VPADDB Z27, Z12, Z12
  VPADDB Z27, Z13, Z13
  KMOVB K1, K3
  KMOVB K2, K4
  VPSCATTERQQ Z12, K3, -16(VIRT_BASE)(Z20*1)
  VPSCATTERQQ Z13, K4, -16(VIRT_BASE)(Z21*1)

  VPSUBQ Z22, Z20, Z20
  VPSUBQ Z23, Z21, Z21

  // Prepend "/X"
  // ------------

  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z27
  VPSHUFB Z27, Z6, Z10
  VPSHUFB Z27, Z7, Z11
  VPORQ Z24, Z10, Z10
  VPORQ Z24, Z11, Z11

  VBROADCASTI32X4 CONST_GET_PTR(const_geotilees_extract_u16_bswap, 16), Z13
  VPSHUFB Z13, Z8, Z12
  VPSHUFB Z13, Z9, Z13

  VPLZCNTQ Z12, Z12
  VPLZCNTQ Z13, Z13

  VPCMPEQQ Z24, Z12, K3
  VPCMPEQQ Z24, Z13, K4

  VPLZCNTQ.Z Z10, K3, Z10
  VPLZCNTQ.Z Z11, K4, Z11

  VPANDNQ Z10, Z25, Z10
  VPANDNQ Z11, Z25, Z11
  VPANDNQ Z12, Z25, Z12
  VPANDNQ Z13, Z25, Z13

  VPSUBQ Z10, Z24, Z14
  VPSUBQ Z11, Z24, Z15
  VPSUBQ Z12, Z24, Z16
  VPSUBQ Z13, Z24, Z17

  VPSRLVQ Z14, Z26, Z10
  VPSRLVQ Z15, Z26, Z11
  VPSRLVQ Z16, Z26, Z12
  VPSRLVQ Z17, Z26, Z13

  VPADDQ Z14, Z16, Z14
  VPADDQ Z15, Z17, Z15
  VPSRLQ $3, Z14, Z14
  VPSRLQ $3, Z15, Z15
  VPADDQ.BCST CONSTQ_1(), Z14, Z22
  VPADDQ.BCST CONSTQ_1(), Z15, Z23

  VBROADCASTI32X4 CONST_GET_PTR(const_geotilees_extract_u16, 16), Z15
  VPSHUFB Z15, Z8, Z14
  VPSHUFB Z15, Z9, Z15

  VPSUBB Z10, Z6, Z6
  VPSUBB Z11, Z7, Z7
  VPSUBB Z12, Z14, Z12
  VPSUBB Z13, Z15, Z13

  VPBROADCASTB CONSTD_48(), Z27
  VPADDB Z27, Z6, Z6
  VPADDB Z27, Z7, Z7
  KMOVB K1, K3
  KMOVB K2, K4
  VPSCATTERQQ Z6, K3, -8(VIRT_BASE)(Z20*1)
  VPSCATTERQQ Z7, K4, -8(VIRT_BASE)(Z21*1)

  VPADDB Z27, Z12, Z12
  VPADDB Z27, Z13, Z13
  KMOVB K1, K3
  KMOVB K2, K4
  VPSCATTERQQ Z12, K3, -16(VIRT_BASE)(Z20*1)
  VPSCATTERQQ Z13, K4, -16(VIRT_BASE)(Z21*1)

  VPSUBQ Z22, Z20, Z20
  VPSUBQ Z23, Z21, Z21

  // Prepend "/Z"
  // ------------

  VBROADCASTI32X4 CONST_GET_PTR(const_geotilees_extract_u16_bswap, 32), Z13
  VPSHUFB Z13, Z8, Z12
  VPSHUFB Z13, Z9, Z13
  VPORQ Z24, Z12, Z12
  VPORQ Z24, Z13, Z13

  VPLZCNTQ Z12, Z12
  VPLZCNTQ Z13, Z13

  VPANDNQ Z12, Z25, Z12
  VPANDNQ Z13, Z25, Z13

  VPSUBQ Z12, Z24, Z16
  VPSUBQ Z13, Z24, Z17

  VPSRLVQ Z16, Z26, Z12
  VPSRLVQ Z17, Z26, Z13

  VPSRLQ $3, Z16, Z22
  VPSRLQ $3, Z17, Z23

  VPSLLQ $16, Z8, Z12
  VPSLLQ $16, Z9, Z13

  VPBROADCASTB CONSTD_48(), Z27
  VPADDB Z27, Z12, Z12
  VPADDB Z27, Z13, Z13
  KMOVB K1, K3
  KMOVB K2, K4
  VPSCATTERQQ Z12, K3, -8(VIRT_BASE)(Z20*1)
  VPSCATTERQQ Z13, K4, -8(VIRT_BASE)(Z21*1)

  VPSUBQ Z22, Z20, Z20
  VPSUBQ Z23, Z21, Z21

  // Finalize
  // --------

  // This calculates the length of each output string based on the current indexes
  // in Z20/Z21 by subtracting them from the initial state (the end of each string).
  VPMOVQD Z20, Y20
  VPMOVQD Z21, Y21
  VINSERTI32X8 $1, Y21, Z20, Z20
  VPBROADCASTD R15, K1, Z3

  VMOVDQA32.Z Z20, K1, Z2
  VPADDD CONST_GET_PTR(consts_offsets_d_32, 4), Z3, K1, Z3
  VPSUBD.Z Z2, Z3, K1, Z3

  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(0)

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// f64[0].k[1] = geodistance(f64[2], f64[3], f64[4], f64[5]).k[6]
TEXT bcgeodistance(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_5xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(DX), OUT(R15), OUT(R8))
  BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_F64_FROM_SLOT(OUT(Z4), OUT(Z5), IN(R15))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  // Z4/Z5 <- Lon2 - Lon1
  VSUBPD 0(VIRT_VALUES)(CX*1), Z4, Z4
  VSUBPD 64(VIRT_VALUES)(CX*1), Z5, Z5

  // Z6/Z7 <- Lat2
  VBROADCASTSD CONSTF64_PI_DIV_180(), Z10
  VBROADCASTSD CONSTF64_HALF(), Z11
  BC_LOAD_F64_FROM_SLOT(OUT(Z6), OUT(Z7), IN(DX))

  // Z4/Z5 <- RADIANS(Lon2 - Lon1)
  VMULPD Z10, Z4, Z4
  VMULPD Z10, Z5, Z5

  // Z2/Z3 <- RADIANS(Lat1)
  VMULPD Z10, Z2, K1, Z2
  VMULPD Z10, Z3, K2, Z3

  // Z6/Z7 <- RADIANS(Lat2)
  VMULPD Z10, Z6, Z6
  VMULPD Z10, Z7, Z7

  // Z8/Z9 <- RADIANS(Lat2 - Lat1)
  VSUBPD Z2, Z6, Z8
  VSUBPD Z3, Z7, Z9

  // Z4/Z5 <- SIN(RADIANS(Lon2 - Lon1) / 2)
  // Z10/Z11 <- SIN(RADIANS(Lat2 - Lat1) / 2)
  VMULPD Z11, Z4, Z4
  VMULPD Z11, Z5, Z5
  VMULPD Z11, Z8, Z8
  VMULPD Z11, Z9, Z9
  BC_FAST_SIN_4ULP(OUT(Z4), OUT(Z5), IN(Z4), IN(Z5))
  BC_FAST_SIN_4ULP(OUT(Z10), OUT(Z11), IN(Z8), IN(Z9))

  // Z8/Z9 <- COS(RADIANS(Lat1))
  // Z6/Z7 <- COS(RADIANS(Lat2))
  BC_FAST_COS_4ULP(OUT(Z8), OUT(Z9), IN(Z2), IN(Z3))
  BC_FAST_COS_4ULP(OUT(Z6), OUT(Z7), IN(Z6), IN(Z7))

  // Z6/Z7 <- COS(RADIANS(Lat1)) * COS(RADIANS(Lat2))
  VMULPD Z8, Z6, Z6
  VMULPD Z9, Z7, Z7

  // Z4/Z5 <- SIN^2(RADIANS(Lon2 - Lon1) / 2)
  VMULPD Z4, Z4, Z4
  VMULPD Z5, Z5, Z5

  // Z4/Z5 <- COS(RADIANS(Lat1)) * COS(RADIANS(Lat2)) * SIN^2(RADIANS(Lon2 - Lon1) / 2)
  VMULPD Z6, Z4, Z4
  VMULPD Z7, Z5, Z5

  // Z4/Z5 <- Q == SIN^2(RADIANS(Lat2 - Lat1) / 2) + COS(RADIANS(Lat1)) * COS(RADIANS(Lat2)) * SIN^2(RADIANS(Lon2 - Lon1) / 2)
  VBROADCASTSD CONSTF64_1(), Z7
  VFMADD231PD Z10, Z10, Z4 // Z4 = (Z10 * Z10) + Z4
  VFMADD231PD Z11, Z11, Z5 // Z5 = (Z11 * Z11) + Z5

  // Z4/Z5 <- ASIN(SQRT(Q))
  VSQRTPD Z4, Z8
  VSQRTPD Z5, Z9
  BC_FAST_ASIN_4ULP(OUT(Z4), OUT(Z5), IN(Z8), IN(Z9))

  VBROADCASTSD CONSTF64_12742000(), Z10
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))

  VMULPD.Z Z10, Z4, K1, Z2
  VMULPD.Z Z10, Z5, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*7)


// Alloc
// -----

// slice[0].k[1] = alloc(i64[2]).k[3]
//
// scratch: PageSize
//
// Allocate a data slice, which can be used for any purpose, the length is described by INT64 elements
TEXT bcalloc(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  // NOTE: We want unsigned saturation here as too large objects would end up with 0xFFFFFFFF length, which is UINT32_MAX.
  VPMOVUSQD Z2, Y4
  VPMOVUSQD Z3, Y5
  VINSERTI32X8 $1, Y5, Z4, Z4

  // R15 (DstSum), Z5 (DstOff), Z7 (DstLen), Z4 (DstEnd), K1 (DstMask)
  BC_HORIZONTAL_LENGTH_SUM(OUT(R15), OUT(Z5), OUT(Z7), OUT(Z4), OUT(K1), IN(Z4), IN(K1), X10, K2)

  BC_ALLOC_SLICE(OUT(Z2), IN(R15), CX, R8)             // Z2 <- Offset of the beginning of the allocated buffer
  VPADDD.Z Z5, Z2, K1, Z2                              // Z2 <- Offsets of each allocated object
  VPXORD X3, X3, X3                                    // Z3 <- Length of each allocated object, initially zero

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

  _BC_ERROR_HANDLER_MORE_SCRATCH()

// String - Concat
// ---------------

// slice[0].k[1] = concatstr(varargs(str[0].k[1]))
//
// scratch: PageSize
TEXT bcconcatstr(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_RU32(BC_SLOT_SIZE*2, OUT(CX))              // CX <- number of variable arguments
  ADDQ $(BC_SLOT_SIZE*2 + 4), VIRT_PCREG               // VIRT_PCREG <- the current va base

  // Calculate Length
  // ----------------

  VPXORQ X2, X2, X2                                    // Z2 <- Concat length (low)
  VPXORQ X3, X3, X3                                    // Z3 <- Concat length (high)
  KXORW K1, K1, K1                                     // K1 <- Disable all lanes if no arguments were given
  VMOVQ VIRT_PCREG, X11                                // X11 <- spilled VIRT_PCREG, the current va base
  MOVL $0xFFFF, R15

  TESTL CX, CX                                         // PARANOIA:
  JZ done                                              // No arguments shouldn't happen, but let's make it safe if it does...

  KMOVW R15, K1                                        // K1 <- All lanes by default, result calculated as AND of all masks

va_len_iter:
  BC_UNPACK_2xSLOT(0, OUT(BX), OUT(R8))                // BX <- slice slot; R8 <- predicate slot
  ADDQ $(BC_SLOT_SIZE*2), VIRT_PCREG                   // VIRT_PCREG <- advance this slot pair

  VPMOVZXDQ 64(VIRT_VALUES)(BX*1), Z4
  VPMOVZXDQ 96(VIRT_VALUES)(BX*1), Z5
  BC_LOAD_K1_FROM_SLOT(OUT(K2), IN(R8))

  VPADDQ Z4, Z2, Z2
  VPADDQ Z5, Z3, Z3
  KANDW K2, K1, K1

  SUBL $1, CX
  JNE va_len_iter

  KSHIFTRW $8, K1, K2
  VMOVQ X11, VIRT_PCREG                                // VIRT_PCREG <- rewind va base as we need to iterate it once more

  // Allocate Scratch
  // ----------------

  VPMOVUSQD.Z Z2, K1, Y4
  VPMOVUSQD.Z Z3, K2, Y5
  BC_UNPACK_RU32(-4, OUT(CX))                          // CX <- number of variable arguments (we know it's non-zero if we are here)
  VINSERTI32X8 $1, Y5, Z4, Z3                          // Z3 <- the final length of all active lanes as 32-bit units (saturated)

  // R15 (DstSum), Z5 (DstOff), Z7 (DstLen), Z4 (DstEnd), K1 (DstMask)
  BC_HORIZONTAL_LENGTH_SUM(OUT(R15), OUT(Z5), OUT(Z7), OUT(Z4), OUT(K1), IN(Z3), IN(K1), X9, K2)

  BC_ALLOC_SLICE(OUT(Z2), IN(R15), BX, R8)             // Z2 <- Offset of the beginning of the allocated buffer
  VPADDD.Z Z5, Z2, K1, Z2                              // Z2 <- Offsets of each allocated object
  VMOVDQA32 Z2, Z6

  // Concatenate Strings
  // -------------------

va_copy_next:
  TESTL CX, CX
  JZ done

va_copy_iter:
  BC_UNPACK_SLOT(0, OUT(BX))
  ADDQ VIRT_VALUES, BX                                 // BX <- absolute address of the slice stack-slot to be appended
  ADDQ $(BC_SLOT_SIZE*2), VIRT_PCREG

  VMOVDQU32.Z 64(BX*1), K1, Z5                         // Z5 <- lengths of all slices to be appended (zero for inactive)
  VPTESTMD Z5, Z5, K1, K2                              // K2 <- mask of all slices to be appended (non-zero length)

  VMOVDQU32 Z6, bytecode_spillArea(VIRT_BCPTR)         // [] <- Save the current end index of each string where content will be copied
  VPADDD Z5, Z6, Z6                                    // Z6 <- End index of each output string including current slices

  KMOVW K2, R8                                         // R8 <- mask of all lanes to be appended having non-zero length
  SUBL $1, CX

  TESTL R8, R8                                         // Go to the next vararg if there are no slices to append
  JZ va_copy_next

  VMOVQ CX, X12                                        // Spill CX (va counter)

lane_copy_iter:                                        // Iterate over the mask and append each string that has a content
  TZCNTL R8, R14                                       // R14 <- Index of the lane to process
  BLSRL R8, R8                                         // R8 <- Clear the index of the iterator

  MOVL 64(BX)(R14 * 4), CX                             // CX <- Input length
  MOVL bytecode_spillArea(VIRT_BCPTR)(R14 * 4), R15    // R15 <- Output index
  MOVL 0(BX)(R14 * 4), R14                             // R14 <- Input index
  ADDQ VIRT_BASE, R15                                  // R15 <- Make output address from output index
  ADDQ VIRT_BASE, R14                                  // R14 <- Make input address from input index

  SUBL $64, CX
  JCS lane_64b_tail

  // Main copy loop that processes 64 bytes at once
lane_64b_iter:
  VMOVDQU8 0(R14), Z7
  ADDQ $64, R14
  VMOVDQU8 Z7, 0(R15)
  ADDQ $64, R15

  SUBL $64, CX
  JCC lane_64b_iter

lane_64b_tail:
  // NOTE: The following line makes sense, but it's not needed. In C it would
  // be undefined behavior to shift with anything outside of [0, 63], but we
  // know that X86 only uses 6 bits in our case (64-bit shift), which would
  // not be changed by adding 64 as it has those 6 bits zero.
  // ADDL $64, CX

  MOVQ $-1, DX
  SHLQ CL, DX
  NOTQ DX
  KMOVQ DX, K2

  VMOVDQU8.Z 0(R14), K2, Z7
  VMOVDQU8 Z7, K2, 0(R15)

  TESTL R8, R8
  JNE lane_copy_iter

  VMOVQ X12, CX                                        // Reload CX (va counter)
  TESTL CX, CX
  JNE va_copy_iter

done:
  VMOVQ X11, BX                                        // BX <- Get the original va base and use it to load output slots
  BC_MOV_SLOT (-BC_SLOT_SIZE*2 - 4)(BX), DX            // DX <- Load the output slice slot
  BC_MOV_SLOT (-BC_SLOT_SIZE*1 - 4)(BX), BX            // BX <- Load the output predicate slot

  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))       // Store the output slice
  BC_STORE_K_TO_SLOT(IN(K1), IN(BX))                   // Store the output predicate
  NEXT_ADVANCE(0)

  _BC_ERROR_HANDLER_MORE_SCRATCH()


// Find Symbol Instructions
// ------------------------

// v[0].k[1] = findsym(b[2], symbol[3]).k[4]
TEXT bcfindsym(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_ZI32(BC_SLOT_SIZE*3, OUT(Z27))             // Z27 <- encoded symbol to match

  VPXORD X2, X2, X2                                    // Z2 <- zeroed, preparation for VPGATHERDD
  VMOVDQU32 0(VIRT_VALUES)(BX*1), Z28                  // Z28 <- struct offsets / initial offsets

  BC_UNPACK_SLOT(BC_SLOT_SIZE*3 + 4, OUT(R8))
  VPADDD 64(VIRT_VALUES)(BX*1), Z28, Z29               // Z29 <- end of struct
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))                // K1 <- struct predicate
  VPCMPUD $VPCMP_IMM_LT, Z29, Z28, K1, K5              // K5 <- lanes to scan

  VMOVDQA32.Z Z28, K1, Z30                             // Z30 <- output offsets
  VPXORD X31, X31, X31                                 // Z31 <- output lengths

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  ADDQ $(BC_SLOT_SIZE*4 + 4), VIRT_PCREG
  JMP findsym_tail(SB)

// v[0].k[1] = findsym2(b[2], v[3], k[4], symbol[5]).k[6]
TEXT bcfindsym2(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_ZI32(BC_SLOT_SIZE*5, OUT(Z27))             // Z27 <- encoded symbol to match

  VPXORD X2, X2, X2                                    // Z2 <- zeroed, preparation for VPGATHERDD
  VMOVDQU32 0(VIRT_VALUES)(BX*1), Z3                   // Z3 <- struct offsets

  BC_UNPACK_SLOT(BC_SLOT_SIZE*5 + 4, OUT(R8))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3, OUT(CX));
  VPADDD 64(VIRT_VALUES)(BX*1), Z3, Z29                // Z29 <- end of struct

  VMOVDQU32 0(VIRT_VALUES)(CX*1), Z30                  // Z30 <- previous match offsets (or initial base)
  VMOVDQU32 64(VIRT_VALUES)(CX*1), Z31                 // Z31 <- previous match lengths (or zero if no match)

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))                // K1 <- struct predicate
  VPADDD Z30, Z31, K1, Z28                             // Z28 <- struct offsets (based on previous match)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VPCMPUD $VPCMP_IMM_LT, Z29, Z28, K1, K5              // K5 <- lanes to scan

  ADDQ $(BC_SLOT_SIZE*6 + 4), VIRT_PCREG
  JMP findsym_tail(SB)

// inputs:
//   DX  <- output value slot
//   R8  <- output mask slot
//   K5  <- active lanes
//   Z27 <- encoded symbol IDs to match
//   Z28 <- current symbol+value offset
//   Z29 <- end of struct
//   Z30 <- previous match offsets (or initial base)
//   Z31 <- previous match lengths (or zero if no match)
//
// outputs:
//   Stack[DX] = value (offset:length:TypeL:hLen), where offset means symbol >= search regardless of a match
//   Stack[R8] = lanes matched predicate (non-matched lanes can still have offset:length values initialized!)

TEXT findsym_tail(SB), NOSPLIT|NOFRAME, $0
  KTESTW K5, K5
  KMOVW K5, K6                                         // K6 <- remaining lanes to match
  VPBROADCASTB CONSTD_0x80(), Z22                      // Z22 <- dword(0x80808080)
  JZ no_symbols

  VPGATHERDD 0(VIRT_BASE)(Z28*1), K5, Z2               // Z2 <- gather bytes for the first iteration here...

  VPSRLD $31, Z22, Z21                                 // Z21 <- dword(1)
  VPSRLD $24, Z22, Z14                                 // Z14 <- dword(0x80)
  VPXORD X26, X26, X26                                 // Z26 <- matched symbol IDs (initially no matched symbols)
  VPSRLD $(24+4), Z22, Z19                             // Z19 <- dword(8)
  VPSUBD Z21, Z14, Z15                                 // Z15 <- dword(0x7F)
  VPADDD Z21, Z21, Z20                                 // Z20 <- dword(2)
  VPSRLD $3, Z15, Z13                                  // Z13 <- dword(0xF)
  VPORD Z15, Z14, Z16                                  // Z16 <- dword(0xFF)
  VPSRLD $(24+2), Z22, Z18                             // Z18 <- dword(32)
  VPXORD Z21, Z13, Z17                                 // Z17 <- dword(14)

  VBROADCASTI32X4 CONST_GET_PTR(bswap32, 0), Z23       // Z23 <- bswap32 predicate for VPSHUFB
  KMOVQ CONSTQ_0x5555555555555555(), K3                // K3 <- predicate for VPADDB (VarUInt decoding)

  VPLZCNTD Z27, Z10
  VPSLLVD Z10, Z27, Z27
  VPSHUFB Z23, Z27, Z27
  VPXORD X10, X10, X10                                 // Z10 <- matched value header lengths
  VPXORD X11, X11, X11                                 // Z11 <- matched value Type|L bytes

  JMP decode_init

decode_loop:
  KMOVW K5, K6                                         // K6 <- remaining lanes to scan
  VPGATHERDD 0(VIRT_BASE)(Z28*1), K5, Z2               // Z2 <- gathered bytes

decode_init:
  VMOVDQU32 Z21, K6, Z10                               // Z10 <- initially set all hLens of active lanes to 1 (we will add Length size later)
  VPTESTMD Z14, Z2, K6, K4                             // K4 <- active lanes having 1-byte SymbolID
  KTESTW K6, K4                                        // CF <- cleared if not all lanes have 1-byte SymbolID

  VPSRLW.Z $8, Z2, K3, Z6                              // Z6 <- Type|L byte that is valid for lanes that have 1-byte SymbolID
  JCC decode_long_symbol                               // jump to a more complex decode if at least one lane has long SymbolID

  VPCMPUD $VPCMP_IMM_GE, Z18, Z6, K6, K5               // K5 <- Type != NULL|BOOL (Type|L >= 32)
  VPANDD.Z Z13, Z6, K5, Z7                             // Z7 <- L field extracted from Type|L and corrected to 0 if NULL/BOOL
  VPANDD Z16, Z2, K6, Z26                              // Z26 <- update encoded SymbolIDs
  VPCMPEQD Z17, Z7, K6, K5                             // K5 <- lanes that need a separate Length data when L == 14
  VPSRLD $16, Z2, K5, Z7                               // Z7 <- L field or 1-byte Length data
  VPCMPUD $VPCMP_IMM_LE, Z27, Z26, K6, K1              // K1 <- lanes where SymbolID <= SymbolIDToMatch
  VPTESTMD Z14, Z7, K5, K4                             // K4 <- lanes that use a separate Length data represented as 1 byte VarUInt
  KTESTW K5, K4                                        // CF <- cleared if there is a lane having incomplete Length data (need more bytes)

  VPSRLD $8, Z2, K6, Z11                               // Z11 <- shift Type|L byte to start at LSB
  VPCMPUD $VPCMP_IMM_LT, Z27, Z26, K6, K2              // K2 <- remaining lanes where SymbolID < SymbolIDToMatch
  VPADDD Z21, Z28, K1, Z30                             // Z30 <- Z28 + len(symbol) (next value offset)
  VPADDD Z21, Z10, K5, Z10                             // Z10 <- updated to either have 1 or 2 bytes that represent Type|L + optional Length
  VPANDD Z7, Z15, Z7                                   // Z7 <- L field or decoded Length data
  VPADDD Z10, Z7, K1, Z31                              // Z31 <- 1 + Length data size + length
  JCC decode_long_length_of_1_byte_symbols             // jump to a more complex decode if the Length data uses 2 or more bytes

decode_advance:
  VPADDD.Z Z30, Z31, K6, Z28                           // Z28 <- update offsets for remaining lanes
  VPCMPUD $VPCMP_IMM_LT, Z29, Z28, K2, K5              // K5 <- remaining lanes to scan considering both symbol ids and end of struct
  KTESTW K5, K5
  JNZ decode_loop                                      // all lanes done if K5 is zero...

finished:
  VPCMPEQD Z27, Z26, K1

  // NOTE: The following 2 lines are not necessary as we are clearing a content that should
  // always be ignored, however, if we don't clear these lines then findsym("b") could output
  // different metadata compared to findsym("a") followed by findsym2("b"), which is something
  // that we verify doesn't happen. Thus, only output TLV and hLen of matched keys, and clear
  // the rest.
  VMOVDQA32.Z Z10, K1, Z10
  VMOVDQA32.Z Z11, K1, Z11

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z11), IN(Z10), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(0)

decode_long_length_of_1_byte_symbols:
  KANDNW K5, K4, K4                                    // K4 <- lanes that need more than 1-byte Length
  VPSRLD $24, Z2, Z3                                   // Z3 <- second Length byte at LSB
  VPTESTNMD Z22, Z3, K4, K1                            // K1 <- lanes that need more than 2-byte Length
  KTESTW K1, K1

  VPANDD Z15, Z3, Z3                                   // Z3 <- Z3 & 0x7F
  JNZ decode_long_length_needs_gather                  // jump to a more complex decode if gather is needed to fetch Length bytes

  VPADDD Z21, Z10, K4, Z10                             // Z10 <- Either 1, 2, or 3 depending on the size of the Length field (includes Type|L)
  VPSLLD $7, Z7, K4, Z7                                // Z7 <- Z7 << 7 (only lanes that use 2-byte length)
  VPORD Z3, Z7, K4, Z7                                 // Z7 <- Either L or a decoded 1-byte or 2-byte length
  VPADDD Z10, Z7, K4, Z31                              // Z31 <- 1 + Length data size + length
  JMP decode_advance

decode_long_symbol:
  VPSHUFB Z23, Z2, Z3                                  // Z3 <- bswap32(bytes)
  VPANDD Z22, Z3, Z4                                   // Z4 <- bswap32(bytes) & 0x80808080
  VPLZCNTD Z4, Z4                                      // Z4 <- lzcnt(bswap32(bytes) & 0x80808080)
  VPADDD.Z Z19, Z4, K6, Z4                             // Z4 <- lzcnt(bswap32(bytes) & 0x80808080) + 8
  VPSLLVD Z4, Z3, Z5                                   // Z5 <- Type|L byte at MSB, preceded by up to 2 non-zero bytes
  VPSUBD.Z Z4, Z18, K6, Z8                             // Z8 <- 32 - lzcnt(bswap32(bytes) & 0x80808080) - 8
  VPSRLD $24, Z5, K6, Z11                              // Z11 <- Type|L byte
  VPCMPUD $VPCMP_IMM_GE, Z18, Z11, K6, K5              // K5 <- Type != NULL|BOOL (Type|L >= 32)

  VPSRLVD Z8, Z3, K6, Z26                              // Z26 <- update encoded symbol ids
  VPANDD.Z Z13, Z11, K5, Z7                            // Z7 <- L field extracted from Type|L and corrected to 0 if NULL/BOOL
  VPCMPEQD Z17, Z7, K6, K5                             // K5 <- lanes that need a separate Length field when L == 14
  KTESTW K5, K5

  VPCMPUD $VPCMP_IMM_LE, Z27, Z26, K6, K1              // K1 <- lanes where SymbolID <= SymbolIDToMatch
  VPSRLD $3, Z4, Z6                                    // Z6 <- len(SymbolID)
  VPCMPUD $VPCMP_IMM_LT, Z27, Z26, K6, K2              // K2 <- remaining lanes where SymbolID < SymbolIDToMatch
  VPADDD Z21, Z7, K1, Z31                              // Z31 <- 1 + L (accounts Type|L byte + L)
  VPADDD Z6, Z28, K1, Z30                              // Z30 <- Z28 + len(symbol) (next value offset)
  JZ decode_advance                                    // done decoding if all values need only L (length < 14)

  VPSLLD.Z $8, Z5, K5, Z3                              // Z3 <- Length bytes, starting at MSB
  VPTESTNMD Z22, Z3, K5, K4                            // K4 <- lanes that need more Length bytes than we have at the moment
  KTESTW K4, K4
  JNZ decode_long_length_needs_gather                  // jump to a slow path where additional gather is needed to fetch Length field

  // The following code handles either 1 byte symbol and
  // 1-2 byte length or 2 byte symbol and 1 byte length.

  VPMOVD2M Z3, K4                                      // K4 <- termination bit at 0x80000000 (at MSB) => Length is 1 byte long
  VPANDND Z3, Z22, Z4                                  // Z4 <- Z3 & 0x7F7F7F7F
  VPADDB Z3, Z3, K3, Z3                                // Z3 <- shift all EVEN bytes left by 1 to get [0BBBBBBB|AAAAAAA0|00000000|00000000]
  VPSRLD $6, Z16, K5, Z10                              // Z10 <- either 1 (K5 not set) or 3 (K5 set)
  VPSRLD $17, Z3, Z3                                   // Z3 <- decoded 2 byte VarUInt Length yieding [00000000|00000000|00BBBBBB|BAAAAAAA]
  VPSUBD Z21, Z10, K4, Z10                             // Z10 <- 1 + Length field size itself (the final value is either 2 or 3)
  VPSRLD $24, Z4, K4, Z3                               // Z3 <- updated to decoded Length from either 1 or 2 bytes of data
  VPADDD Z10, Z3, K5, Z31                              // Z31 <- updates the length of the value: Type|L + Length field + Content length
  JMP decode_advance

decode_long_length_needs_gather:
  KMOVW K5, K4
  VPGATHERDD 1(VIRT_BASE)(Z30*1), K4, Z9

  MOVL $0x40000001, BX
  VPBROADCASTD BX, Z4                                  // Z4 <- constant(0x40000001 == (1 << 30) | 1)

  VPSHUFB Z23, Z9, Z9                                  // Z9 <- bswap32(Z9)
  VPANDD Z9, Z22, Z3                                   // Z3 <- bswap32(Z9) & 0x80808080
  VPANDND Z9, Z22, Z9                                  // Z9 <- bswap32(Z9) & 0x7F7F7F7F

  VPLZCNTD Z3, Z3                                      // Z3 <- lzcnt(bswap32(Z9) & 0x80808080)
  VPADDB Z9, Z9, K3, Z9                                // Z9 <- shift all EVEN bytes left by 1 to get    [0DDDDDDD|CCCCCCC0|0BBBBBBB|AAAAAAA0]

  VPADDD Z19, Z3, Z3                                   // Z3 <- lzcnt(bswap32(Z9) & 0x80808080) + 8
  VPSRLW $1, Z9, Z9                                    // Z6 <- shift WORD pairs right by 1 to get       [00DDDDDD|DCCCCCCC|00BBBBBB|BAAAAAAA]
  VPMADDWD Z4, Z9, Z9                                  // Z9 <- transform length bytes to content length [0000DDDD|DDDCCCCC|CCBBBBBB|BAAAAAAA]

  VPSUBD Z3, Z18, Z4                                   // Z4 <- 32 - lzcnt(bswap32(Z9) & 0x80808080) - 8
  VPSRLD $3, Z4, Z5                                    // Z5 <- (32 - lzcnt(bswap32(Z9) & 0x80808080) - 8) / 8
  VPADDD Z19, Z3, Z3                                   // Z3 <- lzcnt(bswap32(Z9)) + 16
  VPSUBD Z5, Z4, Z4
  VPSRLD $3, Z3, K5, Z10                               // Z10 <- size of the Length field in bytes, including Type|L byte
  VPSRLVD Z4, Z9, Z9                                   // Z9 <- value content length
  VPADDD Z10, Z9, K5, Z31                              // Z31 <- updated to 1 + length field size + content length for lanes having Length field
  JMP decode_advance

no_symbols:
  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z2), IN(Z2), IN(DX))
  MOVW $0, 0(VIRT_VALUES)(R8*1)
  NEXT_ADVANCE(0)


// Blend Instructions
// ------------------

// NOTE: Blend functions works in a way that the second value's lanes are blended with
// the first value lanes. So, for example when the signature of the blend function is
// the following:
//
//   x[0].k[1] = blend(x[2].k[3], x[4].k[5])
//
// The lanes in x[4] described by k[5] are blended with lanes in x[2].k[3] and the
// resulting mask k[1] is the combination of `k[3] | k[5]`

// v[0].k[1] = blend.v(v[2], k[3], v[4], k[5])
TEXT bcblendv(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(DX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(DX))

  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*4, OUT(CX), OUT(DX))
  KUNPCKWD K1, K1, K3

  BC_LOAD_K1_FROM_SLOT(OUT(K2), IN(DX))
  KUNPCKWD K2, K2, K4

  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1))
  VMOVDQU8.Z BC_VSTACK_PTR(BX, 128), K3, Y4
  KORW K1, K2, K1

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VMOVDQU32 BC_VSTACK_PTR(CX, 0), K2, Z2
  VMOVDQU32 BC_VSTACK_PTR(CX, 64), K2, Z3
  VMOVDQU8 BC_VSTACK_PTR(CX, 128), K4, Y4

  VMOVDQU32 Z2, BC_VSTACK_PTR(DX, 0)
  VMOVDQU32 Z3, BC_VSTACK_PTR(DX, 64)
  VMOVDQU8 Y4, BC_VSTACK_PTR(DX, 128)
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*6)

// f64[0].k[1] = blend.f64(f64[2], k[3], f64[4], k[5])
TEXT bcblendf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*4, OUT(CX), OUT(R15))
  BC_LOAD_K1_FROM_SLOT(OUT(K2), IN(R15))

  KORW K1, K2, K1
  KSHIFTRW $8, K2, K3
  KSHIFTRW $8, K1, K4
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1), IN(K4))

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VMOVUPD 0(VIRT_VALUES)(CX*1), K2, Z2
  VMOVUPD 64(VIRT_VALUES)(CX*1), K3, Z3

  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*6)

// Unboxing Instructions
// ---------------------

// slice[0].k[1] = unpack(v[2], imm16[3]).k[4]
//
// unpack string/array/timestamp to scalar slice
TEXT bcunpack(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3 + 2, OUT(R8))

  VPMOVZXBW BC_VSTACK_PTR(BX, vRegData_typeL), Y5 // Y5 <- TLV bytes as 16-bit words
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPSRLW $4, Y5, Y5                              // Y5 <- value tags as 16-bit words
  BC_UNPACK_ZI16(BC_SLOT_SIZE*3, OUT(Y6))

  VPCMPEQW Y6, Y5, K1, K1                        // K1 <- lanes matching the required tag
  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z6), IN(BX))  // Z6 <- header lengths

  VMOVDQU32 BC_VSTACK_PTR(BX, 64), Z3            // Z3 <- value lengths
  VPADDD.Z BC_VSTACK_PTR(BX, 0), Z6, K1, Z2      // Z2 <- slice offsets (value offset + header length)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VPSUBD.Z Z6, Z3, K1, Z3                        // Z3 <- slice lengths (value length - header length)

  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_IMM16_SIZE)

// v[0] = unsymbolize(v[1]).k[2]
//
// replaces symbol values in v[1] with string values and stores the output to v[0]
TEXT bcunsymbolize(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))

  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z2), IN(BX))

  VPBROADCASTD CONSTD_7(), Z20                   // Z20 <- dword(7)
  VPSRLD $4, Z2, Z4                              // Z4 <- value tag
  VPCMPEQD Z20, Z4, K1, K2                       // K2 <- values that are symbols
  KTESTW K2, K2                                  // check whether we actually have to unsymbolize

  BC_UNPACK_SLOT(0, OUT(DX))
  KMOVW K2, K3
  BC_LOAD_VALUE_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z3), IN(BX))
  JZ next

  VPGATHERDD 1(VIRT_BASE)(Z0*1), K3, Z6          // Z6 <- SymbolID bytes (without TLV, which was skipped)
  VPSUBD.Z Z1, Z3, K2, Z4                        // Z4 <- -(SymbolIDLength)
  MOVQ bytecode_symtab+0(VIRT_BCPTR), R8         // R8 <- symbol table base

  VBROADCASTI32X4 CONST_GET_PTR(bswap32, 0), Z16 // Z16 <- bswap32 predicate for VPSHUFB
  VPADDD.BCST CONSTD_4(), Z4, Z4                 // Z4 <- (4 - SymbolIDLength)
  VPSLLD $3, Z4, Z5                              // Z5 <- (4 - SymbolIDLength) << 3
  VPSHUFB Z16, Z6, Z6                            // Z6 <- bswap32(symbol bytes)
  VPSRLVD Z5, Z6, Z6                             // Z6 <- SymbolIDs

  VPCMPUD.BCST $VPCMP_IMM_LT, bytecode_symtab+8(VIRT_BCPTR), Z6, K2, K2
  KMOVB K2, K3                                   // K3 <- gather predicate (low)
  KSHIFTRW $8, K2, K4                            // K4 <- gather predicate (high)
  VEXTRACTI32X8 $1, Z6, Y5                       // Z5 <- SymbolIDs (high)
  VPGATHERDQ 0(R8)(Y6*8), K3, Z8                 // Z8 <- vmrefs (low)

  VPSRLD $2, Z20, Z21                            // Z21 <- dword(1)
  VPADDD Z20, Z20, Z22                           // Z22 <- dword(14)
  VPGATHERDQ 0(R8)(Y5*8), K4, Z9                 // Z9 <- vmrefs (high)

  BC_MERGE_VMREFS_TO_VALUE(IN_OUT(Z0), IN_OUT(Z1), IN(Z8), IN(Z9), IN(K2), Z10, Y10, Y11)
  BC_CALC_STRING_TLV_AND_HLEN(IN_OUT(Z2), IN_OUT(Z3), IN(Z1), IN(K2), IN(Z21), IN(Z22), Z10, K3)

next:
  BC_STORE_VALUE_TO_SLOT(IN(Z0), IN(Z1), IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0].k[1] = unbox.k@i64(v[2]).k[3]
//
// NOTE: This opcode was designed in a way to be followed by cvti64tok, because we
// don't have a way to describe a bool output combined with a predicate in our SSA.
TEXT bcunboxktoi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPCMPEQD.BCST CONSTD_TRUE_BYTE(), Z4, K1, K2        // K2 <- set to ONEs for TRUE values
  VPCMPEQD.BCST CONSTD_FALSE_BYTE(), Z4, K1, K1       // K1 <- set to ONEs for FALSE values

  KSHIFTRW $8, K2, K3
  VPBROADCASTQ CONSTQ_1(), Z4
  KORW K2, K1, K1                                     // K1 <- active lanes (values containing BOOLs)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VMOVDQA64.Z Z4, K2, Z2                              // Z2 <- bools converted to i64 (low)
  VMOVDQA64.Z Z4, K3, Z3                              // Z3 <- bools converted to i64 (high)

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = unbox.coerce.f64(v[2]).k[3]
TEXT bcunboxcoercef64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_ZMM_FROM_SLOT(OUT(Z30), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD CONSTD_2(), Z11                        // Z11 <- constant(2)
  VPSRLD $4, Z4, Z5                                   // Z5 <- ION type
  VPANDD.BCST CONSTD_0x0F(), Z4, Z6                   // Z6 <- L field
  VPSUBD Z11, Z5, Z4                                  // Z4 <- ION type - 2 (this saves us some instructions / constants)
  VPCMPUD $VPCMP_IMM_LE, Z11, Z4, K1, K1              // K1 <- mask of all lanes that contain either 0x2, 0x3, or 0x4 ION type

  KTESTW K1, K1
  VPSRLD $1, Z11, Z10                                 // Z10 <- constant(1)
  VPSLLD $2, Z11, Z12                                 // Z12 <- constant(8)

  VPXORQ X2, X2, X2                                   // Z2 <- zero all lanes in case there are no numbers (low)
  VPXORQ X3, X3, X3                                   // Z3 <- zero all lanes in case there are no numbers (high)
  JZ next                                             // Skip unboxing if there are no numbers

  VEXTRACTI32X8 $1, Z30, Y13
  KMOVB K1, K3
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K3, Z2              // Z2 <- number data (low)
  VPSUBD Z6, Z12, Z12                                 // Z12 <- (8 - L)
  KSHIFTRW $8, K1, K4
  VPGATHERDQ 1(VIRT_BASE)(Y13*1), K4, Z3              // Z3 <- number data (high)
  VPSLLD $3, Z12, Z12                                 // Z12 <- (8 - L) << 3

  VEXTRACTI32X8 $1, Z12, Y13
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z5
  VPMOVZXDQ Y12, Z12                                  // Z12 <- (8 - L) << 3 (low)
  VPMOVZXDQ Y13, Z13                                  // Z13 <- (8 - L) << 3 (high)

  VPSHUFB Z5, Z2, Z2                                  // Z2 <- byteswapped lanes (low)
  VPSHUFB Z5, Z3, Z3                                  // Z3 <- byteswapped lanes (high)

  VPCMPEQD Z10, Z4, K1, K3                            // K3 <- negative integers (low/all)
  VPSRLVQ Z12, Z2, Z2                                 // Z2 <- byteswapped lanes, shifted right by `(8 - L) << 3` (low)
  KSHIFTRW $8, K3, K4                                 // K4 <- negative integers (high)
  VPSRLVQ Z13, Z3, Z3                                 // Z3 <- byteswapped lanes, shifted right by `(8 - L) << 3` (high)

  VPXORQ X10, X10, X10
  VPSUBQ Z2, Z10, K3, Z2                              // Z2 <- negate integer if negative (low)
  VPCMPD $VPCMP_IMM_NE, Z11, Z4, K1, K3               // K3 <- integer values (low/all)
  VPSUBQ Z3, Z10, K4, Z3                              // Z3 <- negate integer if negative (high)
  KSHIFTRW $8, K3, K4                                 // K4 <- integer values (high)

  VCVTQQ2PD Z2, K3, Z2                                // Z2 <- final 64-bit floats (low)
  VCVTQQ2PD Z3, K4, Z3                                // Z3 <- final 64-bit floats (high)

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = unbox.coerce.i64(v[2]).k[3]
TEXT bcunboxcoercei64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_ZMM_FROM_SLOT(OUT(Z30), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD CONSTD_2(), Z11                        // Z11 <- constant(2)
  VPSRLD $4, Z4, Z5                                   // Z5 <- Z4 >> 4

  VPANDD.BCST CONSTD_0x0F(), Z4, Z6                   // Z6 <- L field
  VPSUBD Z11, Z5, Z4                                  // Z4 <- ION type - 2 (this saves us some instructions / constants)

  VPCMPUD $VPCMP_IMM_LE, Z11, Z4, K1, K1              // K1 <- mask of all lanes that contain either 0x2, 0x3, or 0x4 ION type
  VPXORQ X2, X2, X2                                   // Z2 <- zero all lanes in case there are no numbers (low)
  VPXORQ X3, X3, X3                                   // Z3 <- zero all lanes in case there are no numbers (high)

  KTESTW K1, K1
  JZ next                                             // Skip unboxing if there are no numbers

  KMOVB K1, K3
  VEXTRACTI32X8 $1, Z30, Y13
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K3, Z2              // Z2 <- number data (low)

  KSHIFTRW $8, K1, K4
  VPSLLD $2, Z11, Z12                                 // Z12 <- constant(8)
  VPSRLD $1, Z11, Z10                                 // Z10 <- constant(1)
  VPSUBD Z6, Z12, Z12                                 // Z12 <- (8 - L)

  VPGATHERDQ 1(VIRT_BASE)(Y13*1), K4, Z3              // Z3 <- number data (high)
  VPSLLD $3, Z12, Z12                                 // Z12 <- (8 - L) << 3

  VEXTRACTI32X8 $1, Z12, Y13
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z5
  VPMOVZXDQ Y12, Z12                                  // Z12 <- (8 - L) << 3 (low)
  VPMOVZXDQ Y13, Z13                                  // Z13 <- (8 - L) << 3 (high)

  VPSHUFB Z5, Z2, Z2                                  // Z2 <- byteswapped lanes (low)
  VPSHUFB Z5, Z3, Z3                                  // Z3 <- byteswapped lanes (high)

  VPCMPEQD Z10, Z4, K1, K3                            // K3 <- negative integers (low/all)
  VPSRLVQ Z12, Z2, Z2                                 // Z2 <- byteswapped lanes, shifted right by `(8 - L) << 3` (low)
  KSHIFTRW $8, K3, K4                                 // K4 <- negative integers (high)
  VPSRLVQ Z13, Z3, Z3                                 // Z3 <- byteswapped lanes, shifted right by `(8 - L) << 3` (high)

  VPXORQ X10, X10, X10
  VPSUBQ Z2, Z10, K3, Z2                              // Z2 <- negate integer if negative (low)
  VPCMPEQD Z11, Z4, K1, K3                            // K3 <- floating points (low/all)
  VPSUBQ Z3, Z10, K4, Z3                              // Z3 <- negate integer if negative (high)
  KSHIFTRW $8, K3, K4                                 // K4 <- floating points (high)

  VCVTTPD2QQ Z2, K3, Z2                               // Z2 <- final 64-bit integers (low)
  VCVTTPD2QQ Z3, K4, Z3                               // Z3 <- final 64-bit integers (high)

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = unbox.cvt.f64(v[2]).k[3]
//
// A little trick is used to cast bool to i64/f64 - if a value type is bool, we assign
// 0x01 to its binary representation - then if the value is false all bytes are cleared
// (shifted out) as L field is zero; however, if L field is 1 (representing a true value)
// then 0x01 byte would remain, which would be then kept to coerce to i64 or converted to
// a floating point 1.0 representation.
TEXT bcunboxcvtf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_ZMM_FROM_SLOT(OUT(Z30), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD CONSTD_1(), Z10                        // Z10 <- constant(1)
  VPSRLD $4, Z4, Z5                                   // Z5 <- Z4 >> 4
  VPSLLD $1, Z10, Z11                                 // Z11 <- constant(2)
  VPANDD.BCST CONSTD_0x0F(), Z4, Z6                   // Z6 <- L field

  VPSUBD Z10, Z5, Z4                                  // Z4 <- ION type - 1 (this saves us some instructions / constants)
  VPADDD Z10, Z11, Z14                                // Z14 <- constant(3)
  VPSLLD $3, Z10, Z12                                 // Z12 <- constant(8)

  VPCMPUD $VPCMP_IMM_LE, Z14, Z4, K1, K1              // K1 <- mask of all lanes that contain either 0x1, 0x2, 0x3, or 0x4 ION type
  VPXORQ X2, X2, X2                                   // Z2 <- zero all lanes in case there are no numbers (low)
  VPXORQ X3, X3, X3                                   // Z3 <- zero all lanes in case there are no numbers (high)

  KTESTW K1, K1
  JZ next                                             // Skip unboxing if there are no numbers

  KMOVB K1, K3
  VEXTRACTI32X8 $1, Z30, Y13
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K3, Z2              // Z2 <- number data (low)

  KSHIFTRW $8, K1, K4
  VPSUBD Z6, Z12, Z12                                 // Z12 <- (8 - L)
  VPGATHERDQ 1(VIRT_BASE)(Y13*1), K4, Z3              // Z3 <- number data (high)

  VPSLLD $3, Z12, Z12                                 // Z12 <- (8 - L) << 3
  VEXTRACTI32X8 $1, Z12, Y13
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z5
  VPXORQ X15, X15, X15                                // Z15 <- constant(0)
  VPMOVZXDQ Y12, Z12                                  // Z12 <- (8 - L) << 3 (low)
  VPMOVZXDQ Y13, Z13                                  // Z13 <- (8 - L) << 3 (high)

  VPCMPEQD Z15, Z4, K1, K3                            // K3 <- mask of bool values (low/all)
  KSHIFTRW $8, K3, K4                                 // K4 <- mask of bool values (high)

  VMOVDQA64 Z10, K3, Z2                               // Z2 <- value data fixed to have ones for bool values (low)
  VMOVDQA64 Z10, K4, Z3                               // Z3 <- value data fixed to have ones for bool values (high)

  VPSHUFB Z5, Z2, Z2                                  // Z2 <- byteswapped lanes (low)
  VPSHUFB Z5, Z3, Z3                                  // Z3 <- byteswapped lanes (high)

  VPCMPEQD Z11, Z4, K1, K3                            // K3 <- negative integers (low/all)
  VPSRLVQ Z12, Z2, Z2                                 // Z2 <- byteswapped lanes, shifted right by `(8 - L) << 3` (low)
  KSHIFTRW $8, K3, K4                                 // K4 <- negative integers (high)
  VPSRLVQ Z13, Z3, Z3                                 // Z3 <- byteswapped lanes, shifted right by `(8 - L) << 3` (high)

  VPSUBQ Z2, Z15, K3, Z2                              // Z2 <- negate integer if negative (low)
  VPCMPD $VPCMP_IMM_NE, Z14, Z4, K1, K3               // K3 <- integer values (low/all)
  VPSUBQ Z3, Z15, K4, Z3                              // Z3 <- negate integer if negative (high)
  KSHIFTRW $8, K3, K4                                 // K4 <- integer values (high)

  VCVTQQ2PD Z2, K3, Z2                                // Z2 <- final 64-bit floats (low)
  VCVTQQ2PD Z3, K4, Z3                                // Z3 <- final 64-bit floats (high)

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0].k[1] = unbox.cvt.i64(v[2]).k[3]
TEXT bcunboxcvti64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_ZMM_FROM_SLOT(OUT(Z30), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD CONSTD_1(), Z10                        // Z10 <- constant(1)
  VPSLLD $1, Z10, Z11                                 // Z11 <- constant(2)
  VPADDD Z10, Z11, Z14                                // Z14 <- constant(3)

  VPSRLD $4, Z4, Z5                                   // Z5 <- Z4 >> 4
  VPSLLD $3, Z10, Z12                                 // Z12 <- constant(8)

  VPANDD.BCST CONSTD_0x0F(), Z4, Z6                   // Z6 <- L field
  VPSUBD Z10, Z5, Z4                                  // Z4 <- ION type - 1 (this saves us some instructions / constants)

  VPCMPUD $VPCMP_IMM_LE, Z14, Z4, K1, K1              // K1 <- mask of all lanes that contain either 0x2, 0x3, or 0x4 ION type
  VPXORQ X2, X2, X2                                   // Z2 <- zero all lanes in case there are no numbers (low)
  VPXORQ X3, X3, X3                                   // Z3 <- zero all lanes in case there are no numbers (high)

  KTESTW K1, K1
  JZ next                                             // Skip unboxing if there are no numbers

  KMOVB K1, K3
  VEXTRACTI32X8 $1, Z30, Y13
  VPGATHERDQ 1(VIRT_BASE)(Y30*1), K3, Z2              // Z2 <- number data (low)

  KSHIFTRW $8, K1, K4
  VPSUBD Z6, Z12, Z12                                 // Z12 <- (8 - L)
  VPGATHERDQ 1(VIRT_BASE)(Y13*1), K4, Z3              // Z3 <- number data (high)

  VPSLLD $3, Z12, Z12                                 // Z12 <- (8 - L) << 3
  VEXTRACTI32X8 $1, Z12, Y13
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z5
  VPXORQ X15, X15, X15                                // Z15 <- constant(0)
  VPMOVZXDQ Y12, Z12                                  // Z12 <- (8 - L) << 3 (low)
  VPMOVZXDQ Y13, Z13                                  // Z13 <- (8 - L) << 3 (high)

  VPCMPEQD Z15, Z4, K1, K3                            // K3 <- mask of bool values (low/all)
  KSHIFTRW $8, K3, K4                                 // K4 <- mask of bool values (high)

  VMOVDQA64 Z10, K3, Z2                               // Z2 <- value data fixed to have ones for bool values (low)
  VMOVDQA64 Z10, K4, Z3                               // Z3 <- value data fixed to have ones for bool values (high)

  VPSHUFB Z5, Z2, Z2                                  // Z2 <- byteswapped lanes (low)
  VPSHUFB Z5, Z3, Z3                                  // Z3 <- byteswapped lanes (high)

  VPCMPEQD Z11, Z4, K1, K3                            // K3 <- negative integers (low/all)
  VPSRLVQ Z12, Z2, Z2                                 // Z2 <- byteswapped lanes, shifted right by `(8 - L) << 3` (low)
  KSHIFTRW $8, K3, K4                                 // K4 <- negative integers (high)
  VPSRLVQ Z13, Z3, Z3                                 // Z3 <- byteswapped lanes, shifted right by `(8 - L) << 3` (high)

  VPXORQ X10, X10, X10
  VPSUBQ Z2, Z10, K3, Z2                              // Z2 <- negate integer if negative (low)
  VPCMPEQD Z14, Z4, K1, K3                            // K3 <- floating points (low/all)
  VPSUBQ Z3, Z10, K4, Z3                              // Z3 <- negate integer if negative (high)
  KSHIFTRW $8, K3, K4                                 // K4 <- floating points (high)

  VCVTTPD2QQ Z2, K3, Z2                               // Z2 <- final 64-bit integers (low)
  VCVTTPD2QQ Z3, K4, Z3                               // Z3 <- final 64-bit integers (high)

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)


// Boxing Instructions
// -------------------

// boxing procedures take an operand
// with a known register layout and type
// and serialize it as ion, returning the
// data in bytecode.scratch and the offsets
// in each lane as ~offset in Z30 and length in Z31 (as usual)
//
// it is *required* that Z30:Z31 are zeroed
// in boxing procedures when the predicate (K1) register is unset!

CONST_DATA_U64(box_fast_i64_bswap64_7_bytes, 0, $0x01020304050607FF)
CONST_DATA_U64(box_fast_i64_bswap64_7_bytes, 8, $0x090A0B0C0D0E0FFF)
CONST_GLOBAL(box_fast_i64_bswap64_7_bytes, $16)

// v[0] = box.f64(f64[1]).k[2]
//
// scratch: 9 * 16
TEXT bcboxf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1), IN(K2))

  VCVTPD2QQ.Z Z2, K1, Z8
  VCVTPD2QQ.Z Z3, K2, Z9

  VCVTQQ2PD.Z Z8, K1, Z6
  VCVTQQ2PD.Z Z9, K2, Z7

  VCMPPD $VCMP_IMM_EQ_OQ, Z2, Z6, K1, K3 // K3 <- lanes boxed as integers (low)
  VCMPPD $VCMP_IMM_EQ_OQ, Z3, Z7, K2, K4 // K4 <- lanes boxed as integers (high)

  VPABSQ Z8, K3, Z2                      // Z2 <- mixed i64 and f64 values (low)
  KUNPCKBW K3, K4, K3                    // K3 <- lanes boxed as integers (both)
  VPABSQ Z9, K4, Z3                      // Z3 <- mixed i64 and f64 values (high)

  VPLZCNTQ.Z Z2, K3, Z6                  // Z6 <- leading zero bits count of i64 values (low)
  VPLZCNTQ.Z Z3, K4, Z7                  // Z7 <- leading zero bits count of i64 values (high)

  VPSRLQ $3, Z6, Z6                      // Z6 <- leading zero bytes count of i64 values (low)
  VPSRLQ $3, Z7, Z7                      // Z7 <- leading zero bytes count of i64 values (high)

  VPXORD X30, X30, X30
  BC_CHECK_SCRATCH_CAPACITY($(9 * 16), R15, error_handler_more_scratch)
  BC_GET_SCRATCH_BASE_GP(R8)

  // NOTE: Regardless of the strategy used (mixed float/ints or 7-byte ints) we always allocate
  // the same amount of scratch to make the function more deterministic in regards of scratch
  // buffer use (in other words it's input agnostic).
  ADDQ $(9 * 16), bytecode_scratch+8(VIRT_BCPTR)

  VPMOVQ2M Z8, K5                        // K5 <- signs of i64 values (low)
  VPMOVQ2M Z9, K6                        // K6 <- signs of i64 values (high)
  KUNPCKBW K5, K6, K5                    // K5 <- signs of i64 values (both)
  KANDW K3, K5, K5

  // Z8 <- count of leading zero bytes of each boxed number
  VPMOVQD Z6, Y8
  VPMOVQD Z7, Y9
  VINSERTI64X4 $1, Y9, Z8, Z8

  // Z10/Z11 - the number of bits to shift left, so we can properly apply bswap64
  VPBROADCASTD CONSTD_8(), Z13
  VPSLLQ $3, Z6, Z10
  VPSLLQ $3, Z7, Z11

  VPBROADCASTD.Z R8, K1, Z30             // Z30 <- initial value offsets
  VPSUBD.Z Z8, Z13, K1, Z31              // Z31 <- count of bytes each boxed number occupies in ION binary, excluding the descriptor byte
  VPCMPEQD Z13, Z31, K1, K0              // K0 <- contains lanes that are encoded as 1+8 bytes (ints or floats)

  BC_UNPACK_SLOT(0, OUT(DX))
  KTESTW K0, K0

  VPSLLVQ Z10, Z2, Z2                    // Z2 <- correctly shifted, ready for bswap64 (low)
  VPSLLVQ Z11, Z3, Z3                    // Z3 <- correctly shifted, ready for bswap64 (high)
  VPBROADCASTD.Z CONSTD_1(), K1, Z14

  JZ box_fast_i64                        // jump to a fast-path if all integers fit into 1+7 bytes boxed

  VPORD.BCST.Z CONSTD_64(), Z31, K1, Z8  // Z8 - ION Type|L that represents floats.
  VBROADCASTI64X2 CONST_GET_PTR(bswap64, 0), Z12

  VPSUBD.BCST CONSTD_32(), Z8, K3, Z8    // Z8 <- ION Type|L that represents floats and unsigned integers
  VPADDD.Z CONST_GET_PTR(consts_offsets_d_9, 0), Z30, K1, Z30 // Z30 <- offsets of boxed numbers
  VPADDD.BCST CONSTD_16(), Z8, K5, Z8    // Z8 <- ION Type|L that represents floats, signed integers, and unsigned integers

  // Scatter descriptor bytes of each active lane.
  KMOVW K1, K3
  VPSCATTERDD Z8, K3, 0(VIRT_BASE)(Z30*1)
  VEXTRACTI64X4 $1, Z30, Y13

  VPSHUFB Z12, Z2, Z2                    // Z2 <- byteswapped and encoded integers and floating points ready to scatter (low)
  VPSHUFB Z12, Z3, Z3                    // Z3 <- byteswapped and encoded integers and floating points ready to scatter (high)

  KMOVW K1, K3
  VPSCATTERDQ Z2, K3, 1(VIRT_BASE)(Y30*1)

  VPADDD.Z Z14, Z31, K1, Z31              // Z31 <- size of the encoded ion value including Type|L byte
  VPSCATTERDQ Z3, K2, 1(VIRT_BASE)(Y13*1)

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z8), IN(Z14), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

box_fast_i64:
  VPORD.BCST.Z CONSTD_32(), Z31, K1, Z8  // Z8 <- ION Type|L that represents unsigned integers.
  VBROADCASTI64X2 CONST_GET_PTR(box_fast_i64_bswap64_7_bytes, 0), Z12
  VPADDD.BCST CONSTD_16(), Z8, K5, Z8    // Z8 <- ION Type|L that represents unsigned and/or signed integers
  VPADDD.Z CONST_GET_PTR(consts_offsets_d_8, 0), Z30, K1, Z30 // Z30 <- offsets of boxed numbers

  VEXTRACTI32X8 $1, Z8, Y11
  VPMOVZXDQ Y8, Z10
  VPMOVZXDQ Y11, Z11
  VPSHUFB Z12, Z2, Z2                    // Z2 <- byteswapped integers having max 7 bytes (low)
  VPSHUFB Z12, Z3, Z3                    // Z3 <- byteswapped integers having max 7 bytes (high)
  VPORQ Z10, Z2, Z2                      // Z4 <- encoded ION value having max 8 bytes (low)
  VPORQ Z11, Z3, Z3                      // Z3 <- encoded ION value having max 8 bytes (high)
  VPADDD.Z Z14, Z31, K1, Z31             // Z31 <- size of the encoded ion value including Type|L byte

  VMOVDQU32 Z2, 0(VIRT_BASE)(R8*1)
  VMOVDQU32 Z3, 64(VIRT_BASE)(R8*1)

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z8), IN(Z14), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

  _BC_ERROR_HANDLER_MORE_SCRATCH()

// v[0] = box.i64(f64[1]).k[2]
//
// scratch: 9 * 16
TEXT bcboxi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))

  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPABSQ.Z Z2, K1, Z4                    // Z4 <- absolute i64 values (low)
  VPABSQ.Z Z3, K2, Z5                    // Z5 <- absolute i64 values (high)

  VPLZCNTQ.Z Z4, K1, Z6                  // Z6 <- leading zero bits count of i64 values (low)
  VPLZCNTQ.Z Z5, K2, Z7                  // Z7 <- leading zero bits count of i64 values (high)

  VPSRLQ $3, Z6, Z6                      // Z6 <- leading zero bytes count of i64 values (low)
  VPSRLQ $3, Z7, Z7                      // Z7 <- leading zero bytes count of i64 values (high)

  VPXORD X30, X30, X30
  BC_CHECK_SCRATCH_CAPACITY($(9 * 16), R15, error_handler_more_scratch)
  BC_GET_SCRATCH_BASE_GP(R8)

  // NOTE: Regardless of the strategy used (boxing 7-byte or 8-byte integers) we always allocate
  // the same amount of scratch to make the function more deterministic in regards of scratch
  // buffer use (in other words it's input agnostic).
  ADDQ $(9 * 16), bytecode_scratch+8(VIRT_BCPTR)

  VPMOVQ2M Z2, K5                        // K5 <- signs of i64 values (low)
  VPMOVQ2M Z3, K6                        // K6 <- signs of i64 values (high)
  KUNPCKBW K5, K6, K5                    // K5 <- signs of i64 values (both)
  KANDW K1, K5, K5

  // Z8 <- count of leading zero bytes of each boxed number
  VPMOVQD Z6, Y8
  VPMOVQD Z7, Y9
  VINSERTI64X4 $1, Y9, Z8, Z8

  // Z10/Z11 - the number of bits to shift left, so we can properly apply bswap64
  VPBROADCASTD CONSTD_8(), Z13
  VPSLLQ $3, Z6, Z10
  VPSLLQ $3, Z7, Z11
  VPBROADCASTD.Z CONSTD_1(), K1, Z14

  VPBROADCASTD.Z R8, K1, Z30             // Z30 <- initial value offsets
  VPSUBD.Z Z8, Z13, K1, Z31              // Z31 <- size of an ION boxed value, excluding the descriptor byte
  VPCMPEQD Z13, Z31, K1, K6              // K6 <- contains lanes that are encoded as 1+8 bytes

  BC_UNPACK_SLOT(0, OUT(DX))
  KTESTW K6, K6

  VPORD.BCST.Z CONSTD_32(), Z31, K1, Z8  // Z8 <- ION Type|L that represents unsigned integers
  VPSLLVQ Z10, Z4, Z4                    // Z4 <- correctly shifted, ready for bswap64 (low)
  VPSLLVQ Z11, Z5, Z5                    // Z5 <- correctly shifted, ready for bswap64 (high)
  VPADDD.BCST CONSTD_16(), Z8, K5, Z8    // Z8 <- ION Type|L that represents signed integers and unsigned integers
  VPADDD.Z Z14, Z31, K1, Z31             // Z31 <- size of the boxed value including Type|L field

  JZ box_fast_i64                        // jump to a fast-path if all integers fit into 1+7 bytes boxed

  VBROADCASTI64X2 CONST_GET_PTR(bswap64, 0), Z12
  VPADDD.Z CONST_GET_PTR(consts_offsets_d_9, 0), Z30, K1, Z30 // Z30 <- offsets of boxed numbers

  // Scatter descriptor bytes of each active lane.
  KMOVW K1, K3
  VPSCATTERDD Z8, K3, 0(VIRT_BASE)(Z30*1)
  VEXTRACTI64X4 $1, Z30, Y13

  VPSHUFB Z12, Z4, Z4                    // Z4 <- byteswapped and encoded integers ready to scatter (low)
  VPSHUFB Z12, Z5, Z5                    // Z5 <- byteswapped and encoded integers ready to scatter (high)

  KMOVW K1, K3
  VPSCATTERDQ Z4, K3, 1(VIRT_BASE)(Y30*1)
  VPSCATTERDQ Z5, K2, 1(VIRT_BASE)(Y13*1)

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z8), IN(Z14), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

  // fast path - if all integers fit into 7 bytes (8 bytes in ION) we can avoid scatter
box_fast_i64:
  VBROADCASTI64X2 CONST_GET_PTR(box_fast_i64_bswap64_7_bytes, 0), Z12
  VPADDD.Z CONST_GET_PTR(consts_offsets_d_8, 0), Z30, K1, Z30 // Z30 <- offsets of boxed numbers

  VEXTRACTI32X8 $1, Z8, Y11
  VPMOVZXDQ Y8, Z10
  VPMOVZXDQ Y11, Z11
  VPSHUFB Z12, Z4, Z4                    // Z4 <- byteswapped integers having max 7 bytes (low)
  VPSHUFB Z12, Z5, Z5                    // Z5 <- byteswapped integers having max 7 bytes (high)
  VPORQ Z10, Z4, Z4                      // Z4 <- encoded ION value having max 8 bytes (low)
  VPORQ Z11, Z5, Z5                      // Z5 <- encoded ION value having max 8 bytes (high)

  VMOVDQU64 Z4, 0(VIRT_BASE)(R8*1)
  VMOVDQU64 Z5, 64(VIRT_BASE)(R8*1)

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z8), IN(Z14), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

  _BC_ERROR_HANDLER_MORE_SCRATCH()

// v[0] = box.k(k[1]).k[2]
//
// scratch: 16
//
// store (up to) 16 booleans
//
// see boxmask_tail_vbmi2 for a version that
// only writes out the lanes that are valid
TEXT bcboxk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))

  VPBROADCASTB CONSTD_16(), X10                     // X10 <- byte(0x10)
  BC_LOAD_K1_FROM_SLOT(OUT(K2), IN(BX))             // K2 <- true/false

  VPBROADCASTB CONSTD_1(), Z11                      // Z11 <- byte(0x01)
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))             // K1 <- predicate

  BC_CHECK_SCRATCH_CAPACITY($16, R15, error_handler_more_scratch)
  VPADDB       X10, X11, K2, X10                    // X10 <- Type|L (true or false)
  MOVQ         bytecode_scratch(VIRT_BCPTR), R14
  ADDQ         bytecode_scratch+8(VIRT_BCPTR), R14
  VMOVDQU      X10, 0(R14)                          // store 16 bytes unconditionally

  BC_UNPACK_SLOT(0, OUT(DX))

  // offsets are [0, 1, 2, 3...] plus base offset; then complemented for Z30
  VPMOVZXBD byteidx<>+0(SB), Z12
  VPXORD Z30, Z30, Z30
  BC_GET_SCRATCH_BASE_ZMM(Z30, K1)
  VPSRLD.Z $24, Z11, K1, Z31
  VPADDD.Z Z12, Z30, K1, Z30

  // update used scratch space
  ADDQ $16, bytecode_scratch+8(VIRT_BCPTR)

  BC_STORE_VALUE_TO_SLOT_X(IN(Z30), IN(Z31), IN(X10), IN(X11), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

  _BC_ERROR_HANDLER_MORE_SCRATCH()

// v[0] = box.str(slice[1]).k[2]
//
// scratch: PageSize
TEXT bcboxstr(SB), NOSPLIT|NOFRAME, $0
  VPSLLD.BCST $4, CONSTD_8(), Z20 // ION type of a boxed string is 0x8
  JMP boxslice_tail(SB)

// v[0] = box.list(slice[1]).k[2]
//
// scratch: PageSize
TEXT bcboxlist(SB), NOSPLIT|NOFRAME, $0
  VPSLLD.BCST $4, CONSTD_0x0B(), Z20 // ION type of a boxed list is 0xB
  JMP boxslice_tail(SB)

// Boxes a string or list slice
//
// Implementation notes:
//   - Two paths - small slices (up to 13 bytes), large slices (more than 13 bytes).
//   - Do gathers of the leading 16 bytes of each slice as early as possible.
//     These bytes are gathered to Z11, Z12, Z13, and Z14 and used by both code
//     paths - this optimizes a bit storing smaller slices in both cases.
//   - Encoding of the Type|L + Length happens regardless of slice lengths, we
//     do gathers meanwhile so the CPU should be busy enough to hide the latency.
TEXT boxslice_tail(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  // Quickly skip this instruction if there is nothing to box.
  VPXORD Z30, Z30, Z30
  VPXORD Z31, Z31, Z31

  KTESTW K1, K1
  JZ next

  // Gather LO-8 bytes of LO-8 lanes to Z11.
  KMOVW K1, K4
  VPXORD X11, X11, X11
  VPGATHERDQ 0(VIRT_BASE)(Y2*1), K4, Z11

  // Z15 will contain HI-8 indexes in the LO 256-bit part of Z15 (for gathers).
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z2, Z2, Z15

  // Load some constants here.
  VPBROADCASTD CONSTD_1(), Z10

  // K2 will contain each lane that contains string longer than 8 bytes.
  VPCMPD.BCST $VPCMP_IMM_GT, CONSTD_8(), Z3, K1, K2
  // Check whether we can use a fast-path, which requires all strings to be less
  // than 14 characters long. If K3 != K1 it would mean that we have to go slow.
  VPCMPD.BCST $VPCMP_IMM_LT, CONSTD_0x0E(), Z3, K1, K3

  // Calculate an encoded ION length.
  //
  // First encode all lengths to ION RunLength encoding, it's easier to
  // determine the length of the encoded value actually after it's encoded
  // as we can just use LZCNT with shift to get the number of bytes it requires.
  VMOVDQA32.Z Z3, K1, Z4                                         // Z4 = [xxxxxxxx|xxxxxxxx|xxxxxxxx|xAAAAAAA]
  VPSLLD.Z $1, Z3, K1, Z5                                        // Z5 = [xxxxxxxx|xxxxxxxx|xBBBBBBB|xxxxxxxx]
  VPSLLD.Z $2, Z3, K1, Z6                                        // Z6 = [xxxxxxxx|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPSLLD.Z $3, Z3, K1, Z7                                        // Z7 = [xDDDDDDD|xxxxxxxx|xxxxxxxx|xxxxxxxx]

  // Use VPTERNLOGD to combine the extracted bits:
  VPTERNLOGD.BCST $TLOG_BLEND_BA, CONSTD_0x007F007F(), Z4, Z5 // Z5 = [xxxxxxxx|xxxxxxxx|xBBBBBBB|xAAAAAAA]
  VPTERNLOGD.BCST $TLOG_BLEND_BA, CONSTD_0x007F007F(), Z6, Z7 // Z7 = [xDDDDDDD|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPTERNLOGD.BCST $TLOG_BLEND_BA, CONSTD_0xFFFF0000(), Z7, Z5 // Z5 = [xDDDDDDD|xCCCCCCC|xBBBBBBB|xAAAAAAA]
  VPANDD.BCST CONSTD_0x7F7F7F7F(), Z5, Z5                        // Z5 = [0DDDDDDD|0CCCCCCC|0BBBBBBB|0AAAAAAA]

  // Find the last leading bit set, which will be used to determine the number
  // of bytes required for storing each length.
  VPLZCNTD Z5, Z6
  VPBROADCASTD CONSTD_4(), Z7

  // Z5 = [0DDDDDDD|0CCCCCCC|0BBBBBBB|1AAAAAAA] where '1' is a run-length termination bit.
  VPORD.BCST CONSTD_128(), Z5, K1, Z5
  VPBROADCASTD CONSTD_32(), Z8

  // Z6 would contain the number of bytes required to store each length.
  VPSRLD $3, Z6, Z6
  VPSUBD.Z Z6, Z7, K1, Z6
  // Z7 would contain the number of bits (aligned to 8) required to store each length.
  VPSLLD $3, Z6, Z7

  // Gather HI-8 bytes of LO-8 lanes to Z12.
  KMOVW K1, K5
  VPXORD X12, X12, X12
  VPGATHERDQ 8(VIRT_BASE)(Y2*1), K5, Z12

  // Z7 would contain the number of bits to discard in Z5.
  VPSUBD Z7, Z8, Z7

  // Z5 <- [1AAAAAAA|0BBBBBBB|0CCCCCCC|0DDDDDDD] (ByteSwapped).
  VPSHUFB CONST_GET_PTR(bswap32, 0), Z5, Z5
  // Discards bytes in Z5 that are not used to encode the length.
  VPSRLVD Z7, Z5, Z5

  // Clear lanes in Z6 that represent strings having length less than 14 bytes.
  VPXORD Z6, Z6, K3, Z6
  // Z16 would contain the number of bytes that is required to store Type|L + Length.
  VPADDD.Z Z10, Z6, K1, Z16

  // Z7 would contain the number of bytes required to store each string in ION data.
  // What we want is to have offsets for each ION encoded string in the output buffer,
  // which can then be used to calculate the number of bytes required to store all
  // strings in all lanes. We cannot touch the output buffer without having the total.
  VPADDD.Z Z16, Z4, K1, Z7                             // Z7 = [15    14    13    12   |11    10    09    08   |07    06    05    04   |03    02    01    00   ]
  VPSLLDQ $4, Z7, Z8                                   // Z8 = [14    13    12    __   |10    09    08    __   |06    05    04    __   |02    01    00    __   ]
  VPADDD Z8, Z7, Z8                                    // Z8 = [15+14 14+13 13+12 12   |11+10 10+09 09+08 08   |07+06 06+05 05+04 04   |03+02 02+01 01+00 00   ]
  VPSLLDQ $8, Z8, Z9                                   // Z9 = [13+12 12    __    __   |09+08 08    __    __   |05+04 04    __    __   |01+00 00    __    __   ]
  VPADDD Z8, Z9, Z8                                    // Z8 = [15:12 14:12 13:12 12   |11:08 10:08 09:08 08   |07:04 06:04 05:04 04   |03:00 02:00 01:00 00   ]

  // Gather LO-8 bytes of HI-8 lanes to Z13.
  KSHIFTRW $8, K1, K4
  VPXORD X13, X13, X13
  VPGATHERDQ 0(VIRT_BASE)(Y15*1), K4, Z13

  MOVL $0xF0F0, R15
  KMOVW R15, K4
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), Z8, Z9        // Z9 = [15:12 15:12 15:12 15:12|11:08 11:08 11:08 11:08|07:04 07:04 07:04 07:04|03:00 03:00 03:00 03:00]
  VPERMQ $SHUFFLE_IMM_4x2b(1, 1, 1, 1), Z9, Z9         // Z9 = [11:08 11:08 11:08 11:08|<ign> <ign> <ign> <ign>|03:00 03:00 03:00 03:00|<ign> <ign> <ign> <ign>]
  VPADDD Z9, Z8, K4, Z8                                // Z8 = [15:08 14:08 13:08 12:08|11:08 10:08 09:08 08   |07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ]

  MOVL $0xFF00, R15
  KMOVW R15, K4
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), Z8, Z9        // Z9 = [15:08 15:08 15:08 15:08|11:08 11:08 11:08 11:08|07:00 07:00 07:00 07:00|03:00 03:00 03:00 03:00]
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 1, 1, 1), Z9, Z9, Z9 // Z9 = [07:00 07:00 07:00 07:00|07:00 07:00 07:00 07:00|<ign> <ign> <ign> <ign>|<ign> <ign> <ign> <ign>]
  VPADDD Z9, Z8, K4, Z8                                // Z8 = [15:00 14:00 13:00 12:00|11:00 10:00 09:00 08:00|07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ]

  // We need to calculate the the number of bytes we are going to write to the
  // destination - we have to shuffle the content of Z8 in order to do that.
  VEXTRACTI32X4 $3, Z8, X9
  VPEXTRD $3, X9, R15

  // Gather HI-8 bytes of HI-8 lanes to Z14.
  KSHIFTRW $8, K2, K5
  VPXORD X14, X14, X14
  VPGATHERDQ 8(VIRT_BASE)(Y15*1), K5, Z14

  // Z8 now contains the end index of each lane. What we need is, however, the
  // start index, which can be calculated by subtracting start indexes from it.
  VPSUBD Z7, Z8, Z9                                    // Z9 = [14:00 13:00 12:00 11:00|10:00 09:00 08:00 07:00|06:00 05:00 04:00 03:00|02:00 01:00 00    zero ]

  MOVQ bytecode_scratch+8(VIRT_BCPTR), CX              // CX = Output buffer length.
  MOVQ bytecode_scratch+16(VIRT_BCPTR), R8             // R8 = Output buffer capacity.
  LEAQ 16(R15), BX                                     // BX = Capacity required to store the output (let's assume 16 bytes more for 16-byte stores).
  SUBQ CX, R8                                          // R8 = Remaining space in the output buffer.

  // Abort if the output buffer is too small.
  CMPQ R8, BX
  JLT abort

  // Update the output buffer length and Z30/Z31 (boxed value outputs).
  VPBROADCASTD.Z CX, K1, Z30
  VPADDD.BCST bytecode_scratchoff(VIRT_BCPTR), Z30, K1, Z30
  ADDQ CX, R15
  VPADDD Z9, Z30, K1, Z30
  VMOVDQA32.Z Z7, K1, Z31                              // Z31 = ION data length: Type|L + optional VarUInt + string data.
  MOVQ R15, bytecode_scratch+8(VIRT_BCPTR)             // Store output buffer length back to the bytecode_scratch slice.

  MOVL bytecode_scratchoff(VIRT_BCPTR), R8             // R8 = location of scratch base
  ADDQ VIRT_BASE, R8                                   // R8 += base output address.
  ADDQ CX, R8                                          // R8 += adjusted output address by its current length.

  // Unpack string data into 16-byte units, so we can use 16-byte stores.
  VPUNPCKLQDQ Z12, Z11, Z10                            // Z10 = [S06 S06 S06 S06|S04 S04 S04 S04|S02 S02 S02 S02|S00 S00 S00 S00]
  VPUNPCKHQDQ Z12, Z11, Z11                            // Z11 = [S07 S07 S07 S07|S05 S05 S05 S05|S03 S03 S03 S03|S01 S01 S01 S01]
  VPUNPCKLQDQ Z14, Z13, Z12                            // Z12 = [S14 S14 S14 S14|S12 S12 S12 S12|S10 S10 S10 S10|S08 S08 S08 S08]
  VPUNPCKHQDQ Z14, Z13, Z13                            // Z13 = [S15 S15 S15 S15|S13 S13 S13 S13|S11 S11 S11 S11|S09 S09 S09 S09]

  // K3 contains a mask of strings having length lesser than 14. If all strings
  // of all lanes have length lesser than 14 then we can take a fast path.
  KTESTW K1, K3
  JNC large_string

  // --- Fast path for small strings (small string in each lane or MISSING) ---

  // Make Z7 contain Type|L where Type is the requested ION type
  VPORD.Z Z20, Z3, K1, Z20                             // Z20 = [L15 L14 L13 L12|L11 L10 L09 L08|L07 L06 L05 L04|L03 L02 L01 L00]
  VPMOVZXDQ Y20, Z5                                    // Z5  = [___ L07 ___ L06|___ L05 ___ L04|___ L03 ___ L02|___ L01 ___ L00]
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z20, Z20, Z7

  VPSLLDQ $7, Z5, Z6                                   // Z6  = [L07 ___ L06 ___|L05 ___ L04 ___|L03 ___ L02 ___|L01 ___ L00 ___]
  VPSLLDQ $15, Z5, Z5                                  // Z5  = [L06 ___ ___ ___|L04 ___ ___ ___|L02 ___ ___ ___|L00 ___ ___ ___]
  VPMOVZXDQ Y7, Z7                                     // Z7  = [___ L15 ___ L14|___ L13 ___ L12|___ L11 ___ L10|___ L09 ___ L08]

  VPALIGNR $15, Z6, Z11, Z11                           // Z11 = [V07 V07 V07 V07|V05 V05 V05 V05|V03 V03 V03 V03|V01 V01 V01 V01]
  VPALIGNR $15, Z5, Z10, Z10                           // Z10 = [V06 V06 V06 V06|V04 V04 V04 V04|V02 V02 V02 V02|V00 V00 V00 V00]

  VPSLLDQ $7, Z7, Z6                                   // Z6  = [L15 ___ L14 ___|L13 ___ L12 ___|L11 ___ L10 ___|L09 ___ L08 ___]
  VPSLLDQ $15, Z7, Z7                                  // Z7  = [L14 ___ ___ ___|L12 ___ ___ ___|L10 ___ ___ ___|L08 ___ ___ ___]

  VPALIGNR $15, Z6, Z13, Z13                           // Z13 = [V15 V15 V15 V15|V13 V13 V13 V13|V11 V11 V11 V11|V09 V09 V09 V09]
  VPALIGNR $15, Z7, Z12, Z12                           // Z12 = [V14 V14 V14 V14|V12 V12 V12 V12|V10 V10 V10 V10|V08 V08 V08 V08]

  VPEXTRD $0, X8, DX
  VEXTRACTI32X4 $1, Z8, X5
  VMOVDQU32 X10, 0(R8)                                 // {00} Write [V00 V00 V00 V00]
  VPEXTRD $1, X8, CX
  VMOVDQU32 X11, 0(R8)(DX*1)                           // {01} Write [V01 V01 V01 V01]
  VPEXTRD $2, X8, DX
  VEXTRACTI32X4 $1, Z10, 0(R8)(CX*1)                   // {02} Write [V02 V02 V02 V02]
  VPEXTRD $3, X8, CX
  VEXTRACTI32X4 $1, Z11, 0(R8)(DX*1)                   // {03} Write [V03 V03 V03 V03]

  VPEXTRD $0, X5, DX
  VEXTRACTI32X4 $2, Z8, X6
  VEXTRACTI32X4 $2, Z10, 0(R8)(CX*1)                   // {04} Write [V04 V04 V04 V04]
  VPEXTRD $1, X5, CX
  VEXTRACTI32X4 $2, Z11, 0(R8)(DX*1)                   // {05} Write [V05 V05 V05 V05]
  VPEXTRD $2, X5, DX
  VEXTRACTI32X4 $3, Z10, 0(R8)(CX*1)                   // {06} Write [V06 V06 V06 V06]
  VPEXTRD $3, X5, CX
  VEXTRACTI32X4 $3, Z11, 0(R8)(DX*1)                   // {07} Write [V07 V07 V07 V07]

  VPEXTRD $0, X6, DX
  VEXTRACTI32X4 $3, Z8, X5
  VMOVDQU32 X12, 0(R8)(CX*1)                           // {08} Write [V08 V08 V08 V08]
  VPEXTRD $1, X6, CX
  VMOVDQU32 X13, 0(R8)(DX*1)                           // {09} Write [V09 V09 V09 V09]
  VPEXTRD $2, X6, DX
  VEXTRACTI32X4 $1, Z12, 0(R8)(CX*1)                   // {10} Write [V10 V10 V10 V10]
  VPEXTRD $3, X6, CX
  VEXTRACTI32X4 $1, Z13, 0(R8)(DX*1)                   // {11} Write [V11 V11 V11 V11]

  VPEXTRD $0, X5, DX
  VEXTRACTI32X4 $2, Z12, 0(R8)(CX*1)                   // {12} Write [V12 V12 V12 V12]
  VPEXTRD $1, X5, CX
  VEXTRACTI32X4 $2, Z13, 0(R8)(DX*1)                   // {13} Write [V13 V13 V13 V13]
  VPEXTRD $2, X5, DX
  VEXTRACTI32X4 $3, Z12, 0(R8)(CX*1)                   // {14} Write [V14 V14 V14 V14]
  VEXTRACTI32X4 $3, Z13, 0(R8)(DX*1)                   // {15} Write [V15 V15 V15 V15]

  JMP next

large_string:
  // --- Slow path for large strings (one/more lane has a string greater than 13 bytes) ---

  // We already have encoded ION length, including the information regarding how "long" the length is.
  VPBROADCASTD.Z CONSTD_0x0E(), K1, Z15
  VMOVDQA32 Z3, K3, Z15                                // Z15 = [L15 L14 L13 L12|L11 L10 L09 L08|L07 L06 L05 L04|L03 L02 L01 L00]
  VPORD.Z Z20, Z15, K1, Z20                            // Z20 = [T15 T14 T13 T12|T11 T10 T09 T08|T07 T06 T05 T04|T03 T02 T01 T00]
  VPSLLD $24, Z20, Z15

  VPUNPCKLDQ Z5, Z15, Z14                              // Z14 = [L13 T13 L12 T12|L09 T09 L08 T08|L05 T05 L04 T04|L01 T01 L00 T00]
  VPUNPCKHDQ Z5, Z15, Z15                              // Z15 = [L15 T15 L14 T14|L11 T11 L10 T10|L07 T07 L06 T06|L03 T03 L02 T02]

  // This will make each QWORD look like [__ __ __ VU VU VU VU TL] where
  // TL is Type|L and VU is VarUInt representing string length in bytes.
  VPSRLQ $24, Z14, Z14
  VPSRLQ $24, Z15, Z15

  // Z5 now contains 32-bit indexes to RSI (input buffer).
  VMOVDQA32.Z Z2, K1, Z5

  // The following code processes 4 strings each loop iteration.
  MOVL $4, BX

  // Requred by MOVSB, we have to move them temporarily.
  MOVQ DI, R14
  MOVQ SI, R15

large_repeat:
  VPEXTRD $0, X9, DX                                   // {0} Offset in the output buffer.
  VPEXTRD $0, X4, CX                                   // {0} String length in bytes (without ION overhead).
  VPEXTRD $0, X16, DI                                  // {0} Byte length of Type|L followed by VarUInt representing string length.
  VPEXTRD $0, X5, SI                                   // {0} Index into the input buffer.
  VPEXTRQ $0, X14, 0(R8)(DX*1)                         // {0} Write Type|L byte + optional Length if the string is longer than 13.
  ADDQ DX, DI                                          // {0} Adjust output offset to point to the first string data index.
  VMOVDQU32 X10, 0(R8)(DI*1)                           // {0} Write the initial [15:0] slice of the string.

  SUBQ $16, CX                                         // {0} We have written 16 bytes already.
  JBE large_skip_0                                     // {0} Skip MOVSB if this string was not greater than 16 bytes.
  LEAQ 16(R15)(SI*1), SI                               // {0} RSI - source pointer.
  LEAQ 16(R8)(DI*1), DI                                // {0} RDI - destination pointer.
  REP; MOVSB                                           // {0} Move RCX bytes from RSI to RDI.

large_skip_0:
  VPEXTRD $1, X9, DX                                   // {1} Offset in the output buffer.
  VPEXTRD $1, X4, CX                                   // {1} String length in bytes (without ION overhead).
  VPEXTRD $1, X16, DI                                  // {1} Byte length of Type|L followed by VarUInt representing string length.
  VPEXTRD $1, X5, SI                                   // {1} Index into the input buffer.
  VPEXTRQ $1, X14, 0(R8)(DX*1)                         // {1} Write Type|L byte + optional Length if the string is longer than 13.
  ADDQ DX, DI                                          // {1} Adjust output offset to point to the first string data index.
  VMOVDQU32 X11, 0(R8)(DI*1)                           // {1} Write the initial [15:0] slice of the string.

  SUBQ $16, CX                                         // {1} We have written 16 bytes already.
  JBE large_skip_1                                     // {1} Skip MOVSB if this string was not greater than 16 bytes.
  LEAQ 16(R15)(SI*1), SI                               // {1} RSI - source pointer.
  LEAQ 16(R8)(DI*1), DI                                // {1} RDI - destination pointer.
  REP; MOVSB                                           // {1} Move RCX bytes from RSI to RDI.

large_skip_1:
  VPEXTRD $2, X9, DX                                   // {2} Offset in the output buffer.
  VPEXTRD $2, X4, CX                                   // {2} String length in bytes (without ION overhead).
  VPEXTRD $2, X16, DI                                  // {2} Byte length of Type|L followed by VarUInt representing string length.
  VPEXTRD $2, X5, SI                                   // {2} Index into the input buffer.
  VPEXTRQ $0, X15, 0(R8)(DX*1)                         // {2} Write Type|L byte + optional Length if the string is longer than 13.
  ADDQ DX, DI                                          // {2} Adjust output offset to point to the first string data index.
  VEXTRACTI32X4 $1, Z10, 0(R8)(DI*1)                   // {2} Write the initial [15:0] slice of the string.

  SUBQ $16, CX                                         // {2} We have written 16 bytes already.
  JBE large_skip_2                                     // {2} Skip MOVSB if this string was not greater than 16 bytes.
  LEAQ 16(R15)(SI*1), SI                               // {2} RSI - source pointer.
  LEAQ 16(R8)(DI*1), DI                                // {2} RDI - destination pointer.
  REP; MOVSB                                           // {2} Move RCX bytes from RSI to RDI.

large_skip_2:
  VPEXTRD $3, X9, DX                                   // {3} Offset in the output buffer.
  VPEXTRD $3, X4, CX                                   // {3} String length in bytes (without ION overhead).
  VPEXTRD $3, X16, DI                                  // {3} Byte length of Type|L followed by VarUInt representing string length.
  VPEXTRD $3, X5, SI                                   // {3} Index into the input buffer.
  VPEXTRQ $1, X15, 0(R8)(DX*1)                         // {3} Write Type|L byte + optional Length if the string is longer than 13.
  ADDQ DX, DI                                          // {3} Adjust output offset to point to the first string data index.
  VEXTRACTI32X4 $1, Z11, 0(R8)(DI*1)                   // {3} Write the initial [15:0] slice of the string.

  SUBQ $16, CX                                         // {3} We have written 16 bytes already.
  JBE large_skip_3                                     // {3} Skip MOVSB if this string was not greater than 16 bytes.
  LEAQ 16(R15)(SI*1), SI                               // {3} RSI - source pointer.
  LEAQ 16(R8)(DI*1), DI                                // {3} RDI - destination pointer.
  REP; MOVSB                                           // {3} Move RCX bytes from RSI to RDI.

large_skip_3:
  // Shuffle all vectors so we will end up with values in low parts.
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z4, Z4, Z4    // Z4/Z5/Z9/Z16 are indexes and lengths (DWORDS).
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z5, Z5, Z5
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z9, Z9, Z9
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z16, Z16, Z16

  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z12, Z10, Z10 // Z10:Z13 are first 16 bytes of each string (QWORDS).
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z13, Z11, Z11
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z12, Z12, Z12
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z13, Z13, Z13

  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z14, Z14, Z14 // Z14:Z15 are Type|L + encoded string lengths (QWORDS).
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(0, 3, 2, 1), Z15, Z15, Z15

  SUBL $1, BX
  JNZ large_repeat

  MOVQ R14, DI
  MOVQ R15, SI

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z20), IN(Z16), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()


// Make List / Struct
// ------------------

// Boxes a list/struct composed of boxed values (va)
//
// v[0].k[1] = makelist(varargs(v[0].k[1])).k[2]
//
// scratch: PageSize
TEXT bcmakelist(SB), NOSPLIT|NOFRAME, $0
#define BC_GENERATE_MAKE_LIST
#include "evalbc_make_object_impl.h"
#undef BC_GENERATE_MAKE_LIST
    RET

// Boxes a list/struct composed of boxed values (va)
//
// v[0].k[1] = makestruct(varargs(symbol[0], v[1], k[2])).k[2]
//
// scratch: PageSize
TEXT bcmakestruct(SB), NOSPLIT|NOFRAME, $0
#define BC_GENERATE_MAKE_STRUCT
#include "evalbc_make_object_impl.h"
#undef BC_GENERATE_MAKE_STRUCT
    RET


// Hash Instructions
// -----------------

// h[0] = hashvalue(v[1]).k[2]
TEXT bchashvalue(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z28), OUT(Z29), IN(BX), IN(K1))

  BC_UNPACK_SLOT(0, OUT(DX))
  VPXORD X10, X10, X10
  ADDQ $(BC_SLOT_SIZE*3), VIRT_PCREG
  ADDQ VIRT_VALUES, DX

  VMOVDQU32 Z10, (DX)
  VMOVDQU32 Z10, 64(DX)
  VMOVDQU32 Z10, 128(DX)
  VMOVDQU32 Z10, 192(DX)

  MOVQ DX, R14
  MOVQ VIRT_BASE, R15

  JMP hashimpl_tail(SB)

// h[0] = hashvalue+(h[1], v[2]).k[3]
TEXT bchashvalueplus(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(OUT(Z28), OUT(Z29), IN(BX), IN(K1))

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R14))
  ADDQ $(BC_SLOT_SIZE*4), VIRT_PCREG
  ADDQ VIRT_VALUES, R14
  ADDQ VIRT_VALUES, DX
  MOVQ VIRT_BASE, R15

  JMP hashimpl_tail(SB)

// expected input register arguments:
//   DX = destination hash slot
//   R14 = source hash slot (may alias DX)
//   R15 = base memory pointer
//   Z28 = offsets relative to base
//   Z29 = lengths relative to offsets
TEXT hashimpl_tail(SB), NOSPLIT|NOFRAME, $0
  KMOVW            K1, K6        // save current predicate
  VMOVDQU64        0(R14), Z9    // Z9 = k1
  VMOVDQU64        128(R14), Z8  // Z8 = k2
  VMOVDQA32        Y28, Y10      // Y10 = lo 8 offsets
  VMOVDQA32        Y29, Y11      // Y11 = lo 8 lengths
  CALL             siphashx8(SB) // eval first 8
  VMOVDQU64        Z9, 0(DX)     // first 8 of 64 bit upper
  VMOVDQU64        Z10, 128(DX)  // first 8 of 64 bit lower
  VMOVDQU64        64(R14), Z9
  VMOVDQU64        192(R14), Z8
  VEXTRACTI32X8    $1, Z28, Y10  // Y11 = hi 8 offsets
  VEXTRACTI32X8    $1, Z29, Y11  // Y10 = hi 8 lengths
  KSHIFTRW         $8, K1, K1    // shift lanes
  CALL             siphashx8(SB) // eval second 8
  VMOVDQU64        Z9, 64(DX)
  VMOVDQU64        Z10, 192(DX)
  KMOVW            K6, K1        // restore original lanes
  NEXT()

// 1 round of siphash on 4x64bit state x 8 lanes
#define SIPROUND(mask, v0, v1, v2, v3) \
  VPADDQ v0, v1, mask, v0              \
  VPROLQ $13, v1, mask, v1             \
  VPXORQ v0, v1, mask, v1              \
  VPROLQ $32, v0, mask, v0             \
  VPADDQ v2, v3, mask, v2              \
  VPROLQ $16, v3, mask, v3             \
  VPXORQ v3, v2, mask, v3              \
  VPADDQ v0, v3, mask, v0              \
  VPROLQ $21, v3, mask, v3             \
  VPXORQ v3, v0, mask, v3              \
  VPADDQ v2, v1, mask, v2              \
  VPROLQ $17, v1, mask, v1             \
  VPXORQ v1, v2, mask, v1              \
  VPROLQ $32, v2, mask, v2

#define SIPROUNDx2(mask, v0, v1, v2, v3) \
  SIPROUND(mask, v0, v1, v2, v3) \
  SIPROUND(mask, v0, v1, v2, v3)

// tail mask for <8-byte loads:
#define CONST_TAIL_MASK8() CONST_GET_PTR(tail_mask_map8, 0)
CONST_DATA_U64(tail_mask_map8,  0, $0x0000000000000000)
CONST_DATA_U64(tail_mask_map8,  8, $0x00000000000000FF)
CONST_DATA_U64(tail_mask_map8, 16, $0x000000000000FFFF)
CONST_DATA_U64(tail_mask_map8, 24, $0x0000000000FFFFFF)
CONST_DATA_U64(tail_mask_map8, 32, $0x00000000FFFFFFFF)
CONST_DATA_U64(tail_mask_map8, 40, $0x000000FFFFFFFFFF)
CONST_DATA_U64(tail_mask_map8, 48, $0x0000FFFFFFFFFFFF)
CONST_DATA_U64(tail_mask_map8, 56, $0x00FFFFFFFFFFFFFF)
CONST_GLOBAL(tail_mask_map8, $64)

// inputs: K1 = active, R15 = base, Y10:Y11 = offset:ptr, Z9 = k0, Z8 = k1
// outputs: Z9 = lo 64 bits x 8, Z10 = hi 64 bits x 8
// clobbers: K1-K3, Z10-Z20
TEXT siphashx8(SB), NOFRAME|NOSPLIT, $0
  // comments are derived from the reference implementation:
  // https://github.com/veorq/SipHash/blob/master/siphash.c
  VPBROADCASTD   CONSTD_8(), Y18
  VPXORQ.BCST    siphashiv<>+0(SB), Z9, Z12  // v0 = k0 ^ seed0
  VPXORQ.BCST    siphashiv<>+8(SB), Z8, Z13  // v1 = k1 ^ seed1
  VPXORQ.BCST    siphashiv<>+16(SB), Z9, Z14 // v2 = k0 ^ seed2
  VPXORQ.BCST    siphashiv<>+24(SB), Z8, Z15 // v3 = k1 ^ seed3
  VPXORQ.BCST    CONSTQ_0xEE(), Z13, Z13     // v1 ^= 0xee
  VPMOVZXDQ      Y11, Z20       //
  VPSLLQ         $56, Z20, Z20  // Z20 = b = ((uint64_t)inlen) << 56
  JMP            main_loop_tail
loop:
  KMOVW          K2, K3
  VPGATHERDQ     0(R15)(Y10*1), K3, Z16 // Z16 = m
  VPXORQ         Z16, Z15, K2, Z15      // v3 ^= m
  SIPROUNDx2(K2, Z12, Z13, Z14, Z15)
  VPXORQ         Z16, Z12, K2, Z12      // v0 ^= m
  VPADDD         Y18, Y10, K2, Y10      // offset += 8
  VPSUBD         Y18, Y11, K2, Y11      // len -= 8
main_loop_tail:
  VPXORQ         Z16, Z16, Z16          // kill dependency
  VPCMPD         $VPCMP_IMM_GE, Y18, Y11, K1, K2
  KTESTW         K2, K2                 // K2 = lanes where len(input)>=8
  JNZ            loop
final_rounds:
  // load final fragments <8 bytes or 0 value for Z16
  VPTESTMD       Y11, Y11, K1, K2       // K2 = active && (len(inputs) != 0)
  VPGATHERDQ     0(R15)(Y10*1), K2, Z16 // b = load64(ptr)
  VPMOVZXDQ      Y11, Z11               // Z11 = remaining lengths as qwords
  VPERMQ         CONST_TAIL_MASK8(), Z11, Z21
  VPANDQ         Z21, Z16, Z16          // b &= mask
  VPORQ          Z20, Z16, Z16          // b |= (length<<56)
  VPXORQ         Z16, Z15, Z15          // v3 ^= b
  SIPROUNDx2(K1, Z12, Z13, Z14, Z15)
  VPXORQ         Z16, Z12, Z12          // v0 ^= b
  // 4 rounds of finalization for each 64 bits of output:
  VPXORQ.BCST    CONSTQ_0xEE(), Z14, Z14 // v2 ^= 0xee
  SIPROUNDx2(K1, Z12, Z13, Z14, Z15)
  SIPROUNDx2(K1, Z12, Z13, Z14, Z15)
  VPXORQ         Z12, Z13, Z16
  VPXORQ         Z14, Z15, Z17
  VPXORQ         Z17, Z16, Z9            // ret0 = v0 ^ v1 ^ v2 ^ v3 = lo64
  VPXORQ.BCST    CONSTQ_0xDD(), Z13, Z13 // v1 ^= 0xdd
  SIPROUNDx2(K1, Z12, Z13, Z14, Z15)
  SIPROUNDx2(K1, Z12, Z13, Z14, Z15)
  VPXORQ         Z12, Z13, Z16
  VPXORQ         Z14, Z15, Z17
  VPXORQ         Z17, Z16, Z10           // ret1 = v0 ^ v1 ^ v2 ^ v3 = hi64
  RET

// k[0] = hashmember(h[1], imm16[2]).k[3]
//
// given input hash[1], determine if there are members in tree[imm16[2]]
TEXT bchashmember(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_RU16_SLOT(BC_SLOT_SIZE*1, OUT(R8), OUT(R13), OUT(R15))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R15))

  KTESTW  K1, K1
  JZ      next

  ADDQ    VIRT_VALUES, R8       // R8 = pointer to input hash slot
  MOVQ    bytecode_trees(VIRT_BCPTR), R14
  MOVQ    0(R14)(R13*8), R13                     // R13 = tree pointer
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  BC_FILL_ONES(Z10)                // Z10 = all ones
  VPSRLD        $28, Z10, Z6       // Z6 = 0xf
  VPXORQ        Z14, Z14, Z14      // Z14 = constant 0
  VPXORQ        Z7, Z7, Z7         // Z7 = shift count

  // load table[0] into Z8 and copy to Z9
  MOVQ          radixTree64_index(R13), R15
  VMOVDQU32     0(R15), Z8         // Z8 = initial indices for (hash&mask)
  VMOVDQA32     Z8, Z9             // Z9 = same

  // extract low 32-bit words from hashes
  VPMOVQD       Z15, Y24
  VPMOVQD       Z16, Y25
  VINSERTI32X8  $1, Y25, Z24, Z11  // Z11 = lo32 x 16 words
  VPRORQ        $32, Z15, Z26      // rotate 32 bits to get hi 32
  VPRORQ        $32, Z16, Z27
  VPMOVQD       Z26, Y26
  VPMOVQD       Z27, Y27
  VINSERTI32X8  $1, Y27, Z26, Z12  // Z12 = hi32 x 16 words

  // compute the first table offset
  // as a permutation into the correct
  // initial slot (since we have a sixteen-wide splay)
  VPANDD        Z11, Z6, Z11
  VPANDD        Z12, Z6, Z12
  VPERMD        Z8, Z11, Z8
  VPERMD        Z9, Z12, Z9
  JMP           loop_tail

  // inner loop: i = table[i][(hash>>shift)&mask]; shift += 4;
  // Z8 or Z9 = i, Z17 and Z18 are 64-bit hashes
  //
  // loop while i > 0; perform two searches simultaneously
  // with active lanes marked as K2 and K3 respectively
loop:
  // lo 32 bits x 16 -> Z24
  VPMOVQD       Z17, Y24
  VPMOVQD       Z18, Y25
  VINSERTI32X8  $1, Y25, Z24, Z24

  // hi 32 bits x 16 -> Z25
  VPSRLQ        $32, Z17, Z25
  VPSRLQ        $32, Z18, Z26
  VPMOVQD       Z25, Y25
  VPMOVQD       Z26, Y26
  VINSERTI32X8  $1, Y26, Z25, Z25

  VPANDD        Z24, Z6, Z24  // lo 8 &= mask
  VPANDD        Z25, Z6, Z25  // hi 8 &= mask
  VPSLLD        $4, Z8, Z11   // Z11 = index * 16 = ptr0
  VPSLLD        $4, Z9, Z12   // Z12 = index * 16 = ptr1
  VPADDD        Z11, Z24, Z11 // Z11 = (index * 16) + (hash & mask)
  VPADDD        Z12, Z25, Z12 // Z12 = (index * 16) + (hash & mask)
  KMOVW         K2, K4
  VPGATHERDD    0(R15)(Z11*4), K4, Z8 // Z8 = table[Z8][(hash&mask)]
  KMOVW         K3, K5
  VPGATHERDD    0(R15)(Z12*4), K5, Z9 // Z9 = table[Z9][(hash&mask)]
loop_tail:
  VPRORQ        $4, Z17, Z17        // chomp 4 bits of hash
  VPRORQ        $4, Z18, Z18
  VPCMPD        $1, Z8, Z14, K2, K2 // select lanes with index > 0
  VPCMPD        $1, Z9, Z14, K3, K3
  KORTESTW      K2, K3
  JNZ           loop                // loop while any indices are non-negative

  // determine if values[i] == hash in each lane
  VPTESTMD      Z8, Z8, K1, K2  // select index != 0
  VPTESTMD      Z9, Z9, K1, K3  //
  VPXORD        Z8, Z10, K2, Z8 // ^idx = value index
  VPXORD        Z9, Z10, K3, Z9

  MOVQ          radixTree64_values(R13), R15

  // load and test against hash0
  VEXTRACTI32X8 $1, Z8, Y24            // upper 8 indices
  KMOVB         K2, K5
  VPGATHERDQ    0(R15)(Y8*1), K5, Z26  // Z26 = first 8 hashes
  KSHIFTRW      $8, K2, K5
  VPGATHERDQ    0(R15)(Y24*1), K5, Z27 // Z27 = second 8 hashes
  VPCMPEQQ      Z15, Z26, K2, K5       // K5 = lo 8 match
  KSHIFTRW      $8, K2, K6
  VPCMPEQQ      Z16, Z27, K6, K6       // K6 = hi 8 match
  KUNPCKBW      K5, K6, K2             // (K5||K6) -> K2 = found lanes

  // load and test against hash1 (same as above)
  KANDNQ        K3, K2, K3             // unset already found from K3
  VEXTRACTI32X8 $1, Z9, Y25            // lower 8 indices
  VPROLQ        $32, Z15, Z15          // first 8 rol 32
  VPROLQ        $32, Z16, Z16          // second 8 rol 32
  KMOVB         K3, K5
  VPGATHERDQ    0(R15)(Y9*1), K5, Z26
  KSHIFTRW      $8, K3, K5
  VPGATHERDQ    0(R15)(Y25*1), K5, Z27
  VPCMPEQQ      Z15, Z26, K3, K4
  KSHIFTRW      $8, K3, K6
  VPCMPEQQ      Z16, Z27, K6, K6
  KUNPCKBW      K4, K6, K3
  KORW          K2, K3, K1             // K1 = (matched hash0)|(matched hash1)

next:
  BC_UNPACK_SLOT(0, OUT(R8))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM16_SIZE)

// v[0].k[1] = hashlookup(h[2], imm16[3]).k[4]
//
// given input hash[imm0], determine
// if there are members in tree[imm1]
// and put them in the V register
TEXT bchashlookup(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_RU16_SLOT(BC_SLOT_SIZE*2, OUT(R8), OUT(R13), OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(BX))

  VPXORD X30, X30, X30
  VPXORD X31, X31, X31

  KTESTW  K1, K1
  JZ      next

  ADDQ    VIRT_VALUES, R8       // R8 = pointer to input hash slot
  MOVQ    bytecode_trees(VIRT_BCPTR), R14
  MOVQ    0(R14)(R13*8), R13                     // R13 = tree pointer
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  BC_FILL_ONES(Z10)                       // Z10 = all ones
  VPSRLD        $28, Z10, Z6       // Z6 = 0xf
  VPXORQ        Z14, Z14, Z14      // Z14 = constant 0
  VPXORQ        Z7, Z7, Z7         // Z7 = shift count

  // load table[0] into Z8 and copy to Z9
  MOVQ          radixTree64_index(R13), R15
  VMOVDQU32     0(R15), Z8         // Z8 = initial indices for (hash&mask)
  VMOVDQA32     Z8, Z9             // Z9 = same

  // extract low 32-bit words from hashes
  VPMOVQD       Z15, Y24
  VPMOVQD       Z16, Y25
  VINSERTI32X8  $1, Y25, Z24, Z11  // Z11 = lo32 x 16 words
  VPRORQ        $32, Z15, Z26      // rotate 32 bits to get hi 32
  VPRORQ        $32, Z16, Z27
  VPMOVQD       Z26, Y26
  VPMOVQD       Z27, Y27
  VINSERTI32X8  $1, Y27, Z26, Z12  // Z12 = hi32 x 16 words

  // compute the first table offset
  // as a permutation into the correct
  // initial slot (since we have a sixteen-wide splay)
  VPANDD        Z11, Z6, Z11
  VPANDD        Z12, Z6, Z12
  VPERMD        Z8, Z11, Z8
  VPERMD        Z9, Z12, Z9
  JMP           loop_tail

  // inner loop: i = table[i][(hash>>shift)&mask]; shift += 4;
  // Z8 or Z9 = i, Z17 and Z18 are 64-bit hashes
  //
  // loop while i > 0; perform two searches simultaneously
  // with active lanes marked as K2 and K3 respectively
loop:
  // lo 32 bits x 16 -> Z24
  VPMOVQD       Z17, Y24
  VPMOVQD       Z18, Y25
  VINSERTI32X8  $1, Y25, Z24, Z24

  // hi 32 bits x 16 -> Z25
  VPSRLQ        $32, Z17, Z25
  VPSRLQ        $32, Z18, Z26
  VPMOVQD       Z25, Y25
  VPMOVQD       Z26, Y26
  VINSERTI32X8  $1, Y26, Z25, Z25

  VPANDD        Z24, Z6, Z24  // lo 8 &= mask
  VPANDD        Z25, Z6, Z25  // hi 8 &= mask
  VPSLLD        $4, Z8, Z11   // Z11 = index * 16 = ptr0
  VPSLLD        $4, Z9, Z12   // Z12 = index * 16 = ptr1
  VPADDD        Z11, Z24, Z11 // Z11 = (index * 16) + (hash & mask)
  VPADDD        Z12, Z25, Z12 // Z12 = (index * 16) + (hash & mask)
  KMOVW         K2, K4
  VPGATHERDD    0(R15)(Z11*4), K4, Z8 // Z8 = table[Z8][(hash&mask)]
  KMOVW         K3, K5
  VPGATHERDD    0(R15)(Z12*4), K5, Z9 // Z9 = table[Z9][(hash&mask)]

loop_tail:
  VPRORQ        $4, Z17, Z17        // chomp 4 bits of hash
  VPRORQ        $4, Z18, Z18
  VPCMPD        $1, Z8, Z14, K2, K2 // select lanes with index > 0
  VPCMPD        $1, Z9, Z14, K3, K3
  KORTESTW      K2, K3
  JNZ           loop                // loop while any indices are non-negative

  // determine if values[i] == hash in each lane
  VPTESTMD      Z8, Z8, K1, K2  // select index != 0
  VPTESTMD      Z9, Z9, K1, K3  //
  VPXORD        Z8, Z10, K2, Z8 // ^idx = value index
  VPXORD        Z9, Z10, K3, Z9

  MOVQ          radixTree64_values(R13), R15

  // load and test against hash0
  VEXTRACTI32X8 $1, Z8, Y24            // upper 8 indices
  KMOVB         K2, K5
  VPGATHERDQ    0(R15)(Y8*1), K5, Z26  // Z26 = first 8 hashes
  KSHIFTRW      $8, K2, K5
  VPGATHERDQ    0(R15)(Y24*1), K5, Z27 // Z27 = second 8 hashes
  VPCMPEQQ      Z15, Z26, K2, K5       // K5 = lo 8 match
  KMOVB         K5, K6
  KSHIFTRW      $8, K2, K6
  VPCMPEQQ      Z16, Z27, K6, K6       // K6 = hi 8 match
  KUNPCKBW      K5, K6, K2             // (K5||K6) -> K2 = found lanes

  // load and test against hash1 (same as above)
  KANDNQ        K3, K2, K3             // unset already found from K3
  VEXTRACTI32X8 $1, Z9, Y25            // lower 8 indices
  VPROLQ        $32, Z15, Z15          // first 8 rol 32
  VPROLQ        $32, Z16, Z16          // second 8 rol 32
  KMOVB         K3, K5
  VPGATHERDQ    0(R15)(Y9*1), K5, Z26
  KSHIFTRW      $8, K3, K5
  VPGATHERDQ    0(R15)(Y25*1), K5, Z27
  VPCMPEQQ      Z15, Z26, K3, K4
  KSHIFTRW      $8, K3, K6
  VPCMPEQQ      Z16, Z27, K6, K6
  KUNPCKBW      K4, K6, K3
  VMOVDQA32     Z9, K3, Z8             // Z8 = good offsets
  KORW          K2, K3, K1             // K1 = (matched hash0)|(matched hash1)
  KMOVW         K1, K2
  VPXORD        X30, X30, X30
  VPGATHERDD    8(R15)(Z8*1), K2, Z30   // load boxed offsets
  VPXORD        X31, X31, X31
  KMOVW         K1, K3
  VPGATHERDD    12(R15)(Z8*1), K3, Z31  // load boxed lengths

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))

  // read TLV byte and calculate header length
  VPBROADCASTD CONSTD_1(), Z8          // Z8 <- dword(0xFF)
  VPBROADCASTD CONSTD_14(), Z9         // Z9 <- dword(14)

  KMOVW K1, K2
  VPXORD X2, X2, X2
  VPGATHERDD 0(VIRT_BASE)(Z30*1), K2, Z2

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(CX))
  BC_CALC_VALUE_HLEN(OUT(Z3), IN(Z31), IN(K1), IN(Z8), IN(Z9), Z5, K2)

  BC_STORE_VALUE_TO_SLOT(IN(Z30), IN(Z31), IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_IMM16_SIZE)


// Simple Aggregation Instructions
// -------------------------------

// _ = aggand.k(a[0], k[1]).k[2]
TEXT bcaggandk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(CX))
  BC_UNPACK_RU32(0, OUT(DX))

  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))
  BC_LOAD_RU16_FROM_SLOT(OUT(CX), IN(CX))

  ORB CX, 8(VIRT_AGG_BUFFER)(DX*1)     // Mark this aggregation slot if we have non-null lanes
  ANDL CX, BX                          // BX <- Boolean values in non-null lanes

  XORL R8, R8                          // If CX != BX it means that at least one lane is active and that not all BOOLs
  CMPL CX, BX                          // in active lanes are TRUE - this would result in FALSE if not already FALSE.

  SETEQ R8
  ANDB R8, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggor.k(a[0], k[1]).k[2]
TEXT bcaggork(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(CX))
  BC_UNPACK_RU32(0, OUT(DX))

  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))
  BC_LOAD_RU16_FROM_SLOT(OUT(CX), IN(CX))

  ORB CX, 8(VIRT_AGG_BUFFER)(DX*1)     // Mark this aggregation slot if we have non-null lanes

  XORL R8, R8                          // If CX & BX != 0 it means that at least one lane is active and that not all BOOLs
  ANDL CX, BX                          // in active lanes are FALSE - this would result in TRUE if not already TRUE.

  SETNE R8
  ORB R8, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

#include "evalbc_aggsumf.h"

// _ = aggsum.f64(a[0], s[1]).k[2]
TEXT bcaggsumf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K2))

  BC_UNPACK_RU32(0, OUT(DX))
  ADDQ VIRT_AGG_BUFFER, DX

  BC_NEUMAIER_SUM(Z4, Z5, K4, K5, DX, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, Z16, Z17, Z18, Z19, Z20)

  KMOVW K1, R15
  POPCNTQ R15, R15
  ADDQ R15, (32*8)(DX)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggsum.i64(a[0], s[1]).k[2]
TEXT bcaggsumi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K2))

  KMOVW K1, R15
  VPADDQ Z4, Z5, Z5
  VEXTRACTI64X4 $1, Z5, Y4
  VPADDQ Y4, Y5, Y5
  VEXTRACTI64X2 $1, Y5, X4
  VPADDQ X4, X5, X5

  BC_UNPACK_RU32(0, OUT(DX))
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPADDQ X4, X5, X5

  POPCNTL R15, R15
  VPADDQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggmin.f64(a[0], s[1]).k[2]
TEXT bcaggminf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  VBROADCASTSD CONSTF64_POSITIVE_INF(), Z5
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMINPD 0(VIRT_VALUES)(BX*1), Z5, K1, Z5
  VMINPD 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTF64X4 $1, Z5, Y4
  VMINPD Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTF64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VMINPD X4, X5, X5
  VSHUFPD $1, X5, X5, X4
  VMINSD X4, X5, X5

  VMINSD 0(VIRT_AGG_BUFFER)(DX*1), X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVSD X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggmin.i64(a[0], s[1]).k[2]
TEXT bcaggmini(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  VPBROADCASTQ CONSTQ_0x7FFFFFFFFFFFFFFF(), Z5
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPMINSQ 0(VIRT_VALUES)(BX*1), Z5, K1, Z5
  VPMINSQ 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTI64X4 $1, Z5, Y4
  VPMINSQ Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTI64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VPMINSQ X4, X5, X5
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPMINSQ X4, X5, X5

  VPMINSQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggmax.f64(a[0], s[1]).k[2]
TEXT bcaggmaxf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  VBROADCASTSD CONSTF64_NEGATIVE_INF(), Z5
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMAXPD 0(VIRT_VALUES)(BX*1), Z5, K1, Z5
  VMAXPD 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTF64X4 $1, Z5, Y4
  VMAXPD Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTF64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VMAXPD X4, X5, X5
  VSHUFPD $1, X5, X5, X4
  VMAXSD X4, X5, X5

  VMAXSD 0(VIRT_AGG_BUFFER)(DX*1), X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVSD X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggmax.i64(a[0], s[1]).k[2]
TEXT bcaggmaxi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  VPBROADCASTQ CONSTQ_0x8000000000000000(), Z5
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPMAXSQ 0(VIRT_VALUES)(BX*1), Z5, K1, Z5
  VPMAXSQ 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTI64X4 $1, Z5, Y4
  VPMAXSQ Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTI64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VPMAXSQ X4, X5, X5
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPMAXSQ X4, X5, X5

  VPMAXSQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggand.i64(a[0], s[1]).k[2]
TEXT bcaggandi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  VPBROADCASTQ CONSTQ_0xFFFFFFFFFFFFFFFF(), Z5
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPANDQ 0(VIRT_VALUES)(BX*1), Z5, K1, Z5
  VPANDQ 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTI64X4 $1, Z5, Y4
  VPANDQ Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTI64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VPANDQ X4, X5, X5
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPANDQ X4, X5, X5

  VPANDQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggor.i64(a[0], s[1]).k[2]
TEXT bcaggori(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z5
  VPORQ 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTI64X4 $1, Z5, Y4
  VPORQ Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTI64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VPORQ X4, X5, X5
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPORQ X4, X5, X5

  VPORQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggxor.i64(a[0], s[1]).k[2]
TEXT bcaggxori(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z5
  VPXORQ 64(VIRT_VALUES)(BX*1), Z5, K2, Z5

  KMOVW K1, R15
  VEXTRACTI64X4 $1, Z5, Y4
  VPXORQ Y4, Y5, Y5
  POPCNTL R15, R15
  VEXTRACTI64X2 $1, Y5, X4

  BC_UNPACK_RU32(0, OUT(DX))
  VPXORQ X4, X5, X5
  VMOVQ 0(VIRT_AGG_BUFFER)(DX*1), X6
  VPSHUFD $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPXORQ X4, X5, X5

  VPXORQ X6, X5, X5
  ADDQ R15, 8(VIRT_AGG_BUFFER)(DX*1)
  VMOVQ X5, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// _ = aggcount(a[0]).k[1]
TEXT bcaggcount(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_AGGSLOT_SIZE, OUT(BX))
  BC_UNPACK_RU32(0, OUT(DX))
  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))

  POPCNTQ BX, BX
  ADDQ BX, 0(VIRT_AGG_BUFFER)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*1 + BC_AGGSLOT_SIZE)


// _ = aggmergestate(a[0], s[1]).k[2]
TEXT bcaggmergestate(SB), NOSPLIT|NOFRAME, $0
    // bcAggSlot, bcReadS, bcPredicate
    BC_UNPACK_RU32(0, OUT(BX))
    BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(R8))
    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

    VMOVDQU32   BC_VSTACK_PTR(DX, 0),  Z1
    VMOVDQU32   BC_VSTACK_PTR(DX, 64), Z2

    ADDQ        VIRT_AGG_BUFFER, BX         // slot address
    VMOVDQU32   Z1, K1,  0(BX)
    VMOVDQU32   Z2, K1, 64(BX)

    NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

// Slot Aggregation Instructions
// -----------------------------

// take the value of the H register
// and locate the entries associated with
// each hash (for each lane where K1!=0);
//
// returns early if it cannot locate all of K1
//
// l[0] = aggbucket(h[1]).k[2]
TEXT bcaggbucket(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(R8), OUT(R15))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R15))

  KTESTW  K1, K1
  JZ      next

  ADDQ    VIRT_VALUES, R8 // R8 = pointer to input hash slot
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VMOVDQA64   Z15, Z17     // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  BC_FILL_ONES(Z10)                // Z10 = all ones
  VPSRLD        $28, Z10, Z6       // Z6 = 0xf
  VPXORQ        Z14, Z14, Z14      // Z14 = constant 0
  VPXORQ        Z7, Z7, Z7         // Z7 = shift count

  // load table[0] into Z8 and copy to Z9
  MOVQ          radixTree64_index(VIRT_AGG_BUFFER), R15
  VMOVDQU32     0(R15), Z8         // Z8 = initial indices for (hash&mask)
  VMOVDQA32     Z8, Z9             // Z9 = same

  // extract low 32-bit words from hashes
  VPMOVQD       Z15, Y24
  VPMOVQD       Z16, Y25
  VINSERTI32X8  $1, Y25, Z24, Z11  // Z11 = lo32 x 16 words
  VPRORQ        $32, Z15, Z26      // rotate 32 bits to get hi 32
  VPRORQ        $32, Z16, Z27
  VPMOVQD       Z26, Y26
  VPMOVQD       Z27, Y27
  VINSERTI32X8  $1, Y27, Z26, Z12  // Z12 = hi32 x 16 words

  // compute the first table offset
  // as a permutation into the correct
  // initial slot (since we have a sixteen-wide splay)
  VPANDD        Z11, Z6, Z11
  VPANDD        Z12, Z6, Z12
  VPERMD        Z8, Z11, Z8
  VPERMD        Z9, Z12, Z9
  JMP           loop_tail

  // inner loop: i = table[i][(hash>>shift)&mask]; shift += 4;
  // Z8 or Z9 = i, Z17 and Z18 are 64-bit hashes
  //
  // loop while i > 0; perform two searches simultaneously
  // with active lanes marked as K2 and K3 respectively
loop:
  // lo 32 bits x 16 -> Z24
  VPMOVQD       Z17, Y24
  VPMOVQD       Z18, Y25
  VINSERTI32X8  $1, Y25, Z24, Z24

  // hi 32 bits x 16 -> Z25
  VPSRLQ        $32, Z17, Z25
  VPSRLQ        $32, Z18, Z26
  VPMOVQD       Z25, Y25
  VPMOVQD       Z26, Y26
  VINSERTI32X8  $1, Y26, Z25, Z25

  VPANDD        Z24, Z6, Z24  // lo 8 &= mask
  VPANDD        Z25, Z6, Z25  // hi 8 &= mask
  VPSLLD        $4, Z8, Z11   // Z11 = index * 16 = ptr0
  VPSLLD        $4, Z9, Z12   // Z12 = index * 16 = ptr1
  VPADDD        Z11, Z24, Z11 // Z11 = (index * 16) + (hash & mask)
  VPADDD        Z12, Z25, Z12 // Z12 = (index * 16) + (hash & mask)
  KMOVW         K2, K4
  VPGATHERDD    0(R15)(Z11*4), K4, Z8 // Z8 = table[Z8][(hash&mask)]
  KMOVW         K3, K5
  VPGATHERDD    0(R15)(Z12*4), K5, Z9 // Z9 = table[Z9][(hash&mask)]
loop_tail:
  VPRORQ        $4, Z17, Z17        // chomp 4 bits of hash
  VPRORQ        $4, Z18, Z18
  VPCMPD        $1, Z8, Z14, K2, K2 // select lanes with index > 0
  VPCMPD        $1, Z9, Z14, K3, K3
  KORTESTW      K2, K3
  JNZ           loop                // loop while any indices are non-negative

  // determine if values[i] == hash in each lane
  VPTESTMD      Z8, Z8, K1, K2  // select index != 0
  VPTESTMD      Z9, Z9, K1, K3  //
  VPXORD        Z8, Z10, K2, Z8 // ^idx = value index
  VPXORD        Z9, Z10, K3, Z9

  MOVQ          radixTree64_values(VIRT_AGG_BUFFER), R15

  // load and test against hash0
  VEXTRACTI32X8 $1, Z8, Y24            // upper 8 indices
  KMOVB         K2, K5
  VPGATHERDQ    0(R15)(Y8*1), K5, Z26  // Z26 = first 8 hashes
  KSHIFTRW      $8, K2, K5
  VPGATHERDQ    0(R15)(Y24*1), K5, Z27 // Z27 = second 8 hashes
  VPCMPEQQ      Z15, Z26, K2, K5       // K5 = lo 8 match
  KSHIFTRW      $8, K2, K6
  VPCMPEQQ      Z16, Z27, K6, K6       // K6 = hi 8 match
  KUNPCKBW      K5, K6, K2             // (K5||K6) -> K2 = found lanes
  VMOVDQA32.Z   Z8, K2, Z13            // Z13 = ret

  // load and test against hash1 (same as above)
  VEXTRACTI32X8 $1, Z9, Y25            // lower 8 indices
  VPROLQ        $32, Z15, Z15          // first 8 rol 32
  VPROLQ        $32, Z16, Z16          // second 8 rol 32
  KANDNQ        K3, K2, K3             // unset already found from K3
  KMOVB         K3, K5
  VPGATHERDQ    0(R15)(Y9*1), K5, Z26
  KSHIFTRW      $8, K3, K5
  VPGATHERDQ    0(R15)(Y25*1), K5, Z27
  VPCMPEQQ      Z15, Z26, K3, K4
  KSHIFTRW      $8, K3, K6
  VPCMPEQQ      Z16, Z27, K6, K6
  KUNPCKBW      K4, K6, K3
  VMOVDQA32     Z9, K3, Z13            // add matched offsets to ret
  KORW          K2, K3, K2             // K2 = found

  // now test that we found everything we wanted
  KXORW         K2, K1, K2         // K1^K2 = found xor wanted
  KTESTW        K2, K2             // (K1^K2)!=0 -> found != wanted
  JNZ           early_ret          // we didn't locate entries!

next:
  // perform a sanity bounds-check on the returned offsets;
  // each offset should be <= len(tree.values)
  VPCMPD.BCST   $VPCMP_IMM_GT, radixTree64_values+8(VIRT_AGG_BUFFER), Z13, K1, K4
  KTESTW        K4, K4
  JNZ           bad_radix_bucket
  VMOVDQU32     Z13, 0(VIRT_VALUES)(DX*1)
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

early_ret:
  // set bytecode.err to NeedRadix
  // and bytecode.errinfo to the hash slot
  MOVL $const_bcerrNeedRadix, bytecode_err(VIRT_BCPTR)
  BC_UNPACK_SLOT(BC_SLOT_SIZE, OUT(R8))
  MOVQ R8, bytecode_errinfo(VIRT_BCPTR)
  RET_ABORT()

bad_radix_bucket:
  // set bytecode.err to TreeCorrupt
  // and set bytecode.errpc to this pc
  MOVL    $const_bcerrTreeCorrupt, bytecode_err(VIRT_BCPTR)
  SUBQ    bytecode_compiled(VIRT_BCPTR), VIRT_PCREG // get relative position
  MOVL    VIRT_PCREG, bytecode_errpc(VIRT_BCPTR)
  RET_ABORT()

// All aggregate operations except AVG aggregate the value and then mark
// slot+1, so we can decide whether the result of the aggregation should
// be the aggregated value or NULL - in other words it basically describes
// whether there was at least one aggregation.
//
// Expects 64-bit sources in Z4 and Z5, buckets in Z6
#define BC_AGGREGATE_SLOT_MARK_OP(SlotOffset, Instruction)                    \
  VPCONFLICTD.Z Z6, K1, Z11                                                   \
  VEXTRACTI32X8 $1, Z6, Y7                                                    \
                                                                              \
  /* Load the aggregation data pointer. */                                    \
  MOVL SlotOffset(VIRT_PCREG), R15                                            \
  ADDQ $const_aggregateTagSize, R15                                           \
  ADDQ radixTree64_values(VIRT_AGG_BUFFER), R15                               \
                                                                              \
  /* Mark all values that we are gonna update. */                             \
  VPBROADCASTD CONSTD_1(), Z10                                                \
  KMOVW K1, K2                                                                \
  VPSCATTERDD Z10, K2, 8(R15)(Z6*1)                                           \
                                                                              \
  /* Gather the first low 8 values, which are safe to gather at this point. */\
  KMOVB K1, K2                                                                \
  VPXORQ X14, X14, X14                                                        \
  VGATHERDPD 0(R15)(Y6*1), K2, Z14                                            \
                                                                              \
  /* Skip the loop if there are no conflicts. */                              \
  VPANDD CONST_GET_PTR(aggregate_conflictdq_mask, 0), Z11, Z11                \
  VPTESTMD Z11, Z11, K1, K2                                                   \
  KTESTW K2, K2                                                               \
  JZ resolved                                                                 \
                                                                              \
  /* Calculate a predicate for VPERMQ so we can swizzle sources. */           \
  VMOVDQU32 CONST_GET_PTR(aggregate_conflictdq_norm, 0), Z10                  \
  VPLZCNTD Z11, Z12                                                           \
  VPSUBD Z12, Z10, Z12                                                        \
  VEXTRACTI32X8 $1, Z12, Y13                                                  \
  VPMOVZXDQ Y12, Z12                                                          \
  VPMOVZXDQ Y13, Z13                                                          \
                                                                              \
loop:                                                                         \
  /* Z10 - broadcasted conflicting lanes. */                                  \
  VPBROADCASTMW2D K2, Z10                                                     \
                                                                              \
  /* Swizzle sources so we can aggregate conflicting lanes. */                \
  VPERMQ Z4, Z12, Z8                                                          \
  VPERMQ Z5, Z13, Z9                                                          \
                                                                              \
  /* K4/K5 - resolved conflicts in this iteration. */                         \
  VPTESTNMD Z11, Z10, K2, K4                                                  \
  KSHIFTRW $8, K4, K5                                                         \
                                                                              \
  /* K2 - remaining conflicts (to be resolved in the next iteration.) */      \
  KANDNW K2, K4, K2                                                           \
                                                                              \
  /* Aggregate conflicting lanes and mask out lanes we have resolved. */      \
  Instruction Z8, Z4, K4, Z4                                                  \
  Instruction Z9, Z5, K5, Z5                                                  \
                                                                              \
  /* Continue looping if there are still conflicts. */                        \
  KTESTW K2, K2                                                               \
  JNZ loop                                                                    \
                                                                              \
resolved:                                                                     \
  /* Finally, aggregate non-conflicting sources into buckets. */              \
  Instruction Z4, Z14, K1, Z14                                                \
  KMOVB K1, K2                                                                \
  VSCATTERDPD Z14, K2, 0(R15)(Y6*1)                                           \
                                                                              \
  KMOVB K6, K2                                                                \
  VPXORQ X14, X14, X14                                                        \
  VGATHERDPD 0(R15)(Y7*1), K2, Z14                                            \
  Instruction Z5, Z14, K6, Z14                                                \
  VSCATTERDPD Z14, K6, 0(R15)(Y7*1)                                           \
                                                                              \
next:

// This macro is used to implement AVG, which requires more than just a mark.
//
// In order to calculate the average we aggregate the value and also a count
// of values aggregated, this count will then be used to calculate the final
// average and also to decide whether the result is NULL or non-NULL. If the
// COUNT is zero, the result of the aggregation is NULL.
//
// Expects 64-bit sources in Z4 and Z5, buckets in Z6
#define BC_AGGREGATE_SLOT_COUNT_OP(SlotOffset, Instruction)                   \
  VPCONFLICTD.Z Z6, K1, Z11                                                   \
  VEXTRACTI32X8 $1, Z6, Y7                                                    \
                                                                              \
  /* Load the aggregation data pointer. */                                    \
  MOVL  SlotOffset(VIRT_PCREG), R15                                           \
  ADDQ $const_aggregateTagSize, R15                                           \
  ADDQ radixTree64_values(VIRT_AGG_BUFFER), R15                               \
                                                                              \
  /* Gather the first low 8 values, which are safe to gather at this point. */\
  KMOVB K1, K2                                                                \
  VPXORQ X14, X14, X14                                                        \
  VGATHERDPD 0(R15)(Y6*1), K2, Z14                                            \
                                                                              \
  /* Initial COUNT values - conflicts will be resolved later, if any... */    \
  VPBROADCASTD CONSTD_1(), Z15                                                \
                                                                              \
  /* Skip the conflict resolution if there are no conflicts. */               \
  VPANDD CONST_GET_PTR(aggregate_conflictdq_mask, 0), Z11, Z11                \
  VPTESTMD Z11, Z11, K1, K2                                                   \
  KTESTW K2, K2                                                               \
  JZ resolved                                                                 \
                                                                              \
  /* Calculate a predicate for VPERMQ so we can swizzle sources. */           \
  VMOVDQU32 CONST_GET_PTR(aggregate_conflictdq_norm, 0), Z10                  \
  VPLZCNTD Z11, Z12                                                           \
  VPSUBD Z12, Z10, Z12                                                        \
  VEXTRACTI32X8 $1, Z12, Y13                                                  \
  VPMOVZXDQ Y12, Z12                                                          \
  VPMOVZXDQ Y13, Z13                                                          \
                                                                              \
  /* Z16 - ones, for incrementing COUNTs of conflicting lanes. */             \
  VMOVDQA32 Z15, Z16                                                          \
                                                                              \
loop:                                                                         \
  /* Z10 - broadcasted conflicting lanes. */                                  \
  VPBROADCASTMW2D K2, Z10                                                     \
                                                                              \
  /* Swizzle sources so we can aggregate conflicting lanes. */                \
  VPERMQ Z4, Z12, Z8                                                          \
  VPERMQ Z5, Z13, Z9                                                          \
                                                                              \
  /* K4/K5 - resolved conflicts in this iteration. */                         \
  VPTESTNMD Z11, Z10, K2, K4                                                  \
  KSHIFTRW $8, K4, K5                                                         \
                                                                              \
  /* Adds COUNTs of conflicting lanes iteratively. */                         \
  VPADDD Z16, Z15, K2, Z15                                                    \
                                                                              \
  /* K2 - remaining conflicts (to be resolved in the next iteration.) */      \
  KANDNW K2, K4, K2                                                           \
                                                                              \
  /* Aggregate conflicting lanes and mask out lanes we have resolved. */      \
  Instruction Z8, Z4, K4, Z4                                                  \
  Instruction Z9, Z5, K5, Z5                                                  \
                                                                              \
  /* Continue looping if there are still conflicts. */                        \
  KTESTW K2, K2                                                               \
  JNZ loop                                                                    \
                                                                              \
resolved:                                                                     \
  /* Gather first 8 COUNTs. */                                                \
  VPXORQ X13, X13, X13                                                        \
  KMOVB K1, K2                                                                \
  VPGATHERDQ 8(R15)(Y6*1), K2, Z13                                            \
                                                                              \
  /* Convert COUNT aggregates from DWORD to QWORD, so we can add them. */     \
  VEXTRACTI32X8 $1, Z15, Y16                                                  \
  VPMOVZXDQ Y15, Z15                                                          \
  VPMOVZXDQ Y16, Z16                                                          \
                                                                              \
  /* Aggregate non-conflicting values and COUNTs into buckets (low). */       \
  Instruction Z4, Z14, K1, Z14                                                \
  VPADDQ Z15, Z13, K1, Z13                                                    \
  KMOVB K1, K2                                                                \
  VSCATTERDPD Z14, K2, 0(R15)(Y6*1)                                           \
  KMOVB K1, K2                                                                \
  VPSCATTERDQ Z13, K2, 8(R15)(Y6*1)                                           \
                                                                              \
  /* Aggregate non-conflicting values and COUNTs into buckets (high). */      \
  VPXORQ X14, X14, X14                                                        \
  VPXORQ X13, X13, X13                                                        \
  KMOVB K6, K2                                                                \
  KMOVB K6, K3                                                                \
  VGATHERDPD 0(R15)(Y7*1), K2, Z14                                            \
  VPGATHERDQ 8(R15)(Y7*1), K3, Z13                                            \
  KMOVB K6, K2                                                                \
  Instruction Z5, Z14, K2, Z14                                                \
  VPADDQ Z16, Z13, K2, Z13                                                    \
  VSCATTERDPD Z14, K2, 0(R15)(Y7*1)                                           \
  VPSCATTERDQ Z13, K6, 8(R15)(Y7*1)                                           \
                                                                              \
next:

// _ = aggslotand.k(a[0], l[1], k[2], k[3])
TEXT bcaggslotandk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K4), OUT(K5), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  VPMOVM2Q K4, Z4
  VPMOVM2Q K5, Z5
  BC_AGGREGATE_SLOT_MARK_OP(0, VPANDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotor.k(a[0], l[1], k[2], k[3])
TEXT bcaggslotork(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K4), OUT(K5), IN(BX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  VPMOVM2Q K4, Z4
  VPMOVM2Q K5, Z5
  BC_AGGREGATE_SLOT_MARK_OP(0, VPORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotsum.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotsumi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPADDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotavg.f64(a[0], l[1], s[2], k[3])
TEXT bcaggslotavgf(SB), NOSPLIT|NOFRAME, $0
  JMP bcaggslotsumf(SB)
  RET

// _ = aggslotavg.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotavgi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_COUNT_OP(0, VPADDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotmin.f64(a[0], l[1], s[2], k[3])
TEXT bcaggslotminf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VMINPD)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotmin.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotmini(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPMINSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotmax.f64(a[0], l[1], s[2], k[3])
TEXT bcaggslotmaxf(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VMAXPD)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotmax.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotmaxi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPMAXSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotand.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotandi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPANDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotor.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotori(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// _ = aggslotxor.i64(a[0], l[1], s[2], k[3])
TEXT bcaggslotxori(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  BC_AGGREGATE_SLOT_MARK_OP(0, VPXORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)

// COUNT is a special aggregation function that just counts active lanes stored
// in K1. This is the simplest aggregation, which only requres a basic conflict
// resolution that doesn't require to loop over conflicting lanes.
//
// _ = aggslotcount(a[0], l[1], k[2])
TEXT bcaggslotcount(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(BX))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  VPCONFLICTD.Z Z6, K1, Z8

  // Load the aggregation data pointer and prepare high 8 element offsets.
  MOVL 0(VIRT_PCREG), R15
  ADDQ radixTree64_values(VIRT_AGG_BUFFER), R15
  VEXTRACTI32X8 $1, Z6, Y7

  // Z4/Z5 <- gather all 16 lanes representing the current COUNT.
  KMOVB K1, K2
  KSHIFTRW $8, K1, K3
  VPGATHERDQ 8(R15)(Y6*1), K2, Z4
  VPGATHERDQ 8(R15)(Y7*1), K3, Z5

  // Now resolve COUNT conflicts. We know that the most significant element
  // is stored last by scatters, and we know, that conflict detection goes
  // from the most significant to least significant, so the conflicts are
  // resolved in the correct order respecting scatter.
  //
  // NOTE: It would be easier to use VPOPCNTD, but unfortunately it's not
  // available on all machines, so we do the popcount with VPSHUFB, which
  // is like 10 instructions longer, but we can still do it.
  //
  // VPMADDUBSW is used to horizontally add two bytes, Z10 is a vector of
  // 0x0101 values, thus multiplying all bytes with 1, and summing them.
  VBROADCASTI32X4 CONST_GET_PTR(popcnt_nibble, 0), Z10
  VPSRLD $4, Z8, Z9
  VPANDD.BCST CONSTD_0x0F0F0F0F(), Z8, Z8
  VPANDD.BCST CONSTD_0x0F0F0F0F(), Z9, Z9
  VPSHUFB Z8, Z10, Z8
  VPSHUFB Z9, Z10, Z9
  VPBROADCASTD CONSTD_0x01010101(), Z10
  VPADDD Z9, Z8, Z8
  VPMADDUBSW Z10, Z8, Z8

  // Aggregate and store the new COUNT of elements.
  VPADDD.BCST CONSTD_1(), Z8, Z8
  KMOVB K1, K2
  KSHIFTRW $8, K1, K3
  VEXTRACTI32X8 $1, Z8, Y9
  VPMOVZXDQ Y8, Z8
  VPMOVZXDQ Y9, Z9
  VPADDQ Z8, Z4, Z4
  VPADDQ Z9, Z5, Z5
  VPSCATTERDQ Z4, K2, 8(R15)(Y6*1)
  VPSCATTERDQ Z5, K3, 8(R15)(Y7*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)

TEXT bcaggslotcount_v2(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(BX))
  BC_LOAD_BUCKET_FROM_SLOT(OUT(Z6), IN(DX), IN(K1))
  VPCONFLICTD.Z Z6, K1, Z8

  // Load the aggregation data pointer and prepare high 8 element offsets.
  MOVL 0(VIRT_PCREG), R15
  ADDQ radixTree64_values(VIRT_AGG_BUFFER), R15
  VEXTRACTI32X8 $1, Z6, Y7

  // Z4/Z5 <- gather all 16 lanes representing the current COUNT.
  KMOVB K1, K2
  KSHIFTRW $8, K1, K3
  KMOVB K3, K6
  VPGATHERDQ 8(R15)(Y6*1), K2, Z4
  VPGATHERDQ 8(R15)(Y7*1), K3, Z5

  // Now resolve COUNT conflicts. We know that the most significant element
  // is stored last by scatters, and we know, that conflict detection goes
  // from the most significant to least significant, so the conflicts are
  // resolved in the correct order respecting scatter.
  VPOPCNTD  Z8, Z8

  // Aggregate and store the new COUNT of elements.
  VPADDD.BCST CONSTD_1(), Z8, Z8
  KMOVB K1, K2
  VEXTRACTI32X8 $1, Z8, Y9
  VPMOVZXDQ Y8, Z8
  VPMOVZXDQ Y9, Z9
  VPADDQ Z8, Z4, Z4
  VPADDQ Z9, Z5, Z5
  VPSCATTERDQ Z4, K2, 8(R15)(Y6*1)
  VPSCATTERDQ Z5, K6, 8(R15)(Y7*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_AGGSLOT_SIZE)


// _ = aggslotmergestate(a[0], l[1], s[2]).k[3]
TEXT bcaggslotmergestate(SB), NOSPLIT|NOFRAME, $0
    BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(CX), OUT(R8))
    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

    BC_UNPACK_RU32(0, OUT(BX))
    ADDQ $const_aggregateTagSize, BX
    ADDQ radixTree64_values(VIRT_AGG_BUFFER), BX

    // copy 16 x 32-bit bucket offsets
    BC_FILL_ONES(Z1)                        // unused slot = -1
    VMOVDQU32   BC_VSTACK_PTR(DX, 0), K1, Z1
    VMOVDQU32   Z1, (0*64)(BX)

    // copy 16 x vmref to value
    VMOVDQU32   BC_VSTACK_PTR(CX, 0),  Z1
    VMOVDQU32   BC_VSTACK_PTR(CX, 64), Z2
    VMOVDQU32   Z1, (1*64)(BX)
    VMOVDQU32   Z2, (2*64)(BX)

    NEXT_ADVANCE(BC_AGGSLOT_SIZE + BC_SLOT_SIZE*3)


// Uncategorized Instructions
// --------------------------

// take two immediate offsets into the scratch buffer and broadcast them into registers
//
// v[0] = litref(d[1])
TEXT bclitref(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(DX))

  VPBROADCASTD (BC_SLOT_SIZE+0)(VIRT_PCREG), Z2             // Z2 <- value offset part
  VPBROADCASTD.Z (BC_SLOT_SIZE+4)(VIRT_PCREG), K7, Z3       // Z3 <- value length part
  VPADDD.BCST.Z bytecode_scratchoff(VIRT_BCPTR), Z2, K7, Z2 // Z2 <- value offset relative to VM
  VPBROADCASTB.Z (BC_SLOT_SIZE+8)(VIRT_PCREG), K7, X4       // X4 <- tlv byte
  VPBROADCASTB.Z (BC_SLOT_SIZE+9)(VIRT_PCREG), K7, X5       // X5 <- header length

  BC_STORE_VALUE_TO_SLOT_X(IN(Z2), IN(Z3), IN(X4), IN(X5), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE + BC_LITREF_SIZE)

// v[0].k[1] = auxval(p[2])
TEXT bcauxval(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_RU16(BC_SLOT_SIZE*2, OUT(BX))
  IMULQ $24, BX
  MOVQ bytecode_auxvals+0(VIRT_BCPTR), CX
  MOVQ bytecode_auxpos(VIRT_BCPTR), DX
  MOVQ 0(CX)(BX*1), CX

  VMOVDQU32 0(CX)(DX*8), Z1                                 // Z1 <- value [offset, length] (packed) (low)
  VMOVDQU32 64(CX)(DX*8), Z3                                // Z3 <- value [offset, length] (packed) (high)

  VPMOVQD Z1, Y0                                            // Y0 <- extracted offsets (low)
  VPSRLQ $32, Z1, Z1                                        // Z1 <- prepare for length extraction (low)
  VPMOVQD Z3, Y2                                            // Y2 <- extracted offsets (high)
  VPSRLQ $32, Z3, Z3                                        // Z3 <- prepare for length extraction (high)
  VPMOVQD Z1, Y1                                            // Y1 <- extracted lengths (low)
  VPMOVQD Z3, Y3                                            // Y3 <- extracted lengths (high)
  VPBROADCASTD CONSTD_1(), Z8                               // Z8 <- dword(0xFF)

  VINSERTI32X8.Z $1, Y2, Z0, K7, Z0                         // Z0 <- value offsets
  VINSERTI32X8.Z $1, Y3, Z1, K7, Z1                         // Z1 <- value lengths
  VPBROADCASTD CONSTD_14(), Z9                              // Z9 <- dword(14)
  VPTESTMD Z1, Z1, K7, K1                                   // K1 <- MISSING when a value has zero length or lanes aren't valid

  // read TLV byte and calculate header length
  KMOVW K1, K2
  VPXORD X2, X2, X2
  VPGATHERDD 0(VIRT_BASE)(Z0*1), K2, Z2                     // Z2 <- first 4 bytes of the value

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(CX))
  BC_CALC_VALUE_HLEN(OUT(Z3), IN(Z1), IN(K1), IN(Z8), IN(Z9), Z5, K2)

  BC_STORE_VALUE_TO_SLOT(IN(Z0), IN(Z1), IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(CX))

  NEXT_ADVANCE(BC_SLOT_SIZE*2 + BC_IMM16_SIZE)

// v[0], s[1].k[2] = split(s[3]).k[4]
//
// Take the list slice in s[3] and put the first object slice
// in v[0], then update s[1] to point to the rest of the list.
TEXT bcsplit(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*3, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPTESTMD Z3, Z3, K1, K1                                      // K1 <- only keep lanes with len != 0
  VPBROADCASTD CONSTD_1(), Z13                                 // Z13 <- dword(1)

  KTESTW K1, K1
  KMOVW K1, K2
  VPXORD X4, X4, X4
  JZ empty

  VPGATHERDD 0(VIRT_BASE)(Z2*1), K2, Z4                        // Z4 <- first 4 ion bytes
  VPSLLD $5, Z13, Z11                                          // Z11 <- dword(32)
  VPSHUFB BC_CONST(bswap32), Z4, Z5                            // Z5 <- bswap32(bytes)
  VPBROADCASTD CONSTD_0x00808080(), Z7                         // Z7 <- dword(0x808080)
  VPSRLD $24, Z5, Z9                                           // Z9 <- extracted Type|L byte
  VPANDD Z7, Z5, Z6                                            // Z6 <- bswap32(bytes) & 0x00808080
  VPANDND Z5, Z7, Z7                                           // Z7 <- bswap32(bytes) & 0xFF7F7F7F
  VPCMPUD $VPCMP_IMM_GE, Z11, Z9, K1, K3                       // K3 <- Type != NULL|BOOL (Type|L >= 32)

  VPLZCNTD Z6, Z6                                              // Z6 <- lzcnt32(bswap32(bytes) & 0x808080) (number of length bytes in bits)
  VPANDD.BCST.Z CONSTD_15(), Z9, K3, Z8                        // Z8 <- L field extracted from Type|L and corrected to 0 if NULL/BOOL
  VPSLLD $8, Z7, Z7                                            // Z7 <- (bswap32(bytes) & 0x7F7F7F) << 8
  VPCMPEQD.BCST CONSTD_14(), Z8, K1, K3                        // K3 <- lanes that need a separate Length data when L == 14

  VPSUBD Z6, Z11, Z11                                          // Z11 <- 32 - lzcnt32(bswap32(bytes) & 0x808080) (number of bits to trash)
  VPSRLD.Z $3, Z6, K3, Z10                                     // Z10 <- size of Length field, in bytes (or 0, if there is no Length field)
  VPSRLVD Z11, Z7, K3, Z8                                      // Z8 <- length data as [00000000|0CCCCCCCC|0BBBBBBBB|0AAAAAAAA]
  VPADDD.Z Z13, Z10, K1, Z10                                   // Z10 <- header length (includes TLV byte and optional Length field size)

  VPSRLD $1, Z8, Z11                                           // Z11 <- length data as [00000000|00CCCCCCC|C0BBBBBBB|BAAAAAAAA]
  VPSRLD $2, Z8, Z12                                           // Z12 <- length data as [00000000|000CCCCCC|CC0BBBBBB|BBAAAAAAA]
  VPTERNLOGD.BCST $TLOG_BLEND_AB, CONSTD_0x7F(), Z11, Z8       // Z8  <- length data as [00000000|00CCCCCCC|C0BBBBBBB|BAAAAAAAA]
  BC_UNPACK_SLOT(0, OUT(DX))
  VPTERNLOGD.BCST $TLOG_BLEND_AB, CONSTD_0x3FFF(), Z12, Z8     // Z8  <- length data as [00000000|000CCCCCC|CCBBBBBBB|BAAAAAAAA]

  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(CX))
  VPADDD.Z Z8, Z10, K1, Z12                                    // Z12 <- value length
  BC_STORE_VALUE_TO_SLOT(IN(Z2), IN(Z12), IN(Z9), IN(Z10), IN(DX))

  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  VPADDD.Z Z12, Z2, K1, Z13                                    // Z13 <- offset of the output slice (advancing the input slice)
  VPSUBD.Z Z12, Z3, K1, Z14                                    // Z14 <- length of the output slice (advancing the input slice)
  BC_STORE_SLICE_TO_SLOT(IN(Z13), IN(Z14), IN(CX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

empty:
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(CX), OUT(R8))
  BC_STORE_VALUE_TO_SLOT_X(IN(Z4), IN(Z4), IN(X4), IN(X4), OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z4), IN(Z4), IN(CX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// b[0].k[1] = tuple(v[2]).k[3]
//
// take v[0] and parse it as struct, returning offset + length in b[0]
TEXT bctuple(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3, OUT(R8))

  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z4), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  VPSRLD $4, Z4, Z4

  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z5), IN(BX))
  VPCMPEQD.BCST CONSTD_13(), Z4, K1, K1

  VMOVDQU32 BC_VSTACK_PTR(BX, 64), Z3
  VPADDD.Z BC_VSTACK_PTR(BX, 0), Z5, K1, Z2

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VPSUBD.Z Z5, Z3, K1, Z3

  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// k[0] = mov.k(k[1])
TEXT bcmovk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(BX))
  BC_LOAD_RU16_FROM_SLOT(OUT(BX), IN(BX))
  BC_STORE_RU16_TO_SLOT(IN(BX), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*2)

// v[0] = zero.v()
//
// zero a slot (this is effectively the constprop'd version of saving MISSING everywhere)
TEXT bczerov(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(DX))
  VPXORD X2, X2, X2
  VMOVDQU32 Z2, BC_VSTACK_PTR(DX, 0)
  VMOVDQU32 Z2, BC_VSTACK_PTR(DX, 64)
  VMOVDQU32 Y2, BC_VSTACK_PTR(DX, 128)
  NEXT_ADVANCE(BC_SLOT_SIZE*1)

// v[0] = mov.v(v[1]).k[2]
TEXT bcmovv(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(BX))
  KUNPCKWD K1, K1, K2

  BC_UNPACK_SLOT(0, OUT(DX))
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 0), K1, Z2
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 64), K1, Z3
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 128), K2, Y4

  VMOVDQU32 Z2, BC_VSTACK_PTR(DX, 0)
  VMOVDQU32 Z3, BC_VSTACK_PTR(DX, 64)
  VMOVDQU32 Y4, BC_VSTACK_PTR(DX, 128)

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// v[0].k[1] = mov.v.k(v[2]).k[3]
TEXT bcmovvk(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*3, OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  KUNPCKWD K1, K1, K2

  BC_UNPACK_SLOT(0, OUT(DX))
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 0), K1, Z2
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 64), K1, Z3
  VMOVDQU32.Z BC_VSTACK_PTR(BX, 128), K2, Y4

  BC_UNPACK_SLOT(BC_SLOT_SIZE*1, OUT(R8))
  VMOVDQU32 Z2, BC_VSTACK_PTR(DX, 0)
  VMOVDQU32 Z3, BC_VSTACK_PTR(DX, 64)
  VMOVDQU32 Y4, BC_VSTACK_PTR(DX, 128)
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// f64[0] = mov.f64(f64[1]).k[2]
TEXT bcmovf64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1), IN(K2))
  BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = mov.i64(i64[1]).k[2]
TEXT bcmovi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(0, OUT(DX), OUT(BX), OUT(R8))

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_LOAD_I64_FROM_SLOT_MASKED(OUT(Z2), OUT(Z3), IN(BX), IN(K1), IN(K2))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0].k[1] = objectsize(v[2]).k[3]
//
// SIZE(x) function - returns the number of items
// in a struct or list, missing otherwise.
TEXT bcobjectsize(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  BC_LOAD_VALUE_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  BC_LOAD_VALUE_TYPEL_FROM_SLOT(OUT(Z2), IN(BX))
  BC_LOAD_VALUE_HLEN_FROM_SLOT(OUT(Z3), IN(BX))

  VPSRLD.Z $4, Z2, K1, Z2                              // Z2 <- V.type
  VPADDD.Z Z0, Z1, K1, Z1                              // Z1 <- V.dataEnd

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  VPADDD.Z Z3, Z0, K1, Z0                              // Z0 <- V.dataOffset

  VPCMPEQD.BCST CONSTD_0x0D(), Z2, K1, K5              // K5 <- lanes that contain structs
  VPCMPEQD.BCST CONSTD_0x0B(), Z2, K1, K1              // K1 <- lanes that contain lists
  KTESTW K5, K5

  KORW K1, K5, K3                                      // K3 <- output predicate (list | struct)
  BC_STORE_K_TO_SLOT(IN(K3), IN(R8))                   // store output predicate to output slot
  JZ tail_to_array_size                                // use bcarraysize fast-path if there aren't structs

  // either all structs or mixed structs + lists

  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K3, K1                // K1 <- lanes to process (empty structs/lists discarded)
  KTESTW K1, K1

  VPXORD X10, X10, X10                                 // Z10 <- current length (dword)

  VPBROADCASTD CONSTD_1(), Z22                         // Z22 <- dword(1)
  VPBROADCASTD CONSTD_14(), Z23                        // Z23 <- dword(14)
  VPBROADCASTD CONSTD_0x0F(), Z24                      // Z24 <- dword(0xF)
  VPBROADCASTD CONSTD_32(), Z25                        // Z25 <- dword(32)
  VPBROADCASTD CONSTD_0x7F(), Z26                      // Z26 <- dword(0x7F)
  VPBROADCASTD CONSTD_0x00808080(), Z27                // Z27 <- dword(0x00808080)
  VBROADCASTI32X4 CONST_GET_PTR(bswap64, 0), Z30       // Z30 <- bswap64 predicate for VPSHUFB
  JZ done

  KSHIFTRW $8, K5, K6

loop:
  KSHIFTRW $8, K1, K2
  VEXTRACTI32X8 $1, Z0, Y9

  KMOVW K1, K3
  VPXORD X2, X2, X2
  VPGATHERDQ 0(VIRT_BASE)(Y0*1), K3, Z2                // Z2 <- V.hdr64 (struct Symbol+Value or list Value bytes) (low)

  VPADDD Z22, Z10, K1, Z10                             // Z10 <- increment length of active lanes
  VPADDD Z22, Z0, K1, Z0                               // Z0 <- Z0 + 1 (advance by a size of a TVL byte)

  KMOVW K2, K4
  VPXORD X3, X3, X3
  VPGATHERDQ 0(VIRT_BASE)(Y9*1), K4, Z3                // Z3 <- V.hdr64 (struct Symbol+Value or list Value bytes) (high)

  VPSHUFB Z30, Z2, Z2                                  // Z2 <- bswap64(V.hdr64) (low)
  VPSRLQ $8, Z2, Z4                                    // Z4 <- bswap64(V.hdr64) >> 8 (low)
  VPANDQ Z27, Z4, Z4                                   // Z4 <- bswap64(V.hdr64) >> 8 & 0x0080808000808080 (low)
  VPLZCNTQ.Z Z4, K5, Z4                                // Z4 <- lzcnt(bswap64(V.hdr64) >> 8 & 0x0080808000808080) (low)
  VPSLLVQ Z4, Z2, Z2                                   // Z2 <- bswap64(V.hdr64 << len(SymbolID)) (low)
  VPSRLQ.Z $3, Z4, K1, Z4                              // Z4 <- len(SymbolID) of lanes having structs (low)

  VPSHUFB Z30, Z3, Z3                                  // Z3 <- bswap64(V.hdr64) (high)
  VPSRLQ $8, Z3, Z5                                    // Z5 <- bswap64(V.hdr64) >> 8 & 0x0080808000808080 (high)
  VPANDQ Z27, Z5, Z5                                   // Z5 <- bswap64(V.hdr64) >> 8 & 0x0080808000808080 (high)
  VPLZCNTQ.Z Z5, K6, Z5                                // Z5 <- lzcnt(bswap64(V.hdr64) >> 8 & 0x8080808080808080) (high)
  VPSLLVQ Z5, Z3, Z3                                   // Z3 <- bswap64(V.hdr64 << len(SymbolID)) (high)
  VPSRLQ.Z $3, Z5, K2, Z5                              // Z5 <- len(SymbolID) of lanes having structs (high)

  VPSRLQ $32, Z2, Z2                                   // Z2 <- V.hdr32 (low)
  VPSRLQ $32, Z3, Z3                                   // Z3 <- V.hdr32 (high)
  VPMOVQD Z2, Y2                                       // Z2 <- V.hdr32 (low)
  VPMOVQD Z3, Y3                                       // Z3 <- V.hdr32 (high)
  VPMOVQD Z4, Y4                                       // Z4 <- len(SymbolID) of lanes having structs (low)
  VINSERTI32X8 $1, Y3, Z2, Z2                          // Z2 <- V.hdr32 (value of either struct or list)
  VPMOVQD Z5, Y5                                       // Z5 <- len(SymbolID) of lanes having structs (high)
  VINSERTI32X8 $1, Y5, Z4, Z4                          // Z4 <- len(SymbolID)

  VPSRLD $28, Z2, Z5                                   // Z5 <- V.type
  VPSRLD $24, Z2, Z6                                   // Z6 <- V.TLV
  VPCMPUD $VPCMP_IMM_GT, Z22, Z5, K1, K3               // K3 <- V.type != NULL|BOOL
  VPADDD Z4, Z0, K1, Z0                                // Z0 <- Z0 + len(SymbolID) (advance by len(SymbolID))
  VPANDD.Z Z24, Z6, K3, Z6                             // Z6 <- V.L or zero if V.type == NULL|BOOL

  VPCMPEQD Z23, Z6, K1, K3                             // K3 <- lanes where L == 14
  KTESTW K3, K3
  JNZ varuint_length

  VPADDD Z6, Z0, Z0                                    // Z0 <- advance array by the content length
  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to scan

  KTESTW K1, K1
  JNZ loop

done:
  VEXTRACTI32X8 $1, Z10, Y11
  VPMOVZXDQ Y10, Z10
  VPMOVZXDQ Y11, Z11
  BC_STORE_I64_TO_SLOT(IN(Z10), IN(Z11), IN(DX))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

varuint_length:
  VPANDD Z27, Z2, Z8                                   // Z8 <- bswap32(V.hdr32) & 0x00808080
  VPANDND Z2, Z27, Z9                                  // Z9 <- bswap32(V.hdr32) & 0xFF7F7F7F
  VPLZCNTD.Z Z8, K3, Z8                                // Z8 <- lzcnt(bswap32(V.hdr32) & 0x00808080)
  VPSUBD Z8, Z25, Z5                                   // Z5 <- 32 - lzcnt(bswap32(V.hdr32) & 0x00808080) (number of bits to discard)
  VPSLLD $8, Z9, Z4                                    // Z4 <- (bswap32(V.hdr32) & 0xFF7F7F7F) << 8
  VPSRLVD Z5, Z4, K3, Z6                               // Z6 <- V.L or V.optLen [00000000|0CCCCCCC|0BBBBBBB|0AAAAAAA]
  VPSRLD.Z $3, Z8, K3, Z8                              // Z8 <- V.hLen - 1 (without accounting the TLV byte, which we have already accounted in Z0)

  VPSRLD $1, Z6, Z4                                    // Z4 <- V.dataLen >> 1  [00000000|00CCCCCC|C0BBBBBB|B0AAAAAA]
  VPSRLD $2, Z6, Z5                                    // Z5 <- V.dataLen >> 2  [00000000|000CCCCC|CC0BBBBB|BB0AAAAA]
  VPTERNLOGD $TLOG_BLEND_AB, Z26, Z4, Z6               // Z6 <- V.dataLen as    [00000000|00CCCCCC|C0BBBBBB|BAAAAAAA]
  VPTERNLOGD.BCST $TLOG_BLEND_AB, CONSTD_0x3FFF(), Z5, Z6 // Z6 <- V.dataLen

  VPADDD Z8, Z6, Z5                                    // Z5 <- V.valLen - 1 (without accounting the TLV byte, which we have already accounted in Z0)
  VPADDD Z5, Z0, Z0                                    // Z0 <- advance array by V.valLen - 1

  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to scan
  KTESTW K1, K1
  JNZ loop
  JMP done

tail_to_array_size:
  ADDQ $(BC_SLOT_SIZE*4), VIRT_PCREG
  JMP arraysize_tail(SB)

// i64[0] = arraysize(s[1]).k[2]
TEXT bcarraysize(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPADDD.Z Z0, Z1, K1, Z1                              // Z1 <- end of the list

  BC_UNPACK_SLOT(0, OUT(DX))
  ADDQ $(BC_SLOT_SIZE*3), VIRT_PCREG

  JMP arraysize_tail(SB)

// Tail call used by both bcobjectsize and bcarraysize.
//
// Arguments:
//   Z0 <- unboxed list slice start/offset
//   Z1 <- unboxed list slice end
//   K1 <- predicate
//   DX <- destination slot
TEXT arraysize_tail(SB), NOSPLIT|NOFRAME, $0
  VPCMPUD $VPCMP_IMM_NE, Z0, Z1, K1, K1                // K1 <- lanes to prpocess (empty arrays discarded)
  KTESTW K1, K1

  VPXORD X10, X10, X10                                 // Z10 <- current length (dword)

  VPBROADCASTD CONSTD_1(), Z22                         // Z22 <- dword(1)
  VPBROADCASTD CONSTD_14(), Z23                        // Z23 <- dword(14)
  VPBROADCASTD CONSTD_0x0F(), Z24                      // Z24 <- dword(0xF)
  VPBROADCASTD CONSTD_32(), Z25                        // Z25 <- dword(32)
  VPBROADCASTD CONSTD_0x7F(), Z26                      // Z26 <- dword(0x7F)
  VPBROADCASTD CONSTD_0x00808080(), Z27                // Z27 <- dword(0x808080)
  VPBROADCASTD CONSTD_4(), Z28                         // Z28 <- dword(4)
  VBROADCASTI32X4 CONST_GET_PTR(bswap32, 0), Z30       // Z30 <- bswap32 predicate for VPSHUFB
  JZ done

  JMP loop

small_values:
  VPSLLD $3, Z5, Z6
  VPSRLVD Z6, Z2, Z2                                   // Z2 <- remaining bytes

  VPSRLD $4, Z2, Z5                                    // Z5 <- V.hdr32 >> 4
  VPANDD Z24, Z5, Z5                                   // Z5 <- V.type
  VPCMPUD $VPCMP_IMM_GT, Z22, Z5, K3, K3               // K3 <- remaining lanes where V.type != NULL|BOOL
  VPANDD.Z Z24, Z2, K3, Z6                             // Z6 <- V.L or zero if V.type == NULL|BOOL
  VPCMPUD $VPCMP_IMM_NE, Z23, Z6, K3, K4               // K4 <- remaining lanes where V.L != 14

  VPADDD.Z Z22, Z6, K4, Z5                             // Z5 <- V.valLen
  VPADDD Z22, Z10, K4, Z10                             // Z10 <- increment length of remaining lanes where V.L != 14
  VPADDD Z5, Z0, K4, Z0                                // Z0 <- advance small values by V.valLen
  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to scan

  KTESTW K1, K1
  JZ done

loop:
  KMOVW K1, K2
  VPXORD X2, X2, X2
  VPGATHERDD 0(VIRT_BASE)(Z0*1), K2, Z2                // Z2 <- V.hdr32

  VPADDD Z22, Z10, K1, Z10                             // Z10 <- increment length of active lanes
  VPSHUFB Z30, Z2, Z7                                  // Z7 <- bswap32(V.hdr32)
  VPANDD.Z Z24, Z2, K1, Z6                             // Z6 <- V.L
  VPSRLD $28, Z7, Z5                                   // Z5 <- V.type
  VPCMPEQD Z23, Z6, K1, K4                             // K4 <- lanes where L == 14
  KTESTW K4, K4

  VPCMPUD $VPCMP_IMM_GT, Z22, Z5, K1, K3               // K3 <- V.type != NULL|BOOL
  VPANDD.Z Z24, Z2, K3, Z6                             // Z6 <- V.L or zero if V.type == NULL|BOOL
  JNZ varuint_length

  // If we are here it means that all lanes contain a value that
  // doesn't need varuint decoding. This means that if the value
  // is small enough we can try to count another one by inspecting
  // bytes we have already gathered as a part of the first value.
  // This can increase the overall query performance by about 20%.

  VPADDD.Z Z22, Z6, K1, Z5                             // Z5 <- V.valLen
  VPADDD Z5, Z0, Z0                                    // Z0 <- advance array by the current value length
  VPSUBUSW Z5, Z28, Z9                                 // Z9 <- number of remaining bytes in Z2

  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to scan
  VPTESTMD Z9, Z9, K1, K3                              // K3 <- lanes where remaining bytes in Z2 > 0

  KTESTW K3, K3
  JNZ small_values

  KTESTW K1, K1
  JNZ loop

done:
  VEXTRACTI32X8 $1, Z10, Y11
  VPMOVZXDQ Y10, Z10
  VPMOVZXDQ Y11, Z11
  BC_STORE_I64_TO_SLOT(IN(Z10), IN(Z11), IN(DX))

  NEXT_ADVANCE(0)

varuint_length:
  VPANDD Z27, Z7, Z8                                   // Z8 <- bswap32(V.hdr32) & 0x00808080
  VPANDND Z7, Z27, Z9                                  // Z9 <- bswap32(V.hdr32) & 0xFF7F7F7F
  VPLZCNTD.Z Z8, K3, Z8                                // Z8 <- lzcnt(bswap32(V.hdr32) & 0x00808080)
  VPSUBD Z8, Z25, Z5                                   // Z5 <- 32 - lzcnt(bswap32(V.hdr32) & 0x00808080) (number of bits to discard)
  VPSLLD $8, Z9, Z4                                    // Z4 <- (bswap32(V.hdr32) & 0xFF7F7F7F) << 8
  VPSRLVD Z5, Z4, K4, Z6                               // Z6 <- V.L or V.optLen [00000000|0CCCCCCC|0BBBBBBB|0AAAAAAA]
  VPSRLD.Z $3, Z8, K4, Z8                              // Z8 <- V.hLen - 1

  VPSRLD $1, Z6, Z4                                    // Z4 <- V.dataLen >> 1  [00000000|00CCCCCC|C0BBBBBB|B0AAAAAA]
  VPSRLD $2, Z6, Z5                                    // Z5 <- V.dataLen >> 2  [00000000|000CCCCC|CC0BBBBB|BB0AAAAA]
  VPTERNLOGD $TLOG_BLEND_AB, Z26, Z4, Z6               // Z6 <- V.dataLen as    [00000000|00CCCCCC|C0BBBBBB|BAAAAAAA]
  VPADDD Z22, Z8, Z8                                   // Z8 <- V.hLen
  VPTERNLOGD.BCST $TLOG_BLEND_AB, CONSTD_0x3FFF(), Z5, Z6 // Z6 <- V.dataLen

  VPADDD Z8, Z6, Z5                                    // Z5 <- V.valLen
  VPADDD Z5, Z0, Z0                                    // Z0 <- advance array by the current value length

  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to scan
  KTESTW K1, K1
  JNZ loop
  JMP done

// i64[0].k[1] = arrayposition(s[2], v[3]).k[4]
//
// Legend:
//   - 'A' - refers to v[3] (the item to match)
//   - 'B' - refers to values stored in s[2]
//
// NOTES:
//   - This function requires A to be already unsymbolized.
//   - It's guaranteed that if the first 4 bytes match both A and B have the same length.
TEXT bcarrayposition(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPTESTMD Z1, Z1, K1, K1                              // K1 <- items to match (empty arrays discarded)
  KTESTW K1, K1

  MOVQ bytecode_symtab+0(VIRT_BCPTR), R8               // R8 <- symbol table base
  BC_LOAD_VALUE_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(CX)) // Z2:Z3 <- value to match

  VPXORD X20, X20, X20                                 // Z20 <- current position in array (counted from 1)
  VPXORD X21, X21, X21                                 // Z21 <- matched positions
  VPBROADCASTD CONSTD_1(), Z22                         // Z22 <- dword(1)
  VPBROADCASTD CONSTD_14(), Z23                        // Z23 <- dword(14)
  VPBROADCASTD CONSTD_0x0F(), Z24                      // Z24 <- dword(0xF)
  VPBROADCASTD CONSTD_32(), Z25                        // Z25 <- dword(32)
  VPBROADCASTD CONSTD_0x7F(), Z26                      // Z26 <- dword(0x7F)
  VPBROADCASTD CONSTD_0x00808080(), Z27                // Z27 <- dword(0x808080)
  VPBROADCASTD CONSTD_7(), Z29                         // Z29 <- dword(7)
  VBROADCASTI32X4 CONST_GET_PTR(bswap32, 0), Z30       // Z30 <- bswap32 predicate for VPSHUFB
  VMOVDQU64 CONST_GET_PTR(consts_byte_mask_q, 0), Z31  // Z31 <- consts_byte_mask_q
  JZ done

  VEXTRACTI32X8 $1, Z2, Y14

  KMOVW K1, K3
  VPXORD X10, X10, X10
  VPGATHERDQ 0(VIRT_BASE)(Y2*1), K3, Z10               // Z10 <- first 8 bytes of A (low)

  KSHIFTRW $8, K1, K4
  VPXORD X11, X11, X11
  VPGATHERDQ 0(VIRT_BASE)(Y14*1), K4, Z11              // Z11 <- first 8 bytes of A (high)

  VPADDD Z29, Z22, Z28                                 // Z28 <- dword(8)

  VPMOVQD Z10, Y12
  VPMOVQD Z11, Y13
  VINSERTI32X8 $1, Y13, Z12, Z12                       // Z12 <- first 4 bytes of A
  VPSRLD $4, Z12, Z13
  VPANDD Z24, Z13, Z13                                 // Z13 <- type of A

  VPMINUD Z28, Z3, Z8
  VEXTRACTI32X8 $1, Z8, Y9
  VPADDD.Z Z0, Z1, K1, Z1                              // Z1  <- end of the list
  VPADDD.Z Z2, Z3, K1, Z3                              // Z3  <- end of the value

  VPMOVZXDQ Y8, Z8
  VPMOVZXDQ Y9, Z9
  VPERMQ Z31, Z8, Z8                                   // Z8  <- leading byte mask
  VPERMQ Z31, Z9, Z9                                   // Z9  <- leading byte mask

  VPANDQ Z8, Z10, Z10
  VPANDQ Z9, Z11, Z11

  // K6 <- lanes in A that don't contain strings - this is important as we do
  // not want to unsymbolize B values that would not compare against strings.
  VPCMPD $VPCMP_IMM_NE, Z28, Z13, K1, K6

  // Z29 <- comparisong predicate for unsymbolize - it either contains `7` for lanes that
  // need to unsymbolize in case that they contain symbol, or a non-compatible type value
  // for lanes that don't need unsymbolize (not comparing agains a string of A value).
  // We can keep this predicate in K6, but there is much less K registers than ZMM registers
  // so it's just better to keep this in ZMM register and have one more K register available.
  VMOVDQA32 Z26, K6, Z29

array_loop:
  KSHIFTRW $8, K1, K2
  VEXTRACTI32X8 $1, Z0, Y13

  KMOVB K1, K3
  VPXORD X18, X18, X18
  VPGATHERDQ 0(VIRT_BASE)(Y0*1), K3, Z18               // Z18 <- first 8 bytes of B (low)

  KMOVB K2, K4
  VPADDD.Z Z22, Z20, K1, Z20                           // Z20 <- advance current position

  VPXORD X19, X19, X19
  VPGATHERDQ 0(VIRT_BASE)(Y13*1), K4, Z19              // Z19 <- first 8 bytes of B (high)

  VPMOVQD Z18, Y14
  VPMOVQD Z19, Y13
  VINSERTI32X8 $1, Y13, Z14, Z13                       // Z13 <- first 4 bytes of B (B.hdr32)

  VPSRLD $4, Z13, Z14                                  // Z14 <- B.hdr32 >> 4
  VPSHUFB Z30, Z13, Z17                                // Z17 <- bswap32(B.hdr32)
  VPANDD Z24, Z14, Z14                                 // Z14 <- B.type
  VPANDD Z27, Z17, Z16                                 // Z16 <- bswap32(B.hdr32) & 0x00808080
  VPCMPUD $VPCMP_IMM_GT, Z22, Z14, K1, K3              // K3  <- B.type != NULL|BOOL
  VPANDND Z17, Z27, Z6                                 // Z6  <- bswap32(B.hdr32) & 0xFF7F7F7F

  VPLZCNTD.Z Z16, K3, Z16                              // Z16 <- lzcnt(bswap32(B.hdr32) & 0x00808080)
  VPCMPEQD Z29, Z14, K1, K4                            // K4  <- B.type == SYMBOL that needs to be unsymbolized
  VPANDD.Z Z24, Z13, K3, Z15                           // Z15 <- B.L or zero if B.type == NULL|BOOL
  KTESTW K4, K4

  VPSUBD Z16, Z25, Z14                                 // Z14 <- 32 - lzcnt(bswap32(B.hdr32) & 0x00808080) (number of bits to discard)
  VPCMPEQD Z23, Z15, K1, K3                            // K3  <- B.L == 14 (required to decode Length field)
  VPSLLD $8, Z6, Z13                                   // Z13 <- (bswap32(B.hdr32) & 0xFF7F7F7F) << 8
  VPSRLVD Z14, Z13, K3, Z15                            // Z15 <- B.L or B.optLen [00000000|0CCCCCCC|0BBBBBBB|0AAAAAAA]
  VPSRLD.Z $3, Z16, K3, Z16                            // Z16 <- B.hLen - 1

  VPSRLD $1, Z15, Z13                                  // Z13 <- B.dataLen >> 1  [00000000|00CCCCCC|C0BBBBBB|B0AAAAAA]
  VPSRLD $2, Z15, Z14                                  // Z14 <- B.dataLen >> 2  [00000000|000CCCCC|CC0BBBBB|BB0AAAAA]
  VPTERNLOGD $TLOG_BLEND_AB, Z26, Z13, Z15             // Z15 <- B.dataLen as    [00000000|00CCCCCC|C0BBBBBB|BAAAAAAA]
  VPADDD Z22, Z16, Z16                                 // Z16 <- B.hLen
  VPTERNLOGD.BCST $TLOG_BLEND_AB, CONSTD_0x3FFF(), Z14, Z15 // Z15 <- B.dataLen

  VPADDD Z28, Z2, Z6                                   // Z6  <- A.offset + 8
  VPADDD Z16, Z15, Z14                                 // Z14 <- B.valLen
  VPADDD Z28, Z0, Z7                                   // Z7  <- B.offset + 8
  VPADDD Z14, Z0, Z0                                   // Z0  <- advance array by the current value length
  JZ skip_unsymbolize

  VPSLLD $8, Z17, Z17                                  // Z17 <- bswap32(B.hdr32) << 8 (symbol data)
  VPSLLD $3, Z15, Z14                                  // Z14 <- B.dataLen << 3 (data length in bits)
  VPSUBD Z14, Z25, Z14                                 // Z14 <- 32 - B.dataLen << 3 (number of bits to discard in Z13)
  VPSRLVD.Z Z14, Z17, K4, Z17                          // Z17 <- extracted SymbolIDs from B
  VPCMPUD.BCST $VPCMP_IMM_LT, bytecode_symtab+8(VIRT_BCPTR), Z17, K4, K4 // K4 <- only unsymbolize symbols present in symtab

  KMOVW K4, K5
  VPGATHERDD 0(R8)(Z17*8), K5, Z7                      // gather 16 symbol offsets (we don't care of lengths in this case)

  KMOVB K4, K5
  VPGATHERDQ 0(VIRT_BASE)(Y7*1), K5, Z18               // Z18 <- merge first 8 bytes of B to the existing vector (low)

  VEXTRACTI32X8 $1, Z7, Y13
  KSHIFTRW $8, K4, K5
  VPGATHERDQ 0(VIRT_BASE)(Y13*1), K5, Z19              // Z19 <- merge first 8 bytes of B to the existing vector (high)

  VPADDD Z28, Z7, K4, Z7                               // Z7  <- B.offset += 8 (where symbols)

skip_unsymbolize:
  // first 8 bytes of A in (Z10:Z11) and first 8 bytes of B in (Z18:Z19)
  VPANDQ Z8, Z18, Z18                                  // Z18 <- B.hdr64 & lead_mask (low)
  VPANDQ Z9, Z19, Z19                                  // Z19 <- B.hdr64 & lead_mask (high)
  VPCMPEQQ Z10, Z18, K1, K3                            // K3  <- A.hdr64 == B.hdr64 (low)
  VPCMPEQQ Z11, Z19, K2, K4                            // K4  <- A.hdr64 == B.hdr64 (high)
  KORTESTB K3, K4

  KUNPCKBW K3, K4, K3                                  // K3  <- A.hdr64 == B.hdr64
  JZ array_advance                                     // bail early if hdr64 values don't match

  // at least one lane matched all leading bytes - this means that both A and B have a compatible
  // header, which implies that they have the same length as well (as length is part of the header).

  VPCMPUD $VPCMP_IMM_GE, Z3, Z6, K3, K4                // K4  <- lanes that matched (offset >= end)
  KANDNW K3, K4, K3                                    // K3  <- remaining lanes where to continue value comparison
  KTESTW K3, K3

  VMOVDQA32 Z20, K4, Z21                               // Z21 <- update matched positions
  KANDNW K1, K4, K1
  JZ array_advance

value_loop:
  KSHIFTRW $8, K3, K4
  VEXTRACTI32X8 $1, Z6, Y15

  KMOVB K3, K5
  VPXORD X16, X16, X16
  VPGATHERDQ 0(VIRT_BASE)(Y6*1), K5, Z16               // Z16 <- next 8 bytes of A (low)

  VPSUBD Z6, Z3, Z13                                   // Z13 <- remaining_length
  VPADDD Z28, Z6, Z6                                   // Z6  <- A.offset += 8

  KMOVB K4, K5
  VPXORD X17, X17, X17
  VPGATHERDQ 0(VIRT_BASE)(Y15*1), K5, Z17              // Z17 <- next 8 bytes of A (high)

  VPMINUD.Z Z28, Z13, K3, Z13                          // Z13 <- min(remaining_length, 8)
  VEXTRACTI32X8 $1, Z7, Y15

  KMOVB K3, K5
  VPXORD X18, X18, X18
  VPGATHERDQ 0(VIRT_BASE)(Y7*1), K5, Z18               // Z18 <- next 8 bytes of B (low)

  VPADDD Z28, Z7, Z7                                   // Z7  <- B.offset += 8
  VEXTRACTI32X8 $1, Z13, Y14
  VPMOVZXDQ Y13, Z13                                   // Z13 <- min(remaining_length, 8) (low)

  KMOVB K4, K5
  VPXORD X19, X19, X19
  VPGATHERDQ 0(VIRT_BASE)(Y15*1), K5, Z19              // Z19 <- next 8 bytes of B (high)

  VPMOVZXDQ Y14, Z14                                   // Z14 <- min(remaining_length, 8) (high)
  VPERMQ Z31, Z13, Z13                                 // Z14 <- compare byte mask (low)
  VPERMQ Z31, Z14, Z14                                 // Z15 <- compare byte mask (high)

  // 0x28 == (A ^ B) & C
  VPTERNLOGQ $0x28, Z13, Z16, Z18                      // Z18 <- each QWORD lane contains zero if equal (low)
  VPTERNLOGQ $0x28, Z14, Z17, Z19                      // Z19 <- each QWORD lane contains zero if equal (high)

  VPTESTNMQ Z18, Z18, K3, K3                           // K3  <- lanes where 64-bit data or tail bytes are equal (low)
  VPTESTNMQ Z19, Z19, K4, K4                           // K4  <- lanes where 64-bit data or tail bytes are equal (high)
  KUNPCKBW K3, K4, K3                                  // K3  <- lanes where 64-bit data or tail bytes are equal

  VPCMPUD $VPCMP_IMM_GE, Z3, Z6, K3, K4                // K4  <- lanes that matched (offset >= end)
  KANDNW K3, K4, K3                                    // K3  <- remaining lanes where to continue value comparison
  KANDNW K1, K4, K1                                    // K1  <- remaining lanes where to match the next value

  VPCMPUD $VPCMP_IMM_LT, Z3, Z6, K3, K3                // K3  <- remaining lanes where to continue value comparison (and have data)
  VMOVDQA32 Z20, K4, Z21                               // Z21 <- update matched positions

  KTESTW K3, K3
  JNZ value_loop                                       // continue if we don't have a match yet and there are more bytes to compare

array_advance:
  VPCMPUD $VPCMP_IMM_LT, Z1, Z0, K1, K1                // K1 <- remaining lanes to compare
  KTESTW K1, K1
  JNZ array_loop

done:
  // extend UINT32 positions to INT64
  VEXTRACTI32X8 $1, Z21, Y22
  VPTESTMD Z21, Z21, K1
  VPMOVZXDQ Y21, Z21
  VPMOVZXDQ Y22, Z22

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z21), IN(Z22), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// String Instructions
// -------------------

/*
    Common UTF-8 constants:

    CONSTD_UTF8_2B_MASK() = 0b10000000110000000000000000000000
    CONSTD_UTF8_3B_MASK() = 0b10000000100000001110000000000000
    CONSTD_UTF8_4B_MASK() = 0b10000000100000001000000011110000

    CONSTD_0b11000000() = 0b11000000
    CONSTD_0b11100000() = 0b11100000
    CONSTD_0b11110000() = 0b11110000
    CONSTD_0b11111000() = 0b11111000
*/

//; #region string methods

//; #region bcCmpStrEqCs
//
// k[0] = cmp_str_eq_cs(s[1], x[2]).k[3]
TEXT bcCmpStrEqCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter_needle; R14=needle_slice;
  VPTESTMD      Z6,  Z6,  K1,  K1         //;EF7C0710 K1 &= (counter_needle != 0)     ;K1=lane_active; Z6=counter_needle;
  VPCMPD        $0,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_len==counter_needle) ;K1=lane_active; Z3=str_len; Z6=counter_needle; 0=Eq;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;

  JMP           tests                     //;F2A3982D                                 ;
loop:
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPSUBD        Z20, Z3,  K1,  Z3         //;AEDCD850 str_len -= 4                    ;Z3=str_len; K1=lane_active; Z20=4;
  VPADDD        Z20, Z2,  K1,  Z2         //;D7CC90DD str_start += 4                  ;Z2=str_start; K1=lane_active; Z20=4;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data_msg==[needle_ptr])  ;K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tests:
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;8A1022B4 K1 &= (0<=str_len)              ;K1=lane_active; Z11=0; Z3=str_len; 2=LessEq;
  VPCMPD        $6,  Z20, Z3,  K3         //;99392208 K3 := (str_len>4)               ;K3=tmp_mask; Z3=str_len; Z20=4; 6=Greater;
  KTESTW        K1,  K1                   //;FE455439 any lanes still alive           ;K1=lane_active;
  JZ            next                      //;CD5F484F no, exit; jump if zero (ZF = 1) ;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPERMD        CONST_TAIL_MASK(),Z3,  Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z3=str_len;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
//; compare data with needle
  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data_msg==data_needle)   ;K1=lane_active; Z8=data_msg; Z9=data_needle; 0=Eq;
next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcCmpStrEqCs

//; #region bcCmpStrEqCi
//
// k[0] = cmp_str_eq_ci(slice[1], dict[2]).k[3]
TEXT bcCmpStrEqCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter_needle; R14=needle_slice;
  VPTESTMD      Z6,  Z6,  K1,  K1         //;EF7C0710 K1 &= (counter_needle != 0)     ;K1=lane_active; Z6=counter_needle;
  VPCMPD        $0,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_len==counter_needle) ;K1=lane_active; Z3=str_len; Z6=counter_needle; 0=Eq;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;

  JMP           tests                     //;F2A3982D                                 ;
loop:
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPSUBD        Z20, Z3,  K1,  Z3         //;AEDCD850 str_len -= 4                    ;Z3=str_len; K1=lane_active; Z20=4;
  VPADDD        Z20, Z2,  K1,  Z2         //;D7CC90DD str_start += 4                  ;Z2=str_start; K1=lane_active; Z20=4;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==[needle_ptr]);K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tests:
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;8A1022B4 K1 &= (0<=str_len)              ;K1=lane_active; Z11=0; Z3=str_len; 2=LessEq;
  VPCMPD        $6,  Z20, Z3,  K3         //;99392208 K3 := (str_len>4)               ;K3=tmp_mask; Z3=str_len; Z20=4; 6=Greater;
  KTESTW        K1,  K1                   //;FE455439 any lanes still alive           ;K1=lane_active;
  JZ            next                      //;CD5F484F no, exit; jump if zero (ZF = 1) ;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPERMD        CONST_TAIL_MASK(),Z3,  Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z3=str_len;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle
  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcCmpStrEqCi

//; #region bcCmpStrEqUTF8Ci
//; empty needles or empty data always result in a dead lane
//
// k[0] = cmp_str_eq_utf8_ci(s[1], x[2]).k[3]
TEXT bcCmpStrEqUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameters
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141 load number of code-points      ;CX=needle_len; R14=needle_ptr;
  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=needle_len;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;B73A4F83 K1 &= (scratch_Z26<=str_len)    ;K1=lane_active; Z26=scratch_Z26; Z3=str_len; 2=LessEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  ADDQ          $4,  R14                  //;7B0665F3 needle_ptr += 4                 ;R14=needle_ptr;
//; load constants
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;

loop:
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_len != 0); empty data are dead lanes;K1=lane_active; Z3=str_len;
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

//; NOTE: debugging. If you jump from here to mixed_ascii you bypass the 4 ASCII optimization
  CMPL          CX,  $4                   //;E273EEEA are we in the needle tail?      ;CX=needle_len;
  JL            mixed_ascii               //;A8685FD7 yes, then jump; jump if less (SF neq OF);
  VPBROADCASTD.Z 16(R14),K1,  Z9          //;2694A02F load needle data                ;Z9=data_needle; K1=lane_active; R14=needle_ptr;
//; determine if either data or needle has non-ASCII content
  VPORD.Z       Z8,  Z9,  K1,  Z26        //;3692D686 scratch_Z26 := data_needle | data_msg;Z26=scratch_Z26; K1=lane_active; Z9=data_needle; Z8=data_msg;
  VPMOVB2M      Z26, K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);
//; clear tail from data: IN zmm8; OUT zmm8
  VPMINSD       Z3,  Z20, Z26             //;DEC17BF3 scratch_Z26 := min(4, str_len)  ;Z26=scratch_Z26; Z20=4; Z3=str_len;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z13 //;1BB96D97                                 ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle for 4 ASCIIs
  VPCMPD        $0,  Z13, Z9,  K1,  K1    //;BBBDF880 K1 &= (data_needle==data_msg_upper);K1=lane_active; Z9=data_needle; Z13=data_msg_upper; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next 4 ASCIIs
  VPADDD        Z20, Z2,  K1,  Z2         //;D7CC90DD str_start += 4                  ;Z2=str_start; K1=lane_active; Z20=4;
  VPSUBD        Z20, Z3,  K1,  Z3         //;AEDCD850 str_len -= 4                    ;Z3=str_len; K1=lane_active; Z20=4;
  ADDQ          $80, R14                  //;F0BC3163 needle_ptr += 80                ;R14=needle_ptr;
  SUBL          $4,  CX                   //;646B86C9 needle_len -= 4                 ;CX=needle_len;
  JG            loop                      //;1EBC2C20 jump if greater ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;

mixed_ascii:
//; select next UTF8 byte sequence
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; compare data with needle for 1 UTF8 byte sequence
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K3   //;345D0BF3 K3 := K1 & (data_msg==[needle_ptr]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  4(R14),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[needle_ptr+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  8(R14),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[needle_ptr+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  12(R14),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[needle_ptr+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next rune
  VPADDD        Z7,  Z2,  K1,  Z2         //;DFE8D20B str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;24E04BE7 str_len -= n_bytes_data         ;Z3=str_len; K1=lane_active; Z7=n_bytes_data;
  ADDQ          $20, R14                  //;1F8D79B1 needle_ptr += 20                ;R14=needle_ptr;
  DECL          CX                        //;A99E9290 needle_len--                    ;CX=needle_len;
  JG            loop                      //;80013DFA jump if greater ((ZF = 0) and (SF = OF));

next:
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;E555E77C K1 &= (str_len==0)              ;K1=lane_active; Z3=str_len;
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcCmpStrEqUTF8Ci

//; #region bcCmpStrFuzzyA3
//
// k[0] = cmp_str_fuzzy_A3(slice[1], i64[2], dict[3]).k[4]
TEXT bcCmpStrFuzzyA3(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

//; load parameters
  MOVQ          (R15),R15                 //;D2647DF0 load needle_ptr                 ;R15=update_data_ptr;
  MOVQ          R15, R14                  //;EC3C3E7D needle_ptr := update_data_ptr   ;R14=needle_ptr; R15=update_data_ptr;
  ADDQ          $512,R14                  //;46443C9E needle_ptr += 512               ;R14=needle_ptr;
  VPBROADCASTD  (R14),Z13                 //;713DF24F bcst needle_len                 ;Z13=needle_len; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B63DFEAF needle_ptr += 4                 ;R14=needle_ptr;
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch2;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch1;
  VINSERTI64X4  $1,  Y26, Z27, Z14        //;3944001B merge into 16x uint32           ;Z14=threshold; Z27=scratch2; Z26=scratch1;
//; restrict lanes to allow fast bail-out
  VPSUBD        Z14, Z13, Z26             //;10B77777 scratch1 := needle_len - threshold;Z26=scratch1; Z13=needle_len; Z14=threshold;
  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;F08352A0 K1 &= (scratch1<=data_len)      ;K1=lane_active; Z26=scratch1; Z3=data_len; 2=LessEq;
  KTESTW        K1,  K1                   //;8BEF97CD ZF := (K1==0); CF := 1          ;K1=lane_active;
  JZ            next                      //;5FEF8EC0 jump if zero (ZF = 1)           ;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VPBROADCASTD  CONSTD_3(),Z24            //;6F57EE92 load constant 3                 ;Z24=3;
  VPBROADCASTD  CONSTD_0x10101(),Z19      //;2AB82DC0 load constant 0x10101           ;Z19=0x10101;
  VPBROADCASTD  CONSTD_0x10801(),Z22      //;A76098A3 load constant 0x10801           ;Z22=0x10801;
  VPBROADCASTD  CONSTD_0x400001(),Z23     //;3267BFF0 load constant 0x400001          ;Z23=0x400001;
  VPSLLD        $1,  Z19, Z20             //;68F49F35 0x20202 := 0x10101<<1           ;Z20=0x20202; Z19=0x10101;
  VPSLLD        $2,  Z19, Z21             //;2789BA9D 0x40404 := 0x10101<<2           ;Z21=0x40404; Z19=0x10101;
  VMOVDQU32     FUZZY_ROR_APPROX3(),Z25   //;2CEF25D2 load ror approx3                ;Z25=ror_a3;
  VMOVDQU32     CONST_TAIL_INV_MASK(),Z18 //;9653E713 load tail_inv_mask_data         ;Z18=tail_mask_data;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables loop2
  VPXORD        Z4,  Z4,  Z4              //;6D778B5D edit_dist := 0                  ;Z4=edit_dist;
  VPXORD        Z5,  Z5,  Z5              //;B150336A needle_off := 0                 ;Z5=needle_off;
  KMOVW         K1,  K2                   //;FBC36D43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;1607AB46 lane_active := 0                ;K1=lane_active;
loop2:
//; load data ascii approx3
//; clear data
  VPTERNLOGD    $0b11111111,Z8,  Z8,  Z8  //;F81949DF set 0xFFFFFFFF                  ;Z8=data;
  VPTERNLOGD    $0b11111111,Z9,  Z9,  Z9  //;9E9BD820 set 0xFFFFFFFF                  ;Z9=needle;
//; load data and needle
  VPCMPD        $6,  Z11, Z3,  K2,  K4    //;FCFCB494 K4 := K2 & (data_len>0)         ;K4=scratch1; K2=lane_todo; Z3=data_len; Z11=0; 6=Greater;
  VPCMPD        $6,  Z11, Z13, K2,  K5    //;7C687BDA K5 := K2 & (needle_len>0)       ;K5=scratch2; K2=lane_todo; Z13=needle_len; Z11=0; 6=Greater;
  KMOVW         K4,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K4=scratch1;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off;
  VPGATHERDD    (R14)(Z5*1),K5,  Z9       //;4EAE4300 gather needle                   ;Z9=needle; K5=scratch2; R14=needle_ptr; Z5=needle_off;
//; remove tail from data
  VPMINSD       Z3,  Z24, Z26             //;D337D11D scratch1 := min(3, data_len)    ;Z26=scratch1; Z24=3; Z3=data_len;
  VPERMD        Z18, Z26, Z26             //;E882D550                                 ;Z26=scratch1; Z18=tail_mask_data;
  VPORD         Z8,  Z26, K4,  Z8         //;985B667B data |= scratch1                ;Z8=data; K4=scratch1; Z26=scratch1;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch1; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch1; Z8=data; Z15=c_0b00100000;
//; prepare data
  VPSHUFB       Z25, Z9,  Z27             //;9E2DA720                                 ;Z27=scratch2; Z9=needle; Z25=ror_a3;
  VPSHUFB       Z25, Z27, Z28             //;EF2A718F                                 ;Z28=scratch3; Z27=scratch2; Z25=ror_a3;
//; compare data
  VPCMPB        $0,  Z26, Z9,  K3         //;B72E9264 K3 := (needle==scratch1)        ;K3=tmp_mask; Z9=needle; Z26=scratch1; 0=Eq;
  VPCMPB        $0,  Z26, Z27, K4         //;4D744078 K4 := (scratch2==scratch1)      ;K4=scratch1; Z27=scratch2; Z26=scratch1; 0=Eq;
  VPCMPB        $0,  Z26, Z28, K5         //;7D453022 K5 := (scratch3==scratch1)      ;K5=scratch2; Z28=scratch3; Z26=scratch1; 0=Eq;
//; fuzzy kernel approx3: create lookup key in Z26
  VMOVDQU8.Z    Z19, K3,  Z26             //;AE601720 0000_0000 0000_000a 0000_000b 0000_000c;Z26=scratch1; K3=tmp_mask; Z19=0x10101;
  VMOVDQU8.Z    Z20, K4,  Z27             //;C8E83855 0000_0000 0000_00d0 0000_00e0 0000_00f0;Z27=scratch2; K4=scratch1; Z20=0x20202;
  VMOVDQU8.Z    Z21, K5,  Z29             //;1AA38791 0000_0000 0000_0g00 0000_0h00 0000_0i00;Z29=scratch4; K5=scratch2; Z21=0x40404;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z26 //;FF99B822 0000_0000 0000_0gda 0000_0heb 0000_0ifc;Z26=scratch1; Z27=scratch2; Z29=scratch4;
  VPMADDUBSW    Z26, Z22, Z26             //;6483F13D 0000_0000 0000_0gda 0000_0000 00he_bifc;Z26=scratch1; Z22=0x10801;
  VPMADDWD      Z26, Z23, Z26             //;3FA33058 0000_0000 0000_0000 0000_000g dahe_bifc;Z26=scratch1; Z23=0x400001;
//; do the lookup of the advance values
  KMOVW         K2,  K3                   //;45A99B90 tmp_mask := lane_todo           ;K3=tmp_mask; K2=lane_todo;
  VPGATHERDD    (R15)(Z26*1),K3,  Z27     //;C3AEFFBF                                 ;Z27=scratch2; K3=tmp_mask; R15=update_data_ptr; Z26=scratch1;
//; unpack results
  VPSRLD        $4,  Z27, Z28             //;87E05D1F ed_delta := scratch2>>4         ;Z28=ed_delta; Z27=scratch2;
  VPSRLD        $2,  Z27, Z9              //;13C921F9 adv_needle := scratch2>>2       ;Z9=adv_needle; Z27=scratch2;
  VPANDD        Z24, Z28, Z28             //;A7E1848A ed_delta &= 3                   ;Z28=ed_delta; Z24=3;
  VPANDD        Z24, Z27, Z8              //;42B9DA14 adv_data := scratch2 & 3        ;Z8=adv_data; Z27=scratch2; Z24=3;
  VPANDD        Z24, Z9,  Z9              //;E2CB942A adv_needle &= 3                 ;Z9=adv_needle; Z24=3;
//; update ascii
//; update edit distance
  VPADDD        Z28, Z4,  K2,  Z4         //;F07EA991 edit_dist += ed_delta           ;Z4=edit_dist; K2=lane_todo; Z28=ed_delta;
//; advance data
  VPSUBD        Z8,  Z3,  K2,  Z3         //;DF7FB44E data_len -= adv_data            ;Z3=data_len; K2=lane_todo; Z8=adv_data;
  VPADDD        Z8,  Z2,  K2,  Z2         //;F81C97B0 data_off += adv_data            ;Z2=data_off; K2=lane_todo; Z8=adv_data;
//; advance needle
  VPSUBD        Z9,  Z13, K2,  Z13        //;41970AC0 needle_len -= adv_needle        ;Z13=needle_len; K2=lane_todo; Z9=adv_needle;
  VPADDD        Z9,  Z5,  K2,  Z5         //;254FD5C6 needle_off += adv_needle        ;Z5=needle_off; K2=lane_todo; Z9=adv_needle;
//; cmp-tail
//; restrict lanes based on edit distance and threshold
  VPCMPD        $2,  Z14, Z4,  K2,  K2    //;86F95312 K2 &= (edit_dist<=threshold)    ;K2=lane_todo; Z4=edit_dist; Z14=threshold; 2=LessEq;
//; test if we have a match
  VPCMPD        $5,  Z13, Z11, K2,  K3    //;EEF4BAA0 K3 := K2 & (0>=needle_len)      ;K3=tmp_mask; K2=lane_todo; Z11=0; Z13=needle_len; 5=GreaterEq;
  VPCMPD        $5,  Z3,  Z11, K3,  K3    //;4CCAC6C8 K3 &= (0>=data_len)             ;K3=tmp_mask; Z11=0; Z3=data_len; 5=GreaterEq;
  KORW          K1,  K3,  K1              //;8FAB61CD lane_active |= tmp_mask         ;K1=lane_active; K3=tmp_mask;
  KANDNW        K2,  K3,  K2              //;482703BD lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KTESTW        K2,  K2                   //;4661A15A any lanes still todo?           ;K2=lane_todo;
  JNZ           loop2                     //;28307DE7 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcCmpStrFuzzyA3

//; #region bcCmpStrFuzzyUnicodeA3
//
// k[0] = cmp_str_fuzzy_unicode_A3(slice[1], i64[2], dict[3]).k[4]
TEXT bcCmpStrFuzzyUnicodeA3(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

//; load parameters
  MOVQ          (R15),R15                 //;D2647DF0 load needle_ptr                 ;R15=update_data_ptr;
  MOVQ          R15, R14                  //;EC3C3E7D needle_ptr := update_data_ptr   ;R14=needle_ptr; R15=update_data_ptr;
  ADDQ          $512,R14                  //;46443C9E needle_ptr += 512               ;R14=needle_ptr;
  VPBROADCASTD  (R14),Z13                 //;713DF24F bcst needle_len                 ;Z13=needle_len; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B63DFEAF needle_ptr += 4                 ;R14=needle_ptr;
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch2;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch1;
  VINSERTI64X4  $1,  Y26, Z27, Z14        //;3944001B merge into 16x uint32           ;Z14=threshold; Z27=scratch2; Z26=scratch1;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z24  //;B323211A load table_n_bytes_utf8         ;Z24=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z6              //;00000000 constd_2 := 1 + 1               ;Z6=2; Z10=1;
  VPADDD        Z10, Z6,  Z7              //;00000000 constd_3 := 2 + 1               ;Z7=3; Z6=2; Z10=1;
  VPADDD        Z6,  Z6,  Z23             //;00000000 constd_4 := 2 + 2               ;Z23=4; Z6=2;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables loop2
  VPXORD        Z4,  Z4,  Z4              //;6D778B5D edit_dist := 0                  ;Z4=edit_dist;
  VPXORD        Z5,  Z5,  Z5              //;B150336A needle_off := 0                 ;Z5=needle_off;
  KMOVW         K1,  K2                   //;FBC36D43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;1607AB46 lane_active := 0                ;K1=lane_active;
loop2:
//; load data unicode approx3
//; clear data0 and needle0
  VPTERNLOGD    $0b11111111,Z8,  Z8,  Z8  //;B8E7EDD5 set 0xFFFFFFFF                  ;Z8=d0;
  VPTERNLOGD    $0b11111111,Z9,  Z9,  Z9  //;A8E35E7F set 0xFFFFFFFF                  ;Z9=d1;
  VPTERNLOGD    $0b11111111,Z28, Z28, Z28 //;3CB765D7 set 0xFFFFFFFF                  ;Z28=d2;
//; load data0
  VPCMPD        $5,  Z10, Z3,  K2,  K3    //;FCFCB494 K3 := K2 & (data_len>=1)        ;K3=tmp_mask; K2=lane_todo; Z3=data_len; Z10=1; 5=GreaterEq;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data0                    ;Z8=d0; K3=tmp_mask; SI=data_ptr; Z2=data_off;
//; get number of bytes in code-point
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch1 := d0>>4               ;Z26=scratch1; Z8=d0;
  VPERMD        Z24, Z26, Z12             //;68FECBA0 get n_bytes_d0                  ;Z12=n_bytes_d0; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load data1
  VPSUBD        Z12, Z3,  Z20             //;8BA4E60E data_len_alt := data_len - n_bytes_d0;Z20=data_len_alt; Z3=data_len; Z12=n_bytes_d0;
  VPCMPD        $5,  Z10, Z20, K2,  K3    //;FCFCB494 K3 := K2 & (data_len_alt>=1)    ;K3=tmp_mask; K2=lane_todo; Z20=data_len_alt; Z10=1; 5=GreaterEq;
  VPADDD        Z12, Z2,  K2,  Z19        //;88A5638D data_off_alt := data_off + n_bytes_d0;Z19=data_off_alt; K2=lane_todo; Z2=data_off; Z12=n_bytes_d0;
  VPGATHERDD    (VIRT_BASE)(Z19*1),K3,  Z9 //;81256F6A gather data1                    ;Z9=d1; K3=tmp_mask; SI=data_ptr; Z19=data_off_alt;
//; get number of bytes in code-point
  VPSRLD        $4,  Z9,  Z26             //;FE5F1413 scratch1 := d1>>4               ;Z26=scratch1; Z9=d1;
  VPERMD        Z24, Z26, Z22             //;68FECBA0 get n_bytes_d1                  ;Z22=n_bytes_d1; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load data2
  VPSUBD        Z22, Z20, Z20             //;743C9E50 data_len_alt -= n_bytes_d1      ;Z20=data_len_alt; Z22=n_bytes_d1;
  VPCMPD        $5,  Z10, Z20, K2,  K3    //;C500A274 K3 := K2 & (data_len_alt>=1)    ;K3=tmp_mask; K2=lane_todo; Z20=data_len_alt; Z10=1; 5=GreaterEq;
  VPADDD        Z22, Z19, K2,  Z19        //;3656366C data_off_alt += n_bytes_d1      ;Z19=data_off_alt; K2=lane_todo; Z22=n_bytes_d1;
  VPGATHERDD    (VIRT_BASE)(Z19*1),K3,  Z28 //;3640A661 gather data2                    ;Z28=d2; K3=tmp_mask; SI=data_ptr; Z19=data_off_alt;
//; get number of bytes in code-point
  VPSRLD        $4,  Z28, Z26             //;FE5F1413 scratch1 := d2>>4               ;Z26=scratch1; Z28=d2;
  VPERMD        Z24, Z26, Z25             //;68FECBA0 get n_bytes_d2                  ;Z25=n_bytes_d2; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load needles
  VPTERNLOGD    $0b11111111,Z19, Z19, Z19 //;9E9BD820 set 0xFFFFFFFF                  ;Z19=n0;
  VPTERNLOGD    $0b11111111,Z20, Z20, Z20 //;4408EAE3 set 0xFFFFFFFF                  ;Z20=n1;
  VPTERNLOGD    $0b11111111,Z21, Z21, Z21 //;61F40C30 set 0xFFFFFFFF                  ;Z21=n2;
  VPCMPD        $5,  Z10, Z13, K2,  K3    //;7C687BDA K3 := K2 & (needle_len>=1)      ;K3=tmp_mask; K2=lane_todo; Z13=needle_len; Z10=1; 5=GreaterEq;
  VPCMPD        $6,  Z10, Z13, K2,  K4    //;E2B17160 K4 := K2 & (needle_len>1)       ;K4=scratch1; K2=lane_todo; Z13=needle_len; Z10=1; 6=Greater;
  VPCMPD.BCST   $6,  CONSTD_2(),Z13, K2,  K5  //;62D5A597 K5 := K2 & (needle_len>2)   ;K5=scratch2; K2=lane_todo; Z13=needle_len; 6=Greater;
  VPGATHERDD    (R14)(Z5*1),K3,  Z19      //;4EAE4300 gather needle0                  ;Z19=n0; K3=tmp_mask; R14=needle_ptr; Z5=needle_off;
  VPGATHERDD    4(R14)(Z5*1),K4,  Z20     //;7F0BC8AC gather needle1                  ;Z20=n1; K4=scratch1; R14=needle_ptr; Z5=needle_off;
  VPGATHERDD    8(R14)(Z5*1),K5,  Z21     //;2FC91E09 gather needle2                  ;Z21=n2; K5=scratch2; R14=needle_ptr; Z5=needle_off;
//; remove tail from data
  VPERMD        Z18, Z12, Z26             //;83C76A20 get tail_mask (data)            ;Z26=scratch1; Z12=n_bytes_d0; Z18=tail_mask_data;
  VPERMD        Z18, Z22, Z27             //;46B49FFC get tail_mask (data)            ;Z27=scratch2; Z22=n_bytes_d1; Z18=tail_mask_data;
  VPERMD        Z18, Z25, Z29             //;38DC4AAA get tail_mask (data)            ;Z29=scratch3; Z25=n_bytes_d2; Z18=tail_mask_data;
  VPANDD        Z8,  Z26, Z8              //;D1E2261E mask data                       ;Z8=d0; Z26=scratch1;
  VPANDD        Z9,  Z27, Z9              //;A5CA4DE9 mask data                       ;Z9=d1; Z27=scratch2;
  VPANDD        Z28, Z29, Z28             //;C05E8C87 mask data                       ;Z28=d2; Z29=scratch3;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (d0>=char_a)              ;K3=tmp_mask; Z8=d0; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (d0<=char_z)              ;K3=tmp_mask; Z8=d0; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch1; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch1; Z8=d0; Z15=c_0b00100000;
  VMOVDQA32     Z26, Z8                   //;41954CFC d0 := scratch1                  ;Z8=d0; Z26=scratch1;
//; str_to_upper: IN zmm9; OUT zmm27
  VPCMPB        $5,  Z16, Z9,  K3         //;30E9B9FD K3 := (d1>=char_a)              ;K3=tmp_mask; Z9=d1; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z9,  K3,  K3    //;8CE85BA0 K3 &= (d1<=char_z)              ;K3=tmp_mask; Z9=d1; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z27                  //;ADC21F45 mask with selected chars        ;Z27=scratch2; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z9,  Z27 //;1BB96D97                                 ;Z27=scratch2; Z9=d1; Z15=c_0b00100000;
  VMOVDQA32     Z27, Z9                   //;60AA018C d1 := scratch2                  ;Z9=d1; Z27=scratch2;
//; str_to_upper: IN zmm28; OUT zmm29
  VPCMPB        $5,  Z16, Z28, K3         //;30E9B9FD K3 := (d2>=char_a)              ;K3=tmp_mask; Z28=d2; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z28, K3,  K3    //;8CE85BA0 K3 &= (d2<=char_z)              ;K3=tmp_mask; Z28=d2; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z29                  //;ADC21F45 mask with selected chars        ;Z29=scratch3; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z28, Z29 //;1BB96D97                                 ;Z29=scratch3; Z28=d2; Z15=c_0b00100000;
  VMOVDQA32     Z29, Z28                  //;250F442B d2 := scratch3                  ;Z28=d2; Z29=scratch3;
//; create key with bits for NeedleData:02-12-22-21-01-11-10-20-00
  VPCMPD        $0,  Z28, Z21, K3         //;00000000 K3 := (n2==d2)                  ;K3=tmp_mask; Z21=n2; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z21, K4         //;00000000 K4 := (n2==d1)                  ;K4=scratch1; Z21=n2; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z21, K5         //;00000000 K5 := (n2==d0)                  ;K5=scratch2; Z21=n2; Z8=d0; 0=Eq;
  VPSLLD.Z      $6,  Z10, K3,  Z21        //;00000000 key := 1<<6                     ;Z21=key; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $5,  Z10, K4,  Z27        //;00000000 scratch2 := 1<<5                ;Z27=scratch2; K4=scratch1; Z10=1;
  VPSLLD.Z      $1,  Z10, K5,  Z29        //;00000000 scratch3 := 1<<1                ;Z29=scratch3; K5=scratch2; Z10=1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

  VPCMPD        $0,  Z28, Z20, K3         //;00000000 K3 := (n1==d2)                  ;K3=tmp_mask; Z20=n1; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z20, K4         //;00000000 K4 := (n1==d1)                  ;K4=scratch1; Z20=n1; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z20, K5         //;00000000 K5 := (n1==d0)                  ;K5=scratch2; Z20=n1; Z8=d0; 0=Eq;
  VPSLLD.Z      $7,  Z10, K3,  Z27        //;00000000 scratch2 := 1<<7                ;Z27=scratch2; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $3,  Z10, K4,  Z26        //;00000000 scratch1 := 1<<3                ;Z26=scratch1; K4=scratch1; Z10=1;
  VPSLLD.Z      $2,  Z10, K5,  Z29        //;00000000 scratch3 := 1<<2                ;Z29=scratch3; K5=scratch2; Z10=1;
  VPORD         Z26, Z27, Z27             //;00000000 scratch2 |= scratch1            ;Z27=scratch2; Z26=scratch1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

  VPCMPD        $0,  Z28, Z19, K3         //;00000000 K3 := (n0==d2)                  ;K3=tmp_mask; Z19=n0; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z19, K4         //;00000000 K4 := (n0==d1)                  ;K4=scratch1; Z19=n0; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z19, K5         //;00000000 K5 := (n0==d0)                  ;K5=scratch2; Z19=n0; Z8=d0; 0=Eq;
  VPSLLD.Z      $8,  Z10, K3,  Z29        //;00000000 scratch3 := 1<<8                ;Z29=scratch3; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $4,  Z10, K4,  Z27        //;00000000 scratch2 := 1<<4                ;Z27=scratch2; K4=scratch1; Z10=1;
  VPSLLD.Z      $0,  Z10, K5,  Z26        //;00000000 scratch1 := 1<<0                ;Z26=scratch1; K5=scratch2; Z10=1;
  VPORD         Z26, Z27, Z27             //;00000000 scratch2 |= scratch1            ;Z27=scratch2; Z26=scratch1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

//; do the lookup of the advance values
  KMOVW         K2,  K3                   //;45A99B90 tmp_mask := lane_todo           ;K3=tmp_mask; K2=lane_todo;
  VPGATHERDD    (R15)(Z21*1),K3,  Z26     //;C3AEFFBF                                 ;Z26=scratch1; K3=tmp_mask; R15=update_data_ptr; Z21=key;
//; unpack results
  VPSRLD        $4,  Z26, Z19             //;87E05D1F ed_delta := scratch1>>4         ;Z19=ed_delta; Z26=scratch1;
  VPSRLD        $2,  Z26, Z21             //;13C921F9 adv_needle := scratch1>>2       ;Z21=adv_needle; Z26=scratch1;
  VPANDD        Z7,  Z19, Z19             //;A7E1848A ed_delta &= 3                   ;Z19=ed_delta; Z7=3;
  VPANDD        Z7,  Z26, Z20             //;42B9DA14 adv_data := scratch1 & 3        ;Z20=adv_data; Z26=scratch1; Z7=3;
  VPANDD        Z7,  Z21, Z21             //;E2CB942A adv_needle &= 3                 ;Z21=adv_needle; Z7=3;
//; update unicode approx3
//; update edit distance
  VPADDD        Z4,  Z19, Z4              //;15747D59 edit_dist += ed_delta           ;Z4=edit_dist; Z19=ed_delta;
//; advance data
  VPCMPD        $5,  Z10, Z20, K2,  K3    //;7FC1A433 K3 := K2 & (adv_data>=1)        ;K3=tmp_mask; K2=lane_todo; Z20=adv_data; Z10=1; 5=GreaterEq;
  VMOVDQA32.Z   Z12, K3,  Z26             //;6428B608 scratch1 := n_bytes_d0          ;Z26=scratch1; K3=tmp_mask; Z12=n_bytes_d0;
  VPCMPD        $5,  Z6,  Z20, K2,  K3    //;4FC83ED2 K3 := K2 & (adv_data>=2)        ;K3=tmp_mask; K2=lane_todo; Z20=adv_data; Z6=2; 5=GreaterEq;
  VPADDD        Z22, Z26, K3,  Z26        //;BAE14871 scratch1 += n_bytes_d1          ;Z26=scratch1; K3=tmp_mask; Z22=n_bytes_d1;
  VPCMPD        $5,  Z7,  Z20, K2,  K3    //;120E0569 K3 := K2 & (adv_data>=3)        ;K3=tmp_mask; K2=lane_todo; Z20=adv_data; Z7=3; 5=GreaterEq;
  VPADDD        Z25, Z26, K3,  Z26        //;9BAA69A9 scratch1 += n_bytes_d2          ;Z26=scratch1; K3=tmp_mask; Z25=n_bytes_d2;
  VPSUBD        Z26, Z3,  K2,  Z3         //;DF7FB44E data_len -= scratch1            ;Z3=data_len; K2=lane_todo; Z26=scratch1;
  VPADDD        Z26, Z2,  K2,  Z2         //;F81C97B0 data_off += scratch1            ;Z2=data_off; K2=lane_todo; Z26=scratch1;
//; advance needle
  VPCMPD        $5,  Z10, Z21, K2,  K3    //;5578190E K3 := K2 & (adv_needle>=1)      ;K3=tmp_mask; K2=lane_todo; Z21=adv_needle; Z10=1; 5=GreaterEq;
  VMOVDQA32.Z   Z23, K3,  Z26             //;00000000 scratch1 := 4                   ;Z26=scratch1; K3=tmp_mask; Z23=4;
  VPCMPD        $5,  Z6,  Z21, K2,  K3    //;A814294A K3 := K2 & (adv_needle>=2)      ;K3=tmp_mask; K2=lane_todo; Z21=adv_needle; Z6=2; 5=GreaterEq;
  VPADDD        Z23, Z26, K3,  Z26        //;B2BE82E2 scratch1 += 4                   ;Z26=scratch1; K3=tmp_mask; Z23=4;
  VPSUBD        Z21, Z13, K2,  Z13        //;41970AC0 needle_len -= adv_needle        ;Z13=needle_len; K2=lane_todo; Z21=adv_needle;
  VPADDD        Z26, Z5,  K2,  Z5         //;89331149 needle_off += scratch1          ;Z5=needle_off; K2=lane_todo; Z26=scratch1;
//; cmp-tail
//; restrict lanes based on edit distance and threshold
  VPCMPD        $2,  Z14, Z4,  K2,  K2    //;86F95312 K2 &= (edit_dist<=threshold)    ;K2=lane_todo; Z4=edit_dist; Z14=threshold; 2=LessEq;
//; test if we have a match
  VPCMPD        $5,  Z13, Z11, K2,  K3    //;EEF4BAA0 K3 := K2 & (0>=needle_len)      ;K3=tmp_mask; K2=lane_todo; Z11=0; Z13=needle_len; 5=GreaterEq;
  VPCMPD        $5,  Z3,  Z11, K3,  K3    //;4CCAC6C8 K3 &= (0>=data_len)             ;K3=tmp_mask; Z11=0; Z3=data_len; 5=GreaterEq;
  KORW          K1,  K3,  K1              //;8FAB61CD lane_active |= tmp_mask         ;K1=lane_active; K3=tmp_mask;
  KANDNW        K2,  K3,  K2              //;482703BD lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KTESTW        K2,  K2                   //;4661A15A any lanes still todo?           ;K2=lane_todo;
  JNZ           loop2                     //;28307DE7 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcCmpStrFuzzyUnicodeA3

//; #region bcHasSubstrFuzzyA3
// k[0] = contains_fuzzy_A3(slice[1], i64[2], dict[3]).k[4]
TEXT bcHasSubstrFuzzyA3(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

//; load parameters
  MOVQ          (R15),R15                 //;D2647DF0 load needle_ptr                 ;R15=update_data_ptr;
  MOVQ          R15, R14                  //;EC3C3E7D needle_ptr := update_data_ptr   ;R14=needle_ptr; R15=update_data_ptr;
  ADDQ          $512,R14                  //;46443C9E needle_ptr += 512               ;R14=needle_ptr;
  VPBROADCASTD  (R14),Z13                 //;713DF24F bcst needle_len                 ;Z13=needle_len; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B63DFEAF needle_ptr += 4                 ;R14=needle_ptr;
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch2;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch1;
  VINSERTI64X4  $1,  Y26, Z27, Z14        //;3944001B merge into 16x uint32           ;Z14=threshold; Z27=scratch2; Z26=scratch1;
//; restrict lanes to allow fast bail-out
  VPSUBD        Z14, Z13, Z26             //;10B77777 scratch1 := needle_len - threshold;Z26=scratch1; Z13=needle_len; Z14=threshold;
  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;F08352A0 K1 &= (scratch1<=data_len)      ;K1=lane_active; Z26=scratch1; Z3=data_len; 2=LessEq;
  KTESTW        K1,  K1                   //;8BEF97CD ZF := (K1==0); CF := 1          ;K1=lane_active;
  JZ            next                      //;5FEF8EC0 jump if zero (ZF = 1)           ;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VPBROADCASTD  CONSTD_3(),Z24            //;6F57EE92 load constant 3                 ;Z24=3;
  VPBROADCASTD  CONSTD_0x10101(),Z19      //;2AB82DC0 load constant 0x10101           ;Z19=0x10101;
  VPBROADCASTD  CONSTD_0x10801(),Z22      //;A76098A3 load constant 0x10801           ;Z22=0x10801;
  VPBROADCASTD  CONSTD_0x400001(),Z23     //;3267BFF0 load constant 0x400001          ;Z23=0x400001;
  VPSLLD        $1,  Z19, Z20             //;68F49F35 0x20202 := 0x10101<<1           ;Z20=0x20202; Z19=0x10101;
  VPSLLD        $2,  Z19, Z21             //;2789BA9D 0x40404 := 0x10101<<2           ;Z21=0x40404; Z19=0x10101;
  VMOVDQU32     CONST_TAIL_INV_MASK(),Z18 //;9653E713 load tail_inv_mask_data         ;Z18=tail_mask_data;
  VMOVDQU32     FUZZY_ROR_APPROX3(),Z25   //;2CEF25D2 load ror approx3                ;Z25=ror_a3;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables loop2
  KMOVW         K1,  K2                   //;FBC36D43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;1607AB46 lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z2,  Z12                  //;467EBBBC data_off2 := data_off           ;Z12=data_off2; Z2=data_off;
  VMOVDQA32     Z3,  Z6                   //;DD42E20B data_len2 := data_len           ;Z6=data_len2; Z3=data_len;
  VMOVDQA32     Z13, Z7                   //;29B33C21 needle_len2 := needle_len       ;Z7=needle_len2; Z13=needle_len;
loop2:
//; init variables loop1
  VPXORD        Z4,  Z4,  Z4              //;6D778B5D edit_dist := 0                  ;Z4=edit_dist;
  VPXORD        Z5,  Z5,  Z5              //;B150336A needle_off := 0                 ;Z5=needle_off;
  KMOVW         K2,  K6                   //;FDBD9EFA lane_todo2 := lane_todo         ;K6=lane_todo2; K2=lane_todo;
loop1:
//; load data ascii approx3
//; clear data
  VPTERNLOGD    $0b11111111,Z8,  Z8,  Z8  //;F81949DF set 0xFFFFFFFF                  ;Z8=data;
  VPTERNLOGD    $0b11111111,Z9,  Z9,  Z9  //;9E9BD820 set 0xFFFFFFFF                  ;Z9=needle;
//; load data and needle
  VPCMPD        $6,  Z11, Z6,  K6,  K4    //;FCFCB494 K4 := K6 & (data_len2>0)        ;K4=scratch1; K6=lane_todo2; Z6=data_len2; Z11=0; 6=Greater;
  VPCMPD        $6,  Z11, Z13, K6,  K5    //;7C687BDA K5 := K6 & (needle_len>0)       ;K5=scratch2; K6=lane_todo2; Z13=needle_len; Z11=0; 6=Greater;
  KMOVW         K4,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K4=scratch1;
  VPGATHERDD    (VIRT_BASE)(Z12*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z12=data_off2;
  VPGATHERDD    (R14)(Z5*1),K5,  Z9       //;4EAE4300 gather needle                   ;Z9=needle; K5=scratch2; R14=needle_ptr; Z5=needle_off;
//; remove tail from data
  VPMINSD       Z6,  Z24, Z26             //;D337D11D scratch1 := min(3, data_len2)   ;Z26=scratch1; Z24=3; Z6=data_len2;
  VPERMD        Z18, Z26, Z26             //;E882D550                                 ;Z26=scratch1; Z18=tail_mask_data;
  VPORD         Z8,  Z26, K4,  Z8         //;985B667B data |= scratch1                ;Z8=data; K4=scratch1; Z26=scratch1;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch1; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch1; Z8=data; Z15=c_0b00100000;
//; prepare data
  VPSHUFB       Z25, Z9,  Z27             //;9E2DA720                                 ;Z27=scratch2; Z9=needle; Z25=ror_a3;
  VPSHUFB       Z25, Z27, Z28             //;EF2A718F                                 ;Z28=scratch3; Z27=scratch2; Z25=ror_a3;
//; compare data
  VPCMPB        $0,  Z26, Z9,  K3         //;B72E9264 K3 := (needle==scratch1)        ;K3=tmp_mask; Z9=needle; Z26=scratch1; 0=Eq;
  VPCMPB        $0,  Z26, Z27, K4         //;4D744078 K4 := (scratch2==scratch1)      ;K4=scratch1; Z27=scratch2; Z26=scratch1; 0=Eq;
  VPCMPB        $0,  Z26, Z28, K5         //;7D453022 K5 := (scratch3==scratch1)      ;K5=scratch2; Z28=scratch3; Z26=scratch1; 0=Eq;
  VPBROADCASTD  CONSTD_0xFF(),Z27         //;8826339E load constant 0xFF              ;Z27=const_0xFF;
//; fuzzy kernel approx3: create lookup key in Z26
  VMOVDQU8.Z    Z19, K3,  Z26             //;AE601720 0000_0000 0000_000a 0000_000b 0000_000c;Z26=scratch1; K3=tmp_mask; Z19=0x10101;
  VMOVDQU8.Z    Z20, K4,  Z27             //;C8E83855 0000_0000 0000_00d0 0000_00e0 0000_00f0;Z27=scratch2; K4=scratch1; Z20=0x20202;
  VMOVDQU8.Z    Z21, K5,  Z29             //;1AA38791 0000_0000 0000_0g00 0000_0h00 0000_0i00;Z29=scratch4; K5=scratch2; Z21=0x40404;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z26 //;FF99B822 0000_0000 0000_0gda 0000_0heb 0000_0ifc;Z26=scratch1; Z27=scratch2; Z29=scratch4;
  VPMADDUBSW    Z26, Z22, Z26             //;6483F13D 0000_0000 0000_0gda 0000_0000 00he_bifc;Z26=scratch1; Z22=0x10801;
  VPMADDWD      Z26, Z23, Z26             //;3FA33058 0000_0000 0000_0000 0000_000g dahe_bifc;Z26=scratch1; Z23=0x400001;
//; do the lookup of the advance values
  KMOVW         K6,  K3                   //;45A99B90 tmp_mask := lane_todo2          ;K3=tmp_mask; K6=lane_todo2;
  VPGATHERDD    (R15)(Z26*1),K3,  Z27     //;C3AEFFBF                                 ;Z27=scratch2; K3=tmp_mask; R15=update_data_ptr; Z26=scratch1;
//; unpack results
  VPSRLD        $4,  Z27, Z28             //;87E05D1F ed_delta := scratch2>>4         ;Z28=ed_delta; Z27=scratch2;
  VPSRLD        $2,  Z27, Z9              //;13C921F9 adv_needle := scratch2>>2       ;Z9=adv_needle; Z27=scratch2;
  VPANDD        Z24, Z28, Z28             //;A7E1848A ed_delta &= 3                   ;Z28=ed_delta; Z24=3;
  VPANDD        Z24, Z27, Z8              //;42B9DA14 adv_data := scratch2 & 3        ;Z8=adv_data; Z27=scratch2; Z24=3;
  VPANDD        Z24, Z9,  Z9              //;E2CB942A adv_needle &= 3                 ;Z9=adv_needle; Z24=3;
//; update ascii
//; update edit distance
  VPADDD        Z28, Z4,  K6,  Z4         //;F07EA991 edit_dist += ed_delta           ;Z4=edit_dist; K6=lane_todo2; Z28=ed_delta;
//; advance data
  VPSUBD        Z8,  Z6,  K6,  Z6         //;DF7FB44E data_len2 -= adv_data           ;Z6=data_len2; K6=lane_todo2; Z8=adv_data;
  VPADDD        Z8,  Z12, K6,  Z12        //;F81C97B0 data_off2 += adv_data           ;Z12=data_off2; K6=lane_todo2; Z8=adv_data;
//; advance needle
  VPSUBD        Z9,  Z7,  K6,  Z7         //;41970AC0 needle_len2 -= adv_needle       ;Z7=needle_len2; K6=lane_todo2; Z9=adv_needle;
  VPADDD        Z9,  Z5,  K6,  Z5         //;254FD5C6 needle_off += adv_needle        ;Z5=needle_off; K6=lane_todo2; Z9=adv_needle;
//; has-tail
//; restrict lanes based on edit distance and threshold
  VPCMPD        $2,  Z14, Z4,  K6,  K6    //;86F95312 K6 &= (edit_dist<=threshold)    ;K6=lane_todo2; Z4=edit_dist; Z14=threshold; 2=LessEq;
//; test if we have a match
  VPCMPD        $5,  Z7,  Z11, K6,  K3    //;EEF4BAA0 K3 := K6 & (0>=needle_len2)     ;K3=tmp_mask; K6=lane_todo2; Z11=0; Z7=needle_len2; 5=GreaterEq;
  KANDNW        K6,  K3,  K6              //;482703BD lane_todo2 &= ~tmp_mask         ;K6=lane_todo2; K3=tmp_mask;
  KORW          K1,  K3,  K1              //;8FAB61CD lane_active |= tmp_mask         ;K1=lane_active; K3=tmp_mask;
  KTESTW        K6,  K6                   //;9D3A860D any lanes still todo?           ;K6=lane_todo2;
  JNZ           loop1                     //;EA12C247 yes, then loop; jump if not zero (ZF = 0);

  KANDNW        K2,  K6,  K2              //;E3BAA7D5 lane_todo &= ~lane_todo2        ;K2=lane_todo; K6=lane_todo2;
  VPSUBD        Z10, Z3,  K2,  Z3         //;7C100DFB data_len--                      ;Z3=data_len; K2=lane_todo; Z10=1;
  VPADDD        Z10, Z2,  K2,  Z2         //;397684ED data_off++                      ;Z2=data_off; K2=lane_todo; Z10=1;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;591E80A4 K2 &= (data_len>0)              ;K2=lane_todo; Z3=data_len; Z11=0; 6=Greater;
//; reset variables for loop2
  VMOVDQA32     Z3,  Z6                   //;1C31BFD8 data_len2 := data_len           ;Z6=data_len2; Z3=data_len;
  VMOVDQA32     Z2,  Z12                  //;3B8A334A data_off2 := data_off           ;Z12=data_off2; Z2=data_off;
  VMOVDQA32     Z13, Z7                   //;8EFD9390 needle_len2 := needle_len       ;Z7=needle_len2; Z13=needle_len;
  KTESTW        K2,  K2                   //;4661A15A any lanes still todo?           ;K2=lane_todo;
  JNZ           loop2                     //;28307DE7 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcHasSubstrFuzzyA3

//; #region bcHasSubstrFuzzyUnicodeA3
//
// k[0] = contains_fuzzy_unicode_A3(slice[1], i64[2], dict[3]).k[4]
TEXT bcHasSubstrFuzzyUnicodeA3(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(R15), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

//; load parameters
  MOVQ          (R15),R15                 //;D2647DF0 load needle_ptr                 ;R15=update_data_ptr;
  MOVQ          R15, R14                  //;EC3C3E7D needle_ptr := update_data_ptr   ;R14=needle_ptr; R15=update_data_ptr;
  ADDQ          $512,R14                  //;46443C9E needle_ptr += 512               ;R14=needle_ptr;
  VPBROADCASTD  (R14),Z13                 //;713DF24F bcst needle_len                 ;Z13=needle_len; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B63DFEAF needle_ptr += 4                 ;R14=needle_ptr;
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch2;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch1;
  VINSERTI64X4  $1,  Y26, Z27, Z14        //;3944001B merge into 16x uint32           ;Z14=threshold; Z27=scratch2; Z26=scratch1;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z24  //;B323211A load table_n_bytes_utf8         ;Z24=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables loop2
  KMOVW         K1,  K2                   //;FBC36D43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;1607AB46 lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z2,  Z6                   //;467EBBBC data_off2 := data_off           ;Z6=data_off2; Z2=data_off;
  VMOVDQA32     Z3,  Z23                  //;DD42E20B data_len2 := data_len           ;Z23=data_len2; Z3=data_len;
  VMOVDQA32     Z13, Z7                   //;29B33C21 needle_len2 := needle_len       ;Z7=needle_len2; Z13=needle_len;
loop2:
//; init variables loop1
  VPXORD        Z4,  Z4,  Z4              //;6D778B5D edit_dist := 0                  ;Z4=edit_dist;
  VPXORD        Z5,  Z5,  Z5              //;B150336A needle_off := 0                 ;Z5=needle_off;
  KMOVW         K2,  K6                   //;FDBD9EFA lane_todo2 := lane_todo         ;K6=lane_todo2; K2=lane_todo;
loop1:
//; load data unicode approx3
//; clear data0 and needle0
  VPTERNLOGD    $0b11111111,Z8,  Z8,  Z8  //;B8E7EDD5 set 0xFFFFFFFF                  ;Z8=d0;
  VPTERNLOGD    $0b11111111,Z9,  Z9,  Z9  //;A8E35E7F set 0xFFFFFFFF                  ;Z9=d1;
  VPTERNLOGD    $0b11111111,Z28, Z28, Z28 //;3CB765D7 set 0xFFFFFFFF                  ;Z28=d2;
//; load data0
  VPCMPD        $5,  Z10, Z23, K6,  K3    //;FCFCB494 K3 := K6 & (data_len2>=1)       ;K3=tmp_mask; K6=lane_todo2; Z23=data_len2; Z10=1; 5=GreaterEq;
  VPGATHERDD    (VIRT_BASE)(Z6*1),K3,  Z8 //;E4967C89 gather data0                    ;Z8=d0; K3=tmp_mask; SI=data_ptr; Z6=data_off2;
//; get number of bytes in code-point
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch1 := d0>>4               ;Z26=scratch1; Z8=d0;
  VPERMD        Z24, Z26, Z12             //;68FECBA0 get n_bytes_d0                  ;Z12=n_bytes_d0; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load data1
  VPSUBD        Z12, Z23, Z20             //;8BA4E60E data_len_alt := data_len2 - n_bytes_d0;Z20=data_len_alt; Z23=data_len2; Z12=n_bytes_d0;
  VPCMPD        $5,  Z10, Z20, K6,  K3    //;FCFCB494 K3 := K6 & (data_len_alt>=1)    ;K3=tmp_mask; K6=lane_todo2; Z20=data_len_alt; Z10=1; 5=GreaterEq;
  VPADDD        Z12, Z6,  K6,  Z19        //;88A5638D data_off_alt := data_off2 + n_bytes_d0;Z19=data_off_alt; K6=lane_todo2; Z6=data_off2; Z12=n_bytes_d0;
  VPGATHERDD    (VIRT_BASE)(Z19*1),K3, Z9 //;81256F6A gather data1                    ;Z9=d1; K3=tmp_mask; SI=data_ptr; Z19=data_off_alt;
//; get number of bytes in code-point
  VPSRLD        $4,  Z9,  Z26             //;FE5F1413 scratch1 := d1>>4               ;Z26=scratch1; Z9=d1;
  VPERMD        Z24, Z26, Z22             //;68FECBA0 get n_bytes_d1                  ;Z22=n_bytes_d1; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load data2
  VPSUBD        Z22, Z20, Z20             //;743C9E50 data_len_alt -= n_bytes_d1      ;Z20=data_len_alt; Z22=n_bytes_d1;
  VPCMPD        $5,  Z10, Z20, K6,  K3    //;C500A274 K3 := K6 & (data_len_alt>=1)    ;K3=tmp_mask; K6=lane_todo2; Z20=data_len_alt; Z10=1; 5=GreaterEq;
  VPADDD        Z22, Z19, K6,  Z19        //;3656366C data_off_alt += n_bytes_d1      ;Z19=data_off_alt; K6=lane_todo2; Z22=n_bytes_d1;
  VPGATHERDD    (VIRT_BASE)(Z19*1),K3,  Z28 //;3640A661 gather data2                    ;Z28=d2; K3=tmp_mask; SI=data_ptr; Z19=data_off_alt;
//; get number of bytes in code-point
  VPSRLD        $4,  Z28, Z26             //;FE5F1413 scratch1 := d2>>4               ;Z26=scratch1; Z28=d2;
  VPERMD        Z24, Z26, Z25             //;68FECBA0 get n_bytes_d2                  ;Z25=n_bytes_d2; Z26=scratch1; Z24=table_n_bytes_utf8;
//; load needles
  VPTERNLOGD    $0b11111111,Z19, Z19, Z19 //;9E9BD820 set 0xFFFFFFFF                  ;Z19=n0;
  VPTERNLOGD    $0b11111111,Z20, Z20, Z20 //;4408EAE3 set 0xFFFFFFFF                  ;Z20=n1;
  VPTERNLOGD    $0b11111111,Z21, Z21, Z21 //;61F40C30 set 0xFFFFFFFF                  ;Z21=n2;
  VPCMPD        $5,  Z10, Z7,  K6,  K3    //;7C687BDA K3 := K6 & (needle_len2>=1)     ;K3=tmp_mask; K6=lane_todo2; Z7=needle_len2; Z10=1; 5=GreaterEq;
  VPCMPD        $6,  Z10, Z7,  K6,  K4    //;E2B17160 K4 := K6 & (needle_len2>1)      ;K4=scratch1; K6=lane_todo2; Z7=needle_len2; Z10=1; 6=Greater;
  VPCMPD.BCST   $6,  CONSTD_2(),Z7,  K6,  K5  //;62D5A597 K5 := K6 & (needle_len2>2)  ;K5=scratch2; K6=lane_todo2; Z7=needle_len2; 6=Greater;
  VPGATHERDD    (R14)(Z5*1),K3,  Z19      //;4EAE4300 gather needle0                  ;Z19=n0; K3=tmp_mask; R14=needle_ptr; Z5=needle_off;
  VPGATHERDD    4(R14)(Z5*1),K4,  Z20     //;7F0BC8AC gather needle1                  ;Z20=n1; K4=scratch1; R14=needle_ptr; Z5=needle_off;
  VPGATHERDD    8(R14)(Z5*1),K5,  Z21     //;2FC91E09 gather needle2                  ;Z21=n2; K5=scratch2; R14=needle_ptr; Z5=needle_off;
//; remove tail from data
  VPERMD        Z18, Z12, Z26             //;83C76A20 get tail_mask (data)            ;Z26=scratch1; Z12=n_bytes_d0; Z18=tail_mask_data;
  VPERMD        Z18, Z22, Z27             //;46B49FFC get tail_mask (data)            ;Z27=scratch2; Z22=n_bytes_d1; Z18=tail_mask_data;
  VPERMD        Z18, Z25, Z29             //;38DC4AAA get tail_mask (data)            ;Z29=scratch3; Z25=n_bytes_d2; Z18=tail_mask_data;
  VPANDD        Z8,  Z26, Z8              //;D1E2261E mask data                       ;Z8=d0; Z26=scratch1;
  VPANDD        Z9,  Z27, Z9              //;A5CA4DE9 mask data                       ;Z9=d1; Z27=scratch2;
  VPANDD        Z28, Z29, Z28             //;C05E8C87 mask data                       ;Z28=d2; Z29=scratch3;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (d0>=char_a)              ;K3=tmp_mask; Z8=d0; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (d0<=char_z)              ;K3=tmp_mask; Z8=d0; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch1; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch1; Z8=d0; Z15=c_0b00100000;
  VMOVDQA32     Z26, Z8                   //;41954CFC d0 := scratch1                  ;Z8=d0; Z26=scratch1;
//; str_to_upper: IN zmm9; OUT zmm27
  VPCMPB        $5,  Z16, Z9,  K3         //;30E9B9FD K3 := (d1>=char_a)              ;K3=tmp_mask; Z9=d1; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z9,  K3,  K3    //;8CE85BA0 K3 &= (d1<=char_z)              ;K3=tmp_mask; Z9=d1; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z27                  //;ADC21F45 mask with selected chars        ;Z27=scratch2; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z9,  Z27 //;1BB96D97                                 ;Z27=scratch2; Z9=d1; Z15=c_0b00100000;
  VMOVDQA32     Z27, Z9                   //;60AA018C d1 := scratch2                  ;Z9=d1; Z27=scratch2;
//; str_to_upper: IN zmm28; OUT zmm29
  VPCMPB        $5,  Z16, Z28, K3         //;30E9B9FD K3 := (d2>=char_a)              ;K3=tmp_mask; Z28=d2; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z28, K3,  K3    //;8CE85BA0 K3 &= (d2<=char_z)              ;K3=tmp_mask; Z28=d2; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z29                  //;ADC21F45 mask with selected chars        ;Z29=scratch3; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z28, Z29 //;1BB96D97                                 ;Z29=scratch3; Z28=d2; Z15=c_0b00100000;
  VMOVDQA32     Z29, Z28                  //;250F442B d2 := scratch3                  ;Z28=d2; Z29=scratch3;
//; create key with bits for NeedleData:02-12-22-21-01-11-10-20-00
  VPCMPD        $0,  Z28, Z21, K3         //;00000000 K3 := (n2==d2)                  ;K3=tmp_mask; Z21=n2; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z21, K4         //;00000000 K4 := (n2==d1)                  ;K4=scratch1; Z21=n2; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z21, K5         //;00000000 K5 := (n2==d0)                  ;K5=scratch2; Z21=n2; Z8=d0; 0=Eq;
  VPSLLD.Z      $6,  Z10, K3,  Z21        //;00000000 key := 1<<6                     ;Z21=key; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $5,  Z10, K4,  Z27        //;00000000 scratch2 := 1<<5                ;Z27=scratch2; K4=scratch1; Z10=1;
  VPSLLD.Z      $1,  Z10, K5,  Z29        //;00000000 scratch3 := 1<<1                ;Z29=scratch3; K5=scratch2; Z10=1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

  VPCMPD        $0,  Z28, Z20, K3         //;00000000 K3 := (n1==d2)                  ;K3=tmp_mask; Z20=n1; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z20, K4         //;00000000 K4 := (n1==d1)                  ;K4=scratch1; Z20=n1; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z20, K5         //;00000000 K5 := (n1==d0)                  ;K5=scratch2; Z20=n1; Z8=d0; 0=Eq;
  VPSLLD.Z      $7,  Z10, K3,  Z27        //;00000000 scratch2 := 1<<7                ;Z27=scratch2; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $3,  Z10, K4,  Z26        //;00000000 scratch1 := 1<<3                ;Z26=scratch1; K4=scratch1; Z10=1;
  VPSLLD.Z      $2,  Z10, K5,  Z29        //;00000000 scratch3 := 1<<2                ;Z29=scratch3; K5=scratch2; Z10=1;
  VPORD         Z26, Z27, Z27             //;00000000 scratch2 |= scratch1            ;Z27=scratch2; Z26=scratch1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

  VPCMPD        $0,  Z28, Z19, K3         //;00000000 K3 := (n0==d2)                  ;K3=tmp_mask; Z19=n0; Z28=d2; 0=Eq;
  VPCMPD        $0,  Z9,  Z19, K4         //;00000000 K4 := (n0==d1)                  ;K4=scratch1; Z19=n0; Z9=d1; 0=Eq;
  VPCMPD        $0,  Z8,  Z19, K5         //;00000000 K5 := (n0==d0)                  ;K5=scratch2; Z19=n0; Z8=d0; 0=Eq;
  VPSLLD.Z      $8,  Z10, K3,  Z29        //;00000000 scratch3 := 1<<8                ;Z29=scratch3; K3=tmp_mask; Z10=1;
  VPSLLD.Z      $4,  Z10, K4,  Z27        //;00000000 scratch2 := 1<<4                ;Z27=scratch2; K4=scratch1; Z10=1;
  VPSLLD.Z      $0,  Z10, K5,  Z26        //;00000000 scratch1 := 1<<0                ;Z26=scratch1; K5=scratch2; Z10=1;
  VPORD         Z26, Z27, Z27             //;00000000 scratch2 |= scratch1            ;Z27=scratch2; Z26=scratch1;
  VPTERNLOGD    $0b11111110,Z29, Z27, Z21 //;00000000                                 ;Z21=key; Z27=scratch2; Z29=scratch3;

  VPADDD        Z10, Z10, Z8              //;00000000 constd_2 := 1 + 1               ;Z8=2; Z10=1;
  VPADDD        Z10, Z8,  Z9              //;00000000 constd_3 := 2 + 1               ;Z9=3; Z8=2; Z10=1;
  VPADDD        Z8,  Z8,  Z28             //;00000000 constd_4 := 2 + 2               ;Z28=4; Z8=2;
//; do the lookup of the advance values
  KMOVW         K6,  K3                   //;45A99B90 tmp_mask := lane_todo2          ;K3=tmp_mask; K6=lane_todo2;
  VPGATHERDD    (R15)(Z21*1),K3,  Z26     //;C3AEFFBF                                 ;Z26=scratch1; K3=tmp_mask; R15=update_data_ptr; Z21=key;
//; unpack results
  VPSRLD        $4,  Z26, Z19             //;87E05D1F ed_delta := scratch1>>4         ;Z19=ed_delta; Z26=scratch1;
  VPSRLD        $2,  Z26, Z21             //;13C921F9 adv_needle := scratch1>>2       ;Z21=adv_needle; Z26=scratch1;
  VPANDD        Z9,  Z19, Z19             //;A7E1848A ed_delta &= 3                   ;Z19=ed_delta; Z9=3;
  VPANDD        Z9,  Z26, Z20             //;42B9DA14 adv_data := scratch1 & 3        ;Z20=adv_data; Z26=scratch1; Z9=3;
  VPANDD        Z9,  Z21, Z21             //;E2CB942A adv_needle &= 3                 ;Z21=adv_needle; Z9=3;
//; update unicode approx3
//; update edit distance
  VPADDD        Z4,  Z19, Z4              //;15747D59 edit_dist += ed_delta           ;Z4=edit_dist; Z19=ed_delta;
//; advance data
  VPCMPD        $5,  Z10, Z20, K6,  K3    //;7FC1A433 K3 := K6 & (adv_data>=1)        ;K3=tmp_mask; K6=lane_todo2; Z20=adv_data; Z10=1; 5=GreaterEq;
  VMOVDQA32.Z   Z12, K3,  Z26             //;6428B608 scratch1 := n_bytes_d0          ;Z26=scratch1; K3=tmp_mask; Z12=n_bytes_d0;
  VPCMPD        $5,  Z8,  Z20, K6,  K3    //;4FC83ED2 K3 := K6 & (adv_data>=2)        ;K3=tmp_mask; K6=lane_todo2; Z20=adv_data; Z8=2; 5=GreaterEq;
  VPADDD        Z22, Z26, K3,  Z26        //;BAE14871 scratch1 += n_bytes_d1          ;Z26=scratch1; K3=tmp_mask; Z22=n_bytes_d1;
  VPCMPD        $5,  Z9,  Z20, K6,  K3    //;120E0569 K3 := K6 & (adv_data>=3)        ;K3=tmp_mask; K6=lane_todo2; Z20=adv_data; Z9=3; 5=GreaterEq;
  VPADDD        Z25, Z26, K3,  Z26        //;9BAA69A9 scratch1 += n_bytes_d2          ;Z26=scratch1; K3=tmp_mask; Z25=n_bytes_d2;
  VPSUBD        Z26, Z23, K6,  Z23        //;DF7FB44E data_len2 -= scratch1           ;Z23=data_len2; K6=lane_todo2; Z26=scratch1;
  VPADDD        Z26, Z6,  K6,  Z6         //;F81C97B0 data_off2 += scratch1           ;Z6=data_off2; K6=lane_todo2; Z26=scratch1;
//; advance needle
  VPCMPD        $5,  Z10, Z21, K6,  K3    //;5578190E K3 := K6 & (adv_needle>=1)      ;K3=tmp_mask; K6=lane_todo2; Z21=adv_needle; Z10=1; 5=GreaterEq;
  VMOVDQA32.Z   Z28, K3,  Z26             //;00000000 scratch1 := 4                   ;Z26=scratch1; K3=tmp_mask; Z28=4;
  VPCMPD        $5,  Z8,  Z21, K6,  K3    //;A814294A K3 := K6 & (adv_needle>=2)      ;K3=tmp_mask; K6=lane_todo2; Z21=adv_needle; Z8=2; 5=GreaterEq;
  VPADDD        Z28, Z26, K3,  Z26        //;B2BE82E2 scratch1 += 4                   ;Z26=scratch1; K3=tmp_mask; Z28=4;
  VPSUBD        Z21, Z7,  K6,  Z7         //;41970AC0 needle_len2 -= adv_needle       ;Z7=needle_len2; K6=lane_todo2; Z21=adv_needle;
  VPADDD        Z26, Z5,  K6,  Z5         //;89331149 needle_off += scratch1          ;Z5=needle_off; K6=lane_todo2; Z26=scratch1;
//; has-tail
//; restrict lanes based on edit distance and threshold
  VPCMPD        $2,  Z14, Z4,  K6,  K6    //;86F95312 K6 &= (edit_dist<=threshold)    ;K6=lane_todo2; Z4=edit_dist; Z14=threshold; 2=LessEq;
//; test if we have a match
  VPCMPD        $5,  Z7,  Z11, K6,  K3    //;EEF4BAA0 K3 := K6 & (0>=needle_len2)     ;K3=tmp_mask; K6=lane_todo2; Z11=0; Z7=needle_len2; 5=GreaterEq;
  KANDNW        K6,  K3,  K6              //;482703BD lane_todo2 &= ~tmp_mask         ;K6=lane_todo2; K3=tmp_mask;
  KORW          K1,  K3,  K1              //;8FAB61CD lane_active |= tmp_mask         ;K1=lane_active; K3=tmp_mask;
  KTESTW        K6,  K6                   //;9D3A860D any lanes still todo?           ;K6=lane_todo2;
  JNZ           loop1                     //;EA12C247 yes, then loop; jump if not zero (ZF = 0);

  KANDNW        K2,  K6,  K2              //;E3BAA7D5 lane_todo &= ~lane_todo2        ;K2=lane_todo; K6=lane_todo2;
  VPSUBD        Z10, Z3,  K2,  Z3         //;7C100DFB data_len--                      ;Z3=data_len; K2=lane_todo; Z10=1;
  VPADDD        Z10, Z2,  K2,  Z2         //;397684ED data_off++                      ;Z2=data_off; K2=lane_todo; Z10=1;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;591E80A4 K2 &= (data_len>0)              ;K2=lane_todo; Z3=data_len; Z11=0; 6=Greater;
//; reset variables for loop2
  VMOVDQA32     Z3,  Z23                  //;1C31BFD8 data_len2 := data_len           ;Z23=data_len2; Z3=data_len;
  VMOVDQA32     Z2,  Z6                   //;3B8A334A data_off2 := data_off           ;Z6=data_off2; Z2=data_off;
  VMOVDQA32     Z13, Z7                   //;8EFD9390 needle_len2 := needle_len       ;Z7=needle_len2; Z13=needle_len;
  KTESTW        K2,  K2                   //;4661A15A any lanes still todo?           ;K2=lane_todo;
  JNZ           loop2                     //;28307DE7 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcHasSubstrFuzzyUnicodeA3

//; #region bcSkip1charLeft
//; skip the first UTF-8 codepoint in Z2:Z3
//
// slice[0].k[1] = skip_1char_left(slice[2]).k[3]
TEXT bcSkip1charLeft(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPTESTMD      Z3,  Z3,  K1,  K1         //;B1146BCF update lane mask with non-empty lanes;K1=lane_active; Z3=str_length;
  KTESTW        K1,  K1                   //;69D1CDA2 all lanes empty?                ;K1=lane_active;
  JZ            next                      //;A5924904 yes, then exit; jump if zero (ZF = 1);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3, Z8  //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        CONST_N_BYTES_UTF8(),Z26, Z7  //;CFC7AA76 get n_bytes_data            ;Z7=n_bytes_data; Z26=scratch_Z26;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_len -= n_bytes_data         ;Z3=str_len; K1=lane_active; Z7=n_bytes_data;
  VPADDD        Z7,  Z2,  K1,  Z2         //;45909060 str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)
//; #endregion bcSkip1charLeft

//; #region bcSkip1charRight
//; skip the last UTF-8 codepoint in Z2:Z3
//
// slice[0].k[1] = skip_1char_right(slice[2]).k[3]
TEXT bcSkip1charRight(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPTESTMD      Z3,  Z3,  K1,  K1         //;B1146BCF K1 &= (data_len != 0); update lane mask with non-empty lanes;K1=lane_active; Z3=data_len;
  KTESTW        K1,  K1                   //;69D1CDA2 all lanes empty?                ;K1=lane_active;
  JZ            next                      //;A5924904 yes, then exit; jump if zero (ZF = 1);

  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z27 //;F6E81301 load constant UTF8 2byte mask   ;Z27=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z28 //;B1E12620 load constant UTF8 3byte mask   ;Z28=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z29 //;D896A9E1 load constant UTF8 4byte mask   ;Z29=UTF8_4byte_mask;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z24             //;EDD57CAF load constant 2                 ;Z24=2; Z10=1;
  VPADDD        Z10, Z24, Z25             //;7E7A1CB0 load constant 3                 ;Z25=3; Z24=2; Z10=1;
  VPADDD        Z24, Z24, Z20             //;9408A2D9 load constant 4                 ;Z20=4; Z24=2;
  VPADDD        Z2,  Z3,  Z4              //;5684E300 data_end := data_len + data_off ;Z4=data_end; Z3=data_len; Z2=data_off;

  VPTESTMD      Z3,  Z3,  K1,  K3         //;6639548B K3 := K1 & (data_len != 0)      ;K3=tmp_mask; K1=lane_active; Z3=data_len;
//; calculate offset that is always positive
  VPMINUD       Z20, Z3,  Z23             //;B086F272 adjust := min(data_len, 4)      ;Z23=adjust; Z3=data_len; Z20=4;
  VPSUBD        Z23, Z4,  Z5              //;998E9936 offset := data_end - adjust     ;Z5=offset; Z4=data_end; Z23=adjust;
  VPXORD        Z8,  Z8,  Z8              //;1882D069 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z5*1),K3, Z8  //;30D04944 gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z5=offset;
//; adjust data
  VPSUBD        Z23, Z20, Z23             //;83BCC5BB adjust := 4 - adjust            ;Z23=adjust; Z20=4;
  VPSLLD        $3,  Z23, Z23             //;D2F273B1 times 8 gives number of bytes   ;Z23=adjust;
  VPSLLVD       Z23, Z8,  Z8              //;67300525 data <<= adjust                 ;Z8=data; Z23=adjust;
//; count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z27, Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data; Z27=UTF8_2byte_mask;
  VPCMPD        $0,  Z27, Z26, K1,  K3    //;C6890BF4 K3 := K1 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K1=lane_active; Z26=scratch_Z26; Z27=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z28, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data; Z28=UTF8_3byte_mask;
  VPCMPD        $0,  Z28, Z26, K1,  K4    //;14C32DC0 K4 := K1 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=tmp_mask2; K1=lane_active; Z26=scratch_Z26; Z28=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z29, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data; Z29=UTF8_4byte_mask;
  VPCMPD        $0,  Z29, Z26, K1,  K5    //;1AE0A51C K5 := K1 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=tmp_mask3; K1=lane_active; Z26=scratch_Z26; Z29=UTF8_4byte_mask; 0=Eq;
  VMOVDQA32     Z10, Z7                   //;A7640B64 n_bytes_to_skip := 1            ;Z7=n_bytes_to_skip; Z10=1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_to_skip; K3=tmp_mask; Z10=1;
  VPADDD        Z24, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_to_skip; K4=tmp_mask2; Z24=2;
  VPADDD        Z25, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_to_skip; K5=tmp_mask3; Z25=3;

  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 data_len -= n_bytes_to_skip     ;Z3=data_len; K1=lane_active; Z7=n_bytes_to_skip;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)
//; #endregion bcSkip1charRight

//; #region bcSkipNcharLeft
//; skip the first n UTF-8 code-points in Z2:Z3
//
// slice[0].k[1] = skip_nchar_left(slice[2], i64[3]).k[4]
TEXT bcSkipNcharLeft(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z26=scratch_Z26;

  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_len>=counter)        ;K1=lane_active; Z3=str_len; Z6=counter; 5=GreaterEq;
  VPCMPD        $1,  Z6,  Z11, K1,  K2    //;7E49CD56 K2 := K1 & (0<counter)          ;K2=lane_todo; K1=lane_active; Z11=0; Z6=counter; 1=LessThen;
  KTESTW        K2,  K2                   //;69D1CDA2 ZF := (K2==0); CF := 1          ;K2=lane_todo;
  JZ            next                      //;A5924904 jump if zero (ZF = 1)           ;

  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;

loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPSUBD        Z7,  Z3,  K2,  Z3         //;B69EBA11 str_len -= n_bytes_data         ;Z3=str_len; K2=lane_todo; Z7=n_bytes_data;
  VPADDD        Z7,  Z2,  K2,  Z2         //;45909060 str_start += n_bytes_data       ;Z2=str_start; K2=lane_todo; Z7=n_bytes_data;

  VPSUBD        Z10, Z6,  Z6              //;97723E12 counter--                       ;Z6=counter; Z10=1;
//; tests
  VPCMPD        $1,  Z6,  Z11, K2,  K2    //;DF88A710 K2 &= (0<counter)               ;K2=lane_todo; Z11=0; Z6=counter; 1=LessThen;
  VPCMPD        $5,  Z3,  Z11, K2,  K3    //;2E4360D2 K3 := K2 & (0>=str_len)         ;K3=tmp_mask; K2=lane_todo; Z11=0; Z3=str_len; 5=GreaterEq;
  KANDNW        K1,  K3,  K1              //;21163EF3 lane_active &= ~tmp_mask        ;K1=lane_active; K3=tmp_mask;
  KTESTW        K2,  K1                   //;799F076E ZF := ((K1&K2)==0); CF := ((~K1&K2)==0);K1=lane_active; K2=lane_todo;
  JNZ           loop                      //;203DDAE1 any chars left? NO, loop next; jump if not zero (ZF = 0);

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*5)
//; #endregion bcSkipNcharLeft

//; #region bcSkipNcharRight
//; skip the last n UTF-8 code-points in Z2:Z3
//
// slice[0].k[1] = skip_nchar_right(slice[2], i64[3]).k[4]
TEXT bcSkipNcharRight(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z26=scratch_Z26;

  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (data_len>=counter)       ;K1=lane_active; Z3=data_len; Z6=counter; 5=GreaterEq;
  VPTESTMD      Z6,  Z6,  K1,  K2         //;D962A698 K2 := K1 & (counter != 0)       ;K2=lane_todo; K1=lane_active; Z6=counter;
  KTESTW        K2,  K2                   //;69D1CDA2 ZF := (K2==0); CF := 1          ;K2=lane_todo;
  JZ            next                      //;A5924904 jump if zero (ZF = 1)           ;

  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z27 //;F6E81301 load constant UTF8 2byte mask   ;Z27=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z28 //;B1E12620 load constant UTF8 3byte mask   ;Z28=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z29 //;D896A9E1 load constant UTF8 4byte mask   ;Z29=UTF8_4byte_mask;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z22             //;EDD57CAF load constant 2                 ;Z22=2; Z10=1;
  VPADDD        Z10, Z22, Z24             //;7E7A1CB0 load constant 3                 ;Z24=3; Z22=2; Z10=1;
  VPADDD        Z22, Z22, Z20             //;9408A2D9 load constant 4                 ;Z20=4; Z22=2;
loop:
  VPADDD        Z3,  Z2,  Z4              //;813A5F04 data_end := data_off + data_len ;Z4=data_end; Z2=data_off; Z3=data_len;
  VPTESTMD      Z3,  Z3,  K2,  K3         //;6639548B K3 := K2 & (data_len != 0)      ;K3=tmp_mask; K2=lane_todo; Z3=data_len;
//; calculate offset that is always positive
  VPMINUD       Z20, Z3,  Z23             //;B086F272 adjust := min(data_len, 4)      ;Z23=adjust; Z3=data_len; Z20=4;
  VPSUBD        Z23, Z4,  Z5              //;998E9936 offset := data_end - adjust     ;Z5=offset; Z4=data_end; Z23=adjust;
  VPXORD        Z8,  Z8,  Z8              //;1882D069 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z5*1), K3, Z8 //;30D04944 gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z5=offset;
//; adjust data
  VPSUBD        Z23, Z20, Z23             //;83BCC5BB adjust := 4 - adjust            ;Z23=adjust; Z20=4;
  VPSLLD        $3,  Z23, Z23             //;D2F273B1 times 8 gives number of bytes   ;Z23=adjust;
  VPSLLVD       Z23, Z8,  Z8              //;67300525 data <<= adjust                 ;Z8=data; Z23=adjust;
//; count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z27, Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data; Z27=UTF8_2byte_mask;
  VPCMPD        $0,  Z27, Z26, K2,  K3    //;C6890BF4 K3 := K2 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K2=lane_todo; Z26=scratch_Z26; Z27=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z28, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data; Z28=UTF8_3byte_mask;
  VPCMPD        $0,  Z28, Z26, K2,  K4    //;14C32DC0 K4 := K2 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=tmp_mask2; K2=lane_todo; Z26=scratch_Z26; Z28=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z29, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data; Z29=UTF8_4byte_mask;
  VPCMPD        $0,  Z29, Z26, K2,  K5    //;1AE0A51C K5 := K2 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=tmp_mask3; K2=lane_todo; Z26=scratch_Z26; Z29=UTF8_4byte_mask; 0=Eq;
  VMOVDQA32     Z10, Z7                   //;A7640B64 n_bytes_to_skip := 1            ;Z7=n_bytes_to_skip; Z10=1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_to_skip; K3=tmp_mask; Z10=1;
  VPADDD        Z22, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_to_skip; K4=tmp_mask2; Z22=2;
  VPADDD        Z24, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_to_skip; K5=tmp_mask3; Z24=3;
//; advance
  VPSUBD        Z7,  Z3,  K2,  Z3         //;B69EBA11 data_len -= n_bytes_to_skip     ;Z3=data_len; K2=lane_todo; Z7=n_bytes_to_skip;
  VPSUBD        Z10, Z6,  Z6              //;97723E12 counter--                       ;Z6=counter; Z10=1;
//; tests
  VPCMPD        $1,  Z6,  Z11, K2,  K2    //;DF88A710 K2 &= (0<counter)               ;K2=lane_todo; Z11=0; Z6=counter; 1=LessThen;
  VPCMPD        $2,  Z3,  Z11, K2,  K2    //;B623ED13 K2 &= (0<=data_len)             ;K2=lane_todo; Z11=0; Z3=data_len; 2=LessEq;
  VPCMPD        $5,  Z3,  Z11, K2,  K3    //;2E4360D2 K3 := K2 & (0>=data_len)        ;K3=tmp_mask; K2=lane_todo; Z11=0; Z3=data_len; 5=GreaterEq;
  KANDNW        K1,  K3,  K1              //;21163EF3 lane_active &= ~tmp_mask        ;K1=lane_active; K3=tmp_mask;
  KTESTW        K2,  K1                   //;799F076E ZF := ((K1&K2)==0); CF := ((~K1&K2)==0);K1=lane_active; K2=lane_todo;
  JNZ           loop                      //;203DDAE1 any chars left? NO, loop next; jump if not zero (ZF = 0);

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*5)
//; #endregion bcSkipNcharRight

//; #region bcTrimWsLeft
//
// slice[0] = trim_ws_left(slice[1]).k[2]
TEXT bcTrimWsLeft(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VPXORD        Z11, Z11, Z11             //;F4B92302 constd_0 := 0                   ;Z11=constd_0;
//; #region load white space chars
  MOVL          $0xD0920,R8               //;00000000                                 ;R8=tmp_constant;
  VPBROADCASTB  R8,  Z15                  //;7D467BFE load whitespace                 ;Z15=c_char_space; R8=tmp_constant;
  SHRL          $8,  R8                   //;69731820                                 ;R8=tmp_constant;
  VPBROADCASTB  R8,  Z16                  //;1FD6A756 load tab                        ;Z16=c_char_tab; R8=tmp_constant;
  SHRL          $8,  R8                   //;FA1E61C9                                 ;R8=tmp_constant;
  VPBROADCASTB  R8,  Z17                  //;14E0AB16 load cr                         ;Z17=c_char_cr; R8=tmp_constant;
//; #endregion load white space chars
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; #region trim left/right whitespace comparison
  VPCMPB        $0,  Z15, Z8,  K3         //;529F46B9 K3 := (data_msg==c_char_space); test if equal to SPACE char;K3=tmp_mask; Z8=data_msg; Z15=c_char_space; 0=Eq;
  VPCMPB        $2,  Z8,  Z16, K2         //;AD553F19 K2 := (c_char_tab<=data_msg); is TAB (0x09) <= char;K2=scratch2_mask; Z16=c_char_tab; Z8=data_msg; 2=LessEq;
  VPCMPB        $2,  Z17, Z8,  K2,  K2    //;6BC60637 K2 &= (data_msg<=c_char_cr); and is char <= CR (0x0D);K2=scratch2_mask; Z8=data_msg; Z17=c_char_cr; 2=LessEq;
  KORQ          K3,  K2,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  KTESTQ        K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found : no need to update string_start_position; jump if zero (ZF = 1);
//; #endregion

//; #region convert mask to selected byte count
  VPMOVM2B      K3,  Z8                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z8=data_msg; K3=tmp_mask;
  VPTERNLOGQ    $15, Z8,  Z8,  Z8         //;249B4036 negate                          ;Z8=data_msg;
  VPSHUFB       Z22, Z8,  Z8              //;8CF1488E reverse byte order              ;Z8=data_msg; Z22=constant_bswap32;
  VPLZCNTD      Z8,  K1,  Z8              //;90920F43 count leading zeros             ;Z8=data_msg; K1=lane_active;
  VPSRLD        $3,  Z8,  K1,  Z8         //;68276EFE divide by 8 yields byte_count   ;Z8=data_msg; K1=lane_active;
  VPMINSD       Z3,  Z8,  K1,  Z8         //;6616691F take minimun of length          ;Z8=data_msg; K1=lane_active; Z3=str_length;
//; #endregion zmm8 = #bytes

  VPADDD        Z8,  Z2,  K1,  Z2         //;40C40F7D str_start += data_msg           ;Z2=str_start; K1=lane_active; Z8=data_msg;
  VPSUBD        Z8,  Z3,  K1,  Z3         //;63A2C77B str_length -= data_msg          ;Z3=str_length; K1=lane_active; Z8=data_msg;
//; select lanes that have([essential] remaining string length > 0)
  VPCMPD        $2,  Z3,  Z11, K1,  K2    //;94B55922 K2 := K1 & (0<=str_length)      ;K2=scratch_mask1; K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
//; select lanes that have([optimization] number of trimmed chars = 4)
  VPCMPD        $0,  Z20, Z8,  K2,  K2    //;D3BA3C05 K2 &= (data_msg==4)             ;K2=scratch_mask1; Z8=data_msg; Z20=constd_4; 0=Eq;
  KTESTW        K2,  K2                   //;7CB2A200                                 ;K2=scratch_mask1;
  JNZ           loop                      //;00000000 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)
//; #endregion bcTrimWsLeft

//; #region bcTrimWsRight
//
// slice[0] = trim_ws_right(slice[1]).k[2]
TEXT bcTrimWsRight(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  MOVL          $0xD0920,R14              //;4AB6FA4E                                 ;R14=scratch;
  VPBROADCASTB  R14, Z15                  //;7D467BFE load whitespace                 ;Z15=c_char_space; R14=scratch;
  SHRL          $8,  R14                  //;69731820 scratch >>= 8                   ;R14=scratch;
  VPBROADCASTB  R14, Z16                  //;1FD6A756 load tab                        ;Z16=c_char_tab; R14=scratch;
  SHRL          $8,  R14                  //;FA1E61C9 scratch >>= 8                   ;R14=scratch;
  VPBROADCASTB  R14, Z17                  //;14E0AB16 load cr                         ;Z17=c_char_cr; R14=scratch;
  VPADDD        Z3,  Z2,  Z4              //;813A5F04 data_end := data_off + data_len ;Z4=data_end; Z2=data_off; Z3=data_len;
loop:
  VPTESTMD      Z3,  Z3,  K1,  K3         //;6639548B K3 := K1 & (data_len != 0)      ;K3=tmp_mask; K1=lane_active; Z3=data_len;
//; calculate offset that is always positive
  VPMINUD       Z20, Z3,  Z23             //;B086F272 adjust := min(data_len, 4)      ;Z23=adjust; Z3=data_len; Z20=4;
  VPSUBD        Z23, Z4,  Z5              //;998E9936 offset := data_end - adjust     ;Z5=offset; Z4=data_end; Z23=adjust;
  VPXORD        Z8,  Z8,  Z8              //;1882D069 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z5*1), K3, Z8 //;30D04944 gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z5=offset;
//; adjust data
  VPSUBD        Z23, Z20, Z23             //;83BCC5BB adjust := 4 - adjust            ;Z23=adjust; Z20=4;
  VPSLLD        $3,  Z23, Z23             //;D2F273B1 times 8 gives number of bytes   ;Z23=adjust;
  VPSLLVD       Z23, Z8,  Z8              //;67300525 data <<= adjust                 ;Z8=data; Z23=adjust;
//; trim left/right whitespace comparison
  VPCMPB        $0,  Z15, Z8,  K3         //;529F46B9 K3 := (data==c_char_space); test if equal to SPACE char;K3=tmp_mask; Z8=data; Z15=c_char_space; 0=Eq;
  VPCMPB        $2,  Z8,  Z16, K2         //;AD553F19 K2 := (c_char_tab<=data); is TAB (0x09) <= char;K2=scratch2_mask; Z16=c_char_tab; Z8=data; 2=LessEq;
  VPCMPB        $2,  Z17, Z8,  K2,  K2    //;6BC60637 K2 &= (data<=c_char_cr); and is char <= CR (0x0D);K2=scratch2_mask; Z8=data; Z17=c_char_cr; 2=LessEq;
  KORQ          K3,  K2,  K3              //;00000000 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  KTESTQ        K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found: no need to update string_start_position; jump if zero (ZF = 1);
//; trim comparison done: K3 contains matched characters

//; convert mask K3 to selected byte count zmm7
  VPMOVM2B      K3,  Z7                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z7=n_bytes_to_trim; K3=tmp_mask;
  VPTERNLOGQ    $0b00001111,Z7,  Z7,  Z7  //;249B4036 negate                          ;Z7=n_bytes_to_trim;
  VPLZCNTD      Z7,  K1,  Z7              //;90920F43 count leading zeros             ;Z7=n_bytes_to_trim; K1=lane_active;
  VPSRLD        $3,  Z7,  K1,  Z7         //;68276EFE divide by 8 yields byte_count   ;Z7=n_bytes_to_trim; K1=lane_active;
  VPMINSD       Z3,  Z7,  K1,  Z7         //;6616691F take minimun of length          ;Z7=n_bytes_to_trim; K1=lane_active; Z3=data_len;
//; done convert mask: zmm7 = #bytes

  VPSUBD        Z7,  Z4,  K1,  Z4         //;40C40F7D data_end -= n_bytes_to_trim     ;Z4=data_end; K1=lane_active; Z7=n_bytes_to_trim;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;63A2C77B data_len -= n_bytes_to_trim     ;Z3=data_len; K1=lane_active; Z7=n_bytes_to_trim;
  VPCMPD        $0,  Z20, Z7,  K3         //;D3BA3C05 K3 := (n_bytes_to_trim==4)      ;K3=tmp_mask; Z7=n_bytes_to_trim; Z20=4; 0=Eq;
  KTESTW        K1,  K3                   //;7CB2A200 more chars to trim?             ;K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;7E49CD56 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)
//; #endregion bcTrimWsRight

//; #region bcTrim4charLeft
//
// slice[0] = trim_char_left(slice[1], dict[2]).k[3]
TEXT bcTrim4charLeft(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  MOVQ          (R14),R14                 //;26BB22F5 Load ptr of string              ;R14=chars_ptr; R14=chars_slice;
  MOVL          (R14),R14                 //;B7C25D43 Load first 4 chars              ;R14=chars_ptr;
  VPBROADCASTB  R14, Z9                   //;96085025                                 ;Z9=c_char0; R14=chars_ptr;
  SHRL          $8,  R14                  //;63D19F3B chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z10                  //;FCEBCAA6                                 ;Z10=c_char1; R14=chars_ptr;
  SHRL          $8,  R14                  //;E5627E10 chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z12                  //;66A9E2D3                                 ;Z12=c_char2; R14=chars_ptr;
  SHRL          $8,  R14                  //;C5E83B19 chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z13                  //;C18E3641                                 ;Z13=c_char3; R14=chars_ptr;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; #region trim left/right 4char comparison
  VPCMPB        $0,  Z9,  Z8,  K3         //;D8545E6D K3 := (data_msg==c_char0); is char == char0;K3=tmp_mask; Z8=data_msg; Z9=c_char0; 0=Eq;
  VPCMPB        $0,  Z10, Z8,  K2         //;933CFC19 K2 := (data_msg==c_char1); is char == char1;K2=scratch2_mask; Z8=data_msg; Z10=c_char1; 0=Eq;
  KORQ          K2,  K3,  K3              //;7B471502 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z12, Z8,  K2         //;D206A939 K2 := (data_msg==c_char2); is char == char2;K2=scratch2_mask; Z8=data_msg; Z12=c_char2; 0=Eq;
  KORQ          K2,  K3,  K3              //;FD738F32 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z13, Z8,  K2         //;AB8B7AAA K2 := (data_msg==c_char3); is char == char3;K2=scratch2_mask; Z8=data_msg; Z13=c_char3; 0=Eq;
  KORQ          K2,  K3,  K3              //;CDC8B5A9 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  KORTESTQ      K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found : no need to update string_start_position; jump if zero (ZF = 1);
//; #endregion

//; #region convert mask k3 to selected byte count zmm7
  VPMOVM2B      K3,  Z7                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z7=n_bytes_data; K3=tmp_mask;
  VPTERNLOGQ    $15, Z7,  Z7,  Z7         //;249B4036 negate                          ;Z7=n_bytes_data;
  VPSHUFB       Z22, Z7,  Z7              //;8CF1488E reverse byte order              ;Z7=n_bytes_data; Z22=constant_bswap32;
  VPLZCNTD      Z7,  K1,  Z7              //;90920F43 count leading zeros             ;Z7=n_bytes_data; K1=lane_active;
  VPSRLD        $3,  Z7,  K1,  Z7         //;68276EFE divide by 8 yields byte_count   ;Z7=n_bytes_data; K1=lane_active;
  VPMINSD       Z3,  Z7,  K1,  Z7         //;6616691F take minimun of length          ;Z7=n_bytes_data; K1=lane_active; Z3=str_len;
//; #endregion zmm7 = #bytes

  VPADDD        Z7,  Z2,  K1,  Z2         //;40C40F7D str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;63A2C77B str_len -= n_bytes_data         ;Z3=str_len; K1=lane_active; Z7=n_bytes_data;
  VPCMPD        $2,  Z3,  Z11, K1,  K2    //;94B55922 K2 := K1 & (0<=str_len)         ;K2=scratch_mask1; K1=lane_active; Z11=0; Z3=str_len; 2=LessEq;
  VPCMPD        $0,  Z20, Z7,  K2,  K2    //;D3BA3C05 K2 &= (n_bytes_data==4)         ;K2=scratch_mask1; Z7=n_bytes_data; Z20=4; 0=Eq;
  KTESTW        K2,  K2                   //;7CB2A200 ZF := (K2==0); CF := 1          ;K2=scratch_mask1;
  JNZ           loop                      //;7E49CD56 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcTrim4charLeft

//; #region bcTrim4charRight
//
// slice[0] = trim_char_right(slice[1], dict[2]).k[3]
TEXT bcTrim4charRight(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  MOVQ          (R14),R14                 //;26BB22F5 load ptr of string              ;R14=chars_ptr; R14=chars_slice;
  MOVL          (R14),R14                 //;B7C25D43 load first 4 chars              ;R14=chars_ptr;
  VPBROADCASTB  R14, Z9                   //;96085025                                 ;Z9=c_char0; R14=chars_ptr;
  SHRL          $8,  R14                  //;63D19F3B chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z10                  //;FCEBCAA6                                 ;Z10=c_char1; R14=chars_ptr;
  SHRL          $8,  R14                  //;E5627E10 chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z12                  //;66A9E2D3                                 ;Z12=c_char2; R14=chars_ptr;
  SHRL          $8,  R14                  //;C5E83B19 chars_ptr >>= 8                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z13                  //;C18E3641                                 ;Z13=c_char3; R14=chars_ptr;
  VPADDD        Z3,  Z2,  Z4              //;813A5F04 data_end := data_off + data_len ;Z4=data_end; Z2=data_off; Z3=data_len;
loop:
  VPTESTMD      Z3,  Z3,  K1,  K3         //;6639548B K3 := K1 & (data_len != 0)      ;K3=tmp_mask; K1=lane_active; Z3=data_len;
//; calculate offset that is always positive
  VPMINUD       Z20, Z3,  Z23             //;B086F272 adjust := min(data_len, 4)      ;Z23=adjust; Z3=data_len; Z20=4;
  VPSUBD        Z23, Z4,  Z5              //;998E9936 offset := data_end - adjust     ;Z5=offset; Z4=data_end; Z23=adjust;
  VPXORD        Z8,  Z8,  Z8              //;1882D069 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z5*1), K3, Z8 //;30D04944 gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z5=offset;
//; adjust data
  VPSUBD        Z23, Z20, Z23             //;83BCC5BB adjust := 4 - adjust            ;Z23=adjust; Z20=4;
  VPSLLD        $3,  Z23, Z23             //;D2F273B1 times 8 gives number of bytes   ;Z23=adjust;
  VPSLLVD       Z23, Z8,  Z8              //;67300525 data <<= adjust                 ;Z8=data; Z23=adjust;
//; trim left/right 4char comparison
  VPCMPB        $0,  Z9,  Z8,  K3         //;D8545E6D K3 := (data==c_char0); is char == char0;K3=tmp_mask; Z8=data; Z9=c_char0; 0=Eq;
  VPCMPB        $0,  Z10, Z8,  K2         //;933CFC19 K2 := (data==c_char1); is char == char1;K2=scratch2_mask; Z8=data; Z10=c_char1; 0=Eq;
  KORQ          K2,  K3,  K3              //;7B471502 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z12, Z8,  K2         //;D206A939 K2 := (data==c_char2); is char == char2;K2=scratch2_mask; Z8=data; Z12=c_char2; 0=Eq;
  KORQ          K2,  K3,  K3              //;FD738F32 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z13, Z8,  K2         //;AB8B7AAA K2 := (data==c_char3); is char == char3;K2=scratch2_mask; Z8=data; Z13=c_char3; 0=Eq;
  KORQ          K2,  K3,  K3              //;CDC8B5A9 tmp_mask |= scratch2_mask       ;K3=tmp_mask; K2=scratch2_mask;
  KTESTQ        K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found: no need to update string_start_position; jump if zero (ZF = 1);
//; trim comparison done: K3 contains matched characters

//; convert mask K3 to selected byte count zmm7
  VPMOVM2B      K3,  Z7                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z7=n_bytes_to_trim; K3=tmp_mask;
  VPTERNLOGQ    $0b00001111,Z7,  Z7,  Z7  //;249B4036 negate                          ;Z7=n_bytes_to_trim;
  VPLZCNTD      Z7,  K1,  Z7              //;90920F43 count leading zeros             ;Z7=n_bytes_to_trim; K1=lane_active;
  VPSRLD        $3,  Z7,  K1,  Z7         //;68276EFE divide by 8 yields byte_count   ;Z7=n_bytes_to_trim; K1=lane_active;
  VPMINSD       Z3,  Z7,  K1,  Z7         //;6616691F take minimun of length          ;Z7=n_bytes_to_trim; K1=lane_active; Z3=data_len;
//; done convert mask: zmm7 = #bytes

  VPSUBD        Z7,  Z4,  K1,  Z4         //;40C40F7D data_end -= n_bytes_to_trim     ;Z4=data_end; K1=lane_active; Z7=n_bytes_to_trim;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;63A2C77B data_len -= n_bytes_to_trim     ;Z3=data_len; K1=lane_active; Z7=n_bytes_to_trim;
  VPCMPD        $0,  Z20, Z7,  K3         //;D3BA3C05 K3 := (n_bytes_to_trim==4)      ;K3=tmp_mask; Z7=n_bytes_to_trim; Z20=4; 0=Eq;
  KTESTW        K1,  K3                   //;7CB2A200 more chars to trim?             ;K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;7E49CD56 yes, then loop; jump if not zero (ZF = 0);

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcTrim4charRight

// i64[0] = octetlength(slice[1]).k[2]
TEXT bcoctetlength(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VMOVDQU32.Z BC_VSTACK_PTR(BX, 64), K1, Z0
  VEXTRACTI32X8 $1, Z0, Y1

  BC_UNPACK_SLOT(0, OUT(DX))
  VPMOVZXDQ Y0, Z0
  VPMOVZXDQ Y1, Z1

  BC_STORE_I64_TO_SLOT(IN(Z0), IN(Z1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// i64[0] = characterlength(slice[1]).k[2]
//
// The length of **a valid UTF-8 string** can be calculated in the following way:
//
//    the length of input in bytes - the number of continuation bytes
//
// Continuation bytes are in form 0b10xxxxxx, and none of leading bytes (starting
// a UTF-8 sequence) have such bit pattern. This calculation takes advantage of
// the VPSADBW instruction, which sums up to 8 bytes, which match the 0b10xxxxxx
// pattern - this sum is then subtracted from the initial byte length of the string.
TEXT bccharlength(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z0), OUT(Z1), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPTESTMD Z1, Z1, K1, K2
  KTESTW K2, K2

  VMOVDQU64 CONST_GET_PTR(consts_utf8_len_byte_mask_q, 0), Z10
  VEXTRACTI32X8 $1, Z1, Y9

  VPBROADCASTB CONSTD_1(), Z11              // Z11 <- 0x01010101 (LSB of each bit)
  VPBROADCASTB CONSTD_0x80(), Z12           // Z12 <- 0x80808080 (MSB of each bit)
  VPBROADCASTD CONSTD_8(), Z13              // Z13 <- dword(8)
  VPMOVZXDQ Y1, Z8                          // Z8 <- initial character length (low)
  VPMOVZXDQ Y9, Z9                          // Z9 <- initial character length (high)
  JZ next

loop:
  KMOVB K2, K4
  VPXORD X2, X2, X2
  VPGATHERDQ (VIRT_BASE)(Y0*1), K4, Z2      // Z2 <- 8 bytes (low)

  KSHIFTRW $8, K2, K5
  VPMINUD Z13, Z1, Z5                       // Z5 <- min(remaining_length, 8)
  VEXTRACTI32X8 $1, Z0, Y4
  VEXTRACTI32X8 $1, Z5, Y7
  KMOVB K5, K3
  VPXORD X3, X3, X3
  VPMOVZXDQ Y5, Z6                          // Z6 <- min(remaining_length, 8) (low)
  VPMOVZXDQ Y7, Z7                          // Z7 <- min(remaining_length, 8) (high)
  VPGATHERDQ (VIRT_BASE)(Y4*1), K5, Z3      // Z3 <- 8 bytes (high)

  VPERMQ Z10, Z6, Z6                        // Z6 <- data byte mask (low)
  VPERMQ Z10, Z7, Z7                        // Z7 <- data byte mask (high)
  VPSUBD.Z Z5, Z1, K2, Z1                   // Z1 <- length -= remaining_length
  VPADDD.Z Z5, Z0, K2, Z0                   // Z0 <- offset += remaining_length
  VPANDQ.Z Z6, Z2, K2, Z2                   // Z2 <- string data masked for comparison (low)
  VPTESTMD Z1, Z1, K1, K2
  VPANDQ.Z Z7, Z3, K3, Z3                   // Z3 <- string data masked for comparison (high)
  KTESTW K2, K2

  VPXORD Z2, Z12, Z2                        // Z2 <- Z2 ^ 0x80 => 0 if equals 0x80, non-zero othrewise
  VPXORD Z3, Z12, Z3                        // Z3 <- Z3 ^ 0x80 => 0 if equals 0x80, non-zero othrewise
  VPMINUB Z2, Z11, Z2                       // Z2 <- Z2[i] != 0 ? 1 : 0
  VPMINUB Z3, Z11, Z3                       // Z3 <- Z3[i] != 0 ? 1 : 0
  VPSADBW Z2, Z11, Z2                       // Z2:Z3 <- sum(1 - Z2[j]) <- diff is 0 if Z2[j] is 1 (not equal)
  VPSADBW Z3, Z11, Z3                       //                            diff is 1 if Z3[j] is 0 (equal)
  VPSUBQ Z2, Z8, Z8                         // Z8 <- update character length (low)
  VPSUBQ Z3, Z9, Z9                         // Z9 <- update character length (high)
  JNZ loop

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_I64_TO_SLOT(IN(Z8), IN(Z9), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

//; #region bcSubstr
//; Get a substring of UTF-8 code-points in Z2:Z3 (str interpretation). The substring starts
//; from the specified start-index and ends at the specified length or at the last character
//; of the string (which ever is first). The start-index is 1-based! The first index of the
//; string starts at 1. The substring is stored in Z2:Z3 (str interpretation)
//
// slice[0] = substr(slice[1], i64[2], i64[3]).k[4]
TEXT bcSubstr(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_4xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(CX), OUT(DX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))
  BC_LOAD_I64_FROM_SLOT(OUT(Z29), OUT(Z28), IN(DX))

  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z26=scratch_Z26;
  VPMOVQD       Z29, Y29                  //;17FCB103 truncate uint64 to uint32       ;Z29=scratch_z4;
  VPMOVQD       Z28, Y28                  //;8F762E8E truncate uint64 to uint32       ;Z28=scratch_Z28;
  VINSERTI64X4  $1,  Y28, Z29, Z12        //;3944001B merge into 16x uint32           ;Z12=substr_length; Z29=scratch_z4; Z28=scratch_Z28;
//; load constants
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VMOVDQA32     Z2,  Z4                   //;CFB0D832 curr_offset := str_start        ;Z4=curr_offset; Z2=str_start;
//; fixup parameters
  VPSUBD        Z10, Z6,  Z6              //;34951830 1-based to 0-based indices      ;Z6=counter; Z10=1;
  VPMAXSD       Z6,  Z11, Z6              //;18F03020 counter := max(0, counter)      ;Z6=counter; Z11=0;

//; find start of substring
  JMP           test1                     //;4CAF1B53                                 ;
loop1:
//; load next code-point
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (VIRT_BASE)(Z4*1), K3, Z8 //;FC80CF41 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=curr_offset;
  VPSUBD        Z10, Z6,  Z6              //;19C9DC47 counter--                       ;Z6=counter; Z10=1;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
//; advance
  VPADDD        Z7,  Z4,  K2,  Z4         //;45909060 curr_offset += n_bytes_data     ;Z4=curr_offset; K2=lane2_mask; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K2,  Z3         //;B69EBA11 str_len -= n_bytes_data         ;Z3=str_len; K2=lane2_mask; Z7=n_bytes_data;
test1:
  VPCMPD        $6,  Z11, Z6,  K1,  K2    //;2E4360D2 K2 := K1 & (counter>0); any chars left to skip?;K2=lane2_mask; K1=lane_active; Z6=counter; Z11=0; 6=Greater;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;DA211F9B K2 &= (str_len>0)               ;K2=lane2_mask; Z3=str_len; Z11=0; 6=Greater;
  KTESTW        K2,  K2                   //;799F076E all lanes done? 0 means lane is done;K2=lane2_mask;
  JNZ           loop1                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);

//; At this moment Z4 has the start position of the substring. Next we will calculate the length of the substring in Z3.
  VMOVDQA32     Z4,  Z2                   //;60EBBEED str_start := curr_offset        ;Z2=str_start; Z4=curr_offset;

//; find end of substring
  JMP           test2                     //;4CAF1B53                                 ;
loop2:
//; load next code-point
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (VIRT_BASE)(Z4*1),K3,  Z8 //;5A704AF6 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=curr_offset;
  VPSUBD        Z10, Z12, Z12             //;61D287CD substr_length--                 ;Z12=substr_length; Z10=1;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
//; advance
  VPADDD        Z7,  Z4,  K2,  Z4         //;45909060 curr_offset += n_bytes_data     ;Z4=curr_offset; K2=lane2_mask; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K2,  Z3         //;B69EBA11 str_len -= n_bytes_data         ;Z3=str_len; K2=lane2_mask; Z7=n_bytes_data;
test2:
  VPCMPD        $6,  Z11, Z12, K1,  K2    //;2E4360D2 K2 := K1 & (substr_length>0); any chars left to trim;K2=lane2_mask; K1=lane_active; Z12=substr_length; Z11=0; 6=Greater;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;DA211F9B K2 &= (str_len>0); all lanes done?;K2=lane2_mask; Z3=str_len; Z11=0; 6=Greater;
  KTESTW        K2,  K2                   //;799F076E 0 means lane is done            ;K2=lane2_mask;
  JNZ           loop2                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
//; overwrite str_length with correct values
  VPSUBD        Z2,  Z4,  Z3              //;E24AE85F str_len := curr_offset - str_start;Z3=str_len; Z4=curr_offset; Z2=str_start;

//; clear the offsets of slices that are empty and store the result
  VPTESTMD      Z3,  Z3,  K1              //;68596EF0 K1 := (str_len!=0)              ;K1=lane_active; Z3=str_len;
  VMOVDQA32.Z   Z2,  K1,  Z2              //;AF2DA299 str_start := str_start          ;Z2=str_start; K1=lane_active;
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*5)
//; #endregion bcSubstr

//; #region bcSplitPart
//; NOTE: the delimiter cannot be byte 0
//
// slice[0].k[1] = split_part(slice[2], dict[3], i64[4]).k[5]
TEXT bcSplitPart(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(BC_SLOT_SIZE*2, OUT(BX))
  BC_UNPACK_DICT(BC_SLOT_SIZE*3, OUT(R14))
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*3+BC_DICT_SIZE, OUT(CX), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z27), OUT(Z26), IN(CX))

  MOVQ          (R14),R14                 //;FEE415A0                                 ;R14=split_info;
  VPBROADCASTB  (R14),Z21                 //;B4B43F80 bcst delimiter                  ;Z21=delim; R14=split_info;
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z7         //;3944001B merge into 16x uint32           ;Z7=counter_delim; Z27=scratch_Z27; Z26=scratch_Z26;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;

  KMOVW         K1,  K2                   //;FE3838B3 lane2_mask := lane_active       ;K2=lane2_mask; K1=lane_active;
  VMOVDQA32     Z2,  Z4                   //;CFB0D832 search_base := str_start        ;Z4=search_base; Z2=str_start;
  VPADDD        Z2,  Z3,  Z5              //;E5429114 o_data_end := str_len + str_start;Z5=o_data_end; Z3=str_len; Z2=str_start;
  VPSUBD        Z10, Z7,  Z7              //;68858B39 counter_delim--; (1-based indexing);Z7=counter_delim; Z10=1;

//; #region find n-th delimiter
  JMP           tail1                     //;9DD42F87                                 ;
loop1:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (VIRT_BASE)(Z4*1),K3,  Z8 //;FC80CF41 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
//; clear tail from data
  VPMINSD       Z3,  Z20, Z26             //;DEC17BF3 scratch_Z26 := min(4, str_len)  ;Z26=scratch_Z26; Z20=4; Z3=str_len;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; calculate skip_count in zmm14
  VPCMPB        $0,  Z21, Z8,  K3         //;8E3317B0 K3 := (data_msg==delim)         ;K3=tmp_mask; Z8=data_msg; Z21=delim; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count;
//; advance
  VPADDD        Z14, Z4,  K2,  Z4         //;5034DEA0 search_base += skip_count       ;Z4=search_base; K2=lane2_mask; Z14=skip_count;
  VPSUBD        Z14, Z3,  K2,  Z3         //;95AAE700 str_len -= skip_count           ;Z3=str_len; K2=lane2_mask; Z14=skip_count;
//; did we encounter a delimiter?
  VPCMPD        $4,  Z20, Z14, K2,  K3    //;80B9AEA2 K3 := K2 & (skip_count!=4); active lanes where skip != 4;K3=tmp_mask; K2=lane2_mask; Z14=skip_count; Z20=4; 4=NotEqual;
  VPSUBD        Z10, Z7,  K3,  Z7         //;35E75E57 counter_delim--                 ;Z7=counter_delim; K3=tmp_mask; Z10=1;
  VPSUBD        Z10, Z3,  K3,  Z3         //;AF759B00 str_len--                       ;Z3=str_len; K3=tmp_mask; Z10=1;
  VPADDD        Z10, Z4,  K3,  Z4         //;D5281D43 search_base++                   ;Z4=search_base; K3=tmp_mask; Z10=1;

tail1:
//; still a lane todo?
  VPCMPD        $1,  Z7,  Z11, K2,  K2    //;50E6D99D K2 &= (0<counter_delim)         ;K2=lane2_mask; Z11=0; Z7=counter_delim; 1=LessThen;
  VPCMPD        $1,  Z5,  Z4,  K2,  K2    //;A052FCB6 K2 &= (search_base<o_data_end)  ;K2=lane2_mask; Z4=search_base; Z5=o_data_end; 1=LessThen;
  KTESTW        K2,  K2                   //;799F076E all lanes done? 0 means lane is done;K2=lane2_mask;
  JNZ           loop1                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
  VPCMPD        $0,  Z7,  Z11, K1,  K1    //;A0ABF51F K1 &= (0==counter_delim)        ;K1=lane_active; Z11=0; Z7=counter_delim; 0=Eq;
//; #endregion find n-th delimiter

  VMOVDQA32     Z4,  K1,  Z2              //;B69A81FE str_start := search_base        ;Z2=str_start; K1=lane_active; Z4=search_base;

//; #region find next delimiter
  KMOVW         K1,  K2                   //;A543DE2E lane2_mask := lane_active       ;K2=lane2_mask; K1=lane_active;
loop2:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (VIRT_BASE)(Z4*1),K3,  Z8 //;5A704AF6 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
//; calculate skip_count in zmm14
  VPCMPB        $0,  Z21, Z8,  K3         //;E8DC9CCA K3 := (data_msg==delim)         ;K3=tmp_mask; Z8=data_msg; Z21=delim; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count;
//; advance
  VPADDD        Z14, Z4,  K2,  Z4         //;5034DEA0 search_base += skip_count       ;Z4=search_base; K2=lane2_mask; Z14=skip_count;
//; did we encounter a delimiter?
  VPCMPD        $0,  Z20, Z14, K2,  K2    //;80B9AEA2 K2 &= (skip_count==4); active lanes where skip != 4;K2=lane2_mask; Z14=skip_count; Z20=4; 0=Eq;
  VPCMPD        $1,  Z5,  Z4,  K3         //;E2BEF075 K3 := (search_base<o_data_end)  ;K3=tmp_mask; Z4=search_base; Z5=o_data_end; 1=LessThen;
  KTESTW        K3,  K2                   //;799F076E all lanes still todo?           ;K2=lane2_mask; K3=tmp_mask;
  JNZ           loop2                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
//; #endregion find next delimiter

  VPMINSD       Z5,  Z4,  Z4              //;C62A5921 search_base := min(search_base, o_data_end);Z4=search_base; Z5=o_data_end;
  VPSUBD        Z2,  Z4,  K1,  Z3         //;E24AE85F str_len := search_base - str_start;Z3=str_len; K1=lane_active; Z4=search_base; Z2=str_start;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*5 + BC_DICT_SIZE)
//; #endregion bcSplitPart

//; #region bcContainsPrefixCs
//
// s[0].k[0] = contains_prefix_cs(slice[2], dict[3]).k[4]
TEXT bcContainsPrefixCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter; R14=needle_slice;
  VPTESTMD      Z6,  Z6,  K1,  K1         //;EF7C0710 K1 &= (counter != 0)            ;K1=lane_active; Z6=counter;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_len>=counter)        ;K1=lane_active; Z3=str_len; Z6=counter; 5=GreaterEq;
  KTESTW        K1,  K1                   //;C28D3832 any lane still alive            ;K1=lane_active;
  JZ            next                      //;4DA2206F no, exit; jump if zero (ZF = 1) ;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;

  VMOVDQA32     Z2,  Z24                  //;6F6F1342 search_base := str_start        ;Z24=search_base; Z2=str_start;
  VMOVDQA32     Z6,  Z25                  //;6F6F1343 needle_len := counter           ;Z25=needle_len; Z6=counter;
  JMP           tests                     //;F2A3982D                                 ;
loop:
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPSUBD        Z20, Z6,  K1,  Z6         //;AEDCD850 counter -= 4                    ;Z6=counter; K1=lane_active; Z20=4;
  VPADDD        Z20, Z24, K1,  Z24        //;D7CC90DD search_base += 4                ;Z24=search_base; K1=lane_active; Z20=4;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data_msg==[needle_ptr])  ;K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tests:
  VPCMPD        $2,  Z6,  Z11, K1,  K1    //;8A1022B4 K1 &= (0<=counter)              ;K1=lane_active; Z11=0; Z6=counter; 2=LessEq;
  VPCMPD        $6,  Z20, Z6,  K3         //;99392208 K3 := (counter>4)               ;K3=tmp_mask; Z6=counter; Z20=4; 6=Greater;
  KTESTW        K1,  K1                   //;FE455439 any lanes still alive           ;K1=lane_active;
  JZ            next                      //;CD5F484F no, exit; jump if zero (ZF = 1) ;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPERMD        CONST_TAIL_MASK(), Z6, Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z6=counter;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
//; compare data with needle
  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data_msg==data_needle)   ;K1=lane_active; Z8=data_msg; Z9=data_needle; 0=Eq;
  VPADDD        Z25, Z2,  K1,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K1=lane_active; Z25=needle_length;
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_len -= needle_length        ;Z3=str_len; K1=lane_active; Z25=needle_length;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsPrefixCs

//; #region bcContainsPrefixCi
//
// s[0].k[1] = contains_prefix_ci(slice[2], dict[3]).k[4]
TEXT bcContainsPrefixCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter; R14=needle_slice;
  VPTESTMD      Z6,  Z6,  K1,  K1         //;EF7C0710 K1 &= (counter != 0)            ;K1=lane_active; Z6=counter;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_len>=counter)        ;K1=lane_active; Z3=str_len; Z6=counter; 5=GreaterEq;
  KTESTW        K1,  K1                   //;C28D3832 any lane still alive            ;K1=lane_active;
  JZ            next                      //;4DA2206F no, exit; jump if zero (ZF = 1) ;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;

  VMOVDQA32     Z2,  Z24                  //;6F6F1342 search_base := str_start        ;Z24=search_base; Z2=str_start;
  VMOVDQA32     Z6,  Z25                  //;6F6F1343 needle_len := counter           ;Z25=needle_len; Z6=counter;
  JMP           tests                     //;F2A3982D                                 ;
loop:
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPSUBD        Z20, Z6,  K1,  Z6         //;AEDCD850 counter -= 4                    ;Z6=counter; K1=lane_active; Z20=4;
  VPADDD        Z20, Z24, K1,  Z24        //;D7CC90DD search_base += 4                ;Z24=search_base; K1=lane_active; Z20=4;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==[needle_ptr]);K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tests:
  VPCMPD        $2,  Z6,  Z11, K1,  K1    //;8A1022B4 K1 &= (0<=counter)              ;K1=lane_active; Z11=0; Z6=counter; 2=LessEq;
  VPCMPD        $6,  Z20, Z6,  K3         //;99392208 K3 := (counter>4)               ;K3=tmp_mask; Z6=counter; Z20=4; 6=Greater;
  KTESTW        K1,  K1                   //;FE455439 any lanes still alive           ;K1=lane_active;
  JZ            next                      //;CD5F484F no, exit; jump if zero (ZF = 1) ;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; load data
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPERMD        CONST_TAIL_MASK(),Z6,  Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z6=counter;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle
  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
  VPADDD        Z25, Z2,  K1,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K1=lane_active; Z25=needle_length;
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_len -= needle_length        ;Z3=str_len; K1=lane_active; Z25=needle_length;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsPrefixCi

//; #region bcContainsPrefixUTF8Ci
//; empty needles or empty data always result in a dead lane
//
// s[0].k[1] = contains_prefix_utf8_ci(slice[2], dict[3]).k[4]
TEXT bcContainsPrefixUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameters
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141 load number of code-points      ;CX=needle_len; R14=needle_ptr;
  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=needle_len;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;B73A4F83 K1 &= (scratch_Z26<=str_len)    ;K1=lane_active; Z26=scratch_Z26; Z3=str_len; 2=LessEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  ADDQ          $4,  R14                  //;7B0665F3 needle_ptr += 4                 ;R14=needle_ptr;
//; load constants
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;

loop:
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_len != 0); empty data are dead lanes;K1=lane_active; Z3=str_len;
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

//; NOTE: debugging. If you jump from here to mixed_ascii you bypass the 4 ASCII optimization
  CMPL          CX,  $4                   //;E273EEEA are we in the needle tail?      ;CX=needle_len;
  JL            mixed_ascii               //;A8685FD7 yes, then jump; jump if less (SF neq OF);
  VPBROADCASTD.Z 16(R14),K1,  Z9          //;2694A02F load needle data                ;Z9=data_needle; K1=lane_active; R14=needle_ptr;
//; determine if either data or needle has non-ASCII content
  VPORD.Z       Z8,  Z9,  K1,  Z26        //;3692D686 scratch_Z26 := data_needle | data_msg;Z26=scratch_Z26; K1=lane_active; Z9=data_needle; Z8=data_msg;
  VPMOVB2M      Z26, K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);
//; clear tail from data: IN zmm8; OUT zmm8
  VPMINSD       Z3,  Z20, Z26             //;DEC17BF3 scratch_Z26 := min(4, str_len)  ;Z26=scratch_Z26; Z20=4; Z3=str_len;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=char_a)        ;K3=tmp_mask; Z8=data_msg; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=char_z)        ;K3=tmp_mask; Z8=data_msg; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z13 //;1BB96D97                                 ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; compare data with needle for 4 ASCIIs
  VPCMPD        $0,  Z13, Z9,  K1,  K1    //;BBBDF880 K1 &= (data_needle==data_msg_upper);K1=lane_active; Z9=data_needle; Z13=data_msg_upper; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next 4 ASCIIs
  VPADDD        Z20, Z2,  K1,  Z2         //;D7CC90DD str_start += 4                  ;Z2=str_start; K1=lane_active; Z20=4;
  VPSUBD        Z20, Z3,  K1,  Z3         //;AEDCD850 str_len -= 4                    ;Z3=str_len; K1=lane_active; Z20=4;
  ADDQ          $80, R14                  //;F0BC3163 needle_ptr += 80                ;R14=needle_ptr;
  SUBL          $4,  CX                   //;646B86C9 needle_len -= 4                 ;CX=needle_len;
  JG            loop                      //;1EBC2C20 jump if greater ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;

mixed_ascii:
//; select next UTF8 byte sequence
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; compare data with needle for 1 UTF8 byte sequence
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K3   //;345D0BF3 K3 := K1 & (data_msg==[needle_ptr]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  4(R14),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[needle_ptr+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  8(R14),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[needle_ptr+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  12(R14),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[needle_ptr+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next rune
  VPADDD        Z7,  Z2,  K1,  Z2         //;DFE8D20B str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;24E04BE7 str_len -= n_bytes_data         ;Z3=str_len; K1=lane_active; Z7=n_bytes_data;
  ADDQ          $20, R14                  //;1F8D79B1 needle_ptr += 20                ;R14=needle_ptr;
  DECL          CX                        //;A99E9290 needle_len--                    ;CX=needle_len;
  JG            loop                      //;80013DFA jump if greater ((ZF = 0) and (SF = OF));

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsPrefixUTF8Ci

//; #region bcContainsSuffixCs
//
// s[0].k[1] = contains_suffix_cs(slice[2], dict[3]).k[4]
TEXT bcContainsSuffixCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; restrict lanes based on the length of needle
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_len                 ;Z25=needle_len; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (data_len>=needle_len)    ;K1=lane_active; Z3=data_len; Z25=needle_len; 5=GreaterEq;
  VPTESTMD      Z25, Z25, K1,  K1         //;6C36C5E7 K1 &= (needle_len != 0)         ;K1=lane_active; Z25=needle_len;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes still alive??         ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;
//; load constants
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
//; init variables
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQA32     Z25, Z6                   //;6F6F1342 counter := needle_len           ;Z6=counter; Z25=needle_len;
  VPADDD        Z3,  Z2,  Z4              //;813A5F04 data_end := data_off + data_len ;Z4=data_end; Z2=data_off; Z3=data_len;
  VPSUBD.Z      Z25, Z4,  K1,  Z24        //;EB9BEEEE offset := data_end - needle_len ;Z24=offset; K1=lane_active; Z4=data_end; Z25=needle_len;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;E4967C89 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=offset;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data==[needle_ptr])      ;K1=lane_active; Z8=data; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance 4 ASCIIs
  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD offset += 4                     ;Z24=offset; Z20=4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=4; 5=GreaterEq;
  KTESTW        K1,  K3                   //;77067C8D ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; still any needle left to compare
  VPTESTMD      Z6,  Z6,  K3              //;E0E548E4 K3 := (counter!=0); any chars left in needle?;K3=tmp_mask; Z6=counter;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);
//; load the last 1-4 ASCIIs
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;36FEA5FE gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=offset;
  VPERMD        CONST_TAIL_MASK(),Z6,  Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z6=counter;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=needle; Z19=tail_mask; R14=needle_ptr;
//; compare data with needle
  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data==needle)            ;K1=lane_active; Z8=data; Z9=needle; 0=Eq;
update:
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 data_len -= needle_len          ;Z3=data_len; K1=lane_active; Z25=needle_len;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSuffixCs

//; #region bcContainsSuffixCi
//
// s[0].k[1] = contains_suffix_ci(slice[2], dict[3]).k[4]
TEXT bcContainsSuffixCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; restrict lanes based on the length of needle
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_len                 ;Z25=needle_len; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (data_len>=needle_len)    ;K1=lane_active; Z3=data_len; Z25=needle_len; 5=GreaterEq;
  VPTESTMD      Z25, Z25, K1,  K1         //;6C36C5E7 K1 &= (needle_len != 0)         ;K1=lane_active; Z25=needle_len;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes still alive??         ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;
//; load constants
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
//; init variables
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQA32     Z25, Z6                   //;6F6F1342 counter := needle_len           ;Z6=counter; Z25=needle_len;
  VPADDD        Z3,  Z2,  Z4              //;813A5F04 data_end := data_off + data_len ;Z4=data_end; Z2=data_off; Z3=data_len;
  VPSUBD.Z      Z25, Z4,  K1,  Z24        //;EB9BEEEE offset := data_end - needle_len ;Z24=offset; K1=lane_active; Z4=data_end; Z25=needle_len;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;E4967C89 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=offset;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z13 //;1BB96D97                                 ;Z13=data_msg_upper; Z8=data; Z15=c_0b00100000;
//; compare data with needle
  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==[needle_ptr]);K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance 4 ASCIIs
  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD offset += 4                     ;Z24=offset; Z20=4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=4; 5=GreaterEq;
  KTESTW        K1,  K3                   //;77067C8D ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);
//; still any needle left to compare
  VPTESTMD      Z6,  Z6,  K3              //;E0E548E4 K3 := (counter!=0); any chars left in needle?;K3=tmp_mask; Z6=counter;
  KTESTW        K1,  K3                   //;C28D3832 ZF := ((K3&K1)==0); CF := ((~K3&K1)==0);K3=tmp_mask; K1=lane_active;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);
//; load the last 1-4 ASCIIs
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;36FEA5FE gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=offset;
  VPERMD        CONST_TAIL_MASK(), Z6, Z19 //;E5886CFE get tail_mask                  ;Z19=tail_mask; Z6=counter;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=needle; Z19=tail_mask; R14=needle_ptr;
//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z13 //;1BB96D97                                 ;Z13=data_msg_upper; Z8=data; Z15=c_0b00100000;

//; compare data with needle
  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==needle)  ;K1=lane_active; Z13=data_msg_upper; Z9=needle; 0=Eq;
update:
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 data_len -= needle_len          ;Z3=data_len; K1=lane_active; Z25=needle_len;

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSuffixCi

//; #region bcContainsSuffixUTF8Ci
//
// s[0].k[1] = contains_suffix_utf8_ci(slice[2], dict[3]).k[4]
TEXT bcContainsSuffixUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;5B83F09F load number of code-points      ;CX=n_runes; R14=needle_ptr;
  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=n_runes;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;

  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;B73A4F83 K1 &= (scratch_Z26<=data_len)   ;K1=lane_active; Z26=scratch_Z26; Z3=data_len; 2=LessEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  ADDQ          $4,  R14                  //;7B0665F3 calc alt_ptr offset             ;R14=needle_ptr;
  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z5  //;F6E81301 load constant UTF8 2byte mask   ;Z5=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z23 //;B1E12620 load constant UTF8 3byte mask   ;Z23=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z21 //;D896A9E1 load constant UTF8 4byte mask   ;Z21=UTF8_4byte_mask;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z14             //;EDD57CAF load constant 2                 ;Z14=2; Z10=1;
  VPADDD        Z10, Z14, Z12             //;7E7A1CB0 load constant 3                 ;Z12=3; Z14=2; Z10=1;
  VPADDD        Z10, Z12, Z20             //;9CFA6ADD load constant 4                 ;Z20=4; Z12=3; Z10=1;
  VPADDD.Z      Z3,  Z2,  K1,  Z4         //;813A5F04 data_end := data_off + data_len ;Z4=data_end; K1=lane_active; Z2=data_off; Z3=data_len;

loop:
  VPCMPD        $5,  Z10, Z3,  K1,  K1    //;790C4E82 K1 &= (data_len>=1); empty data are dead lanes;K1=lane_active; Z3=data_len; Z10=1; 5=GreaterEq;
  VPTESTMD      Z3,  Z3,  K1,  K3         //;6639548B K3 := K1 & (data_len != 0)      ;K3=tmp_mask; K1=lane_active; Z3=data_len;
//; calculate offset that is always positive
  VPMINUD       Z20, Z3,  Z31             //;B086F272 adjust := min(data_len, 4)      ;Z31=adjust; Z3=data_len; Z20=4;
  VPSUBD.Z      Z31, Z4,  K3,  Z24        //;998E9936 offset := data_end - adjust     ;Z24=offset; K3=tmp_mask; Z4=data_end; Z31=adjust;
  VPXORD        Z8,  Z8,  Z8              //;1882D069 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;30D04944 gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=offset;
//; adjust data
  VPSUBD        Z31, Z20, Z31             //;83BCC5BB adjust := 4 - adjust            ;Z31=adjust; Z20=4;
  VPSLLD        $3,  Z31, Z31             //;D2F273B1 times 8 gives number of bytes   ;Z31=adjust;
  VPSLLVD       Z31, Z8,  Z8              //;67300525 data <<= adjust                 ;Z8=data; Z31=adjust;
//; NOTE: debugging. If you jump from here to mixed_ascii you bypass the 4 ASCII optimization
  CMPL          CX,  $4                   //;E273EEEA are we in the needle tail?      ;CX=n_runes;
  JL            mixed_ascii               //;A8685FD7 yes, then jump; jump if less (SF neq OF);
  VPBROADCASTD.Z 16(R14),K1,  Z9          //;2694A02F load needle data                ;Z9=needle; K1=lane_active; R14=needle_ptr;
//; clear tail from data: IN zmm8; OUT zmm8
  VPMINSD       Z3,  Z20, Z26             //;DEC17BF3 scratch_Z26 := min(4, data_len) ;Z26=scratch_Z26; Z20=4; Z3=data_len;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data; Z19=tail_mask;
//; determine if either data or needle has non-ASCII content
  VPORD.Z       Z8,  Z9,  K1,  Z26        //;3692D686 scratch_Z26 := needle | data    ;Z26=scratch_Z26; K1=lane_active; Z9=needle; Z8=data;
  VPMOVB2M      Z26, K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);

//; str_to_upper: IN zmm8; OUT zmm13
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z13 //;1BB96D97                                 ;Z13=data_msg_upper; Z8=data; Z15=c_0b00100000;
//; compare data with needle for 4 ASCIIs
  VPCMPD        $0,  Z13, Z9,  K1,  K1    //;BBBDF880 K1 &= (needle==data_msg_upper)  ;K1=lane_active; Z9=needle; Z13=data_msg_upper; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next 4 ASCIIs
  VPSUBD.Z      Z20, Z4,  K1,  Z4         //;D7CC90DD data_end -= 4                   ;Z4=data_end; K1=lane_active; Z20=4;
  VPSUBD        Z20, Z3,  K1,  Z3         //;83ADFEDA data_len -= 4                   ;Z3=data_len; K1=lane_active; Z20=4;
  ADDQ          $80, R14                  //;F0BC3163 needle_ptr += 80                ;R14=needle_ptr;
  SUBL          $4,  CX                   //;646B86C9 n_runes -= 4                    ;CX=n_runes;
  JG            loop                      //;1EBC2C20 jump if greater ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;

mixed_ascii:
//; count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z5,  Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data; Z5=UTF8_2byte_mask;
  VPCMPD        $0,  Z5,  Z26, K1,  K3    //;C6890BF4 K3 := K1 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K1=lane_active; Z26=scratch_Z26; Z5=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z23, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data; Z23=UTF8_3byte_mask;
  VPCMPD        $0,  Z23, Z26, K1,  K4    //;14C32DC0 K4 := K1 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=alt2_match; K1=lane_active; Z26=scratch_Z26; Z23=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z21, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data; Z21=UTF8_4byte_mask;
  VPCMPD        $0,  Z21, Z26, K1,  K5    //;1AE0A51C K5 := K1 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=alt3_match; K1=lane_active; Z26=scratch_Z26; Z21=UTF8_4byte_mask; 0=Eq;
  VMOVDQA32     Z10, Z7                   //;A7640B64 n_bytes_data := 1               ;Z7=n_bytes_data; Z10=1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_data; K3=tmp_mask; Z10=1;
  VPADDD        Z14, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_data; K4=alt2_match; Z14=2;
  VPADDD        Z12, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_data; K5=alt3_match; Z12=3;
//; shift code-point to least significant position
  VPSUBD        Z7,  Z20, Z26             //;C8ECAA75 scratch_Z26 := 4 - n_bytes_data ;Z26=scratch_Z26; Z20=4; Z7=n_bytes_data;
  VPSLLD        $3,  Z26, Z26             //;5734792E scratch_Z26 <<= 3               ;Z26=scratch_Z26;
  VPSRLVD       Z26, Z8,  Z8              //;529FFC90 data >>= scratch_Z26            ;Z8=data; Z26=scratch_Z26;
//; compare data with needle for 1 UTF8 byte sequence
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K3   //;345D0BF3 K3 := K1 & (data==[needle_ptr]) ;K3=tmp_mask; K1=lane_active; Z8=data; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  4(R14),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data==[needle_ptr+4]);K4=alt2_match; K1=lane_active; Z8=data; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  8(R14),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data==[needle_ptr+8]);K5=alt3_match; K1=lane_active; Z8=data; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  12(R14),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data==[needle_ptr+12]);K6=alt4_match; K1=lane_active; Z8=data; R14=needle_ptr; 0=Eq;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next rune
  VPSUBD        Z7,  Z4,  K1,  Z4         //;D35D27FB data_end -= n_bytes_data        ;Z4=data_end; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;24E04BE7 data_len -= n_bytes_data        ;Z3=data_len; K1=lane_active; Z7=n_bytes_data;
  ADDQ          $20, R14                  //;1F8D79B1 needle_ptr += 20                ;R14=needle_ptr;
  DECL          CX                        //;A99E9290 n_runes--                       ;CX=n_runes;
  JG            loop                      //;80013DFA jump if greater ((ZF = 0) and (SF = OF));

next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSuffixUTF8Ci

//; #region bcContainsSubstrCs
//
// s[0].k[1] = contains_substr_cs(slice[2], dict[3]).k[4]
TEXT bcContainsSubstrCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  VPBROADCASTD  8(R14),Z6                 //;F6AC18B2 bcst needle_len                 ;Z6=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle pointer
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
//; load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTB  (R14),Z9                  //;54CB0C41 bcst first char from needle     ;Z9=char1_needle; R14=needle_ptr1;

//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 if not cleared 170C9C8F will give unncessary matches;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
  VPCMPB        $0,  Z9,  Z8,  K3         //;170C9C8F K3 := (data==char1_needle)      ;K3=tmp_mask; Z8=data; Z9=char1_needle; 0=Eq;
  KTESTQ        K3,  K3                   //;AD0284F2 found any char1_needle?         ;K3=tmp_mask;
  JNZ           scan_found_something      //;4645455C yes, found something!; jump if not zero (ZF = 0);
  VPSUBD        Z20, Z3,  K2,  Z3         //;BF319EDC data_len1 -= 4                  ;Z3=data_len1; K2=lane_todo; Z20=4;
  VPADDD        Z20, Z2,  K2,  Z2         //;B5CB47F3 data_off1 += 4                  ;Z2=data_off1; K2=lane_todo; Z20=4;
scan_found_something_ret:                 //;2CADDCF3 reentry code path with found char1_needle
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;358E0E6F K2 &= (0<data_len1)             ;K2=lane_todo; Z11=0; Z3=data_len1; 1=LessThen;
  KTESTW        K2,  K2                   //;854AE89B any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;5BB6DC39 yes, then continue scanning; jump if not zero (ZF = 0);
  JMP           scan_done                 //;58F52ADF                                 ;

scan_found_something:
//; calculate skip_count in Z14
  VPMOVM2B      K3,  Z27                  //;E74FDEBD promote 64x bit to 64x byte     ;Z27=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z27, Z27             //;4F265F03 reverse byte order              ;Z27=skip_count; Z22=bswap32;
  VPLZCNTD      Z27, Z27                  //;72202F9A count leading zeros             ;Z27=skip_count;
  VPSRLD.Z      $3,  Z27, K2,  Z27        //;6DC91432 divide by 8 yields skip_count   ;Z27=skip_count; K2=lane_todo;
//; advance
  VPCMPD        $4,  Z20, Z27, K2,  K3    //;2846C84C K3 := K2 & (skip_count!=4)      ;K3=tmp_mask; K2=lane_todo; Z27=skip_count; Z20=4; 4=NotEqual;
  VPADDD        Z27, Z2,  K2,  Z2         //;63F1BACC data_off1 += skip_count         ;Z2=data_off1; K2=lane_todo; Z27=skip_count;
  VPSUBD        Z27, Z3,  K2,  Z3         //;8F7F978F data_len1 -= skip_count         ;Z3=data_len1; K2=lane_todo; Z27=skip_count;
//; update masks with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
  JMP           scan_found_something_ret  //;169A16C1 jump back to where we came from ;

scan_done:                                //;50D019A9 check if the ran out of data
  VPCMPD        $5,  Z6,  Z3,  K4,  K4    //;FA18B497 K4 &= (data_len1>=needle_len1)  ;K4=lane_selected; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VMOVDQA32     Z2,  Z24                  //;835001A1 data_off2 := data_off1          ;Z24=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  VMOVDQA32     Z6,  Z25                  //;E323C56F needle_len2 := needle_len1      ;Z25=needle_len2; Z6=needle_len1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=data_off2;
//; load needle and apply tail masks
  VPMINSD       Z20, Z25, Z13             //;7D091557 adv_needle := min(needle_len2, 4);Z13=adv_needle; Z25=needle_len2; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z8,  Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27;
  VPANDD.BCST   (R13),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R13=needle_ptr2;
//; compare data with needle
  VPCMPD        $0,  Z28, Z8,  K4,  K4    //;18C82D78 K4 &= (data==needle)            ;K4=lane_selected; Z8=data; Z28=needle; 0=Eq;
//; advance
  VPADDD        Z13, Z24, K4,  Z24        //;3371623C data_off2 += adv_needle         ;Z24=data_off2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z5,  K4,  Z5         //;9905C7C3 data_len2 -= adv_needle         ;Z5=data_len2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z25, K4,  Z25        //;5A8AB52E needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z13=adv_needle;
  ADDQ          $4,  R13                  //;5D0D7365 needle_ptr2 += 4                ;R13=needle_ptr2;
//; check needle_loop conditions
  VPCMPD        $1,  Z25, Z11, K3         //;D0432359 K3 := (0<needle_len2)           ;K3=tmp_mask; Z11=0; Z25=needle_len2; 1=LessThen;
  KTESTW        K4,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K4=lane_selected;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z24, K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z24=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; advance the scan-offsets with 1 and if there are any lanes still todo, scan those
  VPADDD        Z10, Z2,  K2,  Z2         //;65A7A575 data_off1++                     ;Z2=data_off1; K2=lane_todo; Z10=1;
  VPSUBD        Z10, Z3,  K2,  Z3         //;F7731EA7 data_len1--                     ;Z3=data_len1; K2=lane_todo; Z10=1;
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSubstrCs

//; #region bcContainsSubstrCi
//
// s[0].k[1] = contains_substr_ci(slice[2], dict[3]).k[4]
TEXT bcContainsSubstrCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  VPBROADCASTD  8(R14),Z6                 //;F6AC18B2 bcst needle_len                 ;Z6=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle pointer
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTB  (R14),Z9                  //;54CB0C41 bcst first char from needle     ;Z9=char1_needle; R14=needle_ptr1;

//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 if not cleared 170C9C8F will give unncessary matches;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1), K3, Z8 //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch_Z26; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch_Z26; Z8=data; Z15=c_0b00100000;
  VPCMPB        $0,  Z9,  Z26, K3         //;170C9C8F K3 := (scratch_Z26==char1_needle);K3=tmp_mask; Z26=scratch_Z26; Z9=char1_needle; 0=Eq;
  KTESTQ        K3,  K3                   //;AD0284F2 found any char1_needle?         ;K3=tmp_mask;
  JNZ           scan_found_something      //;4645455C yes, found something!; jump if not zero (ZF = 0);
  VPSUBD        Z20, Z3,  K2,  Z3         //;BF319EDC data_len1 -= 4                  ;Z3=data_len1; K2=lane_todo; Z20=4;
  VPADDD        Z20, Z2,  K2,  Z2         //;B5CB47F3 data_off1 += 4                  ;Z2=data_off1; K2=lane_todo; Z20=4;
scan_found_something_ret:                 //;2CADDCF3 reentry code path with found char1_needle
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;358E0E6F K2 &= (0<data_len1)             ;K2=lane_todo; Z11=0; Z3=data_len1; 1=LessThen;
  KTESTW        K2,  K2                   //;854AE89B any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;5BB6DC39 yes, then continue scanning; jump if not zero (ZF = 0);
  JMP           scan_done                 //;58F52ADF                                 ;

scan_found_something:
//; calculate skip_count in Z14
  VPMOVM2B      K3,  Z27                  //;E74FDEBD promote 64x bit to 64x byte     ;Z27=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z27, Z27             //;4F265F03 reverse byte order              ;Z27=skip_count; Z22=bswap32;
  VPLZCNTD      Z27, Z27                  //;72202F9A count leading zeros             ;Z27=skip_count;
  VPSRLD.Z      $3,  Z27, K2,  Z27        //;6DC91432 divide by 8 yields skip_count   ;Z27=skip_count; K2=lane_todo;
//; advance
  VPCMPD        $4,  Z20, Z27, K2,  K3    //;2846C84C K3 := K2 & (skip_count!=4)      ;K3=tmp_mask; K2=lane_todo; Z27=skip_count; Z20=4; 4=NotEqual;
  VPADDD        Z27, Z2,  K2,  Z2         //;63F1BACC data_off1 += skip_count         ;Z2=data_off1; K2=lane_todo; Z27=skip_count;
  VPSUBD        Z27, Z3,  K2,  Z3         //;8F7F978F data_len1 -= skip_count         ;Z3=data_len1; K2=lane_todo; Z27=skip_count;
//; update masks with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
  JMP           scan_found_something_ret  //;169A16C1 jump back to where we came from ;

scan_done:                                //;50D019A9 check if the ran out of data
  VPCMPD        $5,  Z6,  Z3,  K4,  K4    //;FA18B497 K4 &= (data_len1>=needle_len1)  ;K4=lane_selected; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VMOVDQA32     Z2,  Z24                  //;835001A1 data_off2 := data_off1          ;Z24=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  VMOVDQA32     Z6,  Z25                  //;E323C56F needle_len2 := needle_len1      ;Z25=needle_len2; Z6=needle_len1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z24*1), K3, Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=data_off2;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch_Z26; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch_Z26; Z8=data; Z15=c_0b00100000;
//; load needle and apply tail masks
  VPMINSD       Z20, Z25, Z13             //;7D091557 adv_needle := min(needle_len2, 4);Z13=adv_needle; Z25=needle_len2; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z26, Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27; Z26=scratch_Z26;
  VPANDD.BCST   (R13),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R13=needle_ptr2;
//; compare data with needle
  VPCMPD        $0,  Z28, Z8,  K4,  K4    //;18C82D78 K4 &= (data==needle)            ;K4=lane_selected; Z8=data; Z28=needle; 0=Eq;
//; advance
  VPADDD        Z13, Z24, K4,  Z24        //;3371623C data_off2 += adv_needle         ;Z24=data_off2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z5,  K4,  Z5         //;9905C7C3 data_len2 -= adv_needle         ;Z5=data_len2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z25, K4,  Z25        //;5A8AB52E needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z13=adv_needle;
  ADDQ          $4,  R13                  //;5D0D7365 needle_ptr2 += 4                ;R13=needle_ptr2;
//; check needle_loop conditions
  VPCMPD        $1,  Z25, Z11, K3         //;D0432359 K3 := (0<needle_len2)           ;K3=tmp_mask; Z11=0; Z25=needle_len2; 1=LessThen;
  KTESTW        K4,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K4=lane_selected;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z24, K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z24=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; advance the scan-offsets with 1 and if there are any lanes still todo, scan those
  VPADDD        Z10, Z2,  K2,  Z2         //;65A7A575 data_off1++                     ;Z2=data_off1; K2=lane_todo; Z10=1;
  VPSUBD        Z10, Z3,  K2,  Z3         //;F7731EA7 data_len1--                     ;Z3=data_len1; K2=lane_todo; Z10=1;
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSubstrCi

//; #region bcContainsSubstrUTF8Ci
//
// s[0].k[1] = contains_substr_utf8_ci(slice[2], dict[3]).k[4]
TEXT bcContainsSubstrUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141 load number of runes in needle  ;CX=needle_len1; R14=needle_ptr1;
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
//; restrict lanes to allow fast bail-out
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTD  4(R14),Z12                //;54CB0C41 bcst alt1 from needle           ;Z12=char1_needle; R14=needle_ptr1;
  VPBROADCASTD  8(R14),Z13                //;EE0650F7 bcst alt2 from needle           ;Z13=char2_needle; R14=needle_ptr1;
  VPBROADCASTD  12(R14),Z14               //;879514D3 bcst alt3 from needle           ;Z14=char3_needle; R14=needle_ptr1;
  VPBROADCASTD  16(R14),Z15               //;A0DE73B0 bcst alt4 from needle           ;Z15=char4_needle; R14=needle_ptr1;
  ADDQ          $20, R14                  //;B6596F07 needle_ptr1 += 20               ;R14=needle_ptr1;
//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; get tail mask
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPERMD        Z21, Z26, Z27             //;68FECBA0 get n_bytes_data                ;Z27=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z27, Z26             //;6D661522 get tail_mask (data)            ;Z26=scratch_Z26; Z27=n_bytes_data; Z18=tail_mask_data;
//; advance data for lanes that are still todo
  VPADDD        Z27, Z2,  K2,  Z2         //;FBFD5F0A data_off1 += n_bytes_data       ;Z2=data_off1; K2=lane_todo; Z27=n_bytes_data;
  VPSUBD        Z27, Z3,  K2,  Z3         //;28FE1865 data_len1 -= n_bytes_data       ;Z3=data_len1; K2=lane_todo; Z27=n_bytes_data;
  VPANDD.Z      Z8,  Z26, K2,  Z8         //;6EEB77B0 mask data                       ;Z8=data; K2=lane_todo; Z26=scratch_Z26;
//; compare with 4 code-points
  VPCMPD        $0,  Z12, Z8,  K2,  K3    //;6D1378C0 K3 := K2 & (data==char1_needle) ;K3=tmp_mask; K2=lane_todo; Z8=data; Z12=char1_needle; 0=Eq;
  VPCMPD        $0,  Z13, Z8,  K2,  K5    //;FC651EE0 K5 := K2 & (data==char2_needle) ;K5=scratch_K5; K2=lane_todo; Z8=data; Z13=char2_needle; 0=Eq;
  VPCMPD        $0,  Z14, Z8,  K2,  K6    //;D9FE5534 K6 := K2 & (data==char3_needle) ;K6=scratch_K6; K2=lane_todo; Z8=data; Z14=char3_needle; 0=Eq;
  VPCMPD        $0,  Z15, Z8,  K2,  K0    //;D65545B1 K0 := K2 & (data==char4_needle) ;K0=scratch_K0; K2=lane_todo; Z8=data; Z15=char4_needle; 0=Eq;
  KORW          K3,  K5,  K3              //;A903EE01 tmp_mask |= scratch_K5          ;K3=tmp_mask; K5=scratch_K5;
  KORW          K3,  K6,  K3              //;31B58C73 tmp_mask |= scratch_K6          ;K3=tmp_mask; K6=scratch_K6;
  KORW          K3,  K0,  K3              //;A20F03C1 tmp_mask |= scratch_K0          ;K3=tmp_mask; K0=scratch_K0;
//; update lanes with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
//; determine if we need to continue searching for a matching start code-point
  VPCMPD        $5,  Z6,  Z3,  K2,  K2    //;30061EEA K2 &= (data_len1>=needle_len1)  ;K2=lane_todo; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K2,  K2                   //;67E2F74E any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;677C9F60 jump if not zero (ZF = 0)       ;

//; we are done scanning for the first code-point; test if there are more
  VMOVDQA32     Z2,  Z4                   //;835001A1 data_off2 := data_off1          ;Z4=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  CMPL          CX,  $1                   //;2BBD50B8 more than 1 char in needle?     ;CX=needle_len1;
  JZ            needle_loop_done          //;AFDB3970 jump if zero (ZF = 1)           ;
//; test if there is there is sufficient data
  VPCMPD        $6,  Z11, Z3,  K4,  K4    //;C0CF3FA2 K4 &= (data_len1>0)             ;K4=lane_selected; Z3=data_len1; Z11=0; 6=Greater;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VPSUBD        Z10, Z6,  Z7              //;75A315B7 needle_len2 := needle_len1 - 1  ;Z7=needle_len2; Z6=needle_len1; Z10=1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPGATHERDD    (VIRT_BASE)(Z4*1),K3,  Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z4=data_off2;
//; advance needle
  VPSUBD        Z10, Z7,  K4,  Z7         //;CAFCD045 needle_len2--                   ;Z7=needle_len2; K4=lane_selected; Z10=1;
//; mask tail data
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPERMD        Z21, Z26, Z27             //;68FECBA0 get n_bytes_data                ;Z27=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z27, Z26             //;488C6CD8 get tail_mask (data)            ;Z26=scratch_Z26; Z27=n_bytes_data; Z18=tail_mask_data;
  VPANDD.Z      Z8,  Z26, K2,  Z8         //;E750B0E2 mask data                       ;Z8=data; K2=lane_todo; Z26=scratch_Z26;
  VPADDD        Z27, Z4,  K4,  Z4         //;4879FB55 data_off2 += n_bytes_data       ;Z4=data_off2; K4=lane_selected; Z27=n_bytes_data;
  VPSUBD        Z27, Z5,  K4,  Z5         //;77CC472F data_len2 -= n_bytes_data       ;Z5=data_len2; K4=lane_selected; Z27=n_bytes_data;
//; compare with 4 code-points
  VPCMPD.BCST   $0,  (R13),Z8,  K4,  K3   //;15105C54 K3 := K4 & (data==[needle_ptr2]);K3=tmp_mask; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  4(R13),Z8,  K4,  K5  //;9C52A210 K5 := K4 & (data==[needle_ptr2+4]);K5=scratch_K5; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  8(R13),Z8,  K4,  K6  //;A8B34D3C K6 := K4 & (data==[needle_ptr2+8]);K6=scratch_K6; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  12(R13),Z8,  K4,  K0  //;343DD0F2 K0 := K4 & (data==[needle_ptr2+12]);K0=scratch_K0; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  ADDQ          $16, R13                  //;5D0D7365 needle_ptr2 += 16               ;R13=needle_ptr2;
  KORW          K3,  K5,  K3              //;1125C81D tmp_mask |= scratch_K5          ;K3=tmp_mask; K5=scratch_K5;
  KORW          K3,  K6,  K3              //;DCB6AFAD tmp_mask |= scratch_K6          ;K3=tmp_mask; K6=scratch_K6;
  KORW          K3,  K0,  K4              //;65791592 lane_selected := scratch_K0 | tmp_mask;K4=lane_selected; K0=scratch_K0; K3=tmp_mask;
//; advance to the next code-point
  VPCMPD        $6,  Z11, Z7,  K4,  K3    //;1185F8E2 K3 := K4 & (needle_len2>0)      ;K3=tmp_mask; K4=lane_selected; Z7=needle_len2; Z11=0; 6=Greater;
  VPCMPD        $2,  Z5,  Z7,  K4,  K4    //;7C50EFFB K4 &= (needle_len2<=data_len2)  ;K4=lane_selected; Z7=needle_len2; Z5=data_len2; 2=LessEq;
  KTESTW        K3,  K3                   //;98CB0D8B ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           needle_loop               //;E6065236 jump if not zero (ZF = 0)       ;
//; update lanes
needle_loop_done:
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z4,  K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z4=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; if there are any lanes still todo, scan those
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsSubstrUTF8Ci

//; #region bcEqPatternCs
//
// s[0].k[1] = eq_pattern_cs(slice[2], dict[3]).k[4]
TEXT bcEqPatternCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141                                 ;CX=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle and wildcard pointers
  ADDQ          $4,  R14                  //;B1A93760 needle_ptr1 += 4                ;R14=needle_ptr1;
  MOVQ          R14, R15                  //;1B1C0A5D wildcard_ptr1 := needle_ptr1    ;R15=wildcard_ptr1; R14=needle_ptr1;
  ADDQ          CX,  R15                  //;FC062E0E wildcard_ptr1 += needle_len1    ;R15=wildcard_ptr1; CX=needle_len1;
//; load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
needle_loop:
  KMOVW         K1,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; load needle and apply tail masks
  VPMINSD       Z20, Z6,  Z13             //;7D091557 adv_needle := min(needle_len1, 4);Z13=adv_needle; Z6=needle_len1; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z8,  Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27;
  VPANDD.BCST   (R14),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R14=needle_ptr1;
  VPBROADCASTD  (R15),Z27                 //;911BC5F9 load wildcard                   ;Z27=wildcard; R15=wildcard_ptr1;
//; test if a unicode code-points matches a wildcard
  VPANDD.Z      Z27, Z8,  K1,  Z26        //;7A4C6E61 scratch_Z26 := data & wildcard  ;Z26=scratch_Z26; K1=lane_active; Z8=data; Z27=wildcard;
  VPMOVB2M      Z26, K3                   //;4B555080                                 ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;14D8E8C5 ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           unicode_match             //;C94028E8 jump if not zero (ZF = 0)       ;
//; compare data with needle
//;    Z27|Z8 |Z28  => Z28
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      0
//;     1 | 0 | 0      1
//;     1 | 0 | 1      0
//;     1 | 1 | 0      0
//;     1 | 1 | 1      0
  VPTERNLOGD    $0b00010100,Z27, Z8,  Z28 //;A64A5655 compute masked equality         ;Z28=needle; Z8=data; Z27=wildcard;
  VPCMPD        $0,  Z11, Z28, K1,  K1    //;D2F6A32B K1 &= (needle==0)               ;K1=lane_active; Z28=needle; Z11=0; 0=Eq;
//; advance
  VPADDD        Z13, Z2,  K1,  Z2         //;3371623C data_off1 += adv_needle         ;Z2=data_off1; K1=lane_active; Z13=adv_needle;
  VPSUBD        Z13, Z3,  K1,  Z3         //;9905C7C3 data_len1 -= adv_needle         ;Z3=data_len1; K1=lane_active; Z13=adv_needle;
  VPSUBD        Z13, Z6,  K1,  Z6         //;5A8AB52E needle_len1 -= adv_needle       ;Z6=needle_len1; K1=lane_active; Z13=adv_needle;
  ADDQ          $4,  R14                  //;5D0D7365 needle_ptr1 += 4                ;R14=needle_ptr1;
  ADDQ          $4,  R15                  //;8A43B166 wildcard_ptr1 += 4              ;R15=wildcard_ptr1;
unicode_match_ret:
//; check needle_loop conditions
  VPCMPD        $1,  Z6,  Z11, K3         //;D0432359 K3 := (0<needle_len1)           ;K3=tmp_mask; Z11=0; Z6=needle_len1; 1=LessThen;
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;1F3D9F3F K1 &= (0<=data_len1)            ;K1=lane_active; Z11=0; Z3=data_len1; 2=LessEq;
  KTESTW        K1,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K1=lane_active;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;E555E77C K1 &= (data_len1==0)            ;K1=lane_active; Z3=data_len1;
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)

unicode_match:                            //;B1B3AECE a wildcard has matched with a unicode code-point
//; with the wildcard mask, get the number of bytes BEFORE the first wildcard (0, 1, 2, 3)
//; at that position get the number of bytes of the code-point, add these two numbers
//; calculate advance in Z26
  VPSHUFB       Z22, Z27, Z26             //;3F4659FF reverse byte order              ;Z26=scratch_Z26; Z27=wildcard; Z22=bswap32;
  VPLZCNTD      Z26, Z27                  //;7DB21322 count leading zeros             ;Z27=zero_count; Z26=scratch_Z26;
  VPSRLD        $3,  Z27, Z19             //;D7D76764 divide by 8 yields advance      ;Z19=advance; Z27=zero_count;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;E6FE9C45 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPSRLVD       Z27, Z26, Z26             //;F99F8396 scratch_Z26 >>= zero_count      ;Z26=scratch_Z26; Z27=zero_count;
  VPERMD        Z21, Z26, Z26             //;DE232505 get n_bytes_code_point          ;Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z19, Z26, Z13             //;D11A93E8 adv_data := scratch_Z26 + advance;Z13=adv_data; Z26=scratch_Z26; Z19=advance;
//; we are only going to test the number of bytes BEFORE the first wildcard
  VPERMD        Z18, Z19, Z26             //;9A394869 get tail_mask data              ;Z26=scratch_Z26; Z19=advance; Z18=tail_mask_data;
  VPANDD        Z26, Z8,  Z8              //;C5A7FCBA data &= scratch_Z26             ;Z8=data; Z26=scratch_Z26;
  VPANDD        Z26, Z28, Z28             //;51343840 needle &= scratch_Z26           ;Z28=needle; Z26=scratch_Z26;
  VPCMPD        $0,  Z8,  Z28, K1,  K1    //;13A45EF9 K1 &= (needle==data)            ;K1=lane_active; Z28=needle; Z8=data; 0=Eq;
//; advance
  VPADDD        Z10, Z19, Z19             //;BFB2C3DE adv_needle := advance + 1       ;Z19=adv_needle; Z19=advance; Z10=1;
  VPSUBD        Z19, Z6,  K1,  Z6         //;B3CDC39C needle_len1 -= adv_needle       ;Z6=needle_len1; K1=lane_active; Z19=adv_needle;
  VPADDD        Z13, Z2,  K1,  Z2         //;AE5AA4CF data_off1 += adv_data           ;Z2=data_off1; K1=lane_active; Z13=adv_data;
  VPSUBD        Z13, Z3,  K1,  Z3         //;DBC41158 data_len1 -= adv_data           ;Z3=data_len1; K1=lane_active; Z13=adv_data;
  VMOVD         X19, R8                   //;B10A9DE5 extract GPR adv_needle          ;R8=scratch; Z19=adv_needle;
  ADDQ          R8,  R14                  //;34EA6A74 needle_ptr1 += scratch          ;R14=needle_ptr1; R8=scratch;
  ADDQ          R8,  R15                  //;8FD33A77 wildcard_ptr1 += scratch        ;R15=wildcard_ptr1; R8=scratch;
  JMP           unicode_match_ret         //;D24820C1                                 ;
//; #endregion bcEqPatternCs

//; #region bcEqPatternCi
//
// s[0].k[1] = eq_pattern_ci(slice[2], dict[3]).k[4]
TEXT bcEqPatternCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141                                 ;CX=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle and wildcard pointers
  ADDQ          $4,  R14                  //;B1A93760 needle_ptr1 += 4                ;R14=needle_ptr1;
  MOVQ          R14, R15                  //;1B1C0A5D wildcard_ptr1 := needle_ptr1    ;R15=wildcard_ptr1; R14=needle_ptr1;
  ADDQ          CX,  R15                  //;FC062E0E wildcard_ptr1 += needle_len1    ;R15=wildcard_ptr1; CX=needle_len1;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
needle_loop:
  KMOVW         K1,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch_Z26; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch_Z26; Z8=data; Z15=c_0b00100000;
//; load needle and apply tail masks
  VPMINSD       Z20, Z6,  Z13             //;7D091557 adv_needle := min(needle_len1, 4);Z13=adv_needle; Z6=needle_len1; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z26, Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27; Z26=scratch_Z26;
  VPANDD.BCST   (R14),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R14=needle_ptr1;
  VPBROADCASTD  (R15),Z27                 //;911BC5F9 load wildcard                   ;Z27=wildcard; R15=wildcard_ptr1;
//; test if a unicode code-points matches a wildcard
  VPANDD.Z      Z27, Z8,  K1,  Z26        //;7A4C6E61 scratch_Z26 := data & wildcard  ;Z26=scratch_Z26; K1=lane_active; Z8=data; Z27=wildcard;
  VPMOVB2M      Z26, K3                   //;4B555080                                 ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;14D8E8C5 ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           unicode_match             //;C94028E8 jump if not zero (ZF = 0)       ;
//; compare data with needle
//;    Z27|Z8 |Z28  => Z28
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      0
//;     1 | 0 | 0      1
//;     1 | 0 | 1      0
//;     1 | 1 | 0      0
//;     1 | 1 | 1      0
  VPTERNLOGD    $0b00010100,Z27, Z8,  Z28 //;A64A5655 compute masked equality         ;Z28=needle; Z8=data; Z27=wildcard;
  VPCMPD        $0,  Z11, Z28, K1,  K1    //;D2F6A32B K1 &= (needle==0)               ;K1=lane_active; Z28=needle; Z11=0; 0=Eq;
//; advance
  VPADDD        Z13, Z2,  K1,  Z2         //;3371623C data_off1 += adv_needle         ;Z2=data_off1; K1=lane_active; Z13=adv_needle;
  VPSUBD        Z13, Z3,  K1,  Z3         //;9905C7C3 data_len1 -= adv_needle         ;Z3=data_len1; K1=lane_active; Z13=adv_needle;
  VPSUBD        Z13, Z6,  K1,  Z6         //;5A8AB52E needle_len1 -= adv_needle       ;Z6=needle_len1; K1=lane_active; Z13=adv_needle;
  ADDQ          $4,  R14                  //;5D0D7365 needle_ptr1 += 4                ;R14=needle_ptr1;
  ADDQ          $4,  R15                  //;8A43B166 wildcard_ptr1 += 4              ;R15=wildcard_ptr1;
unicode_match_ret:
//; check needle_loop conditions
  VPCMPD        $1,  Z6,  Z11, K3         //;D0432359 K3 := (0<needle_len1)           ;K3=tmp_mask; Z11=0; Z6=needle_len1; 1=LessThen;
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;1F3D9F3F K1 &= (0<=data_len1)            ;K1=lane_active; Z11=0; Z3=data_len1; 2=LessEq;
  KTESTW        K1,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K1=lane_active;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;E555E77C K1 &= (data_len1==0)            ;K1=lane_active; Z3=data_len1;
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)

unicode_match:                            //;B1B3AECE a wildcard has matched with a unicode code-point
//; with the wildcard mask, get the number of bytes BEFORE the first wildcard (0, 1, 2, 3)
//; at that position get the number of bytes of the code-point, add these two numbers
//; calculate advance in Z26
  VPSHUFB       Z22, Z27, Z26             //;3F4659FF reverse byte order              ;Z26=scratch_Z26; Z27=wildcard; Z22=bswap32;
  VPLZCNTD      Z26, Z27                  //;7DB21322 count leading zeros             ;Z27=zero_count; Z26=scratch_Z26;
  VPSRLD        $3,  Z27, Z19             //;D7D76764 divide by 8 yields advance      ;Z19=advance; Z27=zero_count;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;E6FE9C45 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPSRLVD       Z27, Z26, Z26             //;F99F8396 scratch_Z26 >>= zero_count      ;Z26=scratch_Z26; Z27=zero_count;
  VPERMD        Z21, Z26, Z26             //;DE232505 get n_bytes_code_point          ;Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z19, Z26, Z13             //;D11A93E8 adv_data := scratch_Z26 + advance;Z13=adv_data; Z26=scratch_Z26; Z19=advance;
//; we are only going to test the number of bytes BEFORE the first wildcard
  VPERMD        Z18, Z19, Z26             //;9A394869 get tail_mask data              ;Z26=scratch_Z26; Z19=advance; Z18=tail_mask_data;
  VPANDD        Z26, Z8,  Z8              //;C5A7FCBA data &= scratch_Z26             ;Z8=data; Z26=scratch_Z26;
  VPANDD        Z26, Z28, Z28             //;51343840 needle &= scratch_Z26           ;Z28=needle; Z26=scratch_Z26;
  VPCMPD        $0,  Z8,  Z28, K1,  K1    //;13A45EF9 K1 &= (needle==data)            ;K1=lane_active; Z28=needle; Z8=data; 0=Eq;
//; advance
  VPADDD        Z10, Z19, Z19             //;BFB2C3DE adv_needle := advance + 1       ;Z19=adv_needle; Z19=advance; Z10=1;
  VPSUBD        Z19, Z6,  K1,  Z6         //;B3CDC39C needle_len1 -= adv_needle       ;Z6=needle_len1; K1=lane_active; Z19=adv_needle;
  VPADDD        Z13, Z2,  K1,  Z2         //;AE5AA4CF data_off1 += adv_data           ;Z2=data_off1; K1=lane_active; Z13=adv_data;
  VPSUBD        Z13, Z3,  K1,  Z3         //;DBC41158 data_len1 -= adv_data           ;Z3=data_len1; K1=lane_active; Z13=adv_data;
  VMOVD         X19, R8                   //;B10A9DE5 extract GPR adv_needle          ;R8=scratch; Z19=adv_needle;
  ADDQ          R8,  R14                  //;34EA6A74 needle_ptr1 += scratch          ;R14=needle_ptr1; R8=scratch;
  ADDQ          R8,  R15                  //;8FD33A77 wildcard_ptr1 += scratch        ;R15=wildcard_ptr1; R8=scratch;
  JMP           unicode_match_ret         //;D24820C1                                 ;
//; #endregion bcEqPatternCi

//; #region bcEqPatternUTF8Ci
//; empty needles or empty data always result in a dead lane
//
// s[0].k[1] = eq_pattern_utf8_ci(slice[2], dict[3]).k[4]
TEXT bcEqPatternUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameters
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141 load number of code-points      ;CX=needle_len; R14=needle_ptr;
  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=needle_len;
  MOVQ          CX,  R8                   //;A83664AE scratch2 := needle_len          ;R8=scratch2; CX=needle_len;
  SHLQ          $4,  R8                   //;EDF8DF09 scratch2 <<= 4                  ;R8=scratch2;
  LEAQ          4(R14)(R8*1),R15          //;1EF280F2 wildcard_ptr := needle_ptr + scratch2 + 4;R15=wildcard_ptr; R14=needle_ptr; R8=scratch2;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $2,  Z3,  Z26, K1,  K1    //;B73A4F83 K1 &= (scratch_Z26<=str_len)    ;K1=lane_active; Z26=scratch_Z26; Z3=str_len; 2=LessEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  ADDQ          $4,  R14                  //;7B0665F3 needle_ptr += 4                 ;R14=needle_ptr;
//; load constants
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;

loop:
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_len != 0); empty data are dead lanes;K1=lane_active; Z3=str_len;
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; select next UTF8 byte sequence
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
//; compare data with needle for 1 UTF8 byte sequence
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K3   //;345D0BF3 K3 := K1 & (data_msg==[needle_ptr]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  4(R14),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[needle_ptr+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  8(R14),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[needle_ptr+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  VPCMPD.BCST   $0,  12(R14),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[needle_ptr+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  ADDQ          $16, R14                  //;5D0D7365 needle_ptr += 16                ;R14=needle_ptr;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KMOVW         (R15),K4                  //;3CD32160 load wildcard                   ;K4=alt2_match; R15=wildcard_ptr;
  ADDQ          $2,  R15                  //;B9CC45F2 wildcard_ptr += 2               ;R15=wildcard_ptr;
  KANDW         K4,  K1,  K4              //;4FE420F5 alt2_match &= lane_active       ;K4=alt2_match; K1=lane_active;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K6              //;AAF6ED91 alt4_match |= tmp_mask          ;K6=alt4_match; K3=tmp_mask;
  KORW          K6,  K4,  K1              //;4FE420F5 lane_active := alt2_match | alt4_match;K1=lane_active; K4=alt2_match; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; advance to the next rune
  VPADDD        Z7,  Z2,  K1,  Z2         //;DFE8D20B str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;24E04BE7 str_len -= n_bytes_data         ;Z3=str_len; K1=lane_active; Z7=n_bytes_data;
  DECL          CX                        //;A99E9290 needle_len--                    ;CX=needle_len;
  JG            loop                      //;80013DFA jump if greater ((ZF = 0) and (SF = OF));

next:
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;E555E77C K1 &= (str_len==0)              ;K1=lane_active; Z3=str_len;
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcEqPatternUTF8Ci

//; #region bcContainsPatternCs
//
// s[0].k[1] = contains_pattern_cs(slice[2], dict[3]).k[4]
TEXT bcContainsPatternCs(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141                                 ;CX=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle and wildcard pointers
  ADDQ          $4,  R14                  //;B1A93760 needle_ptr1 += 4                ;R14=needle_ptr1;
  MOVQ          R14, R15                  //;1B1C0A5D wildcard_ptr1 := needle_ptr1    ;R15=wildcard_ptr1; R14=needle_ptr1;
  ADDQ          CX,  R15                  //;FC062E0E wildcard_ptr1 += needle_len1    ;R15=wildcard_ptr1; CX=needle_len1;
//; load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTB  (R14),Z9                  //;54CB0C41 bcst first char from needle     ;Z9=char1_needle; R14=needle_ptr1;

//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 if not cleared 170C9C8F will give unncessary matches;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
  VPCMPB        $0,  Z9,  Z8,  K3         //;170C9C8F K3 := (data==char1_needle)      ;K3=tmp_mask; Z8=data; Z9=char1_needle; 0=Eq;
  KTESTQ        K3,  K3                   //;AD0284F2 found any char1_needle?         ;K3=tmp_mask;
  JNZ           scan_found_something      //;4645455C yes, found something!; jump if not zero (ZF = 0);
  VPSUBD        Z20, Z3,  K2,  Z3         //;BF319EDC data_len1 -= 4                  ;Z3=data_len1; K2=lane_todo; Z20=4;
  VPADDD        Z20, Z2,  K2,  Z2         //;B5CB47F3 data_off1 += 4                  ;Z2=data_off1; K2=lane_todo; Z20=4;
scan_found_something_ret:                 //;2CADDCF3 reentry code path with found char1_needle
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;358E0E6F K2 &= (0<data_len1)             ;K2=lane_todo; Z11=0; Z3=data_len1; 1=LessThen;
  KTESTW        K2,  K2                   //;854AE89B any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;5BB6DC39 yes, then continue scanning; jump if not zero (ZF = 0);
  JMP           scan_done                 //;58F52ADF                                 ;

scan_found_something:
//; calculate skip_count in Z14
  VPMOVM2B      K3,  Z27                  //;E74FDEBD promote 64x bit to 64x byte     ;Z27=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z27, Z27             //;4F265F03 reverse byte order              ;Z27=skip_count; Z22=bswap32;
  VPLZCNTD      Z27, Z27                  //;72202F9A count leading zeros             ;Z27=skip_count;
  VPSRLD.Z      $3,  Z27, K2,  Z27        //;6DC91432 divide by 8 yields skip_count   ;Z27=skip_count; K2=lane_todo;
//; advance
  VPCMPD        $4,  Z20, Z27, K2,  K3    //;2846C84C K3 := K2 & (skip_count!=4)      ;K3=tmp_mask; K2=lane_todo; Z27=skip_count; Z20=4; 4=NotEqual;
  VPADDD        Z27, Z2,  K2,  Z2         //;63F1BACC data_off1 += skip_count         ;Z2=data_off1; K2=lane_todo; Z27=skip_count;
  VPSUBD        Z27, Z3,  K2,  Z3         //;8F7F978F data_len1 -= skip_count         ;Z3=data_len1; K2=lane_todo; Z27=skip_count;
//; update masks with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
  JMP           scan_found_something_ret  //;169A16C1 jump back to where we came from ;

scan_done:                                //;50D019A9 check if the ran out of data
  VPCMPD        $5,  Z6,  Z3,  K4,  K4    //;FA18B497 K4 &= (data_len1>=needle_len1)  ;K4=lane_selected; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VMOVDQA32     Z2,  Z24                  //;835001A1 data_off2 := data_off1          ;Z24=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  VMOVDQA32     Z6,  Z25                  //;E323C56F needle_len2 := needle_len1      ;Z25=needle_len2; Z6=needle_len1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  MOVQ          R15, BX                   //;76E7C8F2 wildcard_ptr2 := wildcard_ptr1  ;BX=wildcard_ptr2; R15=wildcard_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=data_off2;
//; load needle and apply tail masks
  VPMINSD       Z20, Z25, Z13             //;7D091557 adv_needle := min(needle_len2, 4);Z13=adv_needle; Z25=needle_len2; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z8,  Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27;
  VPANDD.BCST   (R13),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R13=needle_ptr2;
  VPBROADCASTD  (BX),Z27                  //;911BC5F9 load wildcard                   ;Z27=wildcard; BX=wildcard_ptr2;
//; test if a unicode code-points matches a wildcard
  VPANDD.Z      Z27, Z8,  K4,  Z26        //;7A4C6E61 scratch_Z26 := data & wildcard  ;Z26=scratch_Z26; K4=lane_selected; Z8=data; Z27=wildcard;
  VPMOVB2M      Z26, K3                   //;4B555080                                 ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;14D8E8C5 ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           unicode_match             //;C94028E8 jump if not zero (ZF = 0)       ;
//; compare data with needle
//;    Z27|Z8 |Z28  => Z28
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      0
//;     1 | 0 | 0      1
//;     1 | 0 | 1      0
//;     1 | 1 | 0      0
//;     1 | 1 | 1      0
  VPTERNLOGD    $0b00010100,Z27, Z8,  Z28 //;A64A5655 compute masked equality         ;Z28=needle; Z8=data; Z27=wildcard;
  VPCMPD        $0,  Z11, Z28, K4,  K4    //;D2F6A32B K4 &= (needle==0)               ;K4=lane_selected; Z28=needle; Z11=0; 0=Eq;
//; advance
  VPADDD        Z13, Z24, K4,  Z24        //;3371623C data_off2 += adv_needle         ;Z24=data_off2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z5,  K4,  Z5         //;9905C7C3 data_len2 -= adv_needle         ;Z5=data_len2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z25, K4,  Z25        //;5A8AB52E needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z13=adv_needle;
  ADDQ          $4,  R13                  //;5D0D7365 needle_ptr2 += 4                ;R13=needle_ptr2;
  ADDQ          $4,  BX                   //;8A43B166 wildcard_ptr2 += 4              ;BX=wildcard_ptr2;
unicode_match_ret:
//; check needle_loop conditions
  VPCMPD        $1,  Z25, Z11, K3         //;D0432359 K3 := (0<needle_len2)           ;K3=tmp_mask; Z11=0; Z25=needle_len2; 1=LessThen;
  VPCMPD        $2,  Z5,  Z11, K4,  K4    //;1F3D9F3F K4 &= (0<=data_len2)            ;K4=lane_selected; Z11=0; Z5=data_len2; 2=LessEq;
  KTESTW        K4,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K4=lane_selected;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z24, K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z24=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; advance the scan-offsets with 1 and if there are any lanes still todo, scan those
  VPADDD        Z10, Z2,  K2,  Z2         //;65A7A575 data_off1++                     ;Z2=data_off1; K2=lane_todo; Z10=1;
  VPSUBD        Z10, Z3,  K2,  Z3         //;F7731EA7 data_len1--                     ;Z3=data_len1; K2=lane_todo; Z10=1;
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)

unicode_match:                            //;B1B3AECE a wildcard has matched with a unicode code-point
//; with the wildcard mask, get the number of bytes BEFORE the first wildcard (0, 1, 2, 3)
//; at that position get the number of bytes of the code-point, add these two numbers
//; calculate advance in Z26
  VPSHUFB       Z22, Z27, Z26             //;3F4659FF reverse byte order              ;Z26=scratch_Z26; Z27=wildcard; Z22=bswap32;
  VPLZCNTD      Z26, Z27                  //;7DB21322 count leading zeros             ;Z27=zero_count; Z26=scratch_Z26;
  VPSRLD        $3,  Z27, Z19             //;D7D76764 divide by 8 yields advance      ;Z19=advance; Z27=zero_count;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;E6FE9C45 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPSRLVD       Z27, Z26, Z26             //;F99F8396 scratch_Z26 >>= zero_count      ;Z26=scratch_Z26; Z27=zero_count;
  VPERMD        Z21, Z26, Z26             //;DE232505 get n_bytes_code_point          ;Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z19, Z26, Z13             //;D11A93E8 adv_data := scratch_Z26 + advance;Z13=adv_data; Z26=scratch_Z26; Z19=advance;
//; we are only going to test the number of bytes BEFORE the first wildcard
  VPERMD        Z18, Z19, Z26             //;9A394869 get tail_mask data              ;Z26=scratch_Z26; Z19=advance; Z18=tail_mask_data;
  VPANDD        Z26, Z8,  Z8              //;C5A7FCBA data &= scratch_Z26             ;Z8=data; Z26=scratch_Z26;
  VPANDD        Z26, Z28, Z28             //;51343840 needle &= scratch_Z26           ;Z28=needle; Z26=scratch_Z26;
  VPCMPD        $0,  Z8,  Z28, K4,  K4    //;13A45EF9 K4 &= (needle==data)            ;K4=lane_selected; Z28=needle; Z8=data; 0=Eq;
//; advance
  VPADDD        Z10, Z19, Z19             //;BFB2C3DE adv_needle := advance + 1       ;Z19=adv_needle; Z19=advance; Z10=1;
  VPSUBD        Z19, Z25, K4,  Z25        //;B3CDC39C needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z19=adv_needle;
  VPADDD        Z13, Z24, K4,  Z24        //;AE5AA4CF data_off2 += adv_data           ;Z24=data_off2; K4=lane_selected; Z13=adv_data;
  VPSUBD        Z13, Z5,  K4,  Z5         //;DBC41158 data_len2 -= adv_data           ;Z5=data_len2; K4=lane_selected; Z13=adv_data;
  VMOVD         X19, R8                   //;B10A9DE5 extract GPR adv_needle          ;R8=scratch; Z19=adv_needle;
  ADDQ          R8,  R13                  //;34EA6A74 needle_ptr2 += scratch          ;R13=needle_ptr2; R8=scratch;
  ADDQ          R8,  BX                   //;8FD33A77 wildcard_ptr2 += scratch        ;BX=wildcard_ptr2; R8=scratch;
  JMP           unicode_match_ret         //;D24820C1                                 ;
//; #endregion bcContainsPatternCs

//; #region bcContainsPatternCi
//
// s[0].k[1] = contains_pattern_ci(slice[2], dict[3]).k[4]
TEXT bcContainsPatternCi(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141                                 ;CX=needle_len1; R14=needle_ptr1;
//; restrict lanes to allow fast bail-out
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; init needle and wildcard pointers
  ADDQ          $4,  R14                  //;B1A93760 needle_ptr1 += 4                ;R14=needle_ptr1;
  MOVQ          R14, R15                  //;1B1C0A5D wildcard_ptr1 := needle_ptr1    ;R15=wildcard_ptr1; R14=needle_ptr1;
  ADDQ          CX,  R15                  //;FC062E0E wildcard_ptr1 += needle_len1    ;R15=wildcard_ptr1; CX=needle_len1;
//; load constants
  VPBROADCASTB  CONSTB_32(),Z15           //;5B8F2908 load constant 0b00100000        ;Z15=c_0b00100000;
  VPBROADCASTB  CONSTB_97(),Z16           //;5D5B0014 load constant ASCII a           ;Z16=char_a;
  VPBROADCASTB  CONSTB_122(),Z17          //;8E2ED824 load constant ASCII z           ;Z17=char_z;
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTB  (R14),Z9                  //;54CB0C41 bcst first char from needle     ;Z9=char1_needle; R14=needle_ptr1;

//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 if not cleared 170C9C8F will give unncessary matches;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch_Z26; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch_Z26; Z8=data; Z15=c_0b00100000;
  VPCMPB        $0,  Z9,  Z26, K3         //;170C9C8F K3 := (scratch_Z26==char1_needle);K3=tmp_mask; Z26=scratch_Z26; Z9=char1_needle; 0=Eq;
  KTESTQ        K3,  K3                   //;AD0284F2 found any char1_needle?         ;K3=tmp_mask;
  JNZ           scan_found_something      //;4645455C yes, found something!; jump if not zero (ZF = 0);
  VPSUBD        Z20, Z3,  K2,  Z3         //;BF319EDC data_len1 -= 4                  ;Z3=data_len1; K2=lane_todo; Z20=4;
  VPADDD        Z20, Z2,  K2,  Z2         //;B5CB47F3 data_off1 += 4                  ;Z2=data_off1; K2=lane_todo; Z20=4;
scan_found_something_ret:                 //;2CADDCF3 reentry code path with found char1_needle
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;358E0E6F K2 &= (0<data_len1)             ;K2=lane_todo; Z11=0; Z3=data_len1; 1=LessThen;
  KTESTW        K2,  K2                   //;854AE89B any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;5BB6DC39 yes, then continue scanning; jump if not zero (ZF = 0);
  JMP           scan_done                 //;58F52ADF                                 ;

scan_found_something:
//; calculate skip_count in Z14
  VPMOVM2B      K3,  Z27                  //;E74FDEBD promote 64x bit to 64x byte     ;Z27=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z27, Z27             //;4F265F03 reverse byte order              ;Z27=skip_count; Z22=bswap32;
  VPLZCNTD      Z27, Z27                  //;72202F9A count leading zeros             ;Z27=skip_count;
  VPSRLD.Z      $3,  Z27, K2,  Z27        //;6DC91432 divide by 8 yields skip_count   ;Z27=skip_count; K2=lane_todo;
//; advance
  VPCMPD        $4,  Z20, Z27, K2,  K3    //;2846C84C K3 := K2 & (skip_count!=4)      ;K3=tmp_mask; K2=lane_todo; Z27=skip_count; Z20=4; 4=NotEqual;
  VPADDD        Z27, Z2,  K2,  Z2         //;63F1BACC data_off1 += skip_count         ;Z2=data_off1; K2=lane_todo; Z27=skip_count;
  VPSUBD        Z27, Z3,  K2,  Z3         //;8F7F978F data_len1 -= skip_count         ;Z3=data_len1; K2=lane_todo; Z27=skip_count;
//; update masks with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
  JMP           scan_found_something_ret  //;169A16C1 jump back to where we came from ;

scan_done:                                //;50D019A9 check if the ran out of data
  VPCMPD        $5,  Z6,  Z3,  K4,  K4    //;FA18B497 K4 &= (data_len1>=needle_len1)  ;K4=lane_selected; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VMOVDQA32     Z2,  Z24                  //;835001A1 data_off2 := data_off1          ;Z24=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  VMOVDQA32     Z6,  Z25                  //;E323C56F needle_len2 := needle_len1      ;Z25=needle_len2; Z6=needle_len1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  MOVQ          R15, BX                   //;76E7C8F2 wildcard_ptr2 := wildcard_ptr1  ;BX=wildcard_ptr2; R15=wildcard_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data := 0                       ;Z8=data;
  VPGATHERDD    (VIRT_BASE)(Z24*1),K3, Z8 //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z24=data_off2;
//; str_to_upper: IN zmm8; OUT zmm26
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data>=char_a)            ;K3=tmp_mask; Z8=data; Z16=char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data<=char_z)            ;K3=tmp_mask; Z8=data; Z17=char_z; 2=LessEq;
//; Z15 is 64 bytes with 0b00100000
//;    Z15|Z8 |Z26 => Z26
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      1
//;     1 | 0 | 0      0
//;     1 | 0 | 1      0
//;     1 | 1 | 0      1
//;     1 | 1 | 1      0     <= change from lower to upper
  VPMOVM2B      K3,  Z26                  //;ADC21F45 mask with selected chars        ;Z26=scratch_Z26; K3=tmp_mask;
  VPTERNLOGD    $0b01001100,Z15, Z8,  Z26 //;1BB96D97                                 ;Z26=scratch_Z26; Z8=data; Z15=c_0b00100000;
//; load needle and apply tail masks
  VPMINSD       Z20, Z25, Z13             //;7D091557 adv_needle := min(needle_len2, 4);Z13=adv_needle; Z25=needle_len2; Z20=4;
  VPERMD        Z18, Z13, Z27             //;B7D1A978 get tail_mask (needle)          ;Z27=scratch_Z27; Z13=adv_needle; Z18=tail_mask_data;
  VPANDD        Z26, Z27, Z8              //;5669D792 remove tail from data           ;Z8=data; Z27=scratch_Z27; Z26=scratch_Z26;
  VPANDD.BCST   (R13),Z27, Z28            //;C9F5F9B2 load needle and remove tail     ;Z28=needle; Z27=scratch_Z27; R13=needle_ptr2;
  VPBROADCASTD  (BX),Z27                  //;911BC5F9 load wildcard                   ;Z27=wildcard; BX=wildcard_ptr2;
//; test if a unicode code-points matches a wildcard
  VPANDD.Z      Z27, Z8,  K4,  Z26        //;7A4C6E61 scratch_Z26 := data & wildcard  ;Z26=scratch_Z26; K4=lane_selected; Z8=data; Z27=wildcard;
  VPMOVB2M      Z26, K3                   //;4B555080                                 ;K3=tmp_mask; Z26=scratch_Z26;
  KTESTQ        K3,  K3                   //;14D8E8C5 ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           unicode_match             //;C94028E8 jump if not zero (ZF = 0)       ;
//; compare data with needle
//;    Z27|Z8 |Z28  => Z28
//;     0 | 0 | 0      0
//;     0 | 0 | 1      0
//;     0 | 1 | 0      1
//;     0 | 1 | 1      0
//;     1 | 0 | 0      1
//;     1 | 0 | 1      0
//;     1 | 1 | 0      0
//;     1 | 1 | 1      0
  VPTERNLOGD    $0b00010100,Z27, Z8,  Z28 //;A64A5655 compute masked equality         ;Z28=needle; Z8=data; Z27=wildcard;
  VPCMPD        $0,  Z11, Z28, K4,  K4    //;D2F6A32B K4 &= (needle==0)               ;K4=lane_selected; Z28=needle; Z11=0; 0=Eq;
//; advance
  VPADDD        Z13, Z24, K4,  Z24        //;3371623C data_off2 += adv_needle         ;Z24=data_off2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z5,  K4,  Z5         //;9905C7C3 data_len2 -= adv_needle         ;Z5=data_len2; K4=lane_selected; Z13=adv_needle;
  VPSUBD        Z13, Z25, K4,  Z25        //;5A8AB52E needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z13=adv_needle;
  ADDQ          $4,  R13                  //;5D0D7365 needle_ptr2 += 4                ;R13=needle_ptr2;
  ADDQ          $4,  BX                   //;8A43B166 wildcard_ptr2 += 4              ;BX=wildcard_ptr2;
unicode_match_ret:
//; check needle_loop conditions
  VPCMPD        $1,  Z25, Z11, K3         //;D0432359 K3 := (0<needle_len2)           ;K3=tmp_mask; Z11=0; Z25=needle_len2; 1=LessThen;
  VPCMPD        $2,  Z5,  Z11, K4,  K4    //;1F3D9F3F K4 &= (0<=data_len2)            ;K4=lane_selected; Z11=0; Z5=data_len2; 2=LessEq;
  KTESTW        K4,  K3                   //;88EB401D any lanes selected & elegible?  ;K3=tmp_mask; K4=lane_selected;
  JNZ           needle_loop               //;F1339C58 yes, then retry scanning; jump if not zero (ZF = 0);
//; update lanes
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z24, K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z24=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; advance the scan-offsets with 1 and if there are any lanes still todo, scan those
  VPADDD        Z10, Z2,  K2,  Z2         //;65A7A575 data_off1++                     ;Z2=data_off1; K2=lane_todo; Z10=1;
  VPSUBD        Z10, Z3,  K2,  Z3         //;F7731EA7 data_len1--                     ;Z3=data_len1; K2=lane_todo; Z10=1;
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)

unicode_match:                            //;B1B3AECE a wildcard has matched with a unicode code-point
//; with the wildcard mask, get the number of bytes BEFORE the first wildcard (0, 1, 2, 3)
//; at that position get the number of bytes of the code-point, add these two numbers
//; calculate advance in Z26
  VPSHUFB       Z22, Z27, Z26             //;3F4659FF reverse byte order              ;Z26=scratch_Z26; Z27=wildcard; Z22=bswap32;
  VPLZCNTD      Z26, Z27                  //;7DB21322 count leading zeros             ;Z27=zero_count; Z26=scratch_Z26;
  VPSRLD        $3,  Z27, Z19             //;D7D76764 divide by 8 yields advance      ;Z19=advance; Z27=zero_count;
//; get number of bytes in next code-point
  VPSRLD        $4,  Z8,  Z26             //;E6FE9C45 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPSRLVD       Z27, Z26, Z26             //;F99F8396 scratch_Z26 >>= zero_count      ;Z26=scratch_Z26; Z27=zero_count;
  VPERMD        Z21, Z26, Z26             //;DE232505 get n_bytes_code_point          ;Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z19, Z26, Z13             //;D11A93E8 adv_data := scratch_Z26 + advance;Z13=adv_data; Z26=scratch_Z26; Z19=advance;
//; we are only going to test the number of bytes BEFORE the first wildcard
  VPERMD        Z18, Z19, Z26             //;9A394869 get tail_mask data              ;Z26=scratch_Z26; Z19=advance; Z18=tail_mask_data;
  VPANDD        Z26, Z8,  Z8              //;C5A7FCBA data &= scratch_Z26             ;Z8=data; Z26=scratch_Z26;
  VPANDD        Z26, Z28, Z28             //;51343840 needle &= scratch_Z26           ;Z28=needle; Z26=scratch_Z26;
  VPCMPD        $0,  Z8,  Z28, K4,  K4    //;13A45EF9 K4 &= (needle==data)            ;K4=lane_selected; Z28=needle; Z8=data; 0=Eq;
//; advance
  VPADDD        Z10, Z19, Z19             //;BFB2C3DE adv_needle := advance + 1       ;Z19=adv_needle; Z19=advance; Z10=1;
  VPSUBD        Z19, Z25, K4,  Z25        //;B3CDC39C needle_len2 -= adv_needle       ;Z25=needle_len2; K4=lane_selected; Z19=adv_needle;
  VPADDD        Z13, Z24, K4,  Z24        //;AE5AA4CF data_off2 += adv_data           ;Z24=data_off2; K4=lane_selected; Z13=adv_data;
  VPSUBD        Z13, Z5,  K4,  Z5         //;DBC41158 data_len2 -= adv_data           ;Z5=data_len2; K4=lane_selected; Z13=adv_data;
  VMOVD         X19, R8                   //;B10A9DE5 extract GPR adv_needle          ;R8=scratch; Z19=adv_needle;
  ADDQ          R8,  R13                  //;34EA6A74 needle_ptr2 += scratch          ;R13=needle_ptr2; R8=scratch;
  ADDQ          R8,  BX                   //;8FD33A77 wildcard_ptr2 += scratch        ;BX=wildcard_ptr2; R8=scratch;
  JMP           unicode_match_ret         //;D24820C1                                 ;
//; #endregion bcContainsPatternCi

//; #region bcContainsPatternUTF8Ci
//
// s[0].k[1] = contains_pattern_utf8_ci(slice[2], dict[3]).k[4]
TEXT bcContainsPatternUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

//; load parameter
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr1; R14=needle_slice;
  MOVL          (R14),CX                  //;7DF7F141 load number of runes in needle  ;CX=needle_len1; R14=needle_ptr1;
  VPBROADCASTD  CX,  Z6                   //;A2E19B3A bcst needle_len                 ;Z6=needle_len1; CX=needle_len1;
//; restrict lanes to allow fast bail-out
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;EE56D9C0 K1 &= (data_len1>=needle_len1)  ;K1=lane_active; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
  MOVQ          CX,  R8                   //;A83664AE scratch := needle_len1          ;R8=scratch; CX=needle_len1;
  SHLQ          $4,  R8                   //;EDF8DF09 scratch <<= 4                   ;R8=scratch;
  LEAQ          6(R14)(R8*1),R15          //;1EF280F2 wildcard_ptr1 := needle_ptr1 + scratch + 6;R15=wildcard_ptr1; R14=needle_ptr1; R8=scratch;
//; load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;A0BC360A load constant bswap32           ;Z22=bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
//; init variables for scan_loop
  KMOVW         K1,  K2                   //;ECF269E6 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VPBROADCASTD  4(R14),Z12                //;54CB0C41 bcst alt1 from needle           ;Z12=char1_needle; R14=needle_ptr1;
  VPBROADCASTD  8(R14),Z13                //;EE0650F7 bcst alt2 from needle           ;Z13=char2_needle; R14=needle_ptr1;
  VPBROADCASTD  12(R14),Z14               //;879514D3 bcst alt3 from needle           ;Z14=char3_needle; R14=needle_ptr1;
  VPBROADCASTD  16(R14),Z15               //;A0DE73B0 bcst alt4 from needle           ;Z15=char4_needle; R14=needle_ptr1;
  ADDQ          $20, R14                  //;B6596F07 needle_ptr1 += 20               ;R14=needle_ptr1;
//; scan for the char1_needle in all lane_todo (K2) and accumulate in lane_selected (K4)
scan_start:
  KXORW         K4,  K4,  K4              //;8403FAC0 lane_selected := 0              ;K4=lane_selected;
scan_loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane_todo;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3, Z8  //;573D089A gather data from end            ;Z8=data; K3=tmp_mask; SI=data_ptr; Z2=data_off1;
//; get tail mask
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPERMD        Z21, Z26, Z27             //;68FECBA0 get n_bytes_data                ;Z27=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z27, Z26             //;6D661522 get tail_mask (data)            ;Z26=scratch_Z26; Z27=n_bytes_data; Z18=tail_mask_data;
//; advance data for lanes that are still todo
  VPADDD        Z27, Z2,  K2,  Z2         //;FBFD5F0A data_off1 += n_bytes_data       ;Z2=data_off1; K2=lane_todo; Z27=n_bytes_data;
  VPSUBD        Z27, Z3,  K2,  Z3         //;28FE1865 data_len1 -= n_bytes_data       ;Z3=data_len1; K2=lane_todo; Z27=n_bytes_data;
  VPANDD.Z      Z8,  Z26, K2,  Z8         //;6EEB77B0 mask data                       ;Z8=data; K2=lane_todo; Z26=scratch_Z26;
//; compare with 4 code-points
  VPCMPD        $0,  Z12, Z8,  K2,  K3    //;6D1378C0 K3 := K2 & (data==char1_needle) ;K3=tmp_mask; K2=lane_todo; Z8=data; Z12=char1_needle; 0=Eq;
  VPCMPD        $0,  Z13, Z8,  K2,  K5    //;FC651EE0 K5 := K2 & (data==char2_needle) ;K5=scratch_K5; K2=lane_todo; Z8=data; Z13=char2_needle; 0=Eq;
  VPCMPD        $0,  Z14, Z8,  K2,  K6    //;D9FE5534 K6 := K2 & (data==char3_needle) ;K6=scratch_K6; K2=lane_todo; Z8=data; Z14=char3_needle; 0=Eq;
  VPCMPD        $0,  Z15, Z8,  K2,  K0    //;D65545B1 K0 := K2 & (data==char4_needle) ;K0=scratch_K0; K2=lane_todo; Z8=data; Z15=char4_needle; 0=Eq;
  KORW          K3,  K5,  K3              //;A903EE01 tmp_mask |= scratch_K5          ;K3=tmp_mask; K5=scratch_K5;
  KORW          K3,  K6,  K3              //;31B58C73 tmp_mask |= scratch_K6          ;K3=tmp_mask; K6=scratch_K6;
  KORW          K3,  K0,  K3              //;A20F03C1 tmp_mask |= scratch_K0          ;K3=tmp_mask; K0=scratch_K0;
//; update lanes with the found stuff
  KANDNW        K2,  K3,  K2              //;1EA864B0 lane_todo &= ~tmp_mask          ;K2=lane_todo; K3=tmp_mask;
  KORW          K4,  K3,  K4              //;A69E7F6D lane_selected |= tmp_mask       ;K4=lane_selected; K3=tmp_mask;
//; determine if we need to continue searching for a matching start code-point
  VPCMPD        $5,  Z6,  Z3,  K2,  K2    //;30061EEA K2 &= (data_len1>=needle_len1)  ;K2=lane_todo; Z3=data_len1; Z6=needle_len1; 5=GreaterEq;
  KTESTW        K2,  K2                   //;67E2F74E any lanes still todo?           ;K2=lane_todo;
  JNZ           scan_loop                 //;677C9F60 jump if not zero (ZF = 0)       ;

//; we are done scanning for the first code-point; test if there are more
  VMOVDQA32     Z2,  Z4                   //;835001A1 data_off2 := data_off1          ;Z4=data_off2; Z2=data_off1;
  VMOVDQA32     Z3,  Z5                   //;31B4F894 data_len2 := data_len1          ;Z5=data_len2; Z3=data_len1;
  CMPL          CX,  $1                   //;2BBD50B8 more than 1 char in needle?     ;CX=needle_len1;
  JZ            needle_loop_done          //;AFDB3970 jump if zero (ZF = 1)           ;
//; test if there is there is sufficient data
  VPCMPD        $6,  Z11, Z3,  K4,  K4    //;C0CF3FA2 K4 &= (data_len1>0)             ;K4=lane_selected; Z3=data_len1; Z11=0; 6=Greater;
  KTESTW        K4,  K4                   //;9AA8B932 any lanes selected?             ;K4=lane_selected;
  JZ            next                      //;31580AD4 no, then exit; jump if zero (ZF = 1);
//; init variables for needle_loop
  VPSUBD        Z10, Z6,  Z7              //;75A315B7 needle_len2 := needle_len1 - 1  ;Z7=needle_len2; Z6=needle_len1; Z10=1;
  MOVQ          R14, R13                  //;92B163CC needle_ptr2 := needle_ptr1      ;R13=needle_ptr2; R14=needle_ptr1;
  MOVQ          R15, BX                   //;15EE7900 wildcard_ptr2 := wildcard_ptr1  ;BX=wildcard_ptr2; R15=wildcard_ptr1;
  KMOVW         K4,  K2                   //;226BBC9E lane_todo := lane_selected      ;K2=lane_todo; K4=lane_selected;
needle_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=lane_selected;
  VPGATHERDD    (VIRT_BASE)(Z4*1),K3, Z8  //;2CF4C294 gather data                     ;Z8=data; K3=tmp_mask; SI=data_ptr; Z4=data_off2;
//; advance needle
  VPSUBD        Z10, Z7,  K4,  Z7         //;CAFCD045 needle_len2--                   ;Z7=needle_len2; K4=lane_selected; Z10=1;
//; mask tail data
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data>>4          ;Z26=scratch_Z26; Z8=data;
  VPERMD        Z21, Z26, Z27             //;68FECBA0 get n_bytes_data                ;Z27=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z27, Z26             //;488C6CD8 get tail_mask (data)            ;Z26=scratch_Z26; Z27=n_bytes_data; Z18=tail_mask_data;
  VPANDD.Z      Z8,  Z26, K2,  Z8         //;E750B0E2 mask data                       ;Z8=data; K2=lane_todo; Z26=scratch_Z26;
  VPADDD        Z27, Z4,  K4,  Z4         //;4879FB55 data_off2 += n_bytes_data       ;Z4=data_off2; K4=lane_selected; Z27=n_bytes_data;
  VPSUBD        Z27, Z5,  K4,  Z5         //;77CC472F data_len2 -= n_bytes_data       ;Z5=data_len2; K4=lane_selected; Z27=n_bytes_data;
//; compare with 4 code-points
  VPCMPD.BCST   $0,  (R13),Z8,  K4,  K3   //;15105C54 K3 := K4 & (data==[needle_ptr2]);K3=tmp_mask; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  4(R13),Z8,  K4,  K5  //;9C52A210 K5 := K4 & (data==[needle_ptr2+4]);K5=scratch_K5; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  8(R13),Z8,  K4,  K6  //;A8B34D3C K6 := K4 & (data==[needle_ptr2+8]);K6=scratch_K6; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  VPCMPD.BCST   $0,  12(R13),Z8,  K4,  K0  //;343DD0F2 K0 := K4 & (data==[needle_ptr2+12]);K0=scratch_K0; K4=lane_selected; Z8=data; R13=needle_ptr2; 0=Eq;
  ADDQ          $16, R13                  //;5D0D7365 needle_ptr2 += 16               ;R13=needle_ptr2;
  KORW          K3,  K5,  K3              //;1125C81D tmp_mask |= scratch_K5          ;K3=tmp_mask; K5=scratch_K5;
  KMOVW         (BX),K5                   //;3CD32160 load wildcard                   ;K5=scratch_K5; BX=wildcard_ptr2;
  ADDQ          $2,  BX                   //;B9CC45F2 wildcard_ptr2 += 2              ;BX=wildcard_ptr2;
  KANDW         K5,  K4,  K5              //;F6A913D2 scratch_K5 &= lane_selected     ;K5=scratch_K5; K4=lane_selected;
  KORW          K3,  K6,  K3              //;DCB6AFAD tmp_mask |= scratch_K6          ;K3=tmp_mask; K6=scratch_K6;
  KORW          K3,  K0,  K4              //;65791592 lane_selected := scratch_K0 | tmp_mask;K4=lane_selected; K0=scratch_K0; K3=tmp_mask;
  KORW          K4,  K5,  K4              //;4FE420F5 lane_selected |= scratch_K5     ;K4=lane_selected; K5=scratch_K5;
//; advance to the next code-point
  VPCMPD        $6,  Z11, Z7,  K4,  K3    //;1185F8E2 K3 := K4 & (needle_len2>0)      ;K3=tmp_mask; K4=lane_selected; Z7=needle_len2; Z11=0; 6=Greater;
  VPCMPD        $2,  Z5,  Z7,  K4,  K4    //;7C50EFFB K4 &= (needle_len2<=data_len2)  ;K4=lane_selected; Z7=needle_len2; Z5=data_len2; 2=LessEq;
  KTESTW        K3,  K3                   //;98CB0D8B ZF := (K3==0); CF := 1          ;K3=tmp_mask;
  JNZ           needle_loop               //;E6065236 jump if not zero (ZF = 0)       ;
//; update lanes
needle_loop_done:
  KORW          K1,  K4,  K1              //;123EE41E lane_active |= lane_selected    ;K1=lane_active; K4=lane_selected;
  KANDNW        K2,  K4,  K2              //;32D9FB5F lane_todo &= ~lane_selected     ;K2=lane_todo; K4=lane_selected;
  VMOVDQA32     Z4,  K4,  Z2              //;5FA56037 data_off1 := data_off2          ;Z2=data_off1; K4=lane_selected; Z4=data_off2;
  VMOVDQA32     Z5,  K4,  Z3              //;8536472E data_len1 := data_len2          ;Z3=data_len1; K4=lane_selected; Z5=data_len2;
//; if there are any lanes still todo, scan those
  KTESTW        K2,  K2                   //;D8353CAF any lanes todo?                 ;K2=lane_todo;
  JNZ           scan_start                //;68ACA94C yes, then restart scanning; jump if not zero (ZF = 0);
next:
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + BC_DICT_SIZE)
//; #endregion bcContainsPatternUTF8Ci

//; #region bcIsSubnetOfIP4
//; Determine whether the string at Z2:Z3 is an IP address between the 4 provided bytewise min/max values
//; To prevent parsing of the IP string into an integer, every component is compared with a BCD min/max values
//
// k[0] = is_subnet_of_ip4(slice[1], dict[2]).k[3]
TEXT bcIsSubnetOfIP4(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  VPCMPD.BCST   $6,  CONSTD_6(),Z3,  K1,  K1  //;46C90536 K1 &= (str_len>6); only data larger than 6 is considered;K1=lane_active; Z3=str_len; 6=Greater;
  KTESTW        K1,  K1                   //;39066704 any lane still alive?           ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPBROADCASTB  CONSTD_0x2E(),Z21         //;487A092B load constant char_dot          ;Z21=char_dot;
  VPBROADCASTB  CONSTD_48(),Z16           //;62E46916 load constant char_0            ;Z16=char_0;
  VPBROADCASTB  CONSTB_57(),Z17           //;3D8FD928 load constant char_9            ;Z17=char_9;
  VPBROADCASTB  CONSTD_0x0F(),Z19         //;7E33FF0D load constant 0b00001111        ;Z19=0b00001111;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;

  MOVL          $3,  CX                   //;97E4B0BB compare the first 3 components of IP;CX=counter;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; load min and max values for the next component of the IP
  VPBROADCASTD  (R14),Z14                 //;85FE2A68 load min/max BCD values         ;Z14=ip_min; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPSRLD        $4,  Z14, Z15             //;7D831D80 ip_max := ip_min>>4             ;Z15=ip_max; Z14=ip_min;
  VPANDD        Z14, Z19, Z14             //;C8F73FDE ip_min &= 0b00001111            ;Z14=ip_min; Z19=0b00001111;
  VPANDD        Z15, Z19, Z15             //;E5C42B44 ip_max &= 0b00001111            ;Z15=ip_max; Z19=0b00001111;
//; find position of the dot in zmm13, and shift data zmm8 accordingly
  VPSHUFB       Z22, Z8,  Z8              //;4F265F03 reverse byte order              ;Z8=data_msg; Z22=constant_bswap32;
  VPCMPB        $0,  Z21, Z8,  K3         //;FDA19C68 K3 := (data_msg==char_dot)      ;K3=tmp_mask; Z8=data_msg; Z21=char_dot; 0=Eq;
  VPMOVM2B      K3,  Z13                  //;E74FDEBD promote 64x bit to 64x byte     ;Z13=dot_pos; K3=tmp_mask;
  VPLZCNTD      Z13, Z13                  //;72202F9A count leading zeros             ;Z13=dot_pos;
  VPSRLD        $3,  Z13, Z13             //;6DC91432 divide by 8 yields dot_pos      ;Z13=dot_pos;
  VPSUBD        Z13, Z20, Z26             //;BC43621D scratch_Z26 := 4 - dot_pos      ;Z26=scratch_Z26; Z20=4; Z13=dot_pos;
  VPSLLD        $3,  Z26, Z26             //;B533D91C times 8 gives bytes to shift    ;Z26=scratch_Z26;
  VPSRLVD       Z26, Z8,  Z8              //;6D4B355C adjust data                     ;Z8=data_msg; Z26=scratch_Z26;
//; component has length > 0 and < 4
  VPCMPD        $6,  Z11, Z13, K1,  K1    //;DD83DE79 K1 &= (dot_pos>0)               ;K1=lane_active; Z13=dot_pos; Z11=0; 6=Greater;
  VPCMPD        $1,  Z20, Z13, K1,  K1    //;164BDB97 K1 &= (dot_pos<4)               ;K1=lane_active; Z13=dot_pos; Z20=4; 1=LessThen;
//; component length to mask
  VPERMD        Z18, Z13, Z26             //;54D8DBC3 get tail_mask                   ;Z26=scratch_Z26; Z13=dot_pos; Z18=tail_mask_data;
  VPCMPB        $4,  Z26, Z11, K2         //;2F692D99 K2 := (0!=scratch_Z26)          ;K2=ip_component; Z11=0; Z26=scratch_Z26; 4=NotEqual;
//; create mask with numbers in k3
  VPCMPB        $5,  Z16, Z8,  K2,  K3    //;4C1DDB1A K3 := K2 & (data_msg>=char_0)   ;K3=tmp_mask; K2=ip_component; Z8=data_msg; Z16=char_0; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;90E4A177 K3 &= (data_msg<=char_9)        ;K3=tmp_mask; Z8=data_msg; Z17=char_9; 2=LessEq;
//; every character in component should be a number
  KXORQ         K3,  K2,  K3              //;32403AF0 tmp_mask ^= ip_component        ;K3=tmp_mask; K2=ip_component;
  VPMOVM2B      K3,  Z26                  //;E74FDEBD promote 64x bit to 64x byte     ;Z26=scratch_Z26; K3=tmp_mask;
  VPTESTNMD     Z26, Z26, K1,  K1         //;3014812A K1 &= (scratch_Z26==0)          ;K1=lane_active; Z26=scratch_Z26;
//; remaining length should be larger than 1
  VPCMPD        $6,  Z10, Z3,  K1,  K1    //;90F775EC K1 &= (str_len>1)               ;K1=lane_active; Z3=str_len; Z10=1; 6=Greater;
//; remove upper nibble from data zmm8 such that we can compare
  VPANDD        Z8,  Z19, Z8              //;C318FD02 data_msg &= 0b00001111          ;Z8=data_msg; Z19=0b00001111;
  VPCMPD        $5,  Z14, Z8,  K1,  K1    //;982B35DE K1 &= (data_msg>=ip_min)        ;K1=lane_active; Z8=data_msg; Z14=ip_min; 5=GreaterEq;
  VPCMPD        $2,  Z15, Z8,  K1,  K1    //;27BFCA91 K1 &= (data_msg<=ip_max)        ;K1=lane_active; Z8=data_msg; Z15=ip_max; 2=LessEq;
  KTESTW        K1,  K1                   //;50A3F3F1 any lane still alive?           ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;
//; update length and offset
  VPADDD        Z10, Z13, Z26             //;E66940CD scratch_Z26 := dot_pos + 1      ;Z26=scratch_Z26; Z13=dot_pos; Z10=1;
  VPSUBD        Z26, Z3,  K1,  Z3         //;E060A4BE str_len -= scratch_Z26          ;Z3=str_len; K1=lane_active; Z26=scratch_Z26;
  VPADDD        Z26, Z2,  K1,  Z2         //;8DD591CA str_start += scratch_Z26        ;Z2=str_start; K1=lane_active; Z26=scratch_Z26;

  DECL          CX                        //;18ACCC03 counter--                       ;CX=counter;
  JNZ           loop                      //;6929AA0C another component in IP present?; jump if not zero (ZF = 0);
//; load last component of IP address
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; load min and max values for the last component of the IP
  VPBROADCASTD  (R14),Z14                 //;85FE2A68 load min/max BCD values         ;Z14=ip_min; R14=needle_ptr;
  VPSRLD        $4,  Z14, Z15             //;7D831D80 ip_max := ip_min>>4             ;Z15=ip_max; Z14=ip_min;
  VPANDD        Z14, Z19, Z14             //;C8F73FDE ip_min &= 0b00001111            ;Z14=ip_min; Z19=0b00001111;
  VPANDD        Z15, Z19, Z15             //;E5C42B44 ip_max &= 0b00001111            ;Z15=ip_max; Z19=0b00001111;
//; find position of the dot in zmm13, and shift data zmm8 accordingly; but since there is no dot, use remaining bytes instead
  VPSHUFB       Z22, Z8,  Z8              //;4F265F03 reverse byte order              ;Z8=data_msg; Z22=constant_bswap32;
  VPSUBD        Z3,  Z20, Z26             //;BC43621D scratch_Z26 := 4 - str_len      ;Z26=scratch_Z26; Z20=4; Z3=str_len;
  VPSLLD        $3,  Z26, Z26             //;B533D91C times 8 gives bytes to shift    ;Z26=scratch_Z26;
  VPSRLVD       Z26, Z8,  Z8              //;6D4B355C adjust data                     ;Z8=data_msg; Z26=scratch_Z26;
//; component has length > 0 and < 4
  VPCMPD        $6,  Z11, Z3,  K1,  K1    //;C57C0EDA K1 &= (str_len>0)               ;K1=lane_active; Z3=str_len; Z11=0; 6=Greater;
  VPCMPD        $1,  Z20, Z3,  K1,  K1    //;F144CD35 K1 &= (str_len<4)               ;K1=lane_active; Z3=str_len; Z20=4; 1=LessThen;
//; component length to mask
  VPERMD        Z18, Z3,  Z26             //;716E0F84 get tail_mask                   ;Z26=scratch_Z26; Z3=str_len; Z18=tail_mask_data;
  VPCMPB        $4,  Z26, Z11, K2         //;BDEDF5D8 K2 := (0!=scratch_Z26)          ;K2=ip_component; Z11=0; Z26=scratch_Z26; 4=NotEqual;
//; create mask with numbers in k3
  VPCMPB        $5,  Z16, Z8,  K2,  K3    //;7812E56D K3 := K2 & (data_msg>=char_0)   ;K3=tmp_mask; K2=ip_component; Z8=data_msg; Z16=char_0; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;1A6DB788 K3 &= (data_msg<=char_9)        ;K3=tmp_mask; Z8=data_msg; Z17=char_9; 2=LessEq;
//; every character in component should be a number
  KXORQ         K3,  K2,  K3              //;4F099B70 tmp_mask ^= ip_component        ;K3=tmp_mask; K2=ip_component;
  VPMOVM2B      K3,  Z26                  //;E74FDEBD promote 64x bit to 64x byte     ;Z26=scratch_Z26; K3=tmp_mask;
  VPTESTNMD     Z26, Z26, K1,  K1         //;88DAC27C K1 &= (scratch_Z26==0)          ;K1=lane_active; Z26=scratch_Z26;
//; remove upper nibble from data zmm8 such that we can compare
  VPANDD        Z8,  Z19, Z8              //;C318FD02 data_msg &= 0b00001111          ;Z8=data_msg; Z19=0b00001111;
  VPCMPD        $5,  Z14, Z8,  K1,  K1    //;982B35DE K1 &= (data_msg>=ip_min)        ;K1=lane_active; Z8=data_msg; Z14=ip_min; 5=GreaterEq;
  VPCMPD        $2,  Z15, Z8,  K1,  K1    //;27BFCA91 K1 &= (data_msg<=ip_max)        ;K1=lane_active; Z8=data_msg; Z15=ip_max; 2=LessEq;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcIsSubnetOfIP4

//; #region bcDfaT6
//; DfaT6 Deterministic Finite Automaton (DFA) with 6-bits lookup-key and unicode wildcard
//
// k[0] = dfa_tiny6(slice[1], dict[2]).k[3]
TEXT bcDfaT6(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  KMOVW         192(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  194(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  198(R14),Z13              //;6891DA5E load accept state               ;Z13=accept_state; R14=needle_ptr;
//; load constants
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;6FD26853 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;BCCB1762 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;42917E87 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;8DFA55D5 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;A73B0AC3 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;21B4F359 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;1A66952A curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT6

//; #region bcDfaT7
//; DfaT7 Deterministic Finite Automaton (DFA) with 7-bits lookup-key and unicode wildcard
//
// k[0] = dfa_tiny7(slice[1], dict[2]).k[3]
TEXT bcDfaT7(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
  KMOVW         256(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  258(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  262(R14),Z13              //;6891DA5E load accept nodeID              ;Z13=accept_node; R14=needle_ptr;
//; load constants
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;6FD26853 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;BCCB1762 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;42917E87 merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_node==curr_state);K3=scratch; K2=lane_todo; Z13=accept_node; Z7=curr_state; 0=Eq;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;850DE385 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT7

//; #region bcDfaT8
//; DfaT8 Deterministic Finite Automaton (DFA) with 8-bits lookup-key
//
// k[0] = dfa_tiny8(slice[1], dict[2]).k[3]
TEXT bcDfaT8(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
  VMOVDQU32     256(R14),Z25              //;BE3AEA52 trans_table3 := [needle_ptr+256];Z25=trans_table3; R14=needle_ptr;
  VMOVDQU32     320(R14),Z26              //;C346A0C9 trans_table4 := [needle_ptr+320];Z26=trans_table4; R14=needle_ptr;
  KMOVW         384(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  386(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  390(R14),Z13              //;E6CE5A10 load accept nodeID              ;Z13=accept_node; R14=needle_ptr;
//; load constants
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPMOVB2M      Z8,  K3                   //;23A1705D extract non-ASCII mask          ;K3=scratch; Z8=data_msg;
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K3,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K3=scratch; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;850DE385 K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;6C217CFD K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;565C3FAD extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;BBDE408E K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;44C2F748 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;9B0EF476 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;BC53D421 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_node==curr_state);K3=scratch; K2=lane_todo; Z13=accept_node; Z7=curr_state; 0=Eq;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;BFA0A870 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT8

//; #region bcDfaT6Z
//; DfaT6Z Deterministic Finite Automaton (DFA) with 6-bits lookup-key and Zero length remaining assertion
//
// k[0] = dfa_tiny6Z(slice[1], dict[2]).k[3]
TEXT bcDfaT6Z(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  KMOVW         192(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  194(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  198(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
  KMOVQ         202(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
  VPMOVM2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
//; load constants
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  VPXORD        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPCMPD        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPCMPD        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPCMPD        $0,  Z16, Z3,  K5         //;91BAEA96 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPCMPD        $0,  Z20, Z3,  K5         //;2154FFD7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
  VPERMB        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
  VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
  VPMOVD2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KORW          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;89485A8A K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPCMPD        $0,  Z11, Z3,  K5         //;A23A5A84 K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMB        Z23, Z9,  Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT6Z

//; #region bcDfaT7Z
//; DfaT7Z Deterministic Finite Automaton (DFA) with 7-bits lookup-key and Zero length remaining assertion
//
// k[0] = dfa_tiny7Z(slice[1], dict[2]).k[3]
TEXT bcDfaT7Z(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
  KMOVW         256(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  258(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  262(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
  KMOVQ         266(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
  VPMOVM2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
//; load constants
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  VPXORD        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPCMPD        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPCMPD        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPCMPD        $0,  Z16, Z3,  K5         //;91BAEA96 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPCMPD        $0,  Z20, Z3,  K5         //;2154FFD7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
  VPERMB        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
  VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
  VPMOVD2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KORW          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;7C3C9240 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPCMPD        $0,  Z11, Z3,  K5         //;6843E9F0 K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPERMI2B      Z24, Z23, Z9              //;F0A7B6B3 map lookup_key to next_state    ;Z9=next_state; Z23=trans_table1; Z24=trans_table2;
  VMOVDQA32     Z9,  K4,  Z7              //;F3DF61B6 curr_state := next_state        ;Z7=curr_state; K4=char_valid; Z9=next_state;
  VMOVDQA32     Z9,  K5,  Z6              //;8EFED6E5 rlz_state := next_state         ;Z6=rlz_state; K5=lane_non-ASCII; Z9=next_state;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT7Z

//; #region bcDfaT8Z
//; DfaT8Z Deterministic Finite Automaton 8-bits with Zero length remaining assertion
//
// k[0] = dfa_tiny8Z(slice[1], dict[2]).k[3]
TEXT bcDfaT8Z(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))
  KTESTW        K1,  K1                   //;39066704 any lane alive?                 ;K1=lane_active;
  JZ            next                      //;47931531 no, exit; jump if zero (ZF = 1) ;
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  VMOVDQU32     (R14),Z21                 //;CAEE2FF0 char_table1 := [needle_ptr]     ;Z21=char_table1; R14=needle_ptr;
  VMOVDQU32     64(R14),Z22               //;E0639585 char_table2 := [needle_ptr+64]  ;Z22=char_table2; R14=needle_ptr;
  VMOVDQU32     128(R14),Z23              //;15D38369 trans_table1 := [needle_ptr+128];Z23=trans_table1; R14=needle_ptr;
  VMOVDQU32     192(R14),Z24              //;5DE9259D trans_table2 := [needle_ptr+192];Z24=trans_table2; R14=needle_ptr;
  VMOVDQU32     256(R14),Z25              //;BE3AEA52 trans_table3 := [needle_ptr+256];Z25=trans_table3; R14=needle_ptr;
  VMOVDQU32     320(R14),Z26              //;C346A0C9 trans_table4 := [needle_ptr+320];Z26=trans_table4; R14=needle_ptr;
  KMOVW         384(R14),K6               //;2C9E73B8 load wildcard enabled flag      ;K6=enabled_flag; R14=needle_ptr;
  VPBROADCASTD  386(R14),Z5               //;803E3CDF load wildcard char-group        ;Z5=wildcard; R14=needle_ptr;
  VPBROADCASTD  390(R14),Z13              //;E6CE5A10 load accept state               ;Z13=accept_state; R14=needle_ptr;
  KMOVQ         394(R14),K3               //;B925FEF8 load RLZ states                 ;K3=scratch; R14=needle_ptr;
  VPMOVM2B      K3,  Z14                  //;40FAB4CE promote 64x bit to 64x byte     ;Z14=rlz_states; K3=scratch;
//; load constants
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPADDD        Z10, Z10, Z15             //;92620230 constd_2 := 1 + 1               ;Z15=2; Z10=1;
  VPADDD        Z10, Z15, Z16             //;45FD27E2 constd_3 := 2 + 1               ;Z16=3; Z15=2; Z10=1;
  VPADDD        Z15, Z15, Z20             //;D9A45253 constd_4 := 2 + 2               ;Z20=4; Z15=2;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z18  //;B323211A load table_n_bytes_utf8         ;Z18=table_n_bytes_utf8;
//; init variables
  KMOVW         K1,  K2                   //;AE3AAD43 lane_todo := lane_active        ;K2=lane_todo; K1=lane_active;
  KXORW         K1,  K1,  K1              //;FA91A63F lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A start_state is state 1          ;Z7=curr_state; Z10=1;
main_loop:
  VPXORD        Z6,  Z6,  Z6              //;7B026700 rlz_state := 0                  ;Z6=rlz_state;
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPXORD        Z8,  Z8,  Z8              //;220F8650 clear stale non-ASCII content   ;Z8=data_msg;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
  VPMOVB2M      Z8,  K5                   //;385A4763 extract non-ASCII mask          ;K5=lane_non-ASCII; Z8=data_msg;
//; determine whether a lane has a non-ASCII code-point
  VPMOVM2B      K5,  Z12                  //;96C10C0D promote 64x bit to 64x byte     ;Z12=scratch_Z12; K5=lane_non-ASCII;
  VPCMPD        $4,  Z12, Z11, K2,  K3    //;92DE265B K3 := K2 & (0!=scratch_Z12); extract lanes with non-ASCII code-points;K3=scratch; K2=lane_todo; Z11=0; Z12=scratch_Z12; 4=NotEqual;
  KTESTW        K6,  K3                   //;BCE8C4F2 feature enabled and non-ASCII present?;K3=scratch; K6=enabled_flag;
  JNZ           skip_wildcard             //;10BF1BFB jump if not zero (ZF = 0)       ;
//; get char-groups
  VPERMI2B      Z22, Z21, Z8              //;872E1226 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQU8      Z11, K5,  Z8              //;2BDE3FA8 set non-ASCII to zero group     ;Z8=data_msg; K5=lane_non-ASCII; Z11=0;
//; handle 1st ASCII in data
  VPCMPD        $5,  Z10, Z3,  K4         //;89485A8A K4 := (str_len>=1)              ;K4=char_valid; Z3=str_len; Z10=1; 5=GreaterEq;
  VPCMPD        $0,  Z10, Z3,  K5         //;A23A5A84 K5 := (str_len==1)              ;K5=lane_non-ASCII; Z3=str_len; Z10=1; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  VMOVDQA32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
//; handle 2nd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z15, Z3,  K4         //;12B1EB36 K4 := (str_len>=2)              ;K4=char_valid; Z3=str_len; Z15=2; 5=GreaterEq;
  VPCMPD        $0,  Z15, Z3,  K5         //;47BF9EE9 K5 := (str_len==2)              ;K5=lane_non-ASCII; Z3=str_len; Z15=2; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  VMOVDQA32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
//; handle 3rd ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z16, Z3,  K4         //;6E26712A K4 := (str_len>=3)              ;K4=char_valid; Z3=str_len; Z16=3; 5=GreaterEq;
  VPCMPD        $0,  Z16, Z3,  K5         //;2154FFD7 K5 := (str_len==3)              ;K5=lane_non-ASCII; Z3=str_len; Z16=3; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  VMOVDQA32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
//; handle 4th ASCII in data
  VPSRLD        $8,  Z8,  Z8              //;838875D4 data_msg >>= 8                  ;Z8=data_msg;
  VPCMPD        $5,  Z20, Z3,  K4         //;CFBDCA00 K4 := (str_len>=4)              ;K4=char_valid; Z3=str_len; Z20=4; 5=GreaterEq;
  VPCMPD        $0,  Z20, Z3,  K5         //;95E6ECB7 K5 := (str_len==4)              ;K5=lane_non-ASCII; Z3=str_len; Z20=4; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  VMOVDQA32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
//; advance 4 bytes (= 4 code-points)
  VPADDD        Z20, Z2,  Z2              //;F381FC8B str_start += 4                  ;Z2=str_start; Z20=4;
  VPSUBD        Z20, Z3,  Z3              //;D71AFBB0 str_len -= 4                    ;Z3=str_len; Z20=4;
tail:
  VPCMPD        $0,  Z7,  Z13, K2,  K3    //;9A003B95 K3 := K2 & (accept_state==curr_state);K3=scratch; K2=lane_todo; Z13=accept_state; Z7=curr_state; 0=Eq;
  VPERMB        Z14, Z6,  Z12             //;F1661DA9 map RLZ states to 0xFF          ;Z12=scratch_Z12; Z6=rlz_state; Z14=rlz_states;
  VPSLLD.Z      $24, Z12, K2,  Z12        //;7352EFC4 scratch_Z12 <<= 24              ;Z12=scratch_Z12; K2=lane_todo;
  VPMOVD2M      Z12, K4                   //;6832FF1A extract RLZ mask                ;K4=char_valid; Z12=scratch_Z12;
  VPCMPD        $4,  Z7,  Z11, K2,  K2    //;C4336141 K2 &= (0!=curr_state)           ;K2=lane_todo; Z11=0; Z7=curr_state; 4=NotEqual;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;250BE13C K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KORW          K3,  K4,  K3              //;24142563 scratch |= char_valid           ;K3=scratch; K4=char_valid;
  KANDNW        K2,  K3,  K2              //;C9EB9B00 lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;63AD07E8 lane_active |= scratch          ;K1=lane_active; K3=scratch;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)

skip_wildcard:
//; instead of advancing 4 bytes we advance 1 code-point, and set all non-ascii code-points to the wildcard group
  VPSRLD        $4,  Z8,  Z12             //;FE5F1413 scratch_Z12 := data_msg>>4      ;Z12=scratch_Z12; Z8=data_msg;
  VPERMD        Z18, Z12, Z12             //;68FECBA0 get scratch_Z12                 ;Z12=scratch_Z12; Z18=table_n_bytes_utf8;
//; get char-groups
  VPCMPD        $4,  Z12, Z10, K2,  K3    //;411A6A38 K3 := K2 & (1!=scratch_Z12)     ;K3=scratch; K2=lane_todo; Z10=1; Z12=scratch_Z12; 4=NotEqual;
  VPERMI2B      Z22, Z21, Z8              //;285E91E6 map data to char_group          ;Z8=data_msg; Z21=char_table1; Z22=char_table2;
  VMOVDQA32     Z5,  K3,  Z8              //;D9B3425A set non-ASCII to wildcard group ;Z8=data_msg; K3=scratch; Z5=wildcard;
//; advance 1 code-point
  VPSUBD        Z12, Z3,  Z3              //;8575652C str_len -= scratch_Z12          ;Z3=str_len; Z12=scratch_Z12;
  VPADDD        Z12, Z2,  Z2              //;A7D2A209 str_start += scratch_Z12        ;Z2=str_start; Z12=scratch_Z12;
//; handle 1st code-point in data
  VPCMPD        $5,  Z11, Z3,  K4         //;A17DDD33 K4 := (str_len>=0)              ;K4=char_valid; Z3=str_len; Z11=0; 5=GreaterEq;
  VPCMPD        $0,  Z11, Z3,  K5         //;9AA6077F K5 := (str_len==0)              ;K5=lane_non-ASCII; Z3=str_len; Z11=0; 0=Eq;
  VPORD         Z8,  Z7,  Z9              //;C09CA74A merge char_group with curr_state into lookup_key;Z9=next_state; Z7=curr_state; Z8=data_msg;
  VPMOVB2M      Z9,  K3                   //;5ABFD6B8 extract sign for merging        ;K3=scratch; Z9=next_state;
  VMOVDQA32     Z9,  Z17                  //;9B3CF590 alt2_lut8 := next_state         ;Z17=alt2_lut8; Z9=next_state;
  VPERMI2B      Z26, Z25, Z9              //;53BE6E94 map lookup_key to next_state    ;Z9=next_state; Z25=trans_table3; Z26=trans_table4;
  VPERMI2B      Z24, Z23, Z17             //;C82BB72B map lookup_key to next_state    ;Z17=alt2_lut8; Z23=trans_table1; Z24=trans_table2;
  VMOVDQU8      Z9,  K3,  Z17             //;86B7DFF1 alt2_lut8 := next_state         ;Z17=alt2_lut8; K3=scratch; Z9=next_state;
  VMOVDQA32     Z17, K4,  Z7              //;F9049BA0 curr_state := alt2_lut8         ;Z7=curr_state; K4=char_valid; Z17=alt2_lut8;
  VMOVDQA32     Z17, K5,  Z6              //;948A0E75 rlz_state := alt2_lut8          ;Z6=rlz_state; K5=lane_non-ASCII; Z17=alt2_lut8;
  JMP           tail                      //;E21E4B3D                                 ;
//; #endregion bcDfaT8Z

//; #region bcDfaLZ
//; DfaLZ Deterministic Finite Automaton(DFA) with unlimited capacity (Large) and Remaining Length Zero Assertion (RLZA)
//
// k[0] = dfa_largeZ(slice[1], dict[2]).k[3]
TEXT bcDfaLZ(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_DICT_SLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R14), OUT(R8))
  BC_LOAD_SLICE_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))
  BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; load parameters
  MOVL          (R14),R8                  //;6AD2EA95 load n_states                   ;R8=n_states; R14=needle_ptr;
  ADDQ          $4,  R14                  //;3259F7B2 init state_offset               ;R14=needle_ptr;
//; test for special situation with DFA ->s0 -$> s1
  MOVL          R8,  R15                  //;97339D56 scratch := n_states             ;R15=scratch; R8=n_states;
  INCL          R15                       //;91D62E05 scratch++                       ;R15=scratch;
  JNZ           normal_operation          //;19338985 if result==0, then special situation; jump if not zero (ZF = 0);
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;29B38DE0 K1 &= (str_len==0)              ;K1=lane_active; Z3=str_len;
  JMP           next                      //;E5E69BC1                                 ;
normal_operation:
//; test if start state is accepting
  TESTL         R8,  R8                   //;637F12FC are there more than 0 states?   ;R8=n_states;
  JLE           next                      //;AEE3942A no, then there is only an accept state; jump if less or equal ((ZF = 1) or (SF neq OF));
//; load constants
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=4;
  VPBROADCASTD  CONSTD_0x3FFFFFFF(),Z17   //;EF9E72D4 load flags_mask                 ;Z17=flags_mask;
  VMOVDQU32     bswap32<>(SB),Z12         //;A0BC360A load constant bswap32           ;Z12=bswap32;
//; init variables before main loop
  VPCMPD        $1,  Z3,  Z11, K1,  K2    //;95727519 K2 := K1 & (0<str_len)          ;K2=lane_todo; K1=lane_active; Z11=0; Z3=str_len; 1=LessThen;
  KXORW         K1,  K1,  K1              //;C1A15D64 lane_active := 0                ;K1=lane_active;
  VMOVDQA32     Z10, Z7                   //;77B17C9A curr_state := 1                 ;Z7=curr_state; Z10=1;
main_loop:
  KMOVW         K2,  K3                   //;81412269 copy eligible lanes             ;K3=scratch; K2=lane_todo;
  VPGATHERDD    (VIRT_BASE)(Z2*1),K3,  Z8 //;E4967C89 gather data                     ;Z8=data_msg; K3=scratch; SI=msg_ptr; Z2=str_start;
//; init variables before states loop
  VPXORD        Z6,  Z6,  Z6              //;E4D2E400 next_state := 0                 ;Z6=next_state;
  VMOVDQA32     Z10, Z5                   //;A30F50D2 state_id := 1                   ;Z5=state_id; Z10=1;
  MOVL          R8,  CX                   //;B08178D1 init state_counter              ;CX=state_counter; R8=n_states;
  MOVQ          R14, R13                  //;F0D423D2 init state_offset               ;R13=state_offset; R14=needle_ptr;
//; get number of bytes in code-point
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 scratch_Z26 := data_msg>>4      ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z22             //;68FECBA0 get n_bytes_data                ;Z22=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
//; remove tail from data
  VPERMD        Z18, Z22, Z19             //;E5886CFE get tail_mask (data)            ;Z19=tail_mask; Z22=n_bytes_data; Z18=tail_mask_data;
  VPANDD.Z      Z8,  Z19, K2,  Z8         //;BF3EB085 mask data                       ;Z8=data_msg; K2=lane_todo; Z19=tail_mask;
//; transform data such that we can compare
  VPSHUFB       Z12, Z8,  Z8              //;964815FF toggle endiannes                ;Z8=data_msg; Z12=bswap32;
  VPSUBD        Z22, Z20, Z26             //;43F001E9 scratch_Z26 := 4 - n_bytes_data ;Z26=scratch_Z26; Z20=4; Z22=n_bytes_data;
  VPSLLD        $3,  Z26, Z26             //;22D27D9F scratch_Z26 <<= 3               ;Z26=scratch_Z26;
  VPSRLVD       Z26, Z8,  Z8              //;C0B21528 data_msg >>= scratch_Z26        ;Z8=data_msg; Z26=scratch_Z26;
//; advance one code-point
  VPSUBD        Z22, Z3,  Z3              //;CB5D370F str_len -= n_bytes_data         ;Z3=str_len; Z22=n_bytes_data;
  VPADDD        Z22, Z2,  Z2              //;DEE2A990 str_start += n_bytes_data       ;Z2=str_start; Z22=n_bytes_data;
loop_states:
  VPCMPD        $0,  Z7,  Z5,  K2,  K4    //;F998800A K4 := K2 & (state_id==curr_state);K4=state_matched; K2=lane_todo; Z5=state_id; Z7=curr_state; 0=Eq;
  VPADDD        Z5,  Z10, Z5              //;ED016003 state_id++                      ;Z5=state_id; Z10=1;
  KTESTW        K4,  K4                   //;43122CE8 did any states match?           ;K4=state_matched;
  JZ            skip_edges                //;6DE8E146 no, skip the loop with edges; jump if zero (ZF = 1);
  MOVL          4(R13),DX                 //;CA7C9CE3 load number of edges            ;DX=edge_counter; R13=state_offset;
  ADDQ          $8,  R13                  //;729CC51F state_offset += 8               ;R13=state_offset;
loop_edges:
  VPCMPUD.BCST  $5,  (R13),Z8,  K4,  K3   //;510F046E K3 := K4 & (data_msg>=[state_offset]);K3=trans_matched; K4=state_matched; Z8=data_msg; R13=state_offset; 5=GreaterEq;
  VPCMPUD.BCST  $2,  4(R13),Z8,  K3,  K3  //;59D7E2CF K3 &= (data_msg<=[state_offset+4]);K3=scratch; Z8=data_msg; R13=state_offset; 2=LessEq;
  VPBROADCASTD  8(R13),K3,  Z6            //;789252A5 update next_state               ;Z6=next_state; K3=trans_matched; R13=state_offset;
  ADDQ          $12, R13                  //;9997E3C9 state_offset += 12              ;R13=state_offset;
  DECL          DX                        //;F5ED8DBE edge_counter--                  ;DX=edge_counter;
  JNZ           loop_edges                //;314C4D30 jump if not zero (ZF = 0)       ;
  JMP           loop_edges_done           //;D662BEEB                                 ;
skip_edges:
  MOVL          (R13),DX                  //;33839A60 load total bytes of edges       ;DX=edge_counter; R13=state_offset;
  ADDQ          DX,  R13                  //;2E22DACA state_offset += edge_counter    ;R13=state_offset; DX=edge_counter;
loop_edges_done:
  DECL          CX                        //;D33A44D5 state_counter--                 ;CX=state_counter;
  JNZ           loop_states               //;CFB42829 jump if not zero (ZF = 0)       ;

//; test the RLZ condition
  VPMOVD2M      Z6,  K3                   //;E2246D80 retrieve RLZ bit                ;K3=scratch; Z6=next_state;
  VPCMPD        $0,  Z3,  Z11, K3,  K5    //;CF75D163 K5 := K3 & (0==str_len)         ;K5=rlz_condition; K3=scratch; Z11=0; Z3=str_len; 0=Eq;
//; test accept contition
  VPSLLD        $1,  Z6,  Z26             //;185F151B shift accept-bit into most sig pos;Z26=scratch_Z26; Z6=next_state;
  VPMOVD2M      Z26, K3                   //;38627E18 retrieve accept bit             ;K3=scratch; Z26=scratch_Z26;
//; update lane_todo and lane_active
  KORW          K5,  K3,  K3              //;D1E8D8B6 scratch |= rlz_condition        ;K3=scratch; K5=rlz_condition;
  KANDNW        K2,  K3,  K2              //;1C70ECDA lane_todo &= ~scratch           ;K2=lane_todo; K3=scratch;
  KORW          K1,  K3,  K1              //;E320A3B2 lane_active |= scratch          ;K1=lane_active; K3=scratch;
//; determine if there is more data to process
  VPCMPUD       $4,  Z6,  Z11, K2,  K2    //;7D6781E6 K2 &= (0!=next_state)           ;K2=lane_todo; Z11=0; Z6=next_state; 4=NotEqual;
  VPANDD        Z6,  Z17, Z7              //;17DDB755 curr_state := flags_mask & next_state;Z7=curr_state; Z17=flags_mask; Z6=next_state;
  VPCMPD        $1,  Z3,  Z11, K2,  K2    //;7668811F K2 &= (0<str_len)               ;K2=lane_todo; Z11=0; Z3=str_len; 1=LessThen;
  KTESTW        K2,  K2                   //;3D96F6AD any lane still todo?            ;K2=lane_todo;
  JNZ           main_loop                 //;274B80A2 jump if not zero (ZF = 0)       ;

next:
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_DICT_SIZE)
//; #endregion bcDfaLZ

//; #endregion string methods

// tDigest
// --------------------------------------------------

#include "evalbc_tdigest.h"

// LOWER/UPPER functions
// --------------------------------------------------

#include "evalbc_strcase.h"

// APPROX_COUNT_DISTINCT
// --------------------------------------------------

#include "evalbc_approxcount.h"

// POW(x, intpow) implementation

// BC_POWINT generates specialisation for either for floats
// or ints.
//
// Algorithm:
//
// res := 1
// pow := x
// while (exponent != 0) {
//    if exponent & 1 != 0 {
//        res = res * pow
//    }
//    exponent = exponent >> 1
//    pow = pow * pow
// }
//
// Input:
// * Z2:Z3      - unboxed int64 or float64
// * K1, K2     - active masks
// * one        - ZMM register populated with uint64(1) or float64(1.0)
// * VMUL       - AVX512 instruction that multiplies (VMULPD or VPMULUDQ)
// * exponent   - GPR register containing an *unsigned* exponent
// * tmp        - temporary GPR
//
// Clobbers:
// * K3, K4, K5
#define BC_POWINT(one, VMUL, exponent, tmp)                                    \
    VMOVDQA64   one, Z20    /* Z20 - POWINT(Z2, exponent) */                   \
    VMOVDQA64   one, Z21    /* Z21 - POWINT(Z3, exponent) */                   \
    VMOVDQA64   Z2, Z22                                                        \
    VMOVDQA64   Z3, Z23                                                        \
loop:                                                                          \
    TESTQ   exponent, exponent                                                 \
    JZ      zero                                                               \
                                                                               \
    SHRQ    $1, exponent    /* CF = LSB(exponent), exponent = exponent >> 1 */ \
    SBBQ    tmp, tmp        /* populate CF */                                  \
    KMOVQ   tmp, K3                                                            \
                                                                               \
    /* exponent & 1 != 0 */                                                    \
    KANDQ   K3, K2, K4                                                         \
    KANDQ   K3, K1, K3                                                         \
                                                                               \
    /* res = pow * res */                                                      \
    VMUL    Z20, Z22, K3, Z20                                                  \
    VMUL    Z21, Z23, K4, Z21                                                  \
                                                                               \
    /* pow = pow * pow */                                                      \
    VMUL    Z22, Z22, K1, Z22                                                  \
    VMUL    Z23, Z23, K2, Z23                                                  \
    JMP     loop                                                               \
zero:                                                                          \
    VMOVDQA64   Z20, Z2                                                        \
    VMOVDQA64   Z21, Z3


// f64[0] = powuint.f64(f64[1], i64@imm[2]).k[3]
TEXT bcpowuintf64(SB), NOSPLIT|NOFRAME, $0
    BC_UNPACK_SLOT(BC_SLOT_SIZE, OUT(DX))
    BC_UNPACK_RU64(BC_SLOT_SIZE*2, OUT(BX))
    BC_UNPACK_SLOT(BC_SLOT_SIZE*2 + 8, OUT(R8))
    BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
    BC_LOAD_F64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(DX))

    VBROADCASTSD  CONSTF64_1(), Z9
    BC_POWINT(Z9, VMULPD, BX, CX)

    BC_UNPACK_SLOT(0, OUT(BX))
    BC_STORE_F64_TO_SLOT(IN(Z2), IN(Z3), IN(BX))
    NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_IMM64_SIZE)

DATA  siphashiv<>+0(SB)/8, $0x736f6d6570736575
DATA  siphashiv<>+8(SB)/8, $0x646f72616e646f6d
DATA  siphashiv<>+16(SB)/8, $0x6c7967656e657261
DATA  siphashiv<>+24(SB)/8, $0x7465646279746573
GLOBL siphashiv<>(SB), RODATA|NOPTR, $4*8

DATA permute64+0x00(SB)/8, $0
DATA permute64+0x08(SB)/8, $2
DATA permute64+0x10(SB)/8, $4
DATA permute64+0x18(SB)/8, $6
DATA permute64+0x20(SB)/8, $1
DATA permute64+0x28(SB)/8, $3
DATA permute64+0x30(SB)/8, $5
DATA permute64+0x38(SB)/8, $7
GLOBL permute64(SB), RODATA|NOPTR, $64

// byte position to index
DATA byteidx<>+0(SB)/1, $0
DATA byteidx<>+1(SB)/1, $1
DATA byteidx<>+2(SB)/1, $2
DATA byteidx<>+3(SB)/1, $3
DATA byteidx<>+4(SB)/1, $4
DATA byteidx<>+5(SB)/1, $5
DATA byteidx<>+6(SB)/1, $6
DATA byteidx<>+7(SB)/1, $7
DATA byteidx<>+8(SB)/1, $8
DATA byteidx<>+9(SB)/1, $9
DATA byteidx<>+10(SB)/1, $10
DATA byteidx<>+11(SB)/1, $11
DATA byteidx<>+12(SB)/1, $12
DATA byteidx<>+13(SB)/1, $13
DATA byteidx<>+14(SB)/1, $14
DATA byteidx<>+15(SB)/1, $15
GLOBL byteidx<>(SB), RODATA|NOPTR, $16
