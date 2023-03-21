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

// Integer Math Instructions - Helpers
// -----------------------------------

#define BC_ARITH_OP_I64_IMPL(Instruction)                               \
  BC_UNPACK_4xSLOT(0, OUT(DX), OUT(BX), OUT(CX), OUT(R8))               \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                     \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                       \
                                                                        \
  Instruction.Z 0(VIRT_VALUES)(CX*1), Z2, K1, Z2                        \
  Instruction.Z 64(VIRT_VALUES)(CX*1), Z3, K2, Z3                       \
                                                                        \
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

#define BC_ARITH_OP_I64_IMPL_K(Instruction)                             \
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))           \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                     \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                       \
                                                                        \
  Instruction.Z 0(VIRT_VALUES)(CX*1), Z2, K1, Z2                        \
  Instruction.Z 64(VIRT_VALUES)(CX*1), Z3, K2, Z3                       \
                                                                        \
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))                                 \
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))                          \
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

#define BC_ARITH_OP_I64_IMM_IMPL(Instruction)                           \
  BC_UNPACK_2xSLOT_ZI64_SLOT(0, OUT(DX), OUT(BX), OUT(Z4), OUT(R8))     \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                     \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                       \
                                                                        \
  Instruction.Z Z4, Z2, K1, Z2                                          \
  Instruction.Z Z4, Z3, K2, Z3                                          \
                                                                        \
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))

#define BC_ARITH_OP_I64_IMM_IMPL_K(Instruction)                         \
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z4), OUT(R8))   \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                     \
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))                       \
                                                                        \
  Instruction.Z Z4, Z2, K1, Z2                                          \
  Instruction.Z Z4, Z3, K2, Z3                                          \
                                                                        \
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))                                 \
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))                          \
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

#define BC_ARITH_REVERSE_OP_I64_IMM_IMPL(Instruction)                   \
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z4), OUT(R8))   \
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))                     \
                                                                        \
  Instruction.Z 0(VIRT_VALUES)(BX*1), Z4, K1, Z2                        \
  Instruction.Z 64(VIRT_VALUES)(BX*1), Z4, K2, Z3                       \
                                                                        \
  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))                                 \
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))                          \
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

// Integer Math Instructions - Broadcast
// -------------------------------------

// i64[0] = broadcast.i64(i64@imm[1])
TEXT bcbroadcasti64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT(0, OUT(DX))
  BC_UNPACK_ZI64(BC_SLOT_SIZE, OUT(Z2))

  VMOVDQU64 Z2, 0(VIRT_VALUES)(DX*1)
  VMOVDQU64 Z2, 64(VIRT_VALUES)(DX*1)

  NEXT_ADVANCE(BC_SLOT_SIZE*1 + 8)

// Integer Math Instructions - Abs
// -------------------------------

// bcabsi64 calculates absolute value of an int64.
// lanes in k[1] are cleared on overflow
//
// i64[0].k[1] = abs.i64(i64[2]).k[3]
TEXT bcabsi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  VPBROADCASTQ CONSTQ_0x8000000000000000(), Z4
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPABSQ.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VPABSQ.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  // overflows if the input is 0x8000000000000000 (-9223372036854775808), which cannot be negated as int64
  VPCMPUQ $VPCMP_IMM_NE, Z4, Z2, K1, K1
  VPCMPUQ $VPCMP_IMM_NE, Z4, Z3, K2, K2
  KUNPCKBW K1, K2, K1

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// Integer Math Instructions - Neg
// -------------------------------

// lanes in k[1] are cleared on overflow
//
// i64[0].k[1] = neg.i64(i64[2]).k[3]
TEXT bcnegi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  VPXORQ X3, X3, X3
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPBROADCASTQ CONSTQ_0x8000000000000000(), Z4
  VPSUBQ.Z 0(VIRT_VALUES)(BX*1), Z3, K1, Z2
  VPSUBQ.Z 64(VIRT_VALUES)(BX*1), Z3, K2, Z3

  // overflows if the input is 0x8000000000000000 (-9223372036854775808), which cannot be negated
  VPCMPUQ $VPCMP_IMM_NE, Z4, Z2, K1, K1
  VPCMPUQ $VPCMP_IMM_NE, Z4, Z3, K2, K2
  KUNPCKBW K1, K2, K1

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// Integer Math Instructions - Sign
// --------------------------------

// i64[0].k[1] = sign.i64(i64[2]).k[3]
TEXT bcsigni64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  BC_FILL_ONES(Z5)  // Z5 = -1
  VPABSQ Z5, Z4     // Z4 = 1
  VPMAXSQ 0(VIRT_VALUES)(BX*1), Z5, Z2
  VPMAXSQ 64(VIRT_VALUES)(BX*1), Z5, Z3
  VPMINSQ.Z Z2, Z4, K1, Z2
  VPMINSQ.Z Z3, Z4, K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// Integer Math Instructions - Square
// ----------------------------------

// i64[0].k[1] = square.i64(i64[2]).k[3]
TEXT bcsquarei64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))

  VPBROADCASTQ CONSTQ_3037000499(), Z6
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPABSQ 0(VIRT_VALUES)(BX*1), Z2
  VPABSQ 64(VIRT_VALUES)(BX*1), Z3

  // Overflow check - the maximum positive number to square to produce a signed 64-bit result is 3037000499.
  VPCMPQ $VPCMP_IMM_LE, Z6, Z2, K1, K1
  VPCMPQ $VPCMP_IMM_LE, Z6, Z3, K2, K2

  // We use VPMULDQ because any input integer greater than 32-bit would overflow anyway. On many
  // architectures this is much faster than using VPMULLQ, which performs a full 64-bit multiply.
  VPMULDQ.Z Z2, Z2, K1, Z4
  VPMULDQ.Z Z3, Z3, K2, Z5
  KUNPCKBW K1, K2, K1

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// Integer Math Instructions - BitNot
// ----------------------------------

// i64[0] = bitnot.i64(i64[2]).k[3]
TEXT bcbitnoti64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  BC_UNPACK_SLOT(0, OUT(DX))

  VPTERNLOGQ.Z $0x0F, 0(VIRT_VALUES)(BX*1), Z2, K1, Z2
  VPTERNLOGQ.Z $0x0F, 64(VIRT_VALUES)(BX*1), Z3, K2, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// Integer Math Instructions - BitCount
// ------------------------------------

// i64[0] = bitcount.i64(i64[1]).k[2]
TEXT bcbitcounti64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_I64_FROM_SLOT(OUT(Z2), OUT(Z3), IN(BX))

  VPBROADCASTB CONSTD_15(), Z6
  VPSRLQ $4, Z2, Z4
  VPSRLQ $4, Z3, Z5

  VBROADCASTI32X4 CONST_GET_PTR(popcnt_nibble_vpsadbw_pos, 0), Z7
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))
  VPANDQ.Z Z6, Z4, K1, Z4
  VPANDQ.Z Z6, Z5, K2, Z5

  VBROADCASTI32X4 CONST_GET_PTR(popcnt_nibble_vpsadbw_neg, 0), Z8
  VPANDQ.Z Z6, Z2, K1, Z2
  VPANDQ.Z Z6, Z3, K2, Z3

  VPSHUFB Z4, Z7, Z4
  VPSHUFB Z2, Z8, Z2

  BC_UNPACK_SLOT(0, OUT(DX))
  VPSHUFB Z5, Z7, Z5
  VPSHUFB Z3, Z8, Z3

  VPSADBW Z2, Z4, Z2
  VPSADBW Z3, Z5, Z3

  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

TEXT bcbitcounti64_v2(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT(BC_SLOT_SIZE*1, OUT(BX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPOPCNTQ.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VPOPCNTQ.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  BC_UNPACK_SLOT(0, OUT(DX))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  NEXT_ADVANCE(BC_SLOT_SIZE*3)

// Integer Math Instructions - Add
// -------------------------------

// i64[0].k[1] = add.i64(i64[2], i64[3]).k[4]
TEXT bcaddi64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL_K(VPADDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = add.i64@imm(i64[2], i64@imm[3]).k[4]
TEXT bcaddi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REVERSE_OP_I64_IMM_IMPL(VPADDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// Integer Math Instructions - Sub
// -------------------------------

// i64[0].k[1] = sub.i64(i64[2], i64[3]).k[4]
TEXT bcsubi64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL_K(VPSUBQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = sub.i64@imm(i64[2], i64@imm[3]).k[4]
TEXT bcsubi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL_K(VPSUBQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// i64[0].k[1] = rsub.i64@imm(i64@imm[3], i64[2]).k[4]
TEXT bcrsubi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REVERSE_OP_I64_IMM_IMPL(VPSUBQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// Integer Math Instructions - Mul
// -------------------------------

// i64[0].k[1] = mul.i64(i64[2], i64[3]).k[4]
TEXT bcmuli64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL_K(VPMULLQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = mul.i64@imm(i64[2], i64@imm[3]).k[4]
TEXT bcmuli64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL_K(VPMULLQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// Integer Math Instructions - Div
// -------------------------------

// i64[0].k[1] = div.i64(i64[2], i64[3]).k[4]
TEXT bcdivi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VMOVDQU64.Z 0(VIRT_VALUES)(CX*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z3
  VMOVDQU64.Z 64(VIRT_VALUES)(CX*1), K2, Z5

  BC_DIVI64_IMPL(OUT(Z2), OUT(Z3), IN(Z2), IN(Z3), IN(Z4), IN(Z5), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = div.i64@imm(i64[2], i64@imm[3]).k[4]
TEXT bcdivi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z4), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  BC_DIVI64_IMPL(OUT(Z16), OUT(Z17), IN(Z2), IN(Z3), IN(Z4), IN(Z4), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z16), IN(Z17), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// i64[0].k[1] = rdiv.i64@imm(i64@imm[3], i64[2]).k[4]
TEXT bcrdivi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z2), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z5

  BC_DIVI64_IMPL(OUT(Z16), OUT(Z17), IN(Z2), IN(Z2), IN(Z4), IN(Z5), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z16), IN(Z17), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// Integer Math Instructions - Mod
// -------------------------------

// i64[0].k[1] = mod.i64(i64[2], i64[3]).k[4]
TEXT bcmodi64(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_3xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VMOVDQU64.Z 0(VIRT_VALUES)(CX*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z3
  VMOVDQU64.Z 64(VIRT_VALUES)(CX*1), K2, Z5

  BC_MODI64_IMPL(OUT(Z2), OUT(Z3), IN(Z2), IN(Z3), IN(Z4), IN(Z5), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5)

// i64[0].k[1] = mod.i64@imm(i64[2], i64@imm[3]).k[4]
TEXT bcmodi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z4), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z2
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z3

  BC_MODI64_IMPL(OUT(Z2), OUT(Z3), IN(Z2), IN(Z3), IN(Z4), IN(Z4), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// i64[0].k[1] = rmod.i64@imm(i64@imm[3], i64[2]).k[4]
TEXT bcrmodi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_SLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(Z2), OUT(R8))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VMOVDQU64.Z 0(VIRT_VALUES)(BX*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(BX*1), K2, Z5

  BC_MODI64_IMPL(OUT(Z2), OUT(Z3), IN(Z2), IN(Z2), IN(Z4), IN(Z5), IN(K1), IN(K2), Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*4 + 8)

// Integer Math Instructions - FMA
// -------------------------------

// i64[0].k[1] = addmul.i64@imm(i64[2], i64[3], i64@imm[4]).k[5]
TEXT bcaddmuli64imm(SB), NOSPLIT|NOFRAME, $0
  BC_UNPACK_2xSLOT_ZI64_SLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(CX), OUT(Z4), OUT(R8))

  VPMULLQ 0(VIRT_VALUES)(CX*1), Z4, Z2
  VPMULLQ 64(VIRT_VALUES)(CX*1), Z4, Z3

  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K2), IN(R8))

  VPADDQ.Z 0(VIRT_VALUES)(BX*1), Z2, K1, Z2
  VPADDQ.Z 64(VIRT_VALUES)(BX*1), Z3, K2, Z3

  BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))
  BC_STORE_I64_TO_SLOT(IN(Z2), IN(Z3), IN(DX))
  BC_STORE_K_TO_SLOT(IN(K1), IN(R8))

  NEXT_ADVANCE(BC_SLOT_SIZE*5 + 8)

// Integer Math Instructions - Min / Max
// -------------------------------------

// i64[0] = minvalue.i64(i64[1], i64[2]).k[3]
TEXT bcminvaluei64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPMINSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = minvalue.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcminvaluei64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPMINSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// i64[0] = maxvalue.i64(i64[1], i64[2]).k[3]
TEXT bcmaxvaluei64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPMAXSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = maxvalue.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcmaxvaluei64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPMAXSQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - And
// -------------------------------

// i64[0] = and.i64(i64[1], i64[2]).k[3]
TEXT bcandi64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPANDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = and.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcandi64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPANDQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - Or
// ------------------------------

// i64[0] = or.i64(i64[1], i64[2]).k[3]
TEXT bcori64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = or.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcori64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - Xor
// -------------------------------

// i64[0] = xor.i64(i64[1], i64[2]).k[3]
TEXT bcxori64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPXORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = xor.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcxori64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPXORQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - SLL
// -------------------------------

// i64[0] = sll.i64(i64[1], i64[2]).k[3]
TEXT bcslli64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPSLLVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = sll.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcslli64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPSLLVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - SRA
// -------------------------------

// i64[0] = sra.i64(i64[1], i64[2]).k[3]
TEXT bcsrai64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPSRAVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = sra.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcsrai64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPSRAVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - SRL
// -------------------------------

// i64[0] = srl.i64(i64[1], i64[2]).k[3]
TEXT bcsrli64(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMPL(VPSRLVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*4)

// i64[0] = srl.i64@imm(i64[1], i64@imm[2]).k[3]
TEXT bcsrli64imm(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_I64_IMM_IMPL(VPSRLVQ)
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + 8)

// Integer Math Instructions - Cleanup
// -----------------------------------

#undef BC_ARITH_REVERSE_OP_I64_IMM_IMPL
#undef BC_ARITH_OP_I64_IMM_IMPL_K
#undef BC_ARITH_OP_I64_IMM_IMPL
#undef BC_ARITH_OP_I64_IMPL_K
#undef BC_ARITH_OP_I64_IMPL
