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
#include "avx512.h"
#include "bc_amd64.h"
#include "bc_imm_amd64.h"
#include "bc_constant.h"
#include "ops_mask.h" // provides OPMASK

// decode the next instruction from
// the virtual pc register and jump
// into its implementation
#define _NEXT(bpc, tmp, tmp2) \
  MOVWQZX 0(bpc), tmp         \
  ADDQ $2, bpc                \
  ANDQ $OPMASK, tmp           \
  LEAQ opaddrs+0(SB), tmp2    \
  JMP 0(tmp2)(tmp*8)

// every bytecode instruction
// other than 'ret' should end in
// NEXT(), which will branch into
// the next pseudo-instruction
#define NEXT() _NEXT(VIRT_PCREG, BX, DX)

// RET_ABORT returns early
// with the carry flag set to
// indicate an aborted bytecode program
#define RET_ABORT() \
  STC \
  RET

// use FAIL() when you encounter
// an unrecoverable error
#define FAIL() \
  MOVL $const_bcerrCorrupt, bytecode_err(VIRT_BCPTR) \
  RET_ABORT()

#define _POP(pc, dst) \
  MOVQ 0(pc), dst     \
  ADDQ $8, pc

// POP(dst) pops the next item
// of the scalar operand stack
#define POP(dst) _POP(VIRT_PCREG, dst)

// POP + broadcast quadword
#define POP_BCSTQ(zreg)            \
  VPBROADCASTQ 0(VIRT_PCREG), zreg \
  ADDQ $8, VIRT_PCREG

// POP + broadcast double
#define POP_BCSTPD(zreg)           \
  VBROADCASTSD 0(VIRT_PCREG), zreg \
  ADDQ $8, VIRT_PCREG

// POP + broadcast dword
#define POP_BCSTD(zreg)            \
  VPBROADCASTD 0(VIRT_PCREG), zreg \
  ADDQ $8, VIRT_PCREG

// decode an offset immediate
// and load that respective mask word
// into 'dst'
#define LOADMSK(dst)            \
  MOVWQZX 0(VIRT_PCREG), R8     \
  ADDQ $2, VIRT_PCREG           \
  LEAQ 0(VIRT_VALUES)(R8*1), R8 \
  KMOVW 0(R8), dst

#define LOADARG1Z(dst0, dst1)           \
  MOVWQZX 0(VIRT_PCREG), R8             \
  ADDQ $2, VIRT_PCREG                   \
  VMOVDQU64 0(VIRT_VALUES)(R8*1), dst0  \
  VMOVDQU64 64(VIRT_VALUES)(R8*1), dst1

#define SAVEARG1Z(src0, src1)           \
  MOVWQZX 0(VIRT_PCREG), R8             \
  ADDQ $2, VIRT_PCREG                   \
  VMOVDQU64 src0, 0(VIRT_VALUES)(R8*1)  \
  VMOVDQU64 src1, 64(VIRT_VALUES)(R8*1)

#define IMM_FROM_DICT(REG)      \
    MOVWQZX 0(VIRT_PCREG), DX   \
    ADDQ $2, VIRT_PCREG         \
    SHLQ $4, DX                 \ // imm *= sizeof(string)
    MOVQ bytecode_dict(DI), REG \ // REG = dict
    LEAQ 0(REG)(DX*1), REG        // REG = &dict[imm]

// Control Flow Instructions
// -------------------------

// the 'return' instruction
TEXT bcret(SB), NOSPLIT|NOFRAME, $0
  CLC
  RET

// jump forward 'n' bytes if the current mask is zero
TEXT bcjz(SB), NOSPLIT|NOFRAME, $0
  POP(DX)
  KTESTW K1, K1
  JNZ    next
  LEAQ   0(VIRT_PCREG)(DX*1), VIRT_PCREG   // virtual pc += uint32(DX)
next:
  NEXT()

// Load & Save Instructions
// ------------------------

// k1 = vstack[imm]
TEXT bcloadk(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K1)
  NEXT()

// vstack[imm] = k1
TEXT bcsavek(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KMOVW K1, 0(VIRT_VALUES)(R8*1)
  NEXT()

// swap(k1, vstack[imm])
TEXT bcxchgk(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KMOVW 0(VIRT_VALUES)(R8*1), K2
  KMOVW K1, 0(VIRT_VALUES)(R8*1)
  KMOVW K2, K1
  NEXT()

// load row pointer
TEXT bcloadb(SB), NOSPLIT|NOFRAME, $0
  LOADARG1Z(Z0, Z1)
  NEXT()

// save row pointer
TEXT bcsaveb(SB), NOSPLIT|NOFRAME, $0
  SAVEARG1Z(Z0, Z1)
  NEXT()

// load value pointer
TEXT bcloadv(SB), NOSPLIT|NOFRAME, $0
  LOADARG1Z(Z30, Z31)
  NEXT()

// save value pointer
TEXT bcsavev(SB), NOSPLIT|NOFRAME, $0
  SAVEARG1Z(Z30, Z31)
  NEXT()

// load a sub-structure pointer,
// but only set K1 for non-zero-length
// sub-structure components
TEXT bcloadzerov(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  VMOVDQU32   0(VIRT_VALUES)(R8*1), Z30
  VMOVDQU32   64(VIRT_VALUES)(R8*1), Z31
  VPTESTMD    Z31, Z31, K1
  NEXT()

// save a sub-structure pointer,
// but zero results when K1 is unset
TEXT bcsavezerov(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  VMOVDQA32.Z Z30, K1, Z28
  VMOVDQA32.Z Z31, K1, Z29
  VMOVDQU32   Z28, 0(VIRT_VALUES)(R8*1)
  VMOVDQU32   Z29, 64(VIRT_VALUES)(R8*1)
  NEXT()

// load a value pointer from bytecode.outer
// using the permutation specified in
// bytecode.perm
TEXT bcloadpermzerov(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  MOVQ        bytecode_outer(VIRT_BCPTR), R15
  VMOVDQU32   bytecode_perm(VIRT_BCPTR), Z28
  MOVQ        bytecode_vstack(R15), R15
  VPERMD      0(R15)(R8*1), Z28, Z30
  VPERMD      64(R15)(R8*1), Z28, Z31
  VPTESTMD    Z31, Z31, K1
  NEXT()

// save a subset of lanes to a particular slot, leaving existing entries intact
TEXT bcsaveblendv(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  VMOVDQU32   Z30, K1, 0(VIRT_VALUES)(R8*1)
  VMOVDQU32   Z31, K1, 64(VIRT_VALUES)(R8*1)
  NEXT()

// load scalar
TEXT bcloads(SB), NOSPLIT|NOFRAME, $0
  LOADARG1Z(Z2, Z3)
  NEXT()

// save scalar
TEXT bcsaves(SB), NOSPLIT|NOFRAME, $0
  SAVEARG1Z(Z2, Z3)
  NEXT()

TEXT bcloadzeros(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  VMOVDQU32   0(VIRT_VALUES)(R8*1), Z2
  VMOVDQU32   64(VIRT_VALUES)(R8*1), Z3
  VPTESTMD    Z3, Z3, K1
  NEXT()

TEXT bcsavezeros(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG

  KSHIFTRW    $8, K1, K2
  VMOVDQA32.Z Z2, K1, Z4
  VMOVDQA32.Z Z3, K2, Z5

  VMOVDQU32   Z4, 0(VIRT_VALUES)(R8*1)
  VMOVDQU32   Z5, 64(VIRT_VALUES)(R8*1)

  NEXT()

// Mask Instructions
// -----------------

TEXT bcfalse(SB), NOSPLIT|NOFRAME, $0
  VPXORD  Z30, Z30, Z30
  VPXORD  Z31, Z31, Z31
  KXORW   K0, K0, K1
  NEXT()

// K1 &= vstack[imm]
TEXT bcandk(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KANDW K1, K2, K1
  NEXT()

// K1 |= vstack[imm]
TEXT bcork(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KORW  K1, K2, K1
  NEXT()

// K1 = vstack[imm] &^ K1
TEXT bcandnotk(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KANDNW K2, K1, K1
  NEXT()

// K1 = K1 &^ vstack[imm]
TEXT bcnandk(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KANDNW K1, K2, K1
  NEXT()

// K1 = (K1 ^ vstack[imm]) & (valid lanes)
TEXT bcxork(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KXORW K1, K2, K1
  NEXT()

// K1 = K1 ^ true
// (this is roughly NOT, but keeps invalid lanes unset)
TEXT bcnotk(SB), NOSPLIT|NOFRAME, $0
  KXORW K1, K7, K1
  NEXT()

// K1 = (K1 xnor vstack[imm]) & (valid lanes)
TEXT bcxnork(SB), NOSPLIT|NOFRAME, $0
  LOADMSK(K2)
  KXNORW K1, K2, K1
  KANDW  K1, K7, K1
  NEXT()

// Arithmetic Instructions
// -----------------------

// Arithmetic operation macros
#define BC_ARITH_OP_VAR(instruction)                \
  MOVWQZX       0(VIRT_PCREG), R8                   \
  ADDQ          $2, VIRT_PCREG                      \
  KSHIFTRW      $8, K1, K2                          \
  instruction   0(VIRT_VALUES)(R8*1), Z2, K1, Z2    \
  instruction   64(VIRT_VALUES)(R8*1), Z3, K2, Z3

#define BC_ARITH_OP_IMM(instruction, broadcast)     \
  broadcast  0(VIRT_PCREG), Z4                      \
  ADDQ          $8, VIRT_PCREG                      \
  KSHIFTRW      $8, K1, K2                          \
  instruction   Z4, Z2, K1, Z2                      \
  instruction   Z4, Z3, K2, Z3

#define BC_ARITH_REV_OP_VAR(instruction)            \
  MOVWQZX       0(VIRT_PCREG), R8                   \
  ADDQ          $2, VIRT_PCREG                      \
  KSHIFTRW      $8, K1, K2                          \
  VMOVDQU64     0(VIRT_VALUES)(R8*1), Z4            \
  VMOVDQU64     64(VIRT_VALUES)(R8*1), Z5           \
  instruction   Z2, Z4, K1, Z2                      \
  instruction   Z3, Z5, K2, Z3

#define BC_ARITH_REV_OP_IMM(instruction, broadcast) \
  broadcast     0(VIRT_PCREG), Z4                   \
  ADDQ          $8, VIRT_PCREG                      \
  KSHIFTRW      $8, K1, K2                          \
  instruction   Z2, Z4, K1, Z2                      \
  instruction   Z3, Z4, K2, Z3

// Left = Left - Trunc(Left / Right) * Right
#define BC_MODF64_OP(LEFT1, LEFT2, RIGHT1, RIGHT2, TMP1, TMP2)              \
  VDIVPD.RZ_SAE.Z RIGHT1, LEFT1, K1, TMP1                                   \
  VDIVPD.RZ_SAE.Z RIGHT2, LEFT2, K2, TMP2                                   \
  VRNDSCALEPD.Z   $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), TMP1, K1, TMP1 \
  VRNDSCALEPD.Z   $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), TMP2, K2, TMP2 \
  VFNMADD231PD    RIGHT1, TMP1, K1, LEFT1                                   \
  VFNMADD231PD    RIGHT2, TMP2, K2, LEFT2

// This macro implements INT64 division that can be used in bytecode instructions.
//
// An unsigned scalar version could look like this (in C++):
//
// static uint64_t divu64(uint64_t a, uint64_t b) {
//   double fa = double(a);
//   double fb = double(b);
//
//   // First division step.
//   uint64_t w1 = uint64_t(fa / fb);
//   uint64_t x = w1 * b;
//
//   // Remainder of the first division step.
//   double fc = double(int64_t(a) - int64_t(x));
//
//   // Second division step.
//   int64_t w2 = int64_t(fc / fb);
//   uint64_t w = uint64_t(w1 + w2);
//
//   // Correction of a possible "off by 1" result.
//   return w - uint64_t(w * b > a);
// }
#define BC_DIVU64_IMPL(DST_A, DST_B, SRC_A1, SRC_B1, SRC_A2, SRC_B2, MASK_A, MASK_B, TMP_A1, TMP_B1, TMP_A2, TMP_B2, TMP_A3, TMP_B3, TMP_MASK_A, TMP_MASK_B) \
  /* Convert to double precision */                           \
  VCVTUQQ2PD.Z SRC_A1, MASK_A, TMP_A1                         \
  VCVTUQQ2PD.Z SRC_B1, MASK_B, TMP_B1                         \
  VCVTUQQ2PD.Z SRC_A2, MASK_A, TMP_A2                         \
  VCVTUQQ2PD.Z SRC_B2, MASK_B, TMP_B2                         \
                                                              \
  /* First division step */                                   \
  VDIVPD.Z TMP_A2, TMP_A1, MASK_A, TMP_A3                     \
  VDIVPD.Z TMP_B2, TMP_B1, MASK_B, TMP_B3                     \
                                                              \
  VCVTPD2UQQ.Z TMP_A3, MASK_A, TMP_A3                         \
  VCVTPD2UQQ.Z TMP_B3, MASK_B, TMP_B3                         \
                                                              \
  /* Decrease the dividend by the first result */             \
  VPMULLQ.Z SRC_A2, TMP_A3, MASK_A, TMP_A1                    \
  VPMULLQ.Z SRC_B2, TMP_B3, MASK_B, TMP_B1                    \
                                                              \
  VPSUBQ.Z TMP_A1, SRC_A1, MASK_A, TMP_A1                     \
  VPSUBQ.Z TMP_B1, SRC_B1, MASK_B, TMP_B1                     \
                                                              \
  /* Prepare for the second division */                       \
  VCVTQQ2PD.Z TMP_A1, MASK_A, TMP_A1                          \
  VCVTQQ2PD.Z TMP_B1, MASK_B, TMP_B1                          \
                                                              \
  /* Second division step, corrects results from the first */ \
  VDIVPD.Z TMP_A2, TMP_A1, MASK_A, TMP_A1                     \
  VDIVPD.Z TMP_B2, TMP_B1, MASK_B, TMP_B1                     \
                                                              \
  VCVTPD2QQ.Z TMP_A1, MASK_A, TMP_A1                          \
  VCVTPD2QQ.Z TMP_B1, MASK_B, TMP_B1                          \
                                                              \
  VPADDQ TMP_A1, TMP_A3, MASK_A, TMP_A3                       \
  VPADDQ TMP_B1, TMP_B3, MASK_B, TMP_B3                       \
                                                              \
  /* Calculate the result by using the second remainder */    \
  VPMULLQ SRC_A2, TMP_A3, MASK_A, TMP_A1                      \
  VPMULLQ SRC_B2, TMP_B3, MASK_B, TMP_B1                      \
                                                              \
  /* Check whether we need to subtract 1 from the result */   \
  VPCMPUQ $VPCMP_IMM_GT, SRC_A1, TMP_A1, MASK_A, TMP_MASK_A   \
  VPCMPUQ $VPCMP_IMM_GT, SRC_B1, TMP_B1, MASK_B, TMP_MASK_B   \
                                                              \
  /* Subtract 1 from the result, if necessary */              \
  VPSUBQ.BCST CONSTQ_1(), TMP_A3, TMP_MASK_A, TMP_A3          \
  VPSUBQ.BCST CONSTQ_1(), TMP_B3, TMP_MASK_B, TMP_B3          \
                                                              \
  VMOVDQA64 TMP_A3, MASK_A, DST_A                             \
  VMOVDQA64 TMP_B3, MASK_B, DST_B


#define BC_MODU64_IMPL(DST_A, DST_B, SRC_A1, SRC_B1, SRC_A2, SRC_B2, MASK_A, MASK_B, TMP_A1, TMP_B1, TMP_A2, TMP_B2, TMP_A3, TMP_B3, TMP_MASK_A, TMP_MASK_B) \
  /* Convert to double precision */                           \
  VCVTUQQ2PD.Z SRC_A1, MASK_A, TMP_A1                         \
  VCVTUQQ2PD.Z SRC_B1, MASK_B, TMP_B1                         \
  VCVTUQQ2PD.Z SRC_A2, MASK_A, TMP_A2                         \
  VCVTUQQ2PD.Z SRC_B2, MASK_B, TMP_B2                         \
                                                              \
  /* First division step */                                   \
  VDIVPD.Z TMP_A2, TMP_A1, MASK_A, TMP_A3                     \
  VDIVPD.Z TMP_B2, TMP_B1, MASK_B, TMP_B3                     \
                                                              \
  VCVTPD2UQQ.Z TMP_A3, MASK_A, TMP_A3                         \
  VCVTPD2UQQ.Z TMP_B3, MASK_B, TMP_B3                         \
                                                              \
  /* Decrease the dividend by the first result */             \
  VPMULLQ.Z SRC_A2, TMP_A3, MASK_A, TMP_A1                    \
  VPMULLQ.Z SRC_B2, TMP_B3, MASK_B, TMP_B1                    \
                                                              \
  VPSUBQ.Z TMP_A1, SRC_A1, MASK_A, TMP_A1                     \
  VPSUBQ.Z TMP_B1, SRC_B1, MASK_B, TMP_B1                     \
                                                              \
  /* Prepare for the second division */                       \
  VCVTQQ2PD.Z TMP_A1, MASK_A, TMP_A1                          \
  VCVTQQ2PD.Z TMP_B1, MASK_B, TMP_B1                          \
                                                              \
  /* Second division step, corrects results from the first */ \
  VDIVPD.Z TMP_A2, TMP_A1, MASK_A, TMP_A1                     \
  VDIVPD.Z TMP_B2, TMP_B1, MASK_B, TMP_B1                     \
                                                              \
  VCVTPD2QQ.Z TMP_A1, MASK_A, TMP_A1                          \
  VCVTPD2QQ.Z TMP_B1, MASK_B, TMP_B1                          \
                                                              \
  VPADDQ.Z TMP_A1, TMP_A3, MASK_A, TMP_A3                     \
  VPADDQ.Z TMP_B1, TMP_B3, MASK_B, TMP_B3                     \
                                                              \
  /* Calculate the result by using the second remainder */    \
  VPMULLQ.Z SRC_A2, TMP_A3, MASK_A, TMP_A1                    \
  VPMULLQ.Z SRC_B2, TMP_B3, MASK_B, TMP_B1                    \
                                                              \
  /* Check whether we need to subtract 1 from the result */   \
  VPCMPUQ $VPCMP_IMM_GT, SRC_A1, TMP_A1, MASK_A, TMP_MASK_A   \
  VPCMPUQ $VPCMP_IMM_GT, SRC_B1, TMP_B1, MASK_B, TMP_MASK_B   \
                                                              \
  /* Subtract 1 from the result, if necessary */              \
  VPSUBQ.BCST CONSTQ_1(), TMP_A3, TMP_MASK_A, TMP_A3          \
  VPSUBQ.BCST CONSTQ_1(), TMP_B3, TMP_MASK_B, TMP_B3          \
                                                              \
  /* Calculate the final remainder  */                        \
  VPMULLQ SRC_A2, TMP_A3, TMP_A3                              \
  VPMULLQ SRC_B2, TMP_B3, TMP_B3                              \
                                                              \
  VPSUBQ TMP_A3, SRC_A1, MASK_A, DST_A                        \
  VPSUBQ TMP_B3, SRC_B1, MASK_B, DST_B

#define BC_DIVI64_IMPL(DST_A, DST_B, SRC_A1, SRC_B1, SRC_A2, SRC_B2, MASK_A, MASK_B, TMP_A1, TMP_B1, TMP_A2, TMP_B2, TMP_A3, TMP_B3, TMP_A4, TMP_B4, TMP_A5, TMP_B5, TMP_MASK_A, TMP_MASK_B) \
  /* We divide positive/unsigned numbers first */             \
  VPABSQ.Z SRC_A1, MASK_A, TMP_A1                             \
  VPABSQ.Z SRC_B1, MASK_B, TMP_B1                             \
  VPABSQ.Z SRC_A2, MASK_A, TMP_A2                             \
  VPABSQ.Z SRC_B2, MASK_B, TMP_B2                             \
                                                              \
  VCVTUQQ2PD.Z TMP_A1, MASK_A, TMP_A3                         \
  VCVTUQQ2PD.Z TMP_B1, MASK_B, TMP_B3                         \
  VCVTUQQ2PD.Z TMP_A2, MASK_A, TMP_A4                         \
  VCVTUQQ2PD.Z TMP_B2, MASK_B, TMP_B4                         \
                                                              \
  /* First division step */                                   \
  VDIVPD.Z TMP_A4, TMP_A3, MASK_A, TMP_A5                     \
  VDIVPD.Z TMP_B4, TMP_B3, MASK_B, TMP_B5                     \
                                                              \
  VCVTPD2UQQ.Z TMP_A5, MASK_A, TMP_A5                         \
  VCVTPD2UQQ.Z TMP_B5, MASK_B, TMP_B5                         \
                                                              \
  /* Decrease the dividend by the first result */             \
  VPMULLQ.Z TMP_A2, TMP_A5, MASK_A, TMP_A3                    \
  VPMULLQ.Z TMP_B2, TMP_B5, MASK_B, TMP_B3                    \
                                                              \
  VPSUBQ.Z TMP_A3, TMP_A1, MASK_A, TMP_A3                     \
  VPSUBQ.Z TMP_B3, TMP_B1, MASK_B, TMP_B3                     \
                                                              \
  /* Prepare for the second division */                       \
  VCVTQQ2PD.Z TMP_A3, MASK_A, TMP_A3                          \
  VCVTQQ2PD.Z TMP_B3, MASK_B, TMP_B3                          \
                                                              \
  /* Second division step, corrects results from the first */ \
  VDIVPD.Z TMP_A4, TMP_A3, MASK_A, TMP_A3                     \
  VDIVPD.Z TMP_B4, TMP_B3, MASK_B, TMP_B3                     \
                                                              \
  VCVTPD2QQ.Z TMP_A3, MASK_A, TMP_A3                          \
  VCVTPD2QQ.Z TMP_B3, MASK_B, TMP_B3                          \
                                                              \
  /* XOR signs so we can negate the result, if necessary */   \
  VPXORQ.Z SRC_A2, SRC_A1, MASK_A, TMP_A4                     \
  VPXORQ.Z SRC_B2, SRC_B1, MASK_B, TMP_B4                     \
                                                              \
  VPADDQ TMP_A3, TMP_A5, MASK_A, DST_A                        \
  VPADDQ TMP_B3, TMP_B5, MASK_B, DST_B                        \
                                                              \
  /* Calculate the result by using the second remainder */    \
  VPMULLQ TMP_A2, DST_A, MASK_A, TMP_A2                       \
  VPMULLQ TMP_B2, DST_B, MASK_B, TMP_B2                       \
                                                              \
  /* Check whether we need to subtract 1 from the result */   \
  VPCMPUQ $VPCMP_IMM_GT, TMP_A1, TMP_A2, MASK_A, TMP_MASK_A   \
  VPCMPUQ $VPCMP_IMM_GT, TMP_B1, TMP_B2, MASK_B, TMP_MASK_B   \
                                                              \
  /* Subtract 1 from the result, if necessary */              \
  VPSUBQ.BCST CONSTQ_1(), DST_A, TMP_MASK_A, DST_A            \
  VPSUBQ.BCST CONSTQ_1(), DST_B, TMP_MASK_B, DST_B            \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPMOVQ2M TMP_A4, TMP_MASK_A                                 \
  VPMOVQ2M TMP_B4, TMP_MASK_B                                 \
                                                              \
  VPXORQ TMP_A4, TMP_A4, TMP_A4                               \
  VPSUBQ DST_A, TMP_A4, TMP_MASK_A, DST_A                     \
  VPSUBQ DST_B, TMP_A4, TMP_MASK_B, DST_B

#define BC_MODI64_IMPL(DST_A, DST_B, SRC_A1, SRC_B1, SRC_A2, SRC_B2, MASK_A, MASK_B, TMP_A1, TMP_B1, TMP_A2, TMP_B2, TMP_A3, TMP_B3, TMP_A4, TMP_B4, TMP_A5, TMP_B5, TMP_MASK_A, TMP_MASK_B) \
  /* We divide positive/unsigned numbers first */             \
  VPABSQ.Z SRC_A1, MASK_A, TMP_A1                             \
  VPABSQ.Z SRC_B1, MASK_B, TMP_B1                             \
  VPABSQ.Z SRC_A2, MASK_A, TMP_A2                             \
  VPABSQ.Z SRC_B2, MASK_B, TMP_B2                             \
                                                              \
  VCVTUQQ2PD.Z TMP_A1, MASK_A, TMP_A3                         \
  VCVTUQQ2PD.Z TMP_B1, MASK_B, TMP_B3                         \
  VCVTUQQ2PD.Z TMP_A2, MASK_A, TMP_A4                         \
  VCVTUQQ2PD.Z TMP_B2, MASK_B, TMP_B4                         \
                                                              \
  /* First division step */                                   \
  VDIVPD.Z TMP_A4, TMP_A3, MASK_A, TMP_A5                     \
  VDIVPD.Z TMP_B4, TMP_B3, MASK_B, TMP_B5                     \
                                                              \
  VCVTPD2UQQ.Z TMP_A5, MASK_A, TMP_A5                         \
  VCVTPD2UQQ.Z TMP_B5, MASK_B, TMP_B5                         \
                                                              \
  /* Decrease the dividend by the first result */             \
  VPMULLQ.Z TMP_A2, TMP_A5, MASK_A, TMP_A3                    \
  VPMULLQ.Z TMP_B2, TMP_B5, MASK_B, TMP_B3                    \
                                                              \
  VPSUBQ.Z TMP_A3, TMP_A1, MASK_A, TMP_A3                     \
  VPSUBQ.Z TMP_B3, TMP_B1, MASK_B, TMP_B3                     \
                                                              \
  /* Prepare for the second division */                       \
  VCVTQQ2PD.Z TMP_A3, MASK_A, TMP_A3                          \
  VCVTQQ2PD.Z TMP_B3, MASK_B, TMP_B3                          \
                                                              \
  /* Second division step, corrects results from the first */ \
  VDIVPD.Z TMP_A4, TMP_A3, MASK_A, TMP_A3                     \
  VDIVPD.Z TMP_B4, TMP_B3, MASK_B, TMP_B3                     \
                                                              \
  VCVTPD2QQ.Z TMP_A3, MASK_A, TMP_A3                          \
  VCVTPD2QQ.Z TMP_B3, MASK_B, TMP_B3                          \
                                                              \
  VPADDQ.Z TMP_A3, TMP_A5, MASK_A, TMP_A5                     \
  VPADDQ.Z TMP_B3, TMP_B5, MASK_B, TMP_B5                     \
                                                              \
  /* Calculate the result by using the second remainder */    \
  VPMULLQ.Z TMP_A2, TMP_A5, MASK_A, TMP_A3                    \
  VPMULLQ.Z TMP_B2, TMP_B5, MASK_B, TMP_B3                    \
                                                              \
  /* Check whether we need to subtract 1 from the result */   \
  VPCMPUQ $VPCMP_IMM_GT, TMP_A1, TMP_A3, MASK_A, TMP_MASK_A   \
  VPCMPUQ $VPCMP_IMM_GT, TMP_B1, TMP_B3, MASK_B, TMP_MASK_B   \
                                                              \
  /* Subtract 1 from the result, if necessary */              \
  VPSUBQ.BCST CONSTQ_1(), TMP_A5, TMP_MASK_A, TMP_A5          \
  VPSUBQ.BCST CONSTQ_1(), TMP_B5, TMP_MASK_B, TMP_B5          \
                                                              \
  /* Calculate the mask of resulting negative results */      \
  VPMOVQ2M SRC_A1, TMP_MASK_A                                 \
  VPMOVQ2M SRC_B1, TMP_MASK_B                                 \
                                                              \
  /* Calculate the final remainder  */                        \
  VPMULLQ TMP_A2, TMP_A5, MASK_A, DST_A                       \
  VPMULLQ TMP_B2, TMP_B5, MASK_B, DST_B                       \
                                                              \
  VPSUBQ DST_A, TMP_A1, MASK_A, DST_A                         \
  VPSUBQ DST_B, TMP_B1, MASK_B, DST_B                         \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPXORQ TMP_A4, TMP_A4, TMP_A4                               \
  VPSUBQ DST_A, TMP_A4, TMP_MASK_A, DST_A                     \
  VPSUBQ DST_B, TMP_A4, TMP_MASK_B, DST_B

// Broadcast a constant (float)
TEXT bcbroadcastimmf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  0(VIRT_PCREG), Z2
  ADDQ          $8, VIRT_PCREG
  VMOVDQA64     Z2, Z3
  NEXT()

// Broadcast a constant (int)
TEXT bcbroadcastimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ  0(VIRT_PCREG), Z2
  ADDQ          $8, VIRT_PCREG
  VMOVDQA64     Z2, Z3
  NEXT()

// Unary operation - abs (float)
TEXT bcabsf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  CONSTF64_SIGN_BIT(), Z4
  KSHIFTRW      $8, K1, K2
  VANDNPD       Z2, Z4, K1, Z2
  VANDNPD       Z3, Z4, K2, Z3
  NEXT()

// Unary operation - abs (int)
TEXT bcabsi(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPABSQ Z2, K1, Z2
  VPABSQ Z3, K2, Z3
  NEXT()

// Unary operation - neg (float)
TEXT bcnegf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VXORPD        X4, X4, X4
  VSUBPD        Z2, Z4, K1, Z2
  VSUBPD        Z3, Z4, K2, Z3
  NEXT()

// Unary operation - neg (int)
TEXT bcnegi(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VPXORQ        X4, X4, X4
  VPSUBQ        Z2, Z4, K1, Z2
  VPSUBQ        Z3, Z4, K2, Z3
  NEXT()

// Unary operation - sign (float)
TEXT bcsignf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  CONSTF64_SIGN_BIT(), Z4
  VBROADCASTSD  CONSTF64_1(), Z5
  VXORPD        X6, X6, X6
  KSHIFTRW      $8, K1, K2

  VCMPPD        $VCMP_IMM_NEQ_OQ, Z6, Z2, K1, K3
  VCMPPD        $VCMP_IMM_NEQ_OQ, Z6, Z3, K2, K4

  // Clear everything but signs, and combine with ones. This uses a {K3, K4}
  // write mask and would only update numbers that are not zeros nor NaNs.
  VPTERNLOGQ    $0xEA, Z5, Z4, K3, Z2 // Z2{K3} = (Z2 & Z4) | Z5
  VPTERNLOGQ    $0xEA, Z5, Z4, K4, Z3 // Z3{K4} = (Z3 & Z4) | Z5

  NEXT()

// Unary operation - sign (int)
TEXT bcsigni(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPMINSQ.BCST CONSTQ_1(), Z2, K1, Z2
  VPMINSQ.BCST CONSTQ_1(), Z3, K2, Z3
  VPMAXSQ.BCST CONSTQ_NEG_1(), Z2, K1, Z2
  VPMAXSQ.BCST CONSTQ_NEG_1(), Z3, K2, Z3
  NEXT()

// Unary operation - square (float)
TEXT bcsquaref(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VMULPD        Z2, Z2, K1, Z2
  VMULPD        Z3, Z3, K2, Z3
  NEXT()

// Unary operation - square (int)
TEXT bcsquarei(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VPMULLQ       Z2, Z2, K1, Z2
  VPMULLQ       Z3, Z3, K2, Z3
  NEXT()

// Unary operation - square root (float)
TEXT bcsqrtf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VSQRTPD       Z2, K1, Z2
  VSQRTPD       Z3, K2, Z3
  NEXT()

// Unary operation - rounding (float)
TEXT bcroundf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD CONSTF64_HALF(), Z4
  KSHIFTRW $8, K1, K2
  VMOVAPD Z4, Z5

  // 0xD8 <- (a & (~c)) | (b & c)
  VPTERNLOGQ.BCST $0xD8, CONSTF64_SIGN_BIT(), Z2, Z4
  VPTERNLOGQ.BCST $0xD8, CONSTF64_SIGN_BIT(), Z3, Z5

  // Equivalent to trunc(x + 0.5 * sign(x)) having the intermediate calculation truncated.
  VADDPD.RZ_SAE Z4, Z2, K1, Z2
  VADDPD.RZ_SAE Z5, Z3, K2, Z3

  VRNDSCALEPD $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), Z3, K2, Z3

  NEXT()

TEXT bcroundevenf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VRNDSCALEPD   $(VROUND_IMM_NEAREST | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD   $(VROUND_IMM_NEAREST | VROUND_IMM_SUPPRESS), Z3, K2, Z3
  NEXT()

TEXT bctruncf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VRNDSCALEPD   $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD   $(VROUND_IMM_TRUNC | VROUND_IMM_SUPPRESS), Z3, K2, Z3
  NEXT()

TEXT bcfloorf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VRNDSCALEPD   $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD   $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z3, K2, Z3
  NEXT()

TEXT bcceilf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2
  VRNDSCALEPD   $(VROUND_IMM_UP | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD   $(VROUND_IMM_UP | VROUND_IMM_SUPPRESS), Z3, K2, Z3
  NEXT()

// Binary operation - add (float)
TEXT bcaddf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VADDPD)
  NEXT()

TEXT bcaddimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VADDPD, VBROADCASTSD)
  NEXT()

// Binary operation - add (int)
TEXT bcaddi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VPADDQ)
  NEXT()

TEXT bcaddimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VPADDQ, VPBROADCASTQ)
  NEXT()

// Binary operation - sub (float)
TEXT bcsubf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VSUBPD)
  NEXT()

TEXT bcsubimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VSUBPD, VBROADCASTSD)
  NEXT()

// Binary operation - sub (int)
TEXT bcsubi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VPSUBQ)
  NEXT()

TEXT bcsubimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VPSUBQ, VPBROADCASTQ)
  NEXT()

// Binary operation - rsub (float)
TEXT bcrsubf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_VAR(VSUBPD)
  NEXT()

TEXT bcrsubimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_IMM(VSUBPD, VBROADCASTSD)
  NEXT()

// Binary operation - rsub (int)
TEXT bcrsubi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_VAR(VPSUBQ)
  NEXT()

TEXT bcrsubimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_IMM(VPSUBQ, VPBROADCASTQ)
  NEXT()

// Binary operation - mul (float)
TEXT bcmulf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VMULPD)
  NEXT()

TEXT bcmulimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VMULPD, VBROADCASTSD)
  NEXT()

// Binary operation - mul (int)
TEXT bcmuli(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VPMULLQ)
  NEXT()

TEXT bcmulimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VPMULLQ, VPBROADCASTQ)
  NEXT()

// Binary operation - div (float)
TEXT bcdivf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VDIVPD)
  NEXT()

TEXT bcdivimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VDIVPD, VBROADCASTSD)
  NEXT()

// Binary operation - rdiv (float)
TEXT bcrdivf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_VAR(VDIVPD)
  NEXT()

TEXT bcrdivimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_REV_OP_IMM(VDIVPD, VBROADCASTSD)
  NEXT()

// Binary operation - div (int)
TEXT bcdivi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KSHIFTRW $8, K1, K2
  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5
  JMP divi_tail(SB)

TEXT bcdivimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ 0(VIRT_PCREG), Z4
  KSHIFTRW $8, K1, K2
  ADDQ $8, VIRT_PCREG
  VMOVDQA64 Z4, Z5
  JMP divi_tail(SB)

TEXT divi_tail(SB), NOSPLIT|NOFRAME, $0
  BC_DIVI64_IMPL(Z2, Z3, Z2, Z3, Z4, Z5, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)
  NEXT()

// Binary operation - rdiv (int)
TEXT bcrdivi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KSHIFTRW $8, K1, K2
  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5
  JMP rdivi_tail(SB)

TEXT bcrdivimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ 0(VIRT_PCREG), Z4
  KSHIFTRW $8, K1, K2
  ADDQ $8, VIRT_PCREG
  VMOVDQA64 Z4, Z5
  JMP rdivi_tail(SB)

TEXT rdivi_tail(SB), NOSPLIT|NOFRAME, $0
  BC_DIVI64_IMPL(Z2, Z3, Z4, Z5, Z2, Z3, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)
  NEXT()

// Binary operation - mod (float):
TEXT bcmodf(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  VMOVUPD       0(VIRT_VALUES)(R8*1), Z4
  VMOVUPD       64(VIRT_VALUES)(R8*1), Z5
  BC_MODF64_OP(Z2, Z3, Z4, Z5, Z6, Z7)
  NEXT()

TEXT bcmodimmf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  0(VIRT_PCREG), Z4
  ADDQ          $8, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  BC_MODF64_OP(Z2, Z3, Z4, Z4, Z6, Z7)
  NEXT()

// Binary operation - rmod (float):
TEXT bcrmodf(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  VMOVAPD       Z2, Z4
  VMOVAPD       Z3, Z5
  VMOVUPD       0(VIRT_VALUES)(R8*1), Z2
  VMOVUPD       64(VIRT_VALUES)(R8*1), Z3
  BC_MODF64_OP(Z2, Z3, Z4, Z5, Z6, Z7)
  NEXT()

TEXT bcrmodimmf(SB), NOSPLIT|NOFRAME, $0
  VMOVAPD       Z2, Z4
  VMOVAPD       Z3, Z5
  VBROADCASTSD  0(VIRT_PCREG), Z2
  VBROADCASTSD  0(VIRT_PCREG), Z3
  ADDQ          $8, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  BC_MODF64_OP(Z2, Z3, Z4, Z5, Z6, Z7)
  NEXT()

// Binary operation - mod (int):
TEXT bcmodi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KSHIFTRW $8, K1, K2
  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5
  JMP modi_tail(SB)

TEXT bcmodimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ 0(VIRT_PCREG), Z4
  KSHIFTRW $8, K1, K2
  ADDQ $8, VIRT_PCREG
  VMOVDQA64 Z4, Z5
  JMP modi_tail(SB)

TEXT modi_tail(SB), NOSPLIT|NOFRAME, $0
  BC_MODI64_IMPL(Z2, Z3, Z2, Z3, Z4, Z5, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)
  NEXT()

// Binary operation - rmod (int):
TEXT bcrmodi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  KSHIFTRW $8, K1, K2
  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5
  JMP rmodi_tail(SB)

TEXT bcrmodimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ 0(VIRT_PCREG), Z4
  KSHIFTRW $8, K1, K2
  ADDQ $8, VIRT_PCREG
  VMOVDQA64 Z4, Z5
  JMP rmodi_tail(SB)

TEXT rmodi_tail(SB), NOSPLIT|NOFRAME, $0
  BC_MODI64_IMPL(Z2, Z3, Z4, Z5, Z2, Z3, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)
  NEXT()

// Arithmetic muladd (int)
TEXT bcaddmulimmi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  VPBROADCASTQ 2(VIRT_PCREG), Z5
  KSHIFTRW $8, K1, K2
  ADDQ $10, VIRT_PCREG
  VPMULLQ 0(VIRT_VALUES)(R8*1), Z5, Z4
  VPMULLQ 64(VIRT_VALUES)(R8*1), Z5, Z5
  VPADDQ Z4, Z2, K1, Z2
  VPADDQ Z5, Z3, K2, Z3
  NEXT()

// Arithmetic min/max (float)
TEXT bcminvaluef(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VMINPD)
  NEXT()

TEXT bcminvalueimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VMINPD, VBROADCASTSD)
  NEXT()

TEXT bcmaxvaluef(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VMAXPD)
  NEXT()

TEXT bcmaxvalueimmf(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VMAXPD, VBROADCASTSD)
  NEXT()

// Arithmetic min/max (int)
TEXT bcminvaluei(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VPMINSQ)
  NEXT()

TEXT bcminvalueimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VPMINSQ, VPBROADCASTQ)
  NEXT()

TEXT bcmaxvaluei(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_VAR(VPMAXSQ)
  NEXT()

TEXT bcmaxvalueimmi(SB), NOSPLIT|NOFRAME, $0
  BC_ARITH_OP_IMM(VPMAXSQ, VPBROADCASTQ)
  NEXT()

// Conversion Instructions
// -----------------------

// convert the input mask to 0.0 or 1.0 based on whether or not it is set
TEXT bccvtktof64(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  CONSTF64_1(), Z2
  KSHIFTRW      $8, K1, K2
  VMOVDQA64.Z   Z2, K2, Z3
  VMOVDQA64.Z   Z2, K1, Z2
  NEXT()

// convert the input mask to 0 or 1 based on whether or not it is set
TEXT bccvtktoi64(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ CONSTQ_1(), Z2
  KSHIFTRW     $8, K1, K2
  VMOVDQA64.Z  Z2, K2, Z3
  VMOVDQA64.Z  Z2, K1, Z2
  NEXT()

// integer to fp conversion
TEXT bccvti64tof64(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW         $8, K1, K2
  VCVTQQ2PD.RN_SAE Z2, K1, Z2
  VCVTQQ2PD.RN_SAE Z3, K2, Z3
  NEXT()

// convert fp to int
//
// TODO: validate FPCLASS, etc.;
// we should convert +inf/-inf correctly
// in those circumstances...

#define BC_CVT_F64_TO_I64(mode) \
  KSHIFTRW       $8, K1, K2     \
  VCVTPD2QQ.mode Z2, K1, Z2     \
  VCVTPD2QQ.mode Z3, K2, Z3

// TODO: We should truncate by default, and then extend our offered rounding conversions.
TEXT bccvtf64toi64(SB), NOSPLIT|NOFRAME, $0
  BC_CVT_F64_TO_I64(RN_SAE)
  NEXT()

TEXT bcfproundu(SB), NOSPLIT|NOFRAME, $0
  BC_CVT_F64_TO_I64(RU_SAE)
  NEXT()

TEXT bcfproundd(SB), NOSPLIT|NOFRAME, $0
  BC_CVT_F64_TO_I64(RD_SAE)
  NEXT()

// Comparison Instructions
// -----------------------

// computes cmp(Z2|Z3, stack[imm])
#define ICMPQ(imm)                                 \
  MOVWQZX 0(VIRT_PCREG), R8                        \
  ADDQ $2, VIRT_PCREG                              \
  KSHIFTRW $8, K1, K3                              \
  VPCMPQ   imm, 0(VIRT_VALUES)(R8*1), Z2, K1, K2   \
  VPCMPQ   imm, 64(VIRT_VALUES)(R8*1), Z3, K3, K3  \
  KUNPCKBW K2, K3, K1

// computes cmp(Z2|Z3, stack[imm])
#define PDCMP(imm)                                \
  MOVWQZX 0(VIRT_PCREG), R8                       \
  ADDQ     $2, VIRT_PCREG                         \
  KSHIFTRW $8, K1, K3                             \
  VCMPPD   imm, 0(VIRT_VALUES)(R8*1), Z2, K1, K2  \
  VCMPPD   imm, 64(VIRT_VALUES)(R8*1), Z3, K3, K3 \
  KUNPCKBW K2, K3, K1

// computes cmp(Z2|Z3, imm)
#define ICMPQ_IMM(imm)              \
  POP_BCSTQ(Z28)                    \
  KSHIFTRW     $8, K1, K3           \
  VPCMPQ       imm, Z28, Z2, K1, K2 \
  VPCMPQ       imm, Z28, Z3, K3, K3 \
  KUNPCKBW     K2, K3, K1

// computes cmp(Z2|Z3, imm)
#define PDCMP_IMM(imm)             \
  POP_BCSTPD(Z28)                  \
  KSHIFTRW    $8, K1, K3           \
  VCMPPD      imm, Z28, Z2, K1, K2 \
  VCMPPD      imm, Z28, Z3, K3, K3 \
  KUNPCKBW    K2, K3, K1

TEXT bccmpeqf(SB), NOSPLIT|NOFRAME, $0
  PDCMP($VCMP_IMM_EQ_OQ)
  NEXT()

// current integer scalar = saved integer scalar
TEXT bccmpeqi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX      0(VIRT_PCREG), R8
  ADDQ         $2, VIRT_PCREG
  LEAQ         0(VIRT_VALUES)(R8*1), R8
  KSHIFTRW     $8, K1, K3
  VPCMPEQQ     0(R8), Z2, K1, K2
  VPCMPEQQ     64(R8), Z3, K3, K3
  KUNPCKBW     K2, K3, K1
  NEXT()

// current scalar = f64(imm)
TEXT bccmpeqimmf(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTSD  0(VIRT_PCREG), Z28
  ADDQ          $8, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  VCMPPD        $0, Z2, Z28, K1, K1
  VCMPPD        $0, Z3, Z28, K2, K2
  KUNPCKBW      K1, K2, K1
  NEXT()

// current scalar int = i64(imm)
TEXT bccmpeqimmi(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ  0(VIRT_PCREG), Z28
  ADDQ          $8, VIRT_PCREG
  KSHIFTRW      $8, K1, K2
  VPCMPQ        $0, Z2, Z28, K1, K1
  VPCMPQ        $0, Z3, Z28, K2, K2
  KUNPCKBW      K1, K2, K1
  NEXT()

TEXT bccmpltf(SB), NOSPLIT|NOFRAME, $0
  PDCMP($VCMP_IMM_LT_OQ)
  NEXT()

TEXT bccmplti(SB), NOSPLIT|NOFRAME, $0
  ICMPQ($VPCMP_IMM_LT)
  NEXT()

TEXT bccmpltimmf(SB), NOSPLIT|NOFRAME, $0
  PDCMP_IMM($VCMP_IMM_LT_OQ)
  NEXT()

TEXT bccmpltimmi(SB), NOSPLIT|NOFRAME, $0
  ICMPQ_IMM($VPCMP_IMM_LT)
  NEXT()

TEXT bccmplef(SB), NOSPLIT|NOFRAME, $0
  PDCMP($VCMP_IMM_LE_OQ)
  NEXT()

TEXT bccmplei(SB), NOSPLIT|NOFRAME, $0
  ICMPQ($VPCMP_IMM_LE)
  NEXT()

TEXT bccmpleimmf(SB), NOSPLIT|NOFRAME, $0
  PDCMP_IMM($VCMP_IMM_LE_OQ)
  NEXT()

TEXT bccmpleimmi(SB), NOSPLIT|NOFRAME, $0
  ICMPQ_IMM($VPCMP_IMM_LE)
  NEXT()

TEXT bccmpgtf(SB), NOSPLIT|NOFRAME, $0
  PDCMP($VCMP_IMM_GT_OQ)
  NEXT()

TEXT bccmpgti(SB), NOSPLIT|NOFRAME, $0
  ICMPQ($VPCMP_IMM_GT)
  NEXT()

TEXT bccmpgtimmf(SB), NOSPLIT|NOFRAME, $0
  PDCMP_IMM($VCMP_IMM_GT_OQ)
  NEXT()

TEXT bccmpgtimmi(SB), NOSPLIT|NOFRAME, $0
  ICMPQ_IMM($VPCMP_IMM_GT)
  NEXT()

TEXT bccmpgef(SB), NOSPLIT|NOFRAME, $0
  PDCMP($VCMP_IMM_GE_OQ)
  NEXT()

TEXT bccmpgei(SB), NOSPLIT|NOFRAME, $0
  ICMPQ($VPCMP_IMM_GE)
  NEXT()

TEXT bccmpgeimmf(SB), NOSPLIT|NOFRAME, $0
  PDCMP_IMM($VCMP_IMM_GE_OQ)
  NEXT()

TEXT bccmpgeimmi(SB), NOSPLIT|NOFRAME, $0
  ICMPQ_IMM($VPCMP_IMM_GE)
  NEXT()

// Test Instructions
// -----------------

// isnanf(x) is the same as x != x
TEXT bcisnanf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTLW   $8, K1, K2
  VCMPPD     $4, Z2, Z2, K1, K1
  VCMPPD     $4, Z3, Z3, K2, K2
  KUNPCKBW   K1, K2, K1
  NEXT()

// take the tag pointed to in Z30:Z31
// and determine if it contains _any_ of
// the immediate bits provided in the instruction
TEXT bcchecktag(SB), NOSPLIT|NOFRAME, $0
  MOVWLZX      0(VIRT_PCREG), R14
  ADDQ         $2, VIRT_PCREG
  VPBROADCASTD R14, Z14                // Z14 = tag bits
  KMOVW        K1, K3
  VPGATHERDD   0(SI)(Z30*1), K3, Z15   // Z15 = initial object bytes
  VPBROADCASTD CONSTD_1(), Z21
  VPSRLD       $4, Z15, Z15            // Z15 >>= 4
  VPANDD.BCST  CONSTD_0x0F(), Z15, Z15 // Z15 = (bytes >> 4) & 0xf
  VPSLLVD      Z15, Z21, Z15           // Z15 = 1 << ((bytes >> 4) & 0xf)
  VPTESTMD     Z14, Z15, K1, K1        // test tag&z15 != 0
  NEXT()

// current value == NULL
TEXT bcisnull(SB), NOSPLIT|NOFRAME, $0
  // compute data[0]&0xf == 0xf
  KMOVW          K1, K2
  VPGATHERDD     0(SI)(Z30*1), K2, Z29
  VPBROADCASTD   CONSTD_0x0F(), Z28
  VPANDD         Z29, Z28, Z29
  VPCMPEQD       Z29, Z28, K1, K1
  NEXT()

// current value != NULL
TEXT bcisnotnull(SB), NOSPLIT|NOFRAME, $0
  // compute data[0]&0xf != 0xf
  KMOVW          K1, K2
  VPGATHERDD     0(SI)(Z30*1), K2, Z29
  VPBROADCASTD   CONSTD_0x0F(), Z28
  VPANDD         Z29, Z28, Z29
  VPCMPUD        $4, Z29, Z28, K1, K1
  NEXT()

TEXT bcistrue(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(SI)(Z30*1), K2, Z29
  VPANDD.BCST   CONSTD_0xFF(), Z29, Z29
  VPCMPEQD.BCST CONSTD_TRUE_BYTE(), Z29, K1, K1
  NEXT()

TEXT bcisfalse(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(SI)(Z30*1), K2, Z29
  VPANDD.BCST   CONSTD_0xFF(), Z29, Z29
  VPCMPEQD.BCST CONSTD_FALSE_BYTE(), Z29, K1, K1
  NEXT()

// compare slices in z2:z3 to saved slices
// (works identically for strings and timestamps)
TEXT bceqslice(SB), NOSPLIT|NOFRAME, $0
  LOADARG1Z(Z4, Z5)
  VMOVDQA32    Z2, Z6
  VMOVDQA32    Z3, Z7
  JMP          eqmem_tail(SB)

// compare slices z4:z5.k1 and z6:z7.k1 and return K1 equal mask
TEXT eqmem_tail(SB), NOSPLIT|NOFRAME, $0
  VPCMPEQD     Z7, Z5, K1, K1   // only bother comparing equal-length slices
  KTESTW       K1, K1
  JZ           next
  VPBROADCASTD CONSTD_4(), Z24
  VPXORD       Z10, Z10, Z10    // default behavior is 0 = 0 (matching)
  VPXORD       Z11, Z11, Z11
  JMP          loop4tail
loop4:
  KMOVW        K2, K3
  KMOVW        K2, K4
  VPGATHERDD   0(SI)(Z6*1), K2, Z10
  VPGATHERDD   0(SI)(Z4*1), K3, Z11
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
  VPGATHERDD      0(SI)(Z6*1), K2, Z10
  VPGATHERDD      0(SI)(Z4*1), K3, Z11
  VPANDD          Z9, Z10, Z10
  VPANDD          Z9, Z11, Z11
  VPCMPEQD        Z10, Z11, K1, K1
next:
  NEXT()

// equal(Z30:Z31, stack[imm])
TEXT bcequalv(SB), NOSPLIT|NOFRAME, $0
  LOADARG1Z(Z4, Z5)
  VMOVDQA32.Z  Z30, K1, Z6
  VMOVDQA32.Z  Z31, K1, Z7
  JMP          eqmem_tail(SB)

// given 4-byte immediate and mask,
// compute K1 = (*value)&mask == imm
TEXT bceqv4mask(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(SI)(Z30*1), K2, Z26
  VPANDD.BCST   4(VIRT_PCREG), Z26, Z26
  VPCMPEQD.BCST 0(VIRT_PCREG), Z26, K1, K1
  ADDQ          $8, VIRT_PCREG
  LEAQ          4(SI), R8
  NEXT()

// same as above, but use 'R8'
// as an additional pre-increment
// displacement for longer literal
// comparisons
TEXT bceqv4maskplus(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(R8)(Z30*1), K2, Z26
  VPANDD.BCST   4(VIRT_PCREG), Z26, Z26
  VPCMPEQD.BCST 0(VIRT_PCREG), Z26, K1, K1
  ADDQ          $8, VIRT_PCREG
  LEAQ          4(R8), R8
  NEXT()

// begin a comparison with 8 literal bytes,
// resetting the displacement reg
TEXT bceqv8(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(SI)(Z30*1), K2, Z26
  VPCMPEQD.BCST 0(VIRT_PCREG), Z26, K1, K1
  KMOVW         K1, K2
  VPGATHERDD    4(SI)(Z30*1), K2, Z26
  VPCMPEQD.BCST 4(VIRT_PCREG), Z26, K1, K1
  ADDQ          $8, VIRT_PCREG
  LEAQ          8(SI), R8
  NEXT()

// continue a comparison op with 8 more literal bytes
TEXT bceqv8plus(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, K2
  VPGATHERDD    0(R8)(Z30*1), K2, Z26
  VPCMPEQD.BCST 0(VIRT_PCREG), Z26, K1, K1
  KMOVW         K1, K2
  VPGATHERDD    4(R8)(Z30*1), K2, Z26
  VPCMPEQD.BCST 4(VIRT_PCREG), Z26, K1, K1
  ADDQ          $8, VIRT_PCREG
  LEAQ          8(R8), R8
  NEXT()

// select only values where length==imm
TEXT bcleneq(SB), NOSPLIT|NOFRAME, $0
  VPCMPEQD.BCST 0(VIRT_PCREG), Z31, K1, K1
  ADDQ          $4, VIRT_PCREG
  NEXT()

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
  /* First cut off some bits that we don't need to calculate Year/Month/Day, we will */     \
  /* use these bits later to box microseconds. */                                           \
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
  VRNDSCALEPD $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z8, Z8                              \
  VRNDSCALEPD $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z9, Z9                              \
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
  /*          VPTERNLOG(0xD8) = (A & ~C) | (B & C) */                                       \
  VPSLLQ $13, Z6, Z4                                                                        \
  VPSLLQ $13, Z7, Z5                                                                        \
  VPTERNLOGQ.BCST $0xD8, CONSTQ_0x1FFF(), INPUT1, Z4                                        \
  VPTERNLOGQ.BCST $0xD8, CONSTQ_0x1FFF(), INPUT2, Z5                                        \
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
// reuse the existing `bcaddi` and `bcaddimmi` instructions, which are timestamp agnostic.
//
// We don't really need a specific code for adding years, as `year == month * 12`. This
// means that we can just convert years to months and add `year * 12` months and be done.
TEXT bcdateaddmonth(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG
  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z20
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z21
  JMP dateaddmonth_tail(SB)

TEXT bcdateaddmonthimm(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTQ 0(VIRT_PCREG), Z20
  ADDQ $8, VIRT_PCREG
  VMOVDQA64 Z20, Z21
  JMP dateaddmonth_tail(SB)

TEXT bcdateaddyear(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ $2, VIRT_PCREG

  // Multiply years by 12 (shifts have lesser latency than VPMULLQ).
  VPSLLQ $3, 0(VIRT_VALUES)(R8*1), Z20
  VPSLLQ $3, 64(VIRT_VALUES)(R8*1), Z21
  VPSRLQ $1, Z20, Z4
  VPSRLQ $1, Z21, Z5
  VPADDQ Z4, Z20, Z20
  VPADDQ Z5, Z21, Z21

  JMP dateaddmonth_tail(SB)

// Tail instruction implementing DATE_ADD(MONTH, interval, timestamp).
//
// Inputs:
//   K1      - 16-bit mask
//   Z2/Z3   - Timestamp values
//   Z20/Z21 - Months to add
TEXT dateaddmonth_tail(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

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

  VSUBPD.BCST CONSTF64_11(), Z12, K3, Z12
  VSUBPD.BCST CONSTF64_11(), Z13, K4, Z13

  VDIVPD.RD_SAE Z20, Z12, Z12
  VDIVPD.RD_SAE Z20, Z13, Z13

  VCVTPD2QQ.RD_SAE Z12, Z12
  VCVTPD2QQ.RD_SAE Z13, Z13

  // Z8/Z9 <- Final years (int).
  VPADDQ Z12, Z8, Z8
  VPADDQ Z13, Z9, Z9

  // Z10/Z11 <- Corrected month index [0, 11] (where 0 represents March).
  VPMULLQ.BCST CONSTQ_12(), Z12, Z12
  VPMULLQ.BCST CONSTQ_12(), Z13, Z13

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

  // Z6/Z7 <- Final number of days converted to microseconds.
  VPMULLQ.BCST CONSTQ_86400000000(), Z6, Z6
  VPMULLQ.BCST CONSTQ_86400000000(), Z7, Z7

  // Z6/Z7 <- Combined microseconds of all days and microseconds of the remaining day.
  VPADDQ Z4, Z6, Z6
  VPADDQ Z5, Z7, Z7

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z6, K1, Z2
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z7, K2, Z3

  NEXT()

// DATE_DIFF(DAY|HOUR|MINUTE|SECOND|MILLISECOND, t1, t2)
TEXT bcdatediffparam(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  VPBROADCASTQ 2(VIRT_PCREG), Z6

  KSHIFTRW $8, K1, K2
  ADDQ $10, VIRT_PCREG

  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5

  VPSUBQ Z2, Z4, Z4
  VPSUBQ Z3, Z5, Z5

  // We never need the last 3 bits of the value, so cut it off to increase precision.
  VPSRAQ $3, Z4, Z4
  VPSRAQ $3, Z5, Z5
  VPSRAQ $3, Z6, Z6

  VCVTQQ2PD Z6, Z6
  VCVTQQ2PD Z4, Z4
  VCVTQQ2PD Z5, Z5

  VDIVPD.RZ_SAE Z6, Z4, Z4
  VDIVPD.RZ_SAE Z6, Z5, Z5

  VCVTPD2QQ.RZ_SAE Z4, K1, Z2
  VCVTPD2QQ.RZ_SAE Z5, K2, Z3

  NEXT()

// DATE_DIFF(MONTH|YEAR, interval, timestamp)
TEXT bcdatediffmonthyear(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  KSHIFTRW $8, K1, K2

  VMOVDQU64 0(VIRT_VALUES)(R8*1), Z4
  VMOVDQU64 64(VIRT_VALUES)(R8*1), Z5

  MOVWQZX 2(VIRT_PCREG), R8
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

  // Z22/Z23 <- Lesser timestamp's 'Year * 12 + MonthIndex'.
  VPMULLQ.BCST CONSTQ_12(), Z8, Z8
  VPMULLQ.BCST CONSTQ_12(), Z9, Z9
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
  VPMULLQ.BCST CONSTQ_12(), Z8, Z8
  VPMULLQ.BCST CONSTQ_12(), Z9, Z9
  VPADDQ Z8, Z10, Z10
  VPADDQ Z9, Z11, Z11

  // Z4/Z5 <- Rough months difference (greater timestamp - lesser timestamp).
  VPSUBQ Z22, Z10, Z4
  VPSUBQ Z23, Z11, Z5

  // Z4/Z5 <- Rough months difference - 1.
  VPSUBQ.BCST CONSTQ_1(), Z4, Z4
  VPSUBQ.BCST CONSTQ_1(), Z5, Z5

  // Z10 <- Zeros
  // Z11 <- Multiplier used to implement the same bytecode for MONTH and YEAR difference.
  VPXORQ X10, X10, X10
  VPBROADCASTQ 0(R15)(R8 * 8), Z11

  // Increment one month if the lesser timestamp's day of month <= greater timestamp's day of month.
  VPCMPQ $VPCMP_IMM_GE, Z20, Z14, K3
  VPCMPQ $VPCMP_IMM_GE, Z21, Z15, K4

  VPADDQ.BCST CONSTQ_1(), Z4, K3, Z4
  VPADDQ.BCST CONSTQ_1(), Z5, K4, Z5

  // Z4/Z5 <- Final months difference - always positive at this point.
  VPMAXSQ Z10, Z4, Z4
  VPMAXSQ Z10, Z5, Z5

  // Z2/Z3 <- Final months/years difference - depending on the bytecode instruction's predicate.
  VPMULLQ Z11, Z4, Z4
  VPMULLQ Z11, Z5, Z5
  VPSRLQ $35, Z4, K1, Z2
  VPSRLQ $35, Z5, K2, Z3

  // Z2/Z3 <- Final months/years difference - positive or negative depending on which timestamp was greater.
  VPSUBQ Z2, Z10, K5, Z2
  VPSUBQ Z3, Z10, K6, Z3

  ADDQ $4, VIRT_PCREG
  NEXT()

// EXTRACT(MICROSECOND FROM timestamp) - the result includes seconds
TEXT bcdateextractmicrosecond(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_60000000(), Z4
  BC_MODU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  NEXT()

// EXTRACT(MILLISECOND FROM timestamp) - the result includes seconds
TEXT bcdateextractmillisecond(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_60000000(), Z4
  BC_MODU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST_MASKED(Z2, Z3, Z2, Z3, K1, K2, CONSTQ_274877907(), 38)
  NEXT()

// EXTRACT(SECOND FROM timestamp)
TEXT bcdateextractsecond(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_60000000(), Z4
  BC_MODU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST_MASKED(Z2, Z3, Z2, Z3, K1, K2, CONSTQ_1125899907(), 50)
  NEXT()

// EXTRACT(MINUTE FROM timestamp)
TEXT bcdateextractminute(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_3600000000(), Z4
  BC_MODU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPSRLQ $8, Z2, K1, Z2
  VPSRLQ $8, Z3, K2, Z3
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST_MASKED(Z2, Z3, Z2, Z3, K1, K2, CONSTQ_18764999(), 42)
  NEXT()

// EXTRACT(HOUR FROM timestamp)
TEXT bcdateextracthour(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_86400000000(), Z4
  BC_MODU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPSRLQ $12, Z2, K1, Z2
  VPSRLQ $12, Z3, K2, Z3
  BC_DIV_U64_WITH_CONST_RECIPROCAL_BCST_MASKED(Z2, Z3, Z2, Z3, K1, K2, CONSTQ_2562048517(), 51)
  NEXT()

// EXTRACT(DAY FROM timestamp)
TEXT bcdateextractday(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)
  VPADDQ.BCST CONSTQ_1(), Z14, K1, Z2
  VPADDQ.BCST CONSTQ_1(), Z15, K2, Z3
  NEXT()

// EXTRACT(MONTH FROM timestamp)
TEXT bcdateextractmonth(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ.BCST CONSTQ_3(), Z10, Z10
  VPADDQ.BCST CONSTQ_3(), Z11, Z11
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z10, K5
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z11, K6

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ.BCST CONSTQ_12(), Z10, K5, Z10
  VPSUBQ.BCST CONSTQ_12(), Z11, K6, Z11

  VMOVDQA64 Z10, K1, Z2
  VMOVDQA64 Z11, K2, Z3
  NEXT()

// EXTRACT(YEAR FROM timestamp)
TEXT bcdateextractyear(SB), NOSPLIT|NOFRAME, $0
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)
  KSHIFTRW $8, K1, K2

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ.BCST CONSTQ_3(), Z10, Z10
  VPADDQ.BCST CONSTQ_3(), Z11, Z11
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z10, K5
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z11, K6

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ.BCST CONSTQ_12(), Z10, K5, Z10
  VPSUBQ.BCST CONSTQ_12(), Z11, K6, Z11

  // Increment one year if required to adjust for the month greater than 12 after adding the final offset.
  VPADDQ.BCST CONSTQ_1(), Z8, K5, Z8
  VPADDQ.BCST CONSTQ_1(), Z9, K6, Z9

  VMOVDQA64 Z8, K1, Z2
  VMOVDQA64 Z9, K2, Z3
  NEXT()

TEXT bcdatetounixepoch(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  // Discard some bits so we can prepare the timestamp value for division.
  VPSRAQ $6, Z2, K1, Z2
  VPSRAQ $6, Z3, K2, Z3

  // 15625 == 1000000 >> 6
  VPXORQ X5, X5, X5
  VPBROADCASTQ CONSTQ_15625(), Z4

  VPCMPQ $VPCMP_IMM_LT, Z5, Z2, K1, K3
  VPCMPQ $VPCMP_IMM_LT, Z5, Z3, K2, K4

  VPSUBQ.BCST CONSTQ_1(), Z4, Z5

  VPSUBQ Z5, Z2, K3, Z2
  VPSUBQ Z5, Z3, K4, Z3

  BC_DIVI64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)
  NEXT()

// DATE_TRUNC(MILLISECOND, timestamp)
TEXT bcdatetruncmillisecond(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_1000(), Z4
  BC_DIVU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPMULLQ Z4, Z2, K1, Z2
  VPMULLQ Z4, Z3, K2, Z3
  NEXT()

// DATE_TRUNC(SECOND, timestamp)
TEXT bcdatetruncsecond(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_1000000(), Z4
  BC_DIVU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPMULLQ Z4, Z2, K1, Z2
  VPMULLQ Z4, Z3, K2, Z3
  NEXT()

// DATE_TRUNC(MINUTE, timestamp)
TEXT bcdatetruncminute(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_60000000(), Z4
  BC_DIVU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPMULLQ Z4, Z2, K1, Z2
  VPMULLQ Z4, Z3, K2, Z3
  NEXT()

// DATE_TRUNC(HOUR, timestamp)
TEXT bcdatetrunchour(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_3600000000(), Z4
  BC_DIVU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPMULLQ Z4, Z2, K1, Z2
  VPMULLQ Z4, Z3, K2, Z3
  NEXT()

// DATE_TRUNC(DAY, timestamp)
TEXT bcdatetruncday(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2
  VPBROADCASTQ CONSTQ_86400000000(), Z4
  BC_DIVU64_IMPL(Z2, Z3, Z2, Z3, Z4, Z4, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, K3, K4)
  VPMULLQ Z4, Z2, K1, Z2
  VPMULLQ Z4, Z3, K2, Z3
  NEXT()

// DATE_TRUNC(MONTH, timestamp)
TEXT bcdatetruncmonth(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  // Z8/Z9 <- Year index.
  // Z10/Z11 <- Month index - starting from zero, where zero represents March.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // Z4/Z5 <- Number of days in a year [0, 365] got from MonthIndex.
  VMOVDQU64 CONST_GET_PTR(consts_days_until_month_from_march, 0), Z13
  VPERMD Z13, Z10, Z4
  VPERMD Z13, Z11, Z5

  // Z4/Z5 <- Number of days of all years, including days in the last month.
  BC_COMPOSE_YEAR_TO_DAYS(Z4, Z5, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  // Z4/Z5 <- Final number of days converted to microseconds.
  VPMULLQ.BCST CONSTQ_86400000000(), Z4, Z4
  VPMULLQ.BCST CONSTQ_86400000000(), Z5, Z5

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z4, K1, Z2
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z5, K2, Z3

  NEXT()

// DATE_TRUNC(YEAR, timestamp)
TEXT bcdatetruncyear(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  // Z8/Z9 <- Year index.
  // Z10/Z11 <- Month index - starting from zero, where zero represents March.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // Since the month starts from March, we have to check whether the truncation doesn't
  // need to increment one year (January/Februare have 10/11 indexes, respectively)
  VPCMPUQ.BCST $VPCMP_IMM_LT, CONSTQ_10(), Z10, K3
  VPCMPUQ.BCST $VPCMP_IMM_LT, CONSTQ_10(), Z11, K4

  // Increment one year if required.
  VPSUBQ.BCST CONSTQ_1(), Z8, K3, Z8
  VPSUBQ.BCST CONSTQ_1(), Z9, K4, Z9

  VPBROADCASTQ CONSTQ_306(), Z4
  VMOVDQA64 Z4, Z5

  BC_COMPOSE_YEAR_TO_DAYS(Z4, Z5, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15)

  // Z4/Z5 <- Final number of days converted to microseconds.
  VPMULLQ.BCST CONSTQ_86400000000(), Z4, Z4
  VPMULLQ.BCST CONSTQ_86400000000(), Z5, Z5

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z4, K1, Z2
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z5, K2, Z3

  NEXT()

TEXT bcunboxts(SB), NOSPLIT|NOFRAME, $0
  // TernLog:
  //   VPTERNLOG(0xD8) == (A & ~C) | (B & C) == Blend(A, B, ~C)
  KSHIFTRW $8, K1, K2

  // Z4/Z5 <- First 8 bytes of the timestamp to process, ignoring
  //          timezone offset, which is assumed to be zero.
  VEXTRACTI32X8 $1, Z2, Y21
  KMOVB K1, K3
  KSHIFTRW $8, K1, K4
  VPXORQ X4, X4, X4
  VPXORQ X5, X5, X5
  VPGATHERDQ 1(SI)(Y2*1), K3, Z4
  VPGATHERDQ 1(SI)(Y21*1), K4, Z5

  // Z20/Z21 <- Frequently used constants to avoid broadcasts.
  VPBROADCASTQ CONSTQ_0x7F(), Z20
  VPBROADCASTQ CONSTQ_0x80(), Z21
  VPBROADCASTQ CONSTQ_1(), Z22
  VPBROADCASTQ CONSTD_8(), Z23

  // Z4/Z5 <- First 8 bytes of the timestamp cleared so only bytes that
  //          are within the length are non-zero, other bytes cleared.
  VPMINUD Z23, Z3, Z16
  VPSUBD Z16, Z23, Z16
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
  VPTERNLOGQ $0xD8, Z20, Z4, K3, Z6
  VPTERNLOGQ $0xD8, Z20, Z5, K4, Z7
  VPSRLQ $8, Z4, K3, Z4
  VPSRLQ $8, Z5, K4, Z5

  VPTESTNMQ Z21, Z4, K3, K3
  VPTESTNMQ Z21, Z5, K4, K4
  VPSLLQ $7, Z6, K3, Z6
  VPSLLQ $7, Z7, K4, Z7
  VPTERNLOGQ $0xD8, Z20, Z4, K3, Z6
  VPTERNLOGQ $0xD8, Z20, Z5, K4, Z7
  VPSRLQ $8, Z4, K3, Z4
  VPSRLQ $8, Z5, K4, Z5

  // Z4/Z5 <- [?|?|?|Second|Minute|Hour|Day|Month] with 0x80 bit cleared in each value.
  VPANDQ.BCST CONSTQ_0x0000007F7F7F7F7F(), Z4, Z4
  VPANDQ.BCST CONSTQ_0x0000007F7F7F7F7F(), Z5, Z5

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
  VPCMPD.BCST $VPCMP_IMM_GT, CONSTD_10(), Z3, K3
  VPADDD Z2, Z3, Z19
  VPXORD X18, X18, X18
  VPGATHERDD -4(SI)(Z19*1), K3, Z18

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
  VPSUBQ.BCST CONSTQ_1(), Z8, Z8
  VPSUBQ.BCST CONSTQ_1(), Z9, Z9

  // Z8/Z9 <- Number of days of all years, including days in the last month.
  BC_COMPOSE_YEAR_TO_DAYS(Z8, Z9, Z6, Z7, Z10, Z11, Z12, Z13, Z14, Z15)

  // Z18 <- Convert last 4 bytes of the timestamp to microseconds (it's either a value or zero).
  VPSHUFB CONST_GET_PTR(bswap24_zero_last_byte, 0), Z18, Z18

  // Z8/Z9 <- Final number of days converted to microseconds.
  VPMULLQ.BCST CONSTQ_86400000000(), Z8, Z8
  VPMULLQ.BCST CONSTQ_86400000000(), Z9, Z9

  // Z8/Z9 <- Combined microseconds of all days and microseconds of the remaining day.
  VEXTRACTI32X8 $1, Z18, Y19
  VPMULLQ.BCST CONSTQ_1000000(), Z4, Z4
  VPMULLQ.BCST CONSTQ_1000000(), Z5, Z5
  VPMOVZXDQ Y18, Z18
  VPMOVZXDQ Y19, Z19
  VPADDQ Z4, Z8, Z8
  VPADDQ Z5, Z9, Z9
  VPADDQ Z18, Z8, Z8
  VPADDQ Z19, Z9, Z9

  // Z2/Z3 <- Make it a unix timestamp starting from 1970-01-01.
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z8, K1, Z2
  VPSUBQ.BCST CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET(), Z9, K2, Z3

  NEXT()

TEXT bcboxts(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  MOVQ bytecode_scratch+8(VIRT_BCPTR), R15             // R15 = Output buffer length.
  MOVQ bytecode_scratch+16(VIRT_BCPTR), R8             // R8 = Output buffer capacity.
  SUBQ R15, R8                                         // R8 = Remaining space in the output buffer.

  // Make sure we have at least 16 bytes for each lane, we always overallocate to make the boxing simpler.
  CMPQ R8, $(16 * 16)
  JLT abort

  // Z20 - base offset for serialized timestamps.
  VPBROADCASTQ R15, Z20
  VPBROADCASTD R15, Z30

  // Update the length of the output buffer.
  LEAQ 256(R15), R8
  MOVQ R8, bytecode_scratch+8(VIRT_BCPTR)

  // A base pointer where each timestamp will be serialized.
  MOVQ bytecode_scratch+0(VIRT_BCPTR), R15

  // Decompose the timestamp value into Year/Month/DayOfMonth and microseconds of the day.
  //
  // Z4/Z5   - Microseconds of the day (combines hours, minutes, seconds, microseconds).
  // Z8/Z9   - Year index.
  // Z10/Z11 - Month index - starting from zero, where zero represents March.
  // Z14/Z15 - Day of month - starting from zero.
  BC_DECOMPOSE_TIMESTAMP_PARTS(Z2, Z3)

  // Convert our MonthIndex into a month in a range from [1, 12], where 1 is January.
  VPADDQ.BCST CONSTQ_3(), Z10, Z10
  VPADDQ.BCST CONSTQ_3(), Z11, Z11
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z10, K5
  VPCMPUQ.BCST $VPCMP_IMM_GT, CONSTQ_12(), Z11, K6

  // Increment one year if required to adjust for the month greater than 12 after adding the final offset.
  VPADDQ.BCST CONSTQ_1(), Z8, K5, Z8
  VPADDQ.BCST CONSTQ_1(), Z9, K6, Z9

  // Wrap the month if it was greater than 12 after adding the final offset.
  VPSUBQ.BCST CONSTQ_12(), Z10, K5, Z10
  VPSUBQ.BCST CONSTQ_12(), Z11, K6, Z11

  // Increment one day to make the day of the month start from 1.
  VPADDQ.BCST CONSTQ_1(), Z14, Z14
  VPADDQ.BCST CONSTQ_1(), Z15, Z15

  // Construct Type|L, Offset, Year, Month, and DayOfMonth data, where:
  //   - Type|L is  (one byte).
  //   - Offset [0] (one byte).
  //   - Year (1 to 3 bytes).
  //   - Month [1, 12] (one byte)
  //   - DayOfMonth [1, 31] (one byte)
  //
  // Notes:
  //   - VPTERNLOG(0xD8) == (A & ~C) | (B & C) == Blend(A, B, ~C)

  // Z10/Z11 <- [DayOfMonth, Month, 0].
  VPSLLQ $16, Z14, Z14
  VPSLLQ $16, Z15, Z15
  VPSLLQ $8, Z10, Z10
  VPSLLQ $8, Z11, Z11
  VPBROADCASTQ CONSTQ_0x7F(), Z16
  VPBROADCASTQ CONSTQ_1(), Z17
  VPORQ Z14, Z10, Z10
  VPORQ Z15, Z11, Z11

  // Z14/Z15 <- Initial L field (length) is 7 bytes - Offset, Year (1 byte), Month, DayOfMonth, Hour, Minute, Second).
  //   - Modified by the algorithm depending on the year's length.
  //   - Used later to calculate the offset to the higher value (representing Hour/Minute/Second/Microsecond).
  VPBROADCASTQ CONSTQ_7(), Z14
  VPBROADCASTQ CONSTQ_7(), Z15

  // Z10/Z11 <- [DayOfMonth, Month, Year (1 byte)].
  VPTERNLOGQ $0xD8, Z16, Z8, Z10
  VPTERNLOGQ $0xD8, Z16, Z9, Z11
  VPORQ.BCST CONSTQ_0x0000000000808080(), Z10, Z10
  VPORQ.BCST CONSTQ_0x0000000000808080(), Z11, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-2 bytes)].
  VPCMPQ $VPCMP_IMM_GT, Z16, Z8, K5
  VPCMPQ $VPCMP_IMM_GT, Z16, Z9, K6
  VPSRLQ $7, Z8, Z8
  VPSRLQ $7, Z9, Z9
  VPADDQ Z17, Z14, K5, Z14
  VPADDQ Z17, Z15, K6, Z15
  VPSLLQ $8, Z10, K5, Z10
  VPSLLQ $8, Z11, K6, Z11
  VPTERNLOGQ $0xD8, Z16, Z8, K5, Z10
  VPTERNLOGQ $0xD8, Z16, Z9, K6, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-3 bytes)].
  VPCMPQ $VPCMP_IMM_GT, Z16, Z8, K5
  VPCMPQ $VPCMP_IMM_GT, Z16, Z9, K6
  VPSRLQ $7, Z8, Z8
  VPSRLQ $7, Z9, Z9
  VPADDQ Z17, Z14, K5, Z14
  VPADDQ Z17, Z15, K6, Z15
  VPSLLQ $8, Z10, K5, Z10
  VPSLLQ $8, Z11, K6, Z11
  VPTERNLOGQ $0xD8, Z16, Z8, K5, Z10
  VPTERNLOGQ $0xD8, Z16, Z9, K6, Z11

  // Z10/Z11 <- [DayOfMonth, Month, Year (1-3 bytes), Offset (always zero), Type|L (without a possible microsecond encoding length)].
  VPSLLQ $16, Z10, Z10
  VPSLLQ $16, Z11, Z11
  VPTERNLOGQ.BCST $0xFE, CONSTQ_0x0000000000008060(), Z14, Z10
  VPTERNLOGQ.BCST $0xFE, CONSTQ_0x0000000000008060(), Z15, Z11

  // Z14/Z15 - The size of the lower value of the encoded timestamp, in bytes, including Type|L field.
  VPSUBQ.BCST CONSTQ_2(), Z14, Z14
  VPSUBQ.BCST CONSTQ_2(), Z15, Z15

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
  VPMULLQ.BCST CONSTQ_1000000(), Z12, Z16
  VPMULLQ.BCST CONSTQ_1000000(), Z13, Z17
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

  // Z20/Z21 - offsets to scratch buffer (where each timestamp value starts, overallocated).
  VPADDQ CONST_GET_PTR(consts_offsets_q_16, 64), Z20, Z21
  VPADDQ CONST_GET_PTR(consts_offsets_q_16, 0), Z20, Z20

  KMOVB K1, K3
  KSHIFTRW $8, K1, K4
  VPSCATTERQQ Z10, K3, 0(R15)(Z20*1)
  VPSCATTERQQ Z11, K4, 0(R15)(Z21*1)

  VPADDQ Z14, Z20, Z20
  VPADDQ Z15, Z21, Z21
  KMOVB K1, K3
  KSHIFTRW $8, K1, K4
  VPSCATTERQQ Z4, K3, 0(R15)(Z20*1)
  VPSCATTERQQ Z5, K4, 0(R15)(Z21*1)

  VPADDD.Z CONST_GET_PTR(consts_offsets_d_16, 0), Z30, K1, Z30
  VNOTK(Z30, K1, Z30)

  VPMOVQD Z10, Y10
  VPMOVQD Z11, Y11
  VINSERTI64X4 $1, Y11, Z10, Z31
  VPADDD.BCST CONSTD_1(), Z31, Z31
  VPANDD.BCST.Z CONSTD_0x0F(), Z31, K1, Z31

  NEXT()

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// compare
//   (Z9/Z10/Z11) timestamp and
//   (Z6/Z7/Z8) timestamp
// as
//   (Z6 < Z9) || (Z6 == Z9 && Z11 < Z8)
//
// note that comparisons are *unsigned*
#define TIME_COMPARE_TAIL(imm)    \
  KSHIFTRW $8, K1, K2             \
  VPCMPUQ  imm, Z9, Z6, K1, K3    \
  VPCMPUQ  imm, Z10, Z7, K2, K4   \
  VPCMPEQQ Z9, Z6, K1, K1         \
  VPCMPEQQ Z10, Z7, K2, K2        \
  KUNPCKBW K1, K2, K2             \
  KUNPCKBW K3, K4, K1             \
  VPCMPUD  imm, Z11, Z8, K2, K2   \
  KORW     K1, K2, K1

// compare two timestamps using '<'
// with the following register layout:
//   Z6: lhs first 8 timestamps, first 8 sig. bytes
//   Z7: lhs second 8 timestamps, first 8 sig. bytes
//   Z8: lhs all 16 timestamps, last 4 bytes
//   Z9-Z11: same as above, rhs
//
// the bcconsttm() instruction prepares
// registers according to this ABI
TEXT bctimelt(SB), NOSPLIT|NOFRAME, $0
  TIME_COMPARE_TAIL($VPCMP_IMM_LT)
  NEXT()

// same as above, with direction reversed
TEXT bctimegt(SB), NOSPLIT|NOFRAME, $0
  TIME_COMPARE_TAIL($VPCMP_IMM_GT)
  NEXT()

// load constant timestamp plus
// variable timestamp in Z2:Z3;
// this instruction should always be
// followed by bctimegt() or bctimelt()
// (see above)
TEXT bcconsttm(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R8)
  VPXORD       Z6, Z6, Z6
  VPXORD       Z7, Z7, Z7
  VPXORD       Z8, Z8, Z8
  VPXORD       Z11, Z11, Z11  // microseconds = 0
  MOVQ         0(R8), R15     // R15 = &constant[0]
  CMPQ         8(R8), $13     // if len(constant)==13, then microsecond component exists
  JNE          no_tail
  MOVL         9(R15), R8
  BSWAPL       R8
  ANDL         $0xFFFFFF, R8
  VPBROADCASTD R8, Z11        // microseconds = bswap32(encoded[8:]) & 0xFFFFFF
no_tail:
  MOVQ         1(R15), R15
  BSWAPQ       R15
  VPBROADCASTQ R15, Z9        // Z9 = bswap64(first 8 bytes of timestamp)
  VMOVDQA64    Z9, Z10        // Z10 = same as Z9
  // now load variable portion:
  VBROADCASTI64X2  CONST_GET_PTR(bswap64, 0), Z20
  VBROADCASTI32X4  CONST_GET_PTR(bswap24_zero_last_byte, 0), Z24
  KMOVB            K1, K2
  KSHIFTRW         $8, K1, K3
  VEXTRACTI32X8    $1, Z2, Y4
  VPGATHERDQ       0(SI)(Y2*1), K2, Z6 // first 8 lanes, 8 sig. bytes
  VPGATHERDQ       0(SI)(Y4*1), K3, Z7 // second 8 lanes, 8 sig. bytes
  VPCMPEQD.BCST    CONSTD_12(), Z3, K1, K2
  VPGATHERDD       8(SI)(Z2*1), K2, Z8 // all 16 lanes, last 4 bytes when length=12
  VPSHUFB          Z20, Z6, Z6         // bswap first 8 bytes in all 16 lanes
  VPSHUFB          Z20, Z7, Z7
  VPSHUFB          Z24, Z8, Z8         // bswap microseconds in all 16 lanes
  NEXT()

// TODO: This is a remainder from an older timestamp code still in use.
#define TIME_LO  Z5
#define TIME_HI  Z6
#define MASK_LO  Z7
#define MASK_HI  Z8
#define MERGE_LO Z9
#define MERGE_HI Z10
#define TMPZ     Z11
#define TMPY     Y11 /* needs to point to same register as TMPZ */
#define TMP_LO   Z12
#define TMP_HI   Z13

// Load a timestamp from Z2:Z3 into Z5:Z6
// while taking the proper length in Z3 into
// account to 'normalize' unspecified components
#define TIMESTAMP_LOAD_LE                              \
    KMOVB         K1, K2                               \
    KSHIFTRW      $8, K1, K3                           \
    VEXTRACTI32X8 $1, Z2, Y4                           \
    VPGATHERDQ    0(SI)(Y2*1), K2, TIME_LO             \
    VPGATHERDQ    0(SI)(Y4*1), K3, TIME_HI             \
                                                       \
    VPCMPUD.BCST $5, CONSTD_8(), Z3, K1, K2            \
    KTESTW       K1, K2                                \
    /* skip truncation in case all lengths >= 8 */     \
    JC           skip_truncation                       \
                                                       \
    /* compute shift for mask to */                    \
    /* blend out the fields      */                    \
    VPBROADCASTD CONSTD_8(), TMPZ                      \
    VPSUBD       Z3, TMPZ, TMPZ                        \
    VPSLLD       $3, TMPZ, TMPZ                        \
                                                       \
    /* expand shifts into two Z registers */           \
    VPMOVZXDQ     TMPY, TMP_LO                         \
    VEXTRACTI32X8 $1, TMPZ, TMPY                       \
    VPMOVZXDQ     TMPY, TMP_HI                         \
                                                       \
    /* create masks for bytes to keep */               \
    /* by shifting in 0s from the MSB */               \
    VPBROADCASTQ CONSTQ_NEG_1(), MASK_LO               \
    VMOVDQU32    MASK_LO, MASK_HI                      \
    VPSRLVQ      TMP_LO, MASK_LO, MASK_LO              \
    VPSRLVQ      TMP_HI, MASK_HI, MASK_HI              \
                                                       \
    /* mask to merge in '1' for */                     \
    /* both day & month in case */                     \
    /* they are cleared out     */                     \
    VPBROADCASTQ CONSTQ_0x0000000101000000(), TMP_LO   \
    VPANDNQ      TMP_LO, MASK_LO, MERGE_LO             \
    VPANDNQ      TMP_LO, MASK_HI, MERGE_HI             \
                                                       \
    /* make sure termination bits are always set */    \
    VPBROADCASTQ CONSTQ_0x8080808080800080(), TMP_LO   \
    VPORQ        TMP_LO, MERGE_LO, MERGE_LO            \
    VPORQ        TMP_LO, MERGE_HI, MERGE_HI            \
                                                       \
    /* only modify lanes with lengths < 8 */           \
    KNOTW        K2, K3                                \
    /* do TIME_LO = TIME_LO & MASK_LO | MERGE_LO */    \
    VPTERNLOGQ   $0xEA, MERGE_LO, MASK_LO, K3, TIME_LO \
    KSHIFTRW     $8, K3, K3                            \
    VPTERNLOGQ   $0xEA, MERGE_HI, MASK_HI, K3, TIME_HI \
                                                       \
skip_truncation:

// bctmextract
//  input:
//   K1: lanes
//   Z2: timestamp offset
//   Z3: timestamp length
//   R8: year/month/day/hour/minute/second
//  output:
//   Z2: extracted value (lower 8)
//   Z3: extracted value (upper 8)
//  clobbers:
//   Z4-Z11, K2-K3
TEXT bctmextract(SB), NOSPLIT|NOFRAME, $0
    TIMESTAMP_LOAD_LE
    MOVBQZX      0(VIRT_PCREG), R8
    ADDQ         $1, VIRT_PCREG
    CMPQ         R8, $0
    JZ           years

    // extract single byte
    SHLQ         $3, R8
    ADDQ         $16, R8
    VPBROADCASTQ R8, Z4
    // extract months/days/hours/minutes/seconds
    VPSRLVQ      Z4, TIME_LO, Z10
    VPSRLVQ      Z4, TIME_HI, Z11
    VPBROADCASTQ CONSTQ_0x7F(), Z9
    VPANDQ       Z9, Z10, Z2  // months (lower)
    VPANDQ       Z9, Z11, Z3  // months (upper)
    JMP          done

years:
    // extract years
    VPSRLQ       $16, TIME_LO, Z7
    VPSRLQ       $16, TIME_HI, Z8
    VPBROADCASTQ CONSTQ_0x7F(), Z9
    VPANDQ       Z9, Z7, Z7
    VPANDQ       Z9, Z8, Z8
    VPSRLQ       $1, TIME_LO, Z10
    VPSRLQ       $1, TIME_HI, Z11
    VPBROADCASTQ CONSTQ_0x3F80(), Z4
    VPANDQ       Z4, Z10, Z10
    VPANDQ       Z4, Z11, Z11
    VPORQ        Z7, Z10, Z2
    VPORQ        Z8, Z11, Z3

done:
    NEXT()

#undef TIME_LO
#undef TIME_HI
#undef MASK_LO
#undef MASK_HI
#undef MERGE_LO
#undef MERGE_HI
#undef TMPZ
#undef TMPY
#undef TMP_LO
#undef TMP_HI
#undef TIMESTAMP_LOAD_LE

// Bucket Instructions
// -------------------

// Widthbucket (float)
//
// WIDTH_BUCKET semantics is as follows:
//   - When the input is less than MIN, the output is 0
//   - When the input is greater than or equal to MAX, the output is BucketCount+1
//
// Some references that I have found that explicitly state that MAX is outside:
//   - https://www.oreilly.com/library/view/sql-in-a/9780596155322/re91.html
//   - https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions214.htm
//   - https://docs.snowflake.com/en/sql-reference/functions/width_bucket.html
TEXT bcwidthbucketf(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW      $8, K1, K2

  // MinValue
  MOVWQZX       0(VIRT_PCREG), R8
  VMOVUPD.Z     0(VIRT_VALUES)(R8*1), K1, Z4
  VMOVUPD.Z     64(VIRT_VALUES)(R8*1), K2, Z5

  // MaxValue
  MOVWQZX       2(VIRT_PCREG), R8
  VMOVUPD.Z     0(VIRT_VALUES)(R8*1), K1, Z6
  VMOVUPD.Z     64(VIRT_VALUES)(R8*1), K2, Z7

  // Value = Input - MinValue
  VSUBPD.RD_SAE Z4, Z2, K1, Z2
  VSUBPD.RD_SAE Z5, Z3, K2, Z3

  // ValueRange = MaxValue - MinValue
  VSUBPD.RD_SAE Z4, Z6, Z6
  VSUBPD.RD_SAE Z5, Z7, Z7

  // Value = (Input - MinValue) / (MaxValue - MinValue)
  VDIVPD.RD_SAE Z6, Z2, K1, Z2
  VDIVPD.RD_SAE Z7, Z3, K2, Z3

  // BucketCount
  MOVWQZX       4(VIRT_PCREG), R8
  ADDQ          $6, VIRT_PCREG

  VMOVUPD.Z     0(VIRT_VALUES)(R8*1), K1, Z4
  VMOVUPD.Z     64(VIRT_VALUES)(R8*1), K2, Z5

  // Value = ((Input - MinValue) / (MaxValue - MinValue)) * BucketCount
  VMULPD.RD_SAE Z4, Z2, K1, Z2
  VMULPD.RD_SAE Z5, Z3, K2, Z3

  // Round to integer - this operation would preserve special numbers (Inf/NaN).
  VRNDSCALEPD   $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z2, K1, Z2
  VRNDSCALEPD   $(VROUND_IMM_DOWN | VROUND_IMM_SUPPRESS), Z3, K2, Z3

  // Restrict output values to [0, BucketCount + 1] range
  VBROADCASTSD  CONSTF64_1(), Z6
  VMINPD        Z4, Z2, K1, Z2
  VMINPD        Z5, Z3, K2, Z3
  VADDPD        Z6, Z2, K1, Z2
  VADDPD        Z6, Z3, K2, Z3
  VXORPD        X6, X6, X6
  VMAXPD        Z6, Z2, K1, Z2
  VMAXPD        Z6, Z3, K2, Z3

  NEXT()

// widthbucket (int)
//
// NOTE: This function has some precision loss when the arithmetic exceeds 2^53.
TEXT bcwidthbucketi(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  // MinValue.I64
  MOVWQZX 0(VIRT_PCREG), R8
  VMOVDQU64.Z 0(VIRT_VALUES)(R8*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(R8*1), K2, Z5

  // MaxValue.I64
  MOVWQZX 2(VIRT_PCREG), R8
  VMOVDQU64.Z 0(VIRT_VALUES)(R8*1), K1, Z6
  VMOVDQU64.Z 64(VIRT_VALUES)(R8*1), K2, Z7

  // K3/K4 = Value < MinValue
  VPCMPQ $VPCMP_IMM_LT, Z4, Z2, K1, K3
  VPCMPQ $VPCMP_IMM_LT, Z5, Z3, K2, K4

  // Value.U64 = Input - MinValue
  VPSUBQ Z4, Z2, K1, Z2
  VPSUBQ Z5, Z3, K2, Z3

  // ValueRange.U64 = MaxValue - MinValue
  VPSUBQ Z4, Z6, Z6
  VPSUBQ Z5, Z7, Z7

  // Value.F64 = (F64)Value.U64
  VCVTUQQ2PD Z2, K1, Z2
  VCVTUQQ2PD Z3, K2, Z3

  // ValueRange.F64 = (F64)ValueRange.U64
  VCVTUQQ2PD Z6, Z6
  VCVTUQQ2PD Z7, Z7

  // Value.F64 = (Input - MinValue) / (MaxValue - MinValue)
  VDIVPD.RD_SAE Z6, Z2, K1, Z2
  VDIVPD.RD_SAE Z7, Z3, K2, Z3

  // BucketCount.U64
  MOVWQZX 4(VIRT_PCREG), R8
  ADDQ $6, VIRT_PCREG

  VMOVDQU64.Z 0(VIRT_VALUES)(R8*1), K1, Z4
  VMOVDQU64.Z 64(VIRT_VALUES)(R8*1), K2, Z5

  // BucketCount.F64 = (F64)BucketCount.U64
  VCVTQQ2PD Z4, Z6
  VCVTQQ2PD Z5, Z7

  // Value.F64 = ((Input - MinValue) / (MaxValue - MinValue)) * BucketCount
  VMULPD.RD_SAE Z6, Z2, K1, Z2
  VMULPD.RD_SAE Z7, Z3, K2, Z3

  // Value.I64 = (I64)Value.F64
  VCVTTPD2QQ Z2, K1, Z2
  VCVTTPD2QQ Z3, K2, Z3

  // Restrict output values to [0, BucketCount + 1] range
  VPBROADCASTQ CONSTQ_1(), Z10
  VPMINSQ Z4, Z2, K1, Z2
  VPMINSQ Z5, Z3, K2, Z3
  VPADDQ Z10, Z2, K1, Z2
  VPADDQ Z10, Z3, K2, Z3
  VPXORQ Z2, Z2, K3, Z2
  VPXORQ Z3, Z3, K4, Z3

  NEXT()

// timebucket (timestamp)
TEXT bctimebucketts(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW     $8, K1, K2

  // Load interval from stack
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG
  VMOVDQU64.Z   0(VIRT_VALUES)(R8*1), K1, Z4
  VMOVDQU64.Z   64(VIRT_VALUES)(R8*1), K2, Z5

  BC_MODI64_IMPL(Z16, Z17, Z2, Z3, Z4, Z5, K1, K2, Z6, Z7, Z8, Z9, Z10, Z11, Z12, Z13, Z14, Z15, K3, K4)

  // subtract modulo value from source in order
  // to get the start value of the bucket
  VPSUBQ Z16, Z2, K1, Z2
  VPSUBQ Z17, Z3, K2, Z3

  NEXT()

// Geo Instructions
// ----------------

// GEO_GRID_INDEX is a 64-bit value that has encoded the following fields:
//
//   - Precision
//   - Latitude bits
//   - Longitude bits
//
// The value is encoded the following way:
//
//   - [zeros][precision bit][latitude bits][longitude bits]
//
// We can encode at most lat/lon with 31 bits of precision, how it works is
// illustrated below:
//
//   - [0      ][1][31 bits latitude][31 bits longitude]
//   - [000    ][1][30 bits latitude][30 bits longitude]
//   - [00000  ][1][29 bits latitude][29 bits longitude]
//   - [0000000][1][28 bits latitude][28 bits longitude]
//
// To get the precision you just have to count leading zeros:
//
//   - Precision = (63 - LZCNT(gg_index)) / 2
//
// This design has the following properties:
//
//   - GG_INDEX is always non-zero (there is always a bit present that determines precision)
//   - GG_INDEX is never negative (the precision bit is never at [63])
//   - It's trivial to encode/decode GG_INDEX
//
// In the original design of GG_INDEX, the precision was represented by 4 MSB bits, but this
// design had an issue - it was impossible to describe the precision required by ElasticSearch,
// because it allows to gradually set precision in bits up to 30 for each coordinate. With the
// current design, we can actually do that by counting zeros of each GG_INDEX value.
TEXT bcgeogridi(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  MOVWQZX 0(VIRT_PCREG), R8
  MOVWQZX 2(VIRT_PCREG), R15
  ADDQ $4, VIRT_PCREG

  // Z4/Z5 <- Latitude.
  VMOVUPD.Z Z2, K1, Z4
  VMOVUPD.Z Z3, K2, Z5

  // Z6/Z7 <- Longitude.
  VMOVUPD.Z 0(VIRT_VALUES)(R8*1), K1, Z6
  VMOVUPD.Z 64(VIRT_VALUES)(R8*1), K2, Z7

  // Z8/Z9 <- Precision in bits.
  VMOVDQU64.Z 0(VIRT_VALUES)(R15*1), K1, Z8
  VMOVDQU64.Z 64(VIRT_VALUES)(R15*1), K2, Z9

  JMP geogridi_tail(SB)

TEXT bcgeogridimmi(SB), NOSPLIT|NOFRAME, $0
  KSHIFTRW $8, K1, K2

  MOVWQZX 0(VIRT_PCREG), R8
  MOVWQZX 2(VIRT_PCREG), R15
  ADDQ $4, VIRT_PCREG

  // Z4/Z5 <- Latitude.
  VMOVAPD.Z Z2, K1, Z4
  VMOVAPD.Z Z3, K2, Z5

  // Z6/Z7 <- Longitude.
  VMOVUPD.Z 0(VIRT_VALUES)(R8*1), K1, Z6
  VMOVUPD.Z 64(VIRT_VALUES)(R8*1), K2, Z7

  // Z8/Z9 <- Precision in bits.
  VPBROADCASTQ R15, Z8
  VPBROADCASTQ R15, Z9

  JMP geogridi_tail(SB)

TEXT geogridi_tail(SB), NOSPLIT|NOFRAME, $0
  // Z14/Z15 <- Constants for scaling latitude (Z14) and longitude (Z15).
  VBROADCASTSD CONSTQ_0x3D86800000000000(), Z14
  VBROADCASTSD CONSTQ_0x3D96800000000000(), Z15

  // Scale latitude and longitude values.
  VDIVPD.RD_SAE Z14, Z4, Z4
  VDIVPD.RD_SAE Z14, Z5, Z5
  VDIVPD.RD_SAE Z15, Z6, Z6
  VDIVPD.RD_SAE Z15, Z7, Z7

  // Z14/Z15 <- Useful constants.
  VPBROADCASTQ CONSTQ_1(), Z14
  VPBROADCASTQ CONSTQ_46(), Z15

  // Z10/Z11 <- Mask calculated as (1 << Precision) - 1.
  VPSLLVQ Z8, Z14, Z10
  VPSLLVQ Z9, Z14, Z11
  VPSUBQ Z14, Z10, Z10
  VPSUBQ Z14, Z11, Z11

  // Z12/Z13 <- 1 << (Precision * 2).
  VPSLLQ $1, Z8, Z12
  VPSLLQ $1, Z9, Z13
  VPSLLVQ Z12, Z14, Z12
  VPSLLVQ Z13, Z14, Z13

  // Z8/Z9 <- How many bits to shift the scaled latitude/longitude to get the final bits.
  VPSUBQ Z8, Z15, Z14
  VPSUBQ Z9, Z15, Z15

  // Z4/Z5 <- Scaled latitude bits to an integer and chopped to only contain the requred bits.
  // Z6/Z7 <- Scaled longitude bits to an integer and chopped to only contain the requred bits.
  VBROADCASTSD CONSTQ_35184372088832(), Z16
  VCVTPD2QQ.RD_SAE Z4, Z4
  VCVTPD2QQ.RD_SAE Z5, Z5
  VCVTPD2QQ.RD_SAE Z6, Z6
  VCVTPD2QQ.RD_SAE Z7, Z7

  VPADDQ Z16, Z4, Z4
  VPADDQ Z16, Z5, Z5
  VPADDQ Z16, Z6, Z6
  VPADDQ Z16, Z7, Z7

  VPSRLVQ Z14, Z4, Z4
  VPSRLVQ Z15, Z5, Z5
  VPSRLVQ Z14, Z6, Z6
  VPSRLVQ Z15, Z7, Z7

  VPANDQ Z10, Z4, Z4
  VPANDQ Z11, Z5, Z5
  VPANDQ Z10, Z6, Z6
  VPANDQ Z11, Z7, Z7

  // Z2/Z3 <- Scaled latitude bits and shifted to the correct place.
  VPSLLVQ Z8, Z4, K1, Z2
  VPSLLVQ Z9, Z5, K2, Z3

  // Z2/Z3 <- Final index - precision bit marker, latitude, and longitude bits.
  VPTERNLOGQ $0xFE, Z12, Z6, K1, Z2
  VPTERNLOGQ $0xFE, Z13, Z7, K2, Z3

  NEXT()

// Find Symbol Instructions
// ------------------------

// findsym within Z0:Z1 starting at Z0
TEXT bcfindsym(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTD 0(VIRT_PCREG), Z22
  ADDQ         $4, VIRT_PCREG
  VMOVDQA32    Z0, Z30             // Z30 = offset
  VPADDD       Z1, Z0, Z26         // Z26 = end of struct
  JMP          findsym_tail(SB)

// findsym within Z0:Z1 starting at Z30
// or Z30+Z31 depending on the saved mask
// argument
TEXT bcfindsym2(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX      0(VIRT_PCREG), R8
  MOVL         2(VIRT_PCREG), DX
  ADDQ         $6, VIRT_PCREG
  LEAQ         0(VIRT_VALUES)(R8*1), R8
  KMOVW        0(R8), K2
  VPBROADCASTD DX, Z22
  VPADDD       Z30, Z31, K2, Z30
  VPADDD       Z0, Z1, Z26
  JMP          findsym_tail(SB)

// identical to above with reversed
// mask argument ordering
// (addend predicate in K1, active mask in stack slot)
TEXT bcfindsym2rev(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX      0(VIRT_PCREG), R8
  MOVL         2(VIRT_PCREG), DX
  ADDQ         $6, VIRT_PCREG
  LEAQ         0(VIRT_VALUES)(R8*1), R8
  VPBROADCASTD DX, Z22
  VPADDD       Z30, Z31, K1, Z30
  VPADDD       Z0, Z1, Z26
  KMOVW        0(R8), K1
  JMP          findsym_tail(SB)

// same as above, but the K1 argument
// is used for both the lane mask and
// the addend argument
TEXT bcfindsym3(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTD 0(VIRT_PCREG), Z22
  ADDQ         $4, VIRT_PCREG
  VPADDD       Z30, Z31, K1, Z30
  VPADDD       Z0, Z1, Z26
  JMP          findsym_tail(SB)

// inputs:
//   K1 = active lanes
//   Z30 = starting offset (dword)
//   Z26 = end of row (dword)
//   Z22 = symbol ID to match (dword)
//
// outputs:
//   K1 = lanes matched
//   Z30 = offset (location of symbol >= search)
//   Z31 = length (when K1 is set; undef otherwise)
//
TEXT findsym_tail(SB), NOSPLIT|NOFRAME, $0
  VPXORD       Z31, Z31, Z31         // Z31 = length
  KMOVW        K1, K5                // K5 = active
  KXORW        K0, K0, K1            // K1 = found
  VPBROADCASTD CONSTD_1(), Z21       // Z21 = 1
  VPBROADCASTD CONSTD_0x7F(), Z24    // Z24 = 0x7f
  VPBROADCASTD CONSTD_0x80(), Z25    // Z25 = 0x80
  JMP          looptail
loop:
  // load 4 bytes and process the leading uvarint
  XORL         DX, DX                // indicates jump to uvarintdone
  KMOVW        K5, K2
  VPGATHERDD   (SI)(Z30*1), K2, Z29  // Z29 = first 4 bytes
  VMOVDQA32.Z  Z21, K5, Z23          // Z23 = uvarint size = 1 (for now)
  VPANDD       Z24, Z29, Z28         // Z28 = accumulator = byte & 0x7f
  VPTESTNMD    Z25, Z29, K5, K4      // K4 = varint bit set
  KANDNW       K5, K4, K2
  VPSRLD       $8, Z29, K2, Z29      // shift to get descriptor byte as lsb
  KTESTW       K4, K4
  JNZ          uvarint_parse2        // slow path for symbol > 0x7f
uvarintdone:
  VPCMPD        $VPCMP_IMM_GE, Z28, Z22, K5, K5  // only keep lanes active where search >= symbol
  VPCMPEQD      Z28, Z22, K5, K2           // K2 = active & symbol matches
  KORW          K2, K1, K1                 // set result in K1
  VPADDD        Z23, Z30, K5, Z30          // update offset *when search >= symbol!*
  VPANDD.BCST   CONSTD_0xFF(), Z29, Z29    // make sure Z29 is just the lsb
  VPANDD.BCST   CONSTD_0x0F(), Z29, Z28    // Z28 = data&0xf
  VPCMPEQD.BCST CONSTD_0x0E(), Z28, K5, K6 // K6 = Z28==0xe (varint-encoded item)
  KANDNW        K5, K6, K4                 // K4 = active non-varint items
  VPCMPEQD.BCST CONSTD_0x0F(), Z28, K4, K3 // K3 = Z28==0xf (null item)
  VMOVDQA32     Z21, K3, Z31               // length = 1 if null
  KANDNW        K4, K3, K4                 // K4 = immediate but not null
  VPCMPEQD.BCST CONSTD_TRUE_BYTE(), Z29, K4, K3 // K3 = value is 'true'
  VPXORD        Z28, Z28, K3, Z28          // length = 0 for 0x11 ('true')
  VPADDD        Z21, Z28, K4, Z31          // ... in which case length = 1 + immediate
  KTESTW        K6, K6
  JZ            fieldlendone               // fast-path when everything is short

  // parse object size when uvarint-encoded
  INCL         DX                    // DX != 0 indicates jump to fieldlendone
  KMOVW        K6, K4
  VPGATHERDD   1(SI)(Z30*1), K4, Z29 // load next 4 bytes into Z29
  VPADDD       Z21, Z21, Z23         // Z23 = varint size + descriptor = 2 (currently)
  VPANDD.Z     Z24, Z29, K6, Z28     // Z28 = bytes&0x7f or 0 when not varint
  VPTESTNMD    Z25, Z29, K6, K4      // test bytes&0x80
  KTESTW       K4, K4
  JNZ          uvarint_parse3        // common case is 1-byte field length
fieldlendone:
  VPADDD       Z23, Z28, K6, Z31     // for varints: length = varint size + encoding size
  KANDNW       K5, K2, K5            // unset active lanes when symbol matched
  VPADDD       Z30, Z31, K5, Z30     // offset += length when not matched
looptail:
  VPCMPUD      $5, Z26, Z30, K5, K4
  KANDNW       K5, K4, K5            // unset active lanes when at end
  KTESTW       K5, K5                // early exit if we've consumed everything
  JNZ          loop
done:
  NEXT()
// un-rolled uvarint parsing
#define CHOMP()                  \
  VPADDD       Z21, Z23, K4, Z23 \
  VPSLLD       $7, Z28, K4, Z28  \
  VPSRLD       $8, Z29, K4, Z29  \
  VPANDD       Z24, Z29, Z27     \
  VPORD        Z27, Z28, K4, Z28 \
  VPTESTNMD    Z25, Z29, K4, K4
uvarint_parse3:
  CHOMP()
uvarint_parse2:
  CHOMP()
  CHOMP()
  KTESTW       K4, K4
  JNZ          trap                 // assert symbol < max symbol ID
  // since the Go assembler won't let you
  // compute the address of a label (AAAAAHHHH WHY)
  // we use DX to indicate the return branch target
  TESTL        DX, DX
  JNZ          fieldlendone
  JMP          uvarintdone
trap:
  FAIL()

#undef CHOMP

// Blend Instructions
// ------------------

// blend in saved values using K1
// on 32x 32-bit lanes across two registers
#define BLEND32(r0, r1)                    \
  MOVWQZX     0(VIRT_PCREG), R8            \
  ADDQ        $2, VIRT_PCREG               \
  VMOVDQU32   0(VIRT_VALUES)(R8*1), K1, r0 \
  VMOVDQU32   64(VIRT_VALUES)(R8*1), K1, r1

// like BLEND32(), but with the
// register/stack ordering reversed
#define BLEND32REV(r0, r1)                 \
  MOVWQZX     0(VIRT_PCREG), R8            \
  ADDQ        $2, VIRT_PCREG               \
  KXORW       K1, K7, K2                   \
  VMOVDQU32   0(VIRT_VALUES)(R8*1), K2, r0 \
  VMOVDQU32   64(VIRT_VALUES)(R8*1), K2, r1

// blend in saved values using K1
// on 16x 64-bit lanes across two registers
#define BLEND64(r0, r1)                    \
  MOVWQZX     0(VIRT_PCREG), R8            \
  ADDQ        $2, VIRT_PCREG               \
  KSHIFTRW    $8, K1, K2                   \
  VMOVDQU64   0(VIRT_VALUES)(R8*1), K1, r0 \
  VMOVDQU64   64(VIRT_VALUES)(R8*1), K2, r1

// blend in saved values using K1
// on 16x 64-bit lanes across two registers
#define BLEND64REV(r0, r1)                 \
  MOVWQZX     0(VIRT_PCREG), R8            \
  ADDQ        $2, VIRT_PCREG               \
  KXORW       K1, K7, K2                   \
  KSHIFTRW    $8, K2, K3                   \
  VMOVDQU64   0(VIRT_VALUES)(R8*1), K2, r0 \
  VMOVDQU64   64(VIRT_VALUES)(R8*1), K3, r1

// NOTE: PLEASE DO NOT RE-ORDER THE BLEND INSTRUCTIONS;
// the SSA code relies on the reversed-argument version
// of each blend instruction being the regular version
// opcode plus one

// blend stack slot into Z30+Z31 (value pointers)
TEXT bcblendv(SB), NOSPLIT|NOFRAME, $0
  BLEND32(Z30, Z31)
  NEXT()

// blend Z30+Z31 into stack slot value;
// return union of values
TEXT bcblendrevv(SB), NOSPLIT|NOFRAME, $0
  BLEND32REV(Z30, Z31)
  NEXT()

// blend Z2+Z3, assuming packed 64-bit integers or doubles
TEXT bcblendnum(SB), NOSPLIT|NOFRAME, $0
  BLEND64(Z2, Z3)
  NEXT()

// blend Z2+Z3, 64-bit layout, reversed
TEXT bcblendnumrev(SB), NOSPLIT|NOFRAME, $0
  BLEND64REV(Z2, Z3)
  NEXT()

// blend Z2+Z3, assuming slices (strings or timestamps)
TEXT bcblendslice(SB), NOSPLIT|NOFRAME, $0
  BLEND32(Z2, Z3)
  NEXT()

// blend Z2+Z3, assuming slices, reversed
TEXT bcblendslicerev(SB), NOSPLIT|NOFRAME, $0
  BLEND32REV(Z2, Z3)
  NEXT()

// Unboxing Instructions
// ---------------------

// unpack string/array/timestamp to scalar slice
TEXT bcunpack(SB), NOSPLIT|NOFRAME, $0
  MOVBLZX       0(VIRT_PCREG), R8
  ADDQ          $1, VIRT_PCREG
  KTESTW        K1, K1
  JZ            next
  VPBROADCASTD  R8, Z23                    // Z23 = descriptor tag
  KMOVW         K1, K2
  VPBROADCASTD  CONSTD_0x0F(), Z27         // Z27 = 0x0F
  VPGATHERDD    0(SI)(Z30*1), K2, Z26      // Z26 = first 4 bytes
  VPANDD        Z26, Z27, Z25              // Z25 = first 4 & 0x0f = int size
  VPCMPEQD      Z25, Z27, K1, K2           // K2 = field is null
  KANDNW        K1, K2, K1                 // unset str.null lanes
  VPSRLD        $4, Z26, Z26               // first 4 words >>= 4
  VPANDD        Z27, Z26, Z24              // Z24 = (word >> 4) & 0xf = descriptor tag
  VPCMPEQD      Z23, Z24, K1, K1           // match only descriptor tag
  KTESTW        K1, K1
  JZ            next
  VPCMPEQD.BCST CONSTD_0x0E(), Z25, K1, K2 // K2 = descriptor=e (varint-sized)
  KANDNW        K1, K2, K3                 // K3 = non-varint-sized strings
  VPADDD.BCST.Z CONSTD_1(), Z30, K1, Z2    // Z2 = base = offset+1 (will update later for varints)
  VMOVDQA32     Z25, K3, Z3                // Z3 = length = first4&0xf for non-varint-size
  KTESTW        K2, K2
  JZ            next                       // short-circuit if no varint-length objects
  // decode up to 3 varint bytes; we expect
  // not to see 4 bytes because our current chunk
  // alignment would not allow for objects over
  // 2^21 bytes long anyway...
  // TODO: if we need to support longer objects,
  // the end of this unrolled loop can do another
  // gatherdd and jump back up to the top here...
  VPBROADCASTD  CONSTD_1(), Z24         // Z24 = 0x01
  VPBROADCASTD  CONSTD_0x7F(), Z27      // Z27 = 0x7F
  VPBROADCASTD  CONSTD_0x80(), Z29      // Z29 = 0x80
  VPSRLD        $4, Z26, Z26            // now Z26 = 3 bytes following descriptor
  KMOVW         K2, K3
  VPANDD.Z      Z27, Z26, K3, Z28       // Z28 = byte1&0x7f = accumulator
  VPADDD        Z24, Z2, K3, Z2         // base+1
  VPTESTNMD     Z29, Z26, K3, K3        // test byte1&0x80
  KTESTW        K3, K3
  JZ            done
  // decode 2nd varint byte
  VPSRLD        $8, Z26, K3, Z26        // word >>= 8
  VPSLLD        $7, Z28, K3, Z28        // accum <<= 7
  VPANDD        Z27, Z26, K3, Z25
  VPORD         Z25, Z28, K3, Z28       // accum |= (word & 0x7f)
  VPADDD        Z24, Z2, K3, Z2         // base+1
  VPTESTNMD     Z29, Z26, K3, K3        // test word&0x80
  // decode 3rd varint byte
  VPSRLD        $8, Z26, K3, Z26        // word >>= 8
  VPSLLD        $7, Z28, K3, Z28        // accum <<= 7
  VPANDD        Z27, Z26, K3, Z25
  VPORD         Z25, Z28, K3, Z28       // accum |= (word & 0x7f)
  VPADDD        Z24, Z2, K3, Z2         // base+1
  VPTESTNMD     Z29, Z26, K3, K3        // test word&0x80
  KTESTW        K3, K3
  JNZ           trap                    // trap if length(object) > 2^21
done:
  VMOVDQA32     Z28, K2, Z3             // set Z3 = length
next:
  NEXT()
trap:
  FAIL()

// unpack (Z30:Z31).K1 into Z2|Z3 when integers
TEXT bctoint(SB), NOSPLIT|NOFRAME, $0
  KTESTW        K1, K1
  JZ            next
  KMOVW         K1, K2
  VPBROADCASTD  CONSTD_0x0F(), Z27      // Z27 = 0x0F
  VPGATHERDD    0(SI)(Z30*1), K2, Z28   // Z28 = first 4 bytes
  VPANDD        Z27, Z28, Z25           // Z25 = first 4 & 0x0f = int size
  VPCMPEQD      Z25, Z27, K1, K2        // K2 = field is null
  KANDNW        K1, K2, K1              // unset int.null lanes
  VPSRLD        $4, Z28, Z28
  VPANDD        Z27, Z28, Z24           // Z24 = (word >> 4) & 0xf = descriptor tag
  VPCMPEQD.BCST CONSTD_2(), Z24, K1, K2 // K2 = is uint
  VPCMPEQD.BCST CONSTD_3(), Z24, K1, K3 // K3 = is (signed) int
  KORW          K2, K3, K1              // K1 = is (any) integer

  // assert(!(size > 8))
  VPCMPD.BCST $6, CONSTD_8(), Z25, K1, K4
  KTESTW      K4, K4
  JNZ         trap

  // compute shift from size as (8-size)*8,
  // then zero-extend it to (Z25|Z26) as 16 quadwords
  VPBROADCASTD  CONSTD_8(), Z24
  VPSUBD        Z25, Z24, K1, Z25
  VPSLLD        $3, Z25, Z25
  VEXTRACTI32X8 $1, Z25, Y26
  VPMOVZXDQ     Y25, Z25
  VPMOVZXDQ     Y26, Z26

  // load 8-byte values and mask them appropriately
  KSHIFTRW      $8, K1, K4                // K4 = upper 8 mask
  VEXTRACTI32X8 $1, Z30, Y29              // Y29 = upper 8 offsets

  // gather (Y30|Y29) into (Z27|Z28) as 16 quadwords,
  // taking care to mask away sign bits
  KMOVB         K1, K5
  VPGATHERDQ    1(SI)(Y30*1), K5, Z27     // first 8
  KMOVB         K4, K5
  VPGATHERDQ    1(SI)(Y29*1), K5, Z28

  // now compute value &= (mask >> (8-size)*8)
  VONES(Z21)                           // Z21 = -1
  VPSRLVQ       Z25, Z21, Z29
  VPSRLVQ       Z26, Z21, Z22          // Z29|Z22 = -1 >> ((8-size) * 8) = masks
  VPANDQ        Z27, Z29, Z29
  VPANDQ        Z28, Z22, Z22          // Z22|Z29 = 8-byte value & mask

  // convert be64 in (Z29|Z22)  to le64
  VPSLLVQ          Z25, Z29, Z29
  VPSLLVQ          Z26, Z22, Z22          // (Z22|Z29) = (value & mask) <<= (8-size)*8
  VBROADCASTI64X2  bswap64<>(SB), Z27
  VPSHUFB          Z27, Z29, Z29
  VPSHUFB          Z27, Z22, Z22
  VMOVDQA64        Z29, K1, Z2
  VMOVDQA64        Z22, K4, Z3

  // there's no negate operation (or even a complement),
  // so we have to negate the register with (reg ^ -1)+1
  KSHIFTRW     $8, K3, K5
  VPBROADCASTQ CONSTQ_1(), Z22
  VPXORQ       Z2, Z21, K3, Z2
  VPXORQ       Z3, Z21, K5, Z3
  VPADDQ       Z22, Z2, K3, Z2
  VPADDQ       Z22, Z3, K5, Z3
next:
  NEXT()
trap:
  FAIL()

// current scalar = coerce(current value, f64)
TEXT bctof64(SB), NOSPLIT|NOFRAME, $0
  KTESTW        K1, K1
  JZ            next
  KMOVW         K1, K2
  VPBROADCASTD  CONSTD_0x0F(), Z27      // Z27 = 0x0F
  VPGATHERDD    0(SI)(Z30*1), K2, Z28   // Z28 = first 4 bytes
  VPANDD        Z27, Z28, Z25           // Z25 = first 4 & 0x0f = fp size
  VPCMPEQD      Z25, Z27, K1, K2        // K2 = field is null
  KANDNW        K1, K2, K1              // unset int.null lanes
  VPSRLD        $4, Z28, Z28
  VPANDD        Z27, Z28, Z24           // Z24 = (word >> 4) & 0xf = descriptor tag
  VPCMPEQD.BCST CONSTD_4(), Z24, K1, K1 // K1 = is float
  KTESTW        K1, K1
  JZ            next

  // load fp64
  VPCMPEQD.BCST CONSTD_8(), Z25, K1, K2 // K3 = size == 8 (float64)
  KTESTW        K2, K2
  JZ            tryfp32
  VEXTRACTI32X8 $1, Z30, Y29                // Y29 = upper 8 offsets
  KSHIFTRW      $8, K2, K3
  KMOVB         K2, K4
  KMOVB         K3, K5
  // perform 8-byte loads and bwap64 the results
  VBROADCASTI32X4 bswap64<>+0(SB), Z24
  VPGATHERDQ    1(SI)(Y30*1), K4, Z20
  VPGATHERDQ    1(SI)(Y29*1), K5, Z21
  VPSHUFB       Z24, Z20, Z20
  VPSHUFB       Z24, Z21, Z21
  VMOVAPD       Z20, K2, Z2
  VMOVAPD       Z21, K3, Z3

  // load + expand fp32
tryfp32:
  KANDNW        K1, K2, K2
  VPCMPEQD.BCST CONSTD_4(), Z25, K2, K2 // K2 = size == 4 (float32)
  KTESTW        K2, K2
  JZ            next
  KORW          K1, K2, K1
  KMOVW         K2, K3
  // perform 4-byte loads, bswap32 the results,
  // and then extend them to fp64 in Z2:Z3
  VBROADCASTI32X4 bswap32<>+0(SB), Z24
  VPGATHERDD    1(SI)(Z30*1), K3, Z28
  VPSHUFB       Z24, Z28, Z28
  VCVTPS2PD.SAE Y28, Z27      // lo 8 fp32 -> Z27 x 8 fp64
  VEXTRACTF32X8 $1, Z28, Y28
  VCVTPS2PD.SAE Y28, Z28      // hi 8 fp32 -> Z28 x 8 fp64
  KSHIFTRW      $8, K2, K3
  VMOVAPD       Z27, K2, Z2
  VMOVAPD       Z28, K3, Z3
next:
  NEXT()

// VSCRATCH_BASE(mask) sets Z30.mask
// to the current scratch base (equal in all lanes);
// this address can be scattered to safely as long
// as the scratch capacity has been checked in advance
#define VSCRATCH_BASE(mask) \
  VPBROADCASTD  bytecode_scratchoff(VIRT_BCPTR), mask, Z30 \
  VPADDD.BCST   bytecode_scratch+8(VIRT_BCPTR), Z30, mask, Z30

#define CHECK_SCRATCH_CAP(size, sizereg, abrt) \
  MOVQ bytecode_scratch+16(VIRT_BCPTR), sizereg \
  SUBQ bytecode_scratch+8(VIRT_BCPTR), sizereg \
  CMPQ sizereg, size \
  JLT  abrt

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

// box 64-bit floats in Z2:Z3
// (possibly tail-calling into boxint)
TEXT bcboxfloat(SB), NOSPLIT|NOFRAME, $0
  CHECK_SCRATCH_CAP($(9 * 16), R15, abort)

  VPXORD     Z30, Z30, Z30
  VPXORD     Z31, Z31, Z31
  VCVTTPD2QQ Z2, Z4
  VCVTTPD2QQ Z3, Z5
  VCVTQQ2PD  Z4, Z6
  VCVTQQ2PD  Z5, Z7
  VCMPPD     $VCMP_IMM_EQ_OQ, Z2, Z6, K2 // is float64(int64(input)) == input?
  VCMPPD     $VCMP_IMM_EQ_OQ, Z3, Z7, K3
  KUNPCKBW   K2, K3, K2
  KANDW      K1, K2, K2 // K2 = floats that fit into 64-bit signed integers
  KANDNW     K1, K2, K3 // K3 = floats that are actually floats
  KTESTW     K3, K3
  JZ         check_ints

  VPBROADCASTD.Z   CONSTD_9(), K3, Z31           // set len(encoded) = 9
  VSCRATCH_BASE(K3)
  VMOVDQA32        byteidx<>+0(SB), X28
  VPMOVZXBD        X28, Z28
  VPSLLD           $3, Z28, Z29
  VPADDD           Z28, Z29, Z29
  VPADDD           Z29, Z30, K3, Z30             // pos += lane index * 9
  MOVL             $0x48, R8
  VPBROADCASTD     R8, Z28
  VBROADCASTI64X2  bswap64<>(SB), Z27
  VPSHUFB          Z27, Z2, Z6                   // bswap64(input)
  VPSHUFB          Z27, Z3, Z7
  KMOVW            K3, K4
  VPSCATTERDD      Z28, K4, 0(SI)(Z30*1)        // write descriptor byte
  VEXTRACTI32X8    $1, Z30, Y29
  KMOVB            K3, K4
  VSCATTERDPD      Z6, K4, 1(SI)(Y30*1)         // write lo 8 floats
  KSHIFTRW         $8, K3, K4
  VSCATTERDPD      Z7, K4, 1(SI)(Y29*1)         // write hi 8 floats
  ADDQ             $(9*16), bytecode_scratch+8(VIRT_BCPTR)       // update scratch base
check_ints:
  KTESTW     K2, K2
  JZ         next
  JMP        boxint_tail(SB)
next:
  NEXT()
abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// box 64-bit signed integers in Z2:Z3
//
// requires 9*16 bytes of space
TEXT bcboxint(SB), NOSPLIT|NOFRAME, $0
  KMOVW     K1, K2
  VPXORD    Z30, Z30, Z30
  VPXORD    Z31, Z31, Z31 // default value for output is len=0
  VMOVDQA64 Z2, Z4
  VMOVDQA64 Z3, Z5
  JMP       boxint_tail(SB)

// core integer boxing procedure:
//   Z4 + Z5 = 16 x 64-bit signed qwords
//   K2 = lanes to write out
// updates Z30.K2 and Z31.K2, but leaves the other lanes un-touched
TEXT boxint_tail(SB), NOSPLIT|NOFRAME, $0
  CHECK_SCRATCH_CAP($(9 * 16), R15, abort)

  // compute abs(word) and compute
  // the predicate mask for negative signed words
  VPMOVQ2M     Z4, K3
  VPABSQ       Z4, Z4
  VPMOVQ2M     Z5, K4
  VPABSQ       Z5, Z5

  VPLZCNTQ      Z4, Z10
  VPLZCNTQ      Z5, Z11
  VPMOVQD       Z10, Y12
  VPMOVQD       Z11, Y13
  VPBROADCASTD  CONSTD_64(), Z6
  VINSERTI32X8  $1, Y13, Z12, Z12     // Z12 = 16 x 32-bit lzcount
  VPSUBD        Z12, Z6, Z12          // Z12 = 64 - lzcnt(int)
  VPBROADCASTD  CONSTD_8(), Z8        // Z8 = 8
  VPSUBD.BCST   CONSTD_1(), Z8, Z7    // Z7 = 7
  VPADDD        Z7, Z12, Z12          // Z12 = (64-lzcnt(int))+7
  VPSRLD        $3, Z12, Z12          // Z12 = (64-lzcnt(int)+7)/8 = size of encoded big-endian int
  VPCMPEQD      Z8, Z12, K2, K6       // K6 = mask of lanes with eight significant bytes

  VPADDD.BCST      CONSTD_1(), Z12, K2, Z31 // value length = 1 + intwidth
  VPSUBD           Z12, Z8, Z14             // Z14 = (8 - size) = leading zero bytes
  VPSLLD           $3, Z14, Z14             // Z14 = 8 * leading zero bytes = leading zero bits
  VPMOVZXDQ        Y14, Z26
  VEXTRACTI32X8    $1, Z14, Y13
  VPMOVZXDQ        Y13, Z27
  VPSLLVQ          Z26, Z4, Z4              // shift ints left by leading zero bytes
  VPSLLVQ          Z27, Z5, Z5              // so the msb is now in the highest byte position
  VPBROADCASTD     CONSTD_2(), Z13
  KUNPCKBW         K3, K4, K5               // unpack to 16 lanes of sign bits
  KANDW            K2, K5, K5               // K5 = valid & sign bit set
  VPADDD.BCST      CONSTD_1(), Z13, K5, Z13 // Z13 = 2 or 3 (if signed)
  VBROADCASTI64X2  bswap64<>(SB), Z27
  VPSLLD           $4, Z13, Z13             // Z13 = 0x20 or 0x30 (if signed)
  VPADDD           Z12, Z13, Z13            // Z13 = (0x20 or 0x30) + size in bytes
  VEXTRACTI32X8    $1, Z13, Y14
  VPMOVZXDQ        Y14, Z14                 // Z14 = hi 8 descriptors, extended to qwords
  VPMOVZXDQ        Y13, Z13                 // Z13 = lo 8 descriptors, extended to qwords
  VPSHUFB          Z27, Z4, Z4              // Z4 = bswap64(lo 8 words)
  VPSHUFB          Z27, Z5, Z5              // Z5 = bswap64(hi 8 words)
  VPSLLQ           $8, Z4, Z6               // Z6, Z7 = make room for 1-byte descriptor
  VPSLLQ           $8, Z5, Z7
  VPORQ            Z13, Z6, Z6              // OR in descriptor byte
  VPORQ            Z14, Z7, Z7

  VMOVDQU64        byteidx<>(SB), X29
  VPMOVZXBD        X29, Z29           // Z29 = lane index
  VPSLLD           $3, Z29, Z28       // Z28 = lane index * 8
  VSCRATCH_BASE(K2)
  KTESTW           K6, K6
  JNZ              slow_encode

  // fast-path for all integers 8 bytes or less when encoded
  MOVQ             bytecode_scratch(VIRT_BCPTR), R15
  ADDQ             bytecode_scratch+8(VIRT_BCPTR), R15
  KSHIFTRW         $8, K2, K3
  VMOVDQU64        Z6, K2, 0(R15)                       // store the sixteen encoded ion objects
  VMOVDQU64        Z7, K3, 64(R15)
  ADDQ             $128, bytecode_scratch+8(VIRT_BCPTR)
  VPADDD           Z28, Z30, K2, Z30                   // add (lane*8) to offset, or set to zero
  JMP              next
slow_encode:
  // some of the lanes have 8 significant bytes,
  // so we need to perform two overlapped scatters
  ADDQ             $(9*16), bytecode_scratch+8(VIRT_BCPTR)
  VPADDD           Z28, Z30, K2, Z30   // base += (lane index * 8)
  VPADDD           Z29, Z30, K2, Z30   // base += lane index
  VEXTRACTI32X8    $1, Z30, Y28
  KMOVB            K2, K3
  VPSCATTERDQ      Z6, K3, 0(SI)(Y30*1)         // write lo 8, first 8 bytes
  KSHIFTRW         $8, K2, K3
  VPSCATTERDQ      Z7, K3, 0(SI)(Y28*1)         // write hi 8, first 8 bytes
  KMOVB            K2, K3
  VPSCATTERDQ      Z4, K3, 1(SI)(Y30*1)         // write overlapping for final byte of lo 8
  KSHIFTRW         $8, K2, K3
  VPSCATTERDQ      Z5, K3, 1(SI)(Y28*1)         // write overlapping for final byte of hi 8
next:
  NEXT()
abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// take the current set of non-missing lanes (K1)
// and a boolean mask (from stack[imm] -> K2)
// and write out the boolean as encoded ion
// to the scratch buffer for each non-missing lane
TEXT bcboxmask(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX      0(VIRT_PCREG), R8
  ADDQ         $2, VIRT_PCREG
  KMOVW        0(VIRT_VALUES)(R8*1), K2             // K2 = true/false
  JMP          boxmask_tail(SB)

// same as boxmask, but with the arguments reversed
TEXT bcboxmask2(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX      0(VIRT_PCREG), R8
  ADDQ         $2, VIRT_PCREG
  KMOVW        K1, K2                               // K2 = true/false
  KMOVW        0(VIRT_VALUES)(R8*1), K1             // K1 = non-missing
  JMP          boxmask_tail(SB)

// same as boxmask, but with K1 = K2
TEXT bcboxmask3(SB), NOSPLIT|NOFRAME, $0
  KMOVW K1, K2
  JMP   boxmask_tail(SB)

// store (up to) 16 booleans
//
// currently stores the values unconditionally,
// but only updates Z30:Z31 using K1
//
// see boxmask_tail_vbmi2 for a version that
// only writes out the lanes that are valid
TEXT boxmask_tail(SB), NOSPLIT|NOFRAME, $0
  CHECK_SCRATCH_CAP($16, R15, abort)
  MOVL         $0x10, R14
  VPBROADCASTB R14, X10                             // X10 = false byte x 16
  MOVL         $1, R14
  VPBROADCASTB R14, X11
  VPADDB       X10, X11, K2, X10                    // X10 = true or false bytes (0x10 + 1/0)
  MOVQ         bytecode_scratch(VIRT_BCPTR), R14
  ADDQ         bytecode_scratch+8(VIRT_BCPTR), R14
  MOVOU        X10, 0(R14)                          // store 16 bytes unconditionally
  // offsets are [0, 1, 2, 3...] plus base offset;
  // then complemented for Z30
  VPXORD         Z30, Z30, Z30
  VSCRATCH_BASE(K1)
  MOVOU          byteidx<>+0(SB), X10
  VPMOVZXBD      X10, Z10
  VPADDD         Z10, Z30, K1, Z30
  VPBROADCASTD.Z CONSTD_1(), K1, Z31
  // update used scratch space
  ADDQ           $16, bytecode_scratch+8(VIRT_BCPTR)
  NEXT()
abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// FIXME: on machines with VBMI-2 (Ice Lake and after),
// try using this version instead, which writes out fewer
// bytes when some of the lanes are missing
// (this may not be worthwhile; it depends on how much
// scratch space we expect to use)
TEXT boxmask_tail_vbmi2(SB), NOSPLIT|NOFRAME, $0
  KMOVW        K1, R15
  POPCNTL      R15, R15
  ADDQ         bytecode_scratch+8(VIRT_BCPTR), R15  // R15 = len(scratch)+popcnt(K1)
  CMPQ         bytecode_scratch+16(VIRT_BCPTR), R15 // compare w/ cap(scratch)
  JLT          abort
  MOVL         $0x10, R14
  VPBROADCASTB R14, X10                             // X10 = false byte x 16
  MOVL         $1, R14
  VPBROADCASTB R14, X11
  VPADDB       X10, X11, K2, X10                    // X10 = true or false bytes (0x10 + 1/0)
  MOVQ         bytecode_scratch(VIRT_BCPTR), R13
  MOVQ         bytecode_scratch+8(VIRT_BCPTR), R14  // current offset
  VPCOMPRESSB  X10, K1, 0(R13)(R14*1)               // write out true/false bytes
  MOVOU        byteidx<>+0(SB), X11                 // X11 = [0, 1, 2, 3...]
  VPEXPANDB    X11, K1, X11                         // X11 = output offset displ in each lane
  VPMOVZXBD    X11, Z11                             // expand offset to 32 bits
  VPBROADCASTD R14, Z14                             // broadcast original offset
  VPADDD       Z11, Z14, Z14                        // offset = original + displacement
  VNOTINPLACE(Z14)                                  // Z14 = ^offset

  // set Z30 to ^offset in scratch
  // set Z31 to width (one or zero, depending on K2)
  VMOVDQA32.Z    Z14, K1, Z30
  VPBROADCASTD.Z CONSTD_1(), K1, Z31

  // update len(scratch)
  MOVQ     R15, bytecode_scratch+8(VIRT_BCPTR)
  NEXT()
abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// Boxes string slices held in RSI(Z2:Z3)
//
// Inputs:
//   - K1 - 16-bit lane mask
//   - Z2 - 32-bit offsets relative to RSI
//   - Z3 - 32-bit lengths of each string slice
//
// Implementation notes:
//   - Two paths - small strings (up to 13 bytes), large strings (more than 13).
//   - Do gathers of the leading 16 bytes of each string as early as possible.
//     These bytes are gathered to Z11, Z12, Z13, and Z14 and used by both code
//     paths - this optimizes a bit storing smaller strings in both cases.
//   - Encoding of the Type|L + Length happens regardless of string lengths, we
//     do gathers meanwhile so the CPU should be busy enough to hide the latency.
TEXT bcboxstring(SB), NOSPLIT|NOFRAME, $0
  // Quickly skip this instruction if there is nothing to box.
  KTESTW K1, K1
  JZ next

  // Gather LO-8 bytes of LO-8 lanes to Z11.
  KMOVW K1, K4
  VPXORD X11, X11, X11
  VPGATHERDQ 0(SI)(Y2*1), K4, Z11

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
  VMOVDQA32.Z Z3, K1, Z4                               // Z4 = [xxxxxxxx|xxxxxxxx|xxxxxxxx|xAAAAAAA]
  VPSLLD.Z $1, Z3, K1, Z5                              // Z5 = [xxxxxxxx|xxxxxxxx|xBBBBBBB|xxxxxxxx]
  VPSLLD.Z $2, Z3, K1, Z6                              // Z6 = [xxxxxxxx|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPSLLD.Z $3, Z3, K1, Z7                              // Z7 = [xDDDDDDD|xxxxxxxx|xxxxxxxx|xxxxxxxx]

  // Use VPTERNLOGD to combine the extracted bits:
  //   VPTERNLOG(0xD8) == (A & ~C) | (B & C) == Blend(A, B, ~C)
  VPTERNLOGD.BCST $0xD8, CONSTD_0x007F007F(), Z4, Z5   // Z5 = [xxxxxxxx|xxxxxxxx|xBBBBBBB|xAAAAAAA]
  VPTERNLOGD.BCST $0xD8, CONSTD_0x007F007F(), Z6, Z7   // Z7 = [xDDDDDDD|xCCCCCCC|xxxxxxxx|xxxxxxxx]
  VPTERNLOGD.BCST $0xD8, CONSTD_0xFFFF0000(), Z7, Z5   // Z5 = [xDDDDDDD|xCCCCCCC|xBBBBBBB|xAAAAAAA]
  VPANDD.BCST CONSTD_0x7F7F7F7F(), Z5, Z5              // Z5 = [0DDDDDDD|0CCCCCCC|0BBBBBBB|0AAAAAAA]

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
  VPGATHERDQ 8(SI)(Y2*1), K5, Z12

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
  VPGATHERDQ 0(SI)(Y15*1), K4, Z13

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
  VPGATHERDQ 8(SI)(Y15*1), K5, Z14

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
  VPBROADCASTD CX, K1, Z30
  ADDQ CX, R15
  VPADDD Z9, Z30, K1, Z30
  VPTERNLOGD.Z $0x33, Z30, Z30, K1, Z30                // Z30 = Start of each string in scratch buffer (complemented).
  VMOVDQA32 Z7, K1, Z31                                // Z31 = ION data length: Type|L + optional VarUInt + string data.
  MOVQ R15, bytecode_scratch+8(VIRT_BCPTR)             // Store output buffer length back to the bytecode_scratch slice.

  MOVQ bytecode_scratch+0(VIRT_BCPTR), R8              // R8 = base output address.
  ADDQ CX, R8                                          // R8 = adjusted output address by its current length.

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

  // Make Z7 contain Type|L - 128 == 8 (ION String) << 4.
  VPORD.BCST.Z CONSTD_128(), Z3, K1, Z7                // Z7  = [L15 L14 L13 L12|L11 L10 L09 L08|L07 L06 L05 L04|L03 L02 L01 L00]
  VPMOVZXDQ Y7, Z5                                     // Z5  = [___ L07 ___ L06|___ L05 ___ L04|___ L03 ___ L02|___ L01 ___ L00]
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 0, 3, 2), Z7, Z7, Z7

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
  VPORD.BCST.Z CONSTD_128(), Z15, K1, Z15              // Z15 = [T15 T14 T13 T12|T11 T10 T09 T08|T07 T06 T05 T04|T03 T02 T01 T00]
  VPSLLD $24, Z15, Z15

  VPUNPCKLDQ Z5, Z15, Z14                              // Z14 = [L13 T13 L12 T12|L09 T09 L08 T08|L05 T05 L04 T04|L01 T01 L00 T00]
  VPUNPCKHDQ Z5, Z15, Z15                              // Z15 = [L15 T15 L14 T14|L11 T11 L10 T10|L07 T07 L06 T06|L01 T03 L02 T02]

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
  NEXT()

abort:
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)
  RET_ABORT()

// Hash Instructions
// -----------------

TEXT bchashvalue(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX          0(VIRT_PCREG), R8
  ADDQ             $2, VIRT_PCREG
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R8
  MOVQ             R8, R14
  MOVQ             VIRT_BASE, R15
  VPXORD           X10, X10, X10
  VMOVDQU32        Z10, (R8)
  VMOVDQU32        Z10, 64(R8)
  VMOVDQU32        Z10, 128(R8)
  VMOVDQU32        Z10, 192(R8)
  VMOVDQA32.Z      Z30, K1, Z28
  VMOVDQA32.Z      Z31, K1, Z29
  JMP              hashimpl_tail(SB)

TEXT bchashvalueplus(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX          0(VIRT_PCREG), R14
  MOVWQZX          2(VIRT_PCREG), R8
  ADDQ             $4, VIRT_PCREG
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R8
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R14
  MOVQ             VIRT_BASE, R15
  VMOVDQA32.Z      Z30, K1, Z28
  VMOVDQA32.Z      Z31, K1, Z29
  JMP              hashimpl_tail(SB)

TEXT bchashboxed(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX          0(VIRT_PCREG), R8
  ADDQ             $2, VIRT_PCREG
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R8
  MOVQ             R8, R14
  MOVQ             bytecode_scratch(VIRT_BCPTR), R15
  VPXORD           X10, X10, X10
  VMOVDQU32        Z10, (R8)
  VMOVDQU32        Z10, 64(R8)
  VMOVDQU32        Z10, 128(R8)
  VMOVDQU32        Z10, 192(R8)
  VNOTK(Z30, K1, Z28)
  VMOVDQA32.Z      Z31, K1, Z29
  JMP              hashimpl_tail(SB)

TEXT bchashboxedplus(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX          0(VIRT_PCREG), R14
  MOVWQZX          2(VIRT_PCREG), R8
  ADDQ             $4, VIRT_PCREG
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R8
  ADDQ             bytecode_hashmem(VIRT_BCPTR), R14
  MOVQ             bytecode_scratch(VIRT_BCPTR), R15
  VNOTK(Z30, K1, Z28)
  VMOVDQA32.Z      Z31, K1, Z29
  JMP              hashimpl_tail(SB)

// expected input register arguments:
//   R8 = destination hash slot
//   R14 = source hash slot (may alias R8)
//   R15 = base memory pointer
//   Z28 = offsets relative to base
//   Z29 = lengths relative to offsets
TEXT hashimpl_tail(SB), NOSPLIT|NOFRAME, $0
  VBROADCASTI32X4  chachaiv<>+0(SB), Z27
  VMOVDQA32        X28, X10
  VMOVDQA32        X29, X11
  VPXORD           0(R14), Z27, Z9
  CALL             hashx4(SB)
  VMOVDQU32        Z9, 0(R8)
  VEXTRACTI32X4    $1, Z28, X10
  VEXTRACTI32X4    $1, Z29, X11
  VPXORD           64(R14), Z27, Z9
  CALL             hashx4(SB)
  VMOVDQU32        Z9, 64(R8)
  VEXTRACTI32X4    $2, Z28, X10
  VEXTRACTI32X4    $2, Z29, X11
  VPXORD           128(R14), Z27, Z9
  CALL             hashx4(SB)
  VMOVDQU32        Z9, 128(R8)
  VEXTRACTI32X4    $3, Z28, X10
  VEXTRACTI32X4    $3, Z29, X11
  VPXORD           192(R14), Z27, Z9
  CALL             hashx4(SB)
  VMOVDQU32        Z9, 192(R8)
  NEXT()

#define QROUNDx4(rowa, rowb, rowc, rowd, ztmp) \
  VPADDD rowa, rowb, rowa                      \
  VPXORD rowa, rowd, ztmp                      \
  VPROLD $16, ztmp, rowd                       \
  VPADDD rowc, rowd, rowc                      \
  VPXORD rowb, rowc, ztmp                      \
  VPROLD $12, ztmp, rowb                       \
  VPADDD rowa, rowb, rowa                      \
  VPXORD rowd, rowa, ztmp                      \
  VPROLD $8, ztmp, rowd                        \
  VPADDD rowc, rowd, rowc                      \
  VPXORD rowb, rowc, ztmp                      \
  VPROLD $7, ztmp, rowb

// within each 4-dword lane,
// rotate words left by 1
#define ROTLD_1(row) VPSHUFD $57, row, row
// ... left by 2
#define ROTLD_2(row) VPSHUFD $78, row, row
// ... left by 3
#define ROTLD_3(row) VPSHUFD $147, row, row

#define ROUNDx2(rowa, rowb, rowc, rowd, ztmp) \
  QROUNDx4(rowa, rowb, rowc, rowd, ztmp)      \
  ROTLD_1(rowb)                               \
  ROTLD_2(rowc)                               \
  ROTLD_3(rowd)                               \
  QROUNDx4(rowa, rowb, rowc, rowd, ztmp)      \
  ROTLD_3(rowb)                               \
  ROTLD_2(rowc)                               \
  ROTLD_1(rowd)

// inputs:
//   R15 = base, X10:X11 = offset:ptr, Z9 = iv
// outputs:
//   Z9 = 4x128 hash outputs
// clobbers:
//   Z6-Z24, CX, R13
TEXT hashx4(SB), NOFRAME|NOSPLIT, $0
  // populate initial rows (seed should be populated)
  VBROADCASTI32X4 chachaiv<>+16(SB), Z12
  VBROADCASTI32X4 chachaiv<>+32(SB), Z13
  VBROADCASTI32X4 chachaiv<>+48(SB), Z14

  // unpack 4 lanes to 8 lanes for offsets and lengths
  VPMOVZXDQ    X10, Y10         // Y10 = 4*64bit offsets (zero extend)
  VPMOVZXDQ    X11, Y11         // Y11 = 4*64bit lengths (zero extend)
  VPMOVZXDQ    Y10, Z10         // Z10 = 4*128bit offsets (zero extend)
  VPMOVZXDQ    Y11, Z11         // Z11 = 4*128bit lengths (zero extend)
  VPXORD       Z9, Z11, Z9      // fold length into IV

  VPUNPCKLQDQ  Z10, Z10, Z10    // Z10 = 8*64bit offsets, duplicated pair-wise
  VPUNPCKLQDQ  Z11, Z11, Z11    // Z11 = 8*64bit lengths, duplicated pair-wise
  VPBROADCASTQ CONSTQ_8(), Z8   // Z8 = $8
  MOVL         $0xaa, CX
  KMOVB        CX, K4
  VPADDQ       Z8, Z10, K4, Z10 // offset in odd lanes += 8
  VPSUBQ       Z8, Z11, K4, Z11 // length in odd lanes -= 8

  // create masks for each lane
  VPXORQ        Z16, Z16, Z16           // Z16 = zeros
  VPBROADCASTQ  CONSTQ_NEG_1(), Z20     // Z20 = all 1s
  VPCMPQ        $6, Z16, Z11, K2        // K2 = lanes > 0 (signed!)
  VPANDQ.BCST.Z CONSTQ_7(), Z11, K2, Z7 // Z7 = bytes&7 or 0 if <=0
  VPSLLQ        $3, Z7, Z7              // Z7 = valid bytes *= 8 = valid bits
  VPSLLVQ       Z7, Z20, Z7             // Z7 = ones << valid bits
  VPSLLQ        $1, Z8, Z6              // Z6 = $16 as quadwords

  KTESTB       K2, K2
  JZ           done
  KSHIFTRB     $1, K4, K4     // K4 = $0x55
loop:
  // extract the 8-bit K2 mask into a 16-bit dword mask
  KANDB        K2, K4, K3     // K3 = even bits
  KSHIFTLB     $1, K3, K3     // K3 = K2<<1 = odd bits
  KORB         K3, K2, K5     // K5: even bits imply odd bits
  VPMOVM2B     K5, X15        // byte=[0 or 0xff] for each of 8 bits
  VPUNPCKLBW   X15, X15, X15  // interleave bits
  VPMOVB2M     X15, K5        // K5 = dword register mask

  VPXORQ       Z17, Z17, Z17
  VPXORQ       Z18, Z18, Z18
  VPXORQ       Z19, Z19, Z19

  KMOVB        K2, K3
  VPGATHERQQ   0(R15)(Z10*1), K3, Z17  // Z17 = row 0
  VPCMPUQ      $6, Z11, Z8, K2, K3     // K3 = len < 8 (unsigned!)
  VPANDNQ      Z17, Z7, K3, Z17        // &^=mask when len<8
  VPSUBQ       Z6, Z11, K2, Z11        // len -= 16

  VPCMPQ       $6, Z16, Z11, K2, K2    // still > 0?
  KMOVB        K2, K3
  VPGATHERQQ   16(R15)(Z10*1), K3, Z18 // Z18 = row 1
  VPCMPUQ      $6, Z11, Z8, K2, K3     // len<8
  VPANDNQ      Z18, Z7, K3, Z18        // &^=mask when len<8
  VPSUBQ       Z6, Z11, K2, Z11        // len -= 16

  VPCMPQ       $6, Z16, Z11, K2, K2    // k1 = still > 0?
  KMOVB        K2, K3
  VPGATHERQQ   32(R15)(Z10*1), K3, Z19 // Z19 = row 2
  VPCMPUQ      $6, Z11, Z8, K2, K3     // K3 = len<8
  VPANDNQ      Z19, Z7, K3, Z19
  VPSUBQ       Z6, Z11, K2, Z11

  VMOVDQA32 Z9, Z20
  VPXORD    Z12, Z17, Z21
  VPXORD    Z13, Z18, Z22
  VPXORD    Z14, Z19, Z23
  MOVL      $4, R13
rounds:
  ROUNDx2(Z20, Z21, Z22, Z23, Z24)
  DECL      R13
  JNZ       rounds
  VPADDD    Z9,  Z20, K5, Z9
  VPADDD    Z12, Z21, K5, Z12
  VPADDD    Z13, Z22, K5, Z13
  VPADDD    Z14, Z23, K5, Z14

  // loop tail: continue while any(len(lane))>0
  VPCMPQ      $6, Z16, Z11, K2, K2          // len(lane) > 0?
  VPADDQ.BCST CONSTQ_48(), Z10, K2, Z10     // offset += 48
  KTESTB      K2, K2
  JNZ         loop
done:
  RET

// given input hash[imm0], determine
// if there are members in tree[imm1]
TEXT bchashmember(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  MOVWQZX 2(VIRT_PCREG), R13
  ADDQ    $4, VIRT_PCREG
  KTESTW  K1, K1
  JZ      next
  ADDQ    bytecode_hashmem(VIRT_BCPTR), R8       // R8 = pointer to input hash slot
  MOVQ    bytecode_trees(VIRT_BCPTR), R14
  MOVQ    0(R14)(R13*8), R13                     // R13 = tree pointer
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VPUNPCKLQDQ Z16, Z15, Z15
  VMOVDQU64   128(R8), Z16
  VMOVDQU64   192(R8), Z17
  VPUNPCKLQDQ Z17, Z16, Z16
  VMOVDQU64   permute64+0(SB), Z18
  VPERMQ      Z15, Z18, Z15                      // Z15 = low 8 hashes (64-bit)
  VPERMQ      Z16, Z18, Z16                      // Z16 = hi 8 ''
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  VONES(Z10)                       // Z10 = all ones
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
  NEXT()

// given input hash[imm0], determine
// if there are members in tree[imm1]
// and put them in the V register
TEXT bchashlookup(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  MOVWQZX 2(VIRT_PCREG), R13
  ADDQ    $4, VIRT_PCREG
  VPXORD  Z30, Z30, Z30
  VPXORD  Z31, Z31, Z31
  KTESTW  K1, K1
  JZ      next
  ADDQ    bytecode_hashmem(VIRT_BCPTR), R8       // R8 = pointer to input hash slot
  MOVQ    bytecode_trees(VIRT_BCPTR), R14
  MOVQ    0(R14)(R13*8), R13                     // R13 = tree pointer
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VPUNPCKLQDQ Z16, Z15, Z15
  VMOVDQU64   128(R8), Z16
  VMOVDQU64   192(R8), Z17
  VPUNPCKLQDQ Z17, Z16, Z16
  VMOVDQU64   permute64+0(SB), Z18
  VPERMQ      Z15, Z18, Z15                      // Z15 = low 8 hashes (64-bit)
  VPERMQ      Z16, Z18, Z16                      // Z16 = hi 8 ''
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  VONES(Z10)                       // Z10 = all ones
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
  VPGATHERDD    8(R15)(Z8*1), K2, Z30   // load boxed offsets
  KMOVW         K1, K3
  VPGATHERDD    12(R15)(Z8*1), K3, Z31  // load boxed lengths
next:
  NEXT()

// Simple Aggregation Instructions
// -------------------------------

TEXT bcaggsumf(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  KSHIFTRW      $8, K1, K2
  VMOVDQA64.Z   Z2, K1, Z4
  VMOVDQA64.Z   Z3, K2, Z5

  VADDPD        Z4, Z5, Z5
  VEXTRACTF64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VADDPD        Y4, Y5, Y5
  VEXTRACTF64X2 $VEXTRACT_IMM_HI, Y5, X4
  VADDPD        X4, X5, X5
  VSHUFPD       $1, X5, X5, X4
  VADDSD        X4, X5, X5

  VADDSD        0(R10)(R8*1), X5, X5
  VMOVSD        X5, 0(R10)(R8*1)

  KMOVW         K1, R15
  POPCNTL       R15, R15
  ADDQ          R15, 8(R10)(R8*1)

  NEXT()

TEXT bcaggsumi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  KSHIFTRW      $8, K1, K2
  KMOVW         K1, R15
  VMOVQ         0(R10)(R8*1), X6

  VMOVDQA64.Z   Z2, K1, Z5
  VPADDQ        Z3, Z5, K2, Z5
  VEXTRACTI64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VPADDQ        Y4, Y5, Y5
  VEXTRACTI64X2 $VEXTRACT_IMM_HI, Y5, X4

  POPCNTL       R15, R15

  VPADDQ        X4, X5, X5
  VPSHUFD       $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPADDQ        X4, X5, X5
  VPADDQ        X6, X5, X5

  VMOVQ         X5, 0(R10)(R8*1)
  ADDQ          R15, 8(R10)(R8*1)

  NEXT()

TEXT bcaggminf(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  VBROADCASTSD  CONSTF64_POSITIVE_INF(), Z5
  KSHIFTRW      $8, K1, K2
  KMOVW         K1, R15

  VMINPD        Z5, Z2, K1, Z5
  VMINPD        Z5, Z3, K2, Z5
  VEXTRACTF64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VMINPD        Y4, Y5, Y5
  VEXTRACTF64X2 $VEXTRACT_IMM_HI, Y5, X4

  POPCNTL       R15, R15

  VMINPD        X4, X5, X5
  VSHUFPD       $1, X5, X5, X4
  VMINSD        X4, X5, X5

  VMINSD        0(R10)(R8*1), X5, X5
  VMOVSD        X5, 0(R10)(R8*1)
  ADDQ          R15, 8(R10)(R8*1)

  NEXT()

TEXT bcaggmini(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  VPBROADCASTQ  CONSTQ_0x7FFFFFFFFFFFFFFF(), Z5
  KSHIFTRW      $8, K1, K2
  KMOVW         K1, R15
  VMOVQ         0(R10)(R8*1), X6

  VPMINSQ       Z5, Z2, K1, Z5
  VPMINSQ       Z5, Z3, K2, Z5
  VEXTRACTI64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VPMINSQ       Y4, Y5, Y5
  VEXTRACTI64X2 $VEXTRACT_IMM_HI, Y5, X4

  POPCNTL       R15, R15

  VPMINSQ       X4, X5, X5
  VPSHUFD       $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPMINSQ       X4, X5, X5
  VPMINSQ       X6, X5, X5

  VMOVQ         X5, 0(R10)(R8*1)
  ADDQ          R15, 8(R10)(R8*1)
  NEXT()

TEXT bcaggmaxf(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  VBROADCASTSD  CONSTF64_NEGATIVE_INF(), Z5
  KSHIFTRW      $8, K1, K2
  KMOVW         K1, R15

  VMAXPD        Z5, Z2, K1, Z5
  VMAXPD        Z5, Z3, K2, Z5
  VEXTRACTF64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VMAXPD        Y4, Y5, Y5
  VEXTRACTF64X2 $VEXTRACT_IMM_HI, Y5, X4

  POPCNTL       R15, R15

  VMAXPD        X4, X5, X5
  VSHUFPD       $1, X5, X5, X4
  VMAXSD        X4, X5, X5

  VMAXSD        0(R10)(R8*1), X5, X5
  VMOVSD        X5, 0(R10)(R8*1)
  ADDQ          R15, 8(R10)(R8*1)

  NEXT()

TEXT bcaggmaxi(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX       0(VIRT_PCREG), R8
  ADDQ          $2, VIRT_PCREG

  VPBROADCASTQ  CONSTQ_0x8000000000000000(), Z5
  KSHIFTRW      $8, K1, K2
  KMOVW         K1, R15

  VMOVQ         0(R10)(R8*1), X6
  VPMAXSQ       Z5, Z2, K1, Z5
  VPMAXSQ       Z5, Z3, K2, Z5
  VEXTRACTI64X4 $VEXTRACT_IMM_HI, Z5, Y4
  VPMAXSQ       Y4, Y5, Y5
  VEXTRACTI64X2 $VEXTRACT_IMM_HI, Y5, X4

  POPCNTL       R15, R15

  VPMAXSQ       X4, X5, X5
  VPSHUFD       $SHUFFLE_IMM_4x2b(1, 0, 3, 2), X5, X4
  VPMAXSQ       X4, X5, X5
  VPMAXSQ       X6, X5, X5

  VMOVQ         X5, 0(R10)(R8*1)
  ADDQ          R15, 8(R10)(R8*1)
  NEXT()

TEXT bcaggcount(SB), NOSPLIT|NOFRAME, $0
  KMOVW         K1, R15
  MOVWQZX       0(VIRT_PCREG), R8
  POPCNTQ       R15, R15
  ADDQ          $2, VIRT_PCREG
  ADDQ          R15, 0(R10)(R8*1)
  NEXT()

// Slot Aggregation Instructions
// -----------------------------

// In each bytecode_bucket(), aggregate the value Z2:Z3 (float or int)

// take the value of the H register
// and locate the entries associated with
// each hash (for each lane where K1!=0);
//
// returns early if it cannot locate all of K1
TEXT bcaggbucket(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX 0(VIRT_PCREG), R8
  ADDQ    $2, VIRT_PCREG
  KTESTW  K1, K1
  JZ      next
  ADDQ    bytecode_hashmem(VIRT_BCPTR), R8 // R8 = pointer to input hash slot
  KMOVW   K1, K2
  KMOVW   K1, K3

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VPUNPCKLQDQ Z16, Z15, Z15
  VMOVDQU64   128(R8), Z16
  VMOVDQU64   192(R8), Z17
  VPUNPCKLQDQ Z17, Z16, Z16
  VMOVDQU64   permute64+0(SB), Z18
  VPERMQ      Z15, Z18, Z15                      // Z15 = low 8 hashes (64-bit)
  VPERMQ      Z16, Z18, Z16                      // Z16 = hi 8 ''
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // load some immediates
  VONES(Z10)                       // Z10 = all ones
  VPSRLD        $28, Z10, Z6       // Z6 = 0xf
  VPXORQ        Z14, Z14, Z14      // Z14 = constant 0
  VPXORQ        Z7, Z7, Z7         // Z7 = shift count

  // load table[0] into Z8 and copy to Z9
  MOVQ          radixTree64_index(R10), R15
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

  MOVQ          radixTree64_values(R10), R15

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
  VPCMPD.BCST   $VPCMP_IMM_GT, radixTree64_values+8(R10), Z13, K1, K4
  KTESTW        K4, K4
  JNZ           trap
  VMOVDQU32     Z13, bytecode_bucket(VIRT_BCPTR)
  NEXT()
early_ret:
  // set bytecode.err to NeedRadix
  // and bytecode.errinfo to the hash slot
  MOVL    $const_bcerrNeedRadix, bytecode_err(VIRT_BCPTR)
  MOVWQZX -2(VIRT_PCREG), R8
  MOVQ    R8, bytecode_errinfo(VIRT_BCPTR)
  RET_ABORT()
trap:
  FAIL()

// All aggregate operations except AVG aggregate the value and then mark
// slot+1, so we can decide whether the result of the aggregation should
// be the aggregated value or NULL - in other words it basically describes
// whether there was at least one aggregation.
#define BC_AGGREGATE_SLOT_MARK_OP(instruction)                                \
  /* Load buckets as early as possible so we can resolve conflicts early,  */ \
  /* because VPCONFLICTD has a very high latency (higher than VPCONFLICTQ).*/ \
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z6                                        \
  VMOVDQU32 bytecode_bucket(VIRT_BCPTR), K1, Z6                               \
  VPCONFLICTD.Z Z6, K1, Z11                                                   \
  VEXTRACTI32X8 $1, Z6, Y7                                                    \
                                                                              \
  /* Load the aggregation data pointer. */                                    \
  MOVWQZX 0(VIRT_PCREG), R15                                                  \
  ADDQ $2, VIRT_PCREG                                                         \
  ADDQ $8, R15                                                                \
  ADDQ radixTree64_values(R10), R15                                           \
                                                                              \
  /* Mark all values that we are gonna update. */                             \
  VPBROADCASTD CONSTD_1(), Z10                                                \
  KMOVW K1, K2                                                                \
  VPSCATTERDD Z10, K2, 8(R15)(Z6*1)                                           \
                                                                              \
  /* Z4/Z5 - source aggregates having incrementally resolved conflicts. */    \
  VMOVDQA64 Z2, Z4                                                            \
  VMOVDQA64 Z3, Z5                                                            \
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
  instruction Z8, Z4, K4, Z4                                                  \
  instruction Z9, Z5, K5, Z5                                                  \
                                                                              \
  /* Continue looping if there are still conflicts. */                        \
  KTESTW K2, K2                                                               \
  JNZ loop                                                                    \
                                                                              \
resolved:                                                                     \
  /* Finally, aggregate non-conflicting sources into buckets. */              \
  instruction Z4, Z14, K1, Z14                                                \
  KMOVB K1, K2                                                                \
  VSCATTERDPD Z14, K2, 0(R15)(Y6*1)                                           \
                                                                              \
  KSHIFTRW $8, K1, K2                                                         \
  VPXORQ X14, X14, X14                                                        \
  VGATHERDPD 0(R15)(Y7*1), K2, Z14                                            \
  KSHIFTRW $8, K1, K2                                                         \
  instruction Z5, Z14, K2, Z14                                                \
  VSCATTERDPD Z14, K2, 0(R15)(Y7*1)                                           \
                                                                              \
next:

// This macro is used to implement AVG, which requires more than just a mark.
//
// In order to calculate the average we aggregate the value and also a count
// of values aggregated, this count will then be used to calculate the final
// average and also to decide whether the result is NULL or non-NULL. If the
// COUNT is zero, the result of the aggregation is NULL.
#define BC_AGGREGATE_SLOT_COUNT_OP(instruction)                               \
  /* Load buckets as early as possible so we can resolve conflicts early,  */ \
  /* because VPCONFLICTD has a very high latency (higher than VPCONFLICTQ).*/ \
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z6                                        \
  VMOVDQU32 bytecode_bucket(VIRT_BCPTR), K1, Z6                               \
  VPCONFLICTD.Z Z6, K1, Z11                                                   \
  VEXTRACTI32X8 $1, Z6, Y7                                                    \
                                                                              \
  /* Load the aggregation data pointer. */                                    \
  MOVWQZX 0(VIRT_PCREG), R15                                                  \
  ADDQ $2, VIRT_PCREG                                                         \
  ADDQ $8, R15                                                                \
  ADDQ radixTree64_values(R10), R15                                           \
                                                                              \
  /* Z4/Z5 - source aggregates having incrementally resolved conflicts. */    \
  VMOVDQA64 Z2, Z4                                                            \
  VMOVDQA64 Z3, Z5                                                            \
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
  instruction Z8, Z4, K4, Z4                                                  \
  instruction Z9, Z5, K5, Z5                                                  \
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
  instruction Z4, Z14, K1, Z14                                                \
  VPADDQ Z15, Z13, K1, Z13                                                    \
  KMOVB K1, K2                                                                \
  VSCATTERDPD Z14, K2, 0(R15)(Y6*1)                                           \
  KMOVB K1, K2                                                                \
  VPSCATTERDQ Z13, K2, 8(R15)(Y6*1)                                           \
                                                                              \
  /* Aggregate non-conflicting values and COUNTs into buckets (high). */      \
  VPXORQ X14, X14, X14                                                        \
  VPXORQ X13, X13, X13                                                        \
  KSHIFTRW $8, K1, K2                                                         \
  VGATHERDPD 0(R15)(Y7*1), K2, Z14                                            \
  KSHIFTRW $8, K1, K2                                                         \
  VPGATHERDQ 8(R15)(Y7*1), K2, Z13                                            \
  KSHIFTRW $8, K1, K2                                                         \
  instruction Z5, Z14, K2, Z14                                                \
  VPADDQ Z16, Z13, K2, Z13                                                    \
  VSCATTERDPD Z14, K2, 0(R15)(Y7*1)                                           \
  KSHIFTRW $8, K1, K2                                                         \
  VPSCATTERDQ Z13, K2, 8(R15)(Y7*1)                                           \
                                                                              \
next:

TEXT bcaggslotaddf(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VADDPD)
  NEXT()

TEXT bcaggslotaddi(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VPADDQ)
  NEXT()

TEXT bcaggslotavgf(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_COUNT_OP(VADDPD)
  NEXT()

TEXT bcaggslotavgi(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_COUNT_OP(VPADDQ)
  NEXT()

TEXT bcaggslotminf(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VMINPD)
  NEXT()

TEXT bcaggslotmini(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VPMINSQ)
  NEXT()

TEXT bcaggslotmaxf(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VMAXPD)
  NEXT()

TEXT bcaggslotmaxi(SB), NOSPLIT|NOFRAME, $0
  BC_AGGREGATE_SLOT_MARK_OP(VPMAXSQ)
  NEXT()

// COUNT is a special aggregation function that just counts active lanes stored
// in K1. This is the simplest aggregation, which only requres a basic conflict
// resolution that doesn't require to loop over conflicting lanes.
TEXT bcaggslotcount(SB), NOSPLIT|NOFRAME, $0
  // Load buckets as early as possible so we can resolve conflicts early,
  // because VPCONFLICTD has a very high latency (higher than VPCONFLICTQ).
  VPBROADCASTD CONSTD_0xFFFFFFFF(), Z6
  VMOVDQU32 bytecode_bucket(VIRT_BCPTR), K1, Z6
  VPCONFLICTD.Z Z6, K1, Z8

  // Load the aggregation data pointer and prepare high 8 element offsets.
  MOVWQZX 0(VIRT_PCREG), R15
  ADDQ $2, VIRT_PCREG
  ADDQ radixTree64_values(R10), R15
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
  //
  // NOTE: This chain can be replaced by `VPOPCNTD Z8, Z8`
  VMOVDQU32 CONST_GET_PTR(popcnt_nibble, 0), Z10
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

  NEXT()

// Uncategorized Instructions
// --------------------------

// take two immediate offsets into the scratch buffer and broadcast them into registers
TEXT bclitref(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTD  0(VIRT_PCREG), Z30 // offset in scratch
  VPBROADCASTD  4(VIRT_PCREG), Z31 // length
  VPADDD.BCST   bytecode_scratchoff(VIRT_BCPTR), Z30, Z30 // offset += displ
  ADDQ          $8, VIRT_PCREG
  NEXT()

// take the list slice in Z2:Z3
// and put the first object slice in Z30:Z31,
// then update Z2:Z3 to point to the rest of
// the list
TEXT bcsplit(SB), NOSPLIT|NOFRAME, $0
  VPTESTMD      Z3, Z3, K1, K1           // only keep lanes with len != 0
  KTESTW        K1, K1
  JZ            next
  KMOVW         K1, K2
  VPBROADCASTD  CONSTD_0x0F(), Z27         // Z27 = 0x0F
  VPBROADCASTD  CONSTD_1(), Z21            // Z21 = 1
  VPGATHERDD    0(SI)(Z2*1), K2, Z26       // Z26 = first 4 bytes
  VPANDD        Z26, Z27, Z25              // Z25 = first 4 & 0x0f = int size
  VPSRLD        $4, Z26, Z26               // first 4 words >>= 4
  VPANDD        Z27, Z26, Z24              // Z24 = (word >> 4) & 0xf = descriptor tag
  VPCMPEQD      Z21, Z24, K1, K4           // K4 = descriptor=1 (boolean)
  VPCMPEQD      Z25, Z27, K1, K2           // K2 = field is null
  KORW          K2, K4, K2                 // K2 = field is null or boolean (must be 1 byte)
  KANDNW        K1, K2, K3                 // K3 = field is active and not 1-byte-sized
  VPCMPEQD.BCST CONSTD_0x0E(), Z25, K1, K2 // K2 = descriptor=e (varint-sized)
  KANDNW        K3, K2, K3                 // K3 = non-varint-sized objects
  VMOVDQA32.Z   Z25, K3, Z22               // Z22 = length = first4&0xf for non-varint-size
  VPADDD        Z21, Z22, K1, Z22          // Z22++ for all lanes (for descriptor byte)
  VPCMPEQD      Z24, Z21, K1, K4           // K4 = descriptor tag == 1
  VMOVDQA32     Z21, K4, Z22               // Z22 = 1 for booleans independent of size bits
  // decode up to 3 varint bytes; we expect
  // not to see 4 bytes because our current chunk
  // alignment would not allow for objects over
  // 2^21 bytes long anyway...
  VPBROADCASTD  CONSTD_0x7F(), Z27      // Z27 = 0x7F
  VPBROADCASTD  CONSTD_0x80(), Z29      // Z29 = 0x80
  VPSRLD        $4, Z26, Z26            // now Z26 = 3 bytes following descriptor
  KMOVW         K2, K3
  VPANDD.Z      Z27, Z26, K3, Z28       // Z28 = byte1&0x7f = accumulator
  VPADDD        Z21, Z22, K3, Z22       // total++
  VPTESTNMD     Z29, Z26, K3, K3        // test byte1&0x80
  KTESTW        K3, K3
  JZ            done
  VPSRLD        $8, Z26, K3, Z26        // word >>= 8
  VPSLLD        $7, Z28, K3, Z28        // accum <<= 7
  VPANDD        Z27, Z26, K3, Z25
  VPORD         Z25, Z28, K3, Z28       // accum |= (word & 0x7f)
  VPADDD        Z21, Z22, K3, Z22       // total++
  VPTESTNMD     Z29, Z26, K3, K3        // test word&0x80
  VPSRLD        $8, Z26, K3, Z26        // word >>= 8
  VPSLLD        $7, Z28, K3, Z28        // accum <<= 7
  VPANDD        Z27, Z26, K3, Z25
  VPORD         Z25, Z28, K3, Z28       // accum |= (word & 0x7f)
  VPADDD        Z21, Z22, K3, Z22       // total++
  VPTESTNMD     Z29, Z26, K3, K3        // test word&0x80
  KTESTW        K3, K3
  JNZ           trap                    // trap if length(object) > 2^21
done:
  VPADDD        Z28, Z22, K2, Z22       // size += varint size
  VPCMPUD       $VPCMP_IMM_GT, Z3, Z22, K1, K3   // bounds check: we are still inside the array
  KTESTW        K3, K3
  JNZ           trap
  VMOVDQA32.Z   Z2, K1, Z30             // Z30 = object base = Z2
  VMOVDQA32.Z   Z22, K1, Z31            // Z31 = object size = Z22
  VPADDD        Z22, Z2, K1, Z2         // offset += object size
  VPSUBD.Z      Z22, Z3, K1, Z3         // length -= object size
next:
  NEXT()
trap:
  FAIL()

// take value regs Z30:Z31 and parse
// them as structure offset + length
// into Z0:Z1
TEXT bctuple(SB), NOSPLIT|NOFRAME, $0
  KTESTW        K1, K1
  JZ            next
  KMOVW         K1, K2
  VPBROADCASTD  CONSTD_0x0F(), Z27         // Z27 = 0x0F
  VPBROADCASTD  CONSTD_1(), Z21            // Z21 = 1
  VMOVDQA32     Z21, Z23                   // Z23 = 1 = offset addend
  VPGATHERDD    0(SI)(Z30*1), K2, Z28      // Z28 = first 4 bytes
  VPANDD        Z27, Z28, Z25              // Z25 = first 4 & 0x0f = immediate size
  VPCMPEQD      Z27, Z25, K1, K2           // K2 = lane is null
  KANDNW        K1, K2, K1                 // unset lanes that are null values
  VPSRLD        $4, Z28, Z28               // Z28 >>= 4
  VPANDD        Z27, Z28, Z26              // Z26 = field tag
  VPCMPEQD.BCST CONSTD_0x0D(), Z26, K1, K1 // K1 = keep lanes that are actually structures
  VPCMPEQD.BCST CONSTD_0x0E(), Z25, K1, K2 // K2 = lane has non-immediate length
  VPSRLD        $4, Z28, Z28               // shift away first byte completely
  VPBROADCASTD  CONSTD_0x7F(), Z24         // Z24 = 0x7f
  VPBROADCASTD  CONSTD_0x80(), Z26         // Z26 = 0x80
  VPADDD        Z21, Z23, K2, Z23          // offset++ (now 2)
  VPANDD        Z24, Z28, K2, Z25          // outsize = byte&0x7f
  VPTESTNMD     Z26, Z28, K2, K2           // test if we've hit the stop bit
  KTESTW        K2, K2
  JNZ           two_more                   // keep the fast path (length < 127) short
done:
  VPADDD        Z23, Z30, K1, Z0           // Z0 = base = offset + encoding size
  VMOVDQA32     Z25, K1, Z1                // Z1 = length
next:
  NEXT()
trap:
  FAIL()
two_more:
  VPADDD        Z21, Z23, K2, Z23
  VPSLLD        $7, Z25, K2, Z25
  VPSRLD        $8, Z28, Z28
  VPANDD        Z24, Z28, K2, Z27
  VPORD         Z27, Z25, K2, Z25
  VPTESTNMD     Z26, Z28, K2, K2
  VPADDD        Z21, Z23, K2, Z23
  VPSLLD        $7, Z25, K2, Z25
  VPSRLD        $8, Z28, Z28
  VPANDD        Z24, Z28, K2, Z27
  VPORD         Z27, Z25, K2, Z25
  VPTESTNMD     Z26, Z28, K2, K2
  KTESTW        K2, K2
  JNZ           trap
  JMP           done

// duplicate a value stack slot
// (used when a value is returned multiple times)
TEXT bcdupv(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX    0(VIRT_PCREG), R8
  MOVWQZX    2(VIRT_PCREG), R15
  ADDQ       $4, VIRT_PCREG
  VMOVDQU32.Z 0(VIRT_VALUES)(R8*1), K1, Z28
  VMOVDQU32.Z 64(VIRT_VALUES)(R8*1), K1, Z29
  VMOVDQU32 Z28, 0(VIRT_VALUES)(R15*1)
  VMOVDQU32 Z29, 64(VIRT_VALUES)(R15*1)
  NEXT()

// zero a slot (this is effectively the constprop'd version of saving MISSING everywhere)
TEXT bczerov(SB), NOSPLIT|NOFRAME, $0
  MOVWQZX     0(VIRT_PCREG), R8
  ADDQ        $2, VIRT_PCREG
  VPXORD      Z28, Z28, Z28
  VMOVDQU32   Z28, 0(VIRT_VALUES)(R8*1)
  VMOVDQU32   Z28, 64(VIRT_VALUES)(R8*1)
  NEXT()

// Defines for function SIZE()

#define     HEAD_BYTES      Z29
#define     T_FIELD         Z28
#define     L_FIELD         Z27
#define     OBJECT_SIZE     Z26
#define     HEADER_LENGTH   Z25
#define     VALID           K1
#define     LIST_SEXP       K5
#define     STRUCT          K6

#define     TMP                 Z5
#define     TMP2                Z6
#define     TMP3                Z7
#define     CONST_0x80          Z8
#define     CONST_0x7f          Z9
#define     CONST_0x01          Z10
#define     CONST_0x0e          Z11
#define     CONST_0x0f          Z12
#define     CONST_0x00          Z13
#define     CONST_BSWAPD        Z14
#define     CONST_0x80808080    Z15
#define     CONST_0x03          Z16


// Calculate the number bytes occupied by uvint value.
// Assumes that uvint has at most 3 bytes, for longer
// values jumps to `trap`.
//
// Inputs:
// - BYTES  - 4 initial bytes
// - VALID  - valid lanes
//
// Outputs:
// - COUNT  - the number of bytes ([0..3] each)
//
// Modifies:
// - BYTES
#define CALCULATE_UVINT_LENGTH(VALID, BYTES, COUNT)     \
    VPSHUFB     CONST_BSWAPD, BYTES, BYTES              \
    VPANDD      CONST_0x80808080, BYTES, BYTES          \
    VPLZCNTD    BYTES, COUNT                            \
    VPSRLD      $3, COUNT, VALID, COUNT                 \
    VPADDD.Z    CONST_0x01, COUNT, VALID, COUNT         \
    /* check if length > 3 */                           \
    VPCMPUD     $VPCMP_IMM_GT, CONST_0x03, COUNT, K2    \
    KTESTW      K2, K2                                  \
    JNZ trap


#define DWORD_CONST(value, target)    \
    MOVD $value, CX                   \
    VPBROADCASTD CX, target


// Function exposes macro CALCULATE_UVINT_LENGTH for unit test purposes.
//
// input:
// - Z30 - offsets
// - K1  - active lanes
// output:
// - K7  - masks too long uvints
// - Z31 - 32-bit lengths
TEXT objectsize_test_uvint_length(SB), NOSPLIT|NOFRAME, $0
    // init
    DWORD_CONST(0x80, CONST_0x80)
    DWORD_CONST(0x01, CONST_0x01)
    DWORD_CONST(0x03, CONST_0x03)
    DWORD_CONST(0x80808080, CONST_0x80808080)
    VBROADCASTI32X4 bswap32<>+0(SB), CONST_BSWAPD
    DWORD_CONST(0xcacacaca, Z1)

    KMOVW   K1, K2
    VPGATHERDD (SI)(Z30*1), K2, Z1

    VPXORD Z31, Z31, Z31
    CALCULATE_UVINT_LENGTH(K1, Z1, Z31)
    KMOVW K2, K7
    RET

trap:
    MOVD $0xffffffff, AX
    VPBROADCASTD AX, Z31
    KMOVW K2, K7
    RET


// Loads Ion object TV byte and splits it into T and L parts
//
// Inputs:
// - SI    - data pointer
// - Z30   - offsets
// - VALID - active lanes
//
// Outputs:
// - HEAD_BYTES - 4 leading bytes
// - T_FIELD    - Ion type (T field)
// - L_FIELD    - raw Ion length (L field)
#define LOAD_OBJECT_HEADER(VALID)                     \
    KMOVW       VALID, K2                             \
    VPGATHERDD  (SI)(Z30*1), K2, HEAD_BYTES           \
                                                      \
    VPSRLD      $4, HEAD_BYTES, T_FIELD               \
    VPANDD      CONST_0x0f, HEAD_BYTES, L_FIELD       \
    VPANDD      CONST_0x0f, T_FIELD, T_FIELD


// Calculates the size of an Ion object: its header and contents
//
// Inputs:
// - HEAD_BYTES - 4 initial object bytes
// - L_FIELD    - the L field of Ion object
// - T_FIELD    - the T field of Ion object
// - VALID      - active lanes
//
// Outputs:
// - HEADER_LENGTH
// - OBJECT_SIZE
//
// Clobbers:
// - K2, K3, K4
//
#define CALCULATE_OBJECT_SIZE(VALID, no_uvint, uvint_done)       \
    /* 1. Assume all object are in short form */                 \
    VMOVDQA32.Z CONST_0x01, VALID, HEADER_LENGTH                 \
    VMOVDQA32   L_FIELD, OBJECT_SIZE                             \
                                                                 \
    /* 2. Fix up for bool=true and nulls --- size is 0 */        \
    /*    not ((T == 1 and L == 1) or (L == 15)) = */            \
    /*    (T != 1 or L != 1) and L != 15 */                      \
    VPCMPD      $VPCMP_IMM_NE, CONST_0x01, T_FIELD, VALID, K2    \
    VPCMPD      $VPCMP_IMM_NE, CONST_0x01, L_FIELD, VALID, K3    \
    VPCMPD      $VPCMP_IMM_NE, CONST_0x0f, L_FIELD, VALID, K4    \
    KORW        K2, K3, K2                                       \
    KANDW       K4, K2, K2                                       \
    VMOVDQA32.Z OBJECT_SIZE, K2, OBJECT_SIZE                     \
                                                                 \
    /* 3. Check if we need to decode any uvint */                \
    VPCMPD      $VPCMP_IMM_EQ, CONST_0x0e, L_FIELD, VALID, K2    \
    KTESTW      K2, K2                                           \
    JZ          no_uvint                                         \
                                                                 \
    /* 4. Decode uvint into TMP */                               \
    VPXORD      TMP, TMP, TMP                                    \
    VPSRLD.Z    $8, HEAD_BYTES, K2, TMP2                         \
                                                                 \
    /* 4a. reset object size for uvint-encoded objects */        \
    KNOTW       K2, K3                                           \
    VMOVDQU32.Z OBJECT_SIZE, K3, OBJECT_SIZE                     \
    VPADDD      CONST_0x01, HEADER_LENGTH, K2, HEADER_LENGTH     \
                                                                 \
    /* 4b. the first byte */                                     \
    VPTESTNMD   CONST_0x80, TMP2, K2, K2                         \
    VPANDD      CONST_0x7f, TMP2, TMP                            \
    KTESTW      K2, K2 /* fast-path for all-1-byte-lengths */    \
    JZ          uvint_done                                       \
                                                                 \
    /* 4c. the second byte */                                    \
    VPADDD      CONST_0x01, HEADER_LENGTH, K2, HEADER_LENGTH     \
    VPSRLD.Z    $8, TMP2, K2, TMP2                               \
    VPANDD      CONST_0x7f, TMP2, TMP3                           \
    VPSLLD      $7, TMP, K2, TMP                                 \
    VPADDD      TMP3, TMP, TMP                                   \
                                                                 \
    /* 4d. the third byte */                                     \
    VPTESTNMD   CONST_0x80, TMP2, K2, K2                         \
    VPADDD      CONST_0x01, HEADER_LENGTH, K2, HEADER_LENGTH     \
    VPSRLD.Z    $8, TMP2, K2, TMP2                               \
    VPANDD      CONST_0x7f, TMP2, TMP3                           \
    VPSLLD      $7, TMP, K2, TMP                                 \
    VPADDD      TMP3, TMP, K2, TMP                               \
                                                                 \
    /* 4e. test if uvint is not longer than 3 bytes */           \
    VPTESTNMD   CONST_0x80, TMP2, K2, K2                         \
    KTESTW      K2, K2                                           \
    JNZ         trap                                             \
                                                                 \
uvint_done:                                                      \
    /* 4c. update the length */                                  \
    VPADDD      TMP, OBJECT_SIZE, OBJECT_SIZE                    \
no_uvint:


// Function exposes macro CALCULATE_OBJECT_SIZE for unit test purposes.
//
// input:
// - Z30 - offsets
// - K1  - active lanes
// output:
// - K7  - masks invalid entries
// - Z30 - header length (TV byte + optional uvint length)
// - Z31 - object size
TEXT objectsize_test_object_header_size(SB), NOSPLIT|NOFRAME, $0

    DWORD_CONST(0x01, CONST_0x01)
    DWORD_CONST(0x0e, CONST_0x0e)
    DWORD_CONST(0x0f, CONST_0x0f)
    DWORD_CONST(0x7f, CONST_0x7f)
    DWORD_CONST(0x80, CONST_0x80)

    // test
    LOAD_OBJECT_HEADER(K1)
    CALCULATE_OBJECT_SIZE(K1, no_uvint, uvint_done)

    // store result
    VMOVDQA32 HEADER_LENGTH, Z1
    VMOVDQA32 OBJECT_SIZE, Z2
    KMOVW K2, K7
    RET

trap:
    DWORD_CONST(0xffffffff, Z1)
    DWORD_CONST(0xffffffff, Z2)
    RET


// SIZE(x) function --- returns the number of items
// in a struct or list, missing otherwise.
TEXT bcobjectsize(SB), NOSPLIT|NOFRAME, $0
    VPBROADCASTD CONSTD_1(), CONST_0x01
    VPBROADCASTD CONSTD_0x0F(), CONST_0x0f

    /* 1. Determine object types */
    LOAD_OBJECT_HEADER(K1)

    VPXORD      Z2, Z2, Z2 /* set the count to zero */
    VPXORD      Z3, Z3, Z3

    VPCMPD.BCST $VPCMP_IMM_EQ, CONSTD_0x0B(), T_FIELD, K1, K2 /* list */
    VPCMPD.BCST $VPCMP_IMM_EQ, CONSTD_0x0C(), T_FIELD, K1, LIST_SEXP /* sexp */
    VPCMPD.BCST $VPCMP_IMM_EQ, CONSTD_0x0D(), T_FIELD, K1, STRUCT /* struct */

    KORW    K2, LIST_SEXP, LIST_SEXP
    KORW    LIST_SEXP, STRUCT, K1 /* non-containers -> missing */
    KTESTW  K1, K1
    JZ      no_compbound_values_found

    /* 2. unset all null values */
    VPCMPD  $VPCMP_IMM_EQ, CONST_0x0f, L_FIELD, K1, K2 /* K2 - null values */
    KANDNW  K1, K2, K1
    KTESTW  K1, K1
    JZ      all_nulls

    VPBROADCASTD CONSTD_0x0E(), CONST_0x0e
    VPBROADCASTD CONSTD_0x7F(), CONST_0x7f
    VPBROADCASTD CONSTD_0x80(), CONST_0x80
    VPBROADCASTD CONSTD_3(), CONST_0x03
    DWORD_CONST(0x80808080, CONST_0x80808080)
    VBROADCASTI32X4 bswap32<>+0(SB), CONST_BSWAPD
    VPXORD CONST_0x00, CONST_0x00, CONST_0x00

    /* 3. find the containers' size */
    KORW    LIST_SEXP, STRUCT, K2
    CALCULATE_OBJECT_SIZE(K2, no_uvint1, uvint_done1)

    VPADDD  Z30, HEADER_LENGTH, Z30 /* Z30 - points the inner structure */
    VPADDD  Z30, OBJECT_SIZE, Z31   /* Z31 = points the end of the inner structure */

    /* 4. iterate over lists/sexprs */
count_list_sexp_values:
    VPCMPD $VPCMP_IMM_LT, Z31, Z30, LIST_SEXP, LIST_SEXP
    KTESTW  LIST_SEXP, LIST_SEXP
    JZ count_list_sexp_values_end

    LOAD_OBJECT_HEADER(LIST_SEXP)
    CALCULATE_OBJECT_SIZE(LIST_SEXP, no_uvint2, uvint_done2)

    VPADDD  CONST_0x01, Z2, LIST_SEXP, Z2       /* count += 1 */
    VPADDD  HEADER_LENGTH, Z30, LIST_SEXP, Z30  /* offset += header_size */
    VPADDD  OBJECT_SIZE, Z30, LIST_SEXP, Z30    /* offset += object_size */

    JMP count_list_sexp_values
count_list_sexp_values_end:

    /* 5. iterate over structs */
count_fields:
    VPCMPD $VPCMP_IMM_LT, Z31, Z30, STRUCT, STRUCT
    KTESTW STRUCT, STRUCT
    JZ count_fields_end

    /* skip field id */
    KMOVW STRUCT, K2
    VPGATHERDD  (SI)(Z30*1), K2, HEAD_BYTES
    CALCULATE_UVINT_LENGTH(STRUCT, HEAD_BYTES, TMP)
    VPADDD  TMP, Z30, Z30

    /* skip field value */
    LOAD_OBJECT_HEADER(STRUCT)
    CALCULATE_OBJECT_SIZE(STRUCT, no_uvint3, uvint_done3)

    VPADDD  CONST_0x01, Z2, STRUCT, Z2       /* count += 1 */
    VPADDD  HEADER_LENGTH, Z30, STRUCT, Z30  /* offset += header_size */
    VPADDD  OBJECT_SIZE, Z30, STRUCT, Z30    /* offset += object_size */

    JMP count_fields
count_fields_end:
    VEXTRACTI32X8   $1, Z2, Y3
    VPMOVZXDQ       Y2, Z2
    VPMOVZXDQ       Y3, Z3
    NEXT()

no_compbound_values_found:
all_nulls:
    KXORW           K1, K1, K1
    NEXT()
trap:
    FAIL()


#undef DWORD_CONST
#undef CALCULATE_UVINT_LENGTH
#undef LOAD_OBJECT_HEADER
#undef CALCULATE_OBJECT_SIZE

#undef HEAD_BYTES
#undef T_FIELD
#undef L_FIELD
#undef OBJECT_SIZE
#undef HEADER_LENGTH
#undef VALID
#undef LIST_SEXP
#undef STRUCT

#undef TMP
#undef TMP2
#undef TMP3
#undef CONST_0x80
#undef CONST_0x7f
#undef CONST_0x01
#undef CONST_0x0e
#undef CONST_0x0f
#undef CONST_0x00
#undef CONST_0x80808080
#undef CONST_0x03

// String Instructions
// -------------------

//; #region string methods

//; #region bcCmpStrEqCs
//; equal ascii string in slice in Z2:Z3, with stack[imm]
TEXT bcCmpStrEqCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter_needle; R14=needle_slice;
  VPCMPD        $0,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_length==counter_needle);K1=lane_active; Z3=str_length; Z6=counter_needle; 0=Eq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z4                   //;6F6F1342 search_base := str_start        ;Z4=search_base; Z2=str_start;

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data_msg==Address())     ;K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter_needle -= 4             ;Z6=counter_needle; Z20=constd_4;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tail:
  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter_needle;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter_needle>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter_needle; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter_needle; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data_msg==data_needle)   ;K1=lane_active; Z8=data_msg; Z9=data_needle; 0=Eq;
next:
  NEXT()
//; #endregion bcCmpStrEqCs

//; #region bcCmpStrEqCi
//; equal ascii string in slice in Z2:Z3, with stack[imm]
TEXT bcCmpStrEqCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z6                 //;713DF24F bcst needle_length              ;Z6=counter_needle; R14=needle_slice;
  VPCMPD        $0,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_length==counter_needle);K1=lane_active; Z3=str_length; Z6=counter_needle; 0=Eq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z4                   //;6F6F1342 search_base := str_start        ;Z4=search_base; Z2=str_start;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper
  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==Address());K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter_needle -= 4             ;Z6=counter_needle; Z20=constd_4;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
tail:
  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter_needle;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter_needle>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter_needle; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter_needle; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
next:
  NEXT()
//; #endregion bcCmpStrEqCi

//; #region bcCmpStrEqUTF8Ci
//; case-insensitive UTF-8 string compare in slice in Z2:Z3, with stack[imm]
//; empty needles or empty data always result in a dead lane
TEXT bcCmpStrEqUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;5B83F09F load number of code-points      ;CX=n_runes; R14=needle_ptr;
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_length != 0); empty data are dead lanes;K1=lane_active; Z3=str_length;

  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=n_runes;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $5,  Z26, Z3,  K1,  K1    //;74222733 K1 &= (str_length>=scratch_Z26) ;K1=lane_active; Z3=str_length; Z26=scratch_Z26; 5=GreaterEq;
  KTESTW        K1,  K1                   //;A808AD8E any lanes still todo?           ;K1=lane_active;
  JZ            next                      //;1CA4B42D no, then exit; jump if zero (ZF = 1);

  MOVL          4(R14),R13                //;00000000                                 ;R13=n_alt; R14=needle_ptr;
  MOVL          8(R14),R12                //;1EEAB85B                                 ;R12=alt_ptr; R14=needle_ptr;
  ADDQ          R14, R12                  //;7B0665F3 alt_ptr += needle_ptr           ;R12=alt_ptr; R14=needle_ptr;
  ADDQ          $16, R14                  //;48EB17D0 needle_ptr += 16                ;R14=needle_ptr;

  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  XORL          DX,  DX                   //;CF90D470                                 ;DX=rune_index;
//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion

loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data_msg := 0                   ;Z8=data_msg;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPBROADCASTD.Z (R14),K1,  Z9            //;B556F1BC load needle data                ;Z9=data_needle; K1=lane_active; R14=needle_ptr;

//; clear tail from data
  VPMINSD       Z3,  Z20, Z7              //;DEC17BF3 n_bytes_data := min(4, str_length);Z7=n_bytes_data; Z20=constd_4; Z3=str_length;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;

//; test to distinguish between all-ascii or mixed-ascii
  VPMOVB2M      Z8,  K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z8=data_msg;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPB        $4,  Z13, Z9,  K3         //;BBBDF880 K3 := (data_needle!=data_msg_upper);K3=tmp_mask; Z9=data_needle; Z13=data_msg_upper; 4=NotEqual;
  VPMOVM2B      K3,  Z26                  //;F3452970 promote 64x bit to 64x byte     ;Z26=scratch_Z26; K3=tmp_mask;
  VPTESTNMD     Z26, Z26, K1,  K1         //;E2969ED8 K1 &= (scratch_Z26 == 0); non zero means does not match;K1=lane_active; Z26=scratch_Z26;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

//; advance:
  VPADDD        Z7,  Z2,  Z2              //;302348A4 str_start += n_bytes_data       ;Z2=str_start; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  Z3              //;6569898C str_length -= n_bytes_data      ;Z3=str_length; Z7=n_bytes_data;
  ADDQ          $4,  R14                  //;2BC9E208 needle_ptr += 4                 ;R14=needle_ptr;
  ADDL          $48, DX                   //;F0BC3163 rune_index += 48                ;DX=rune_index;
  SUBL          $4,  CX                   //;646B86C9 n_runes -= 4                    ;CX=n_runes;
  JNLE          loop                      //;1EBC2C20 jump if not less or equal ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;
mixed_ascii:
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;

  VPCMPD.BCST   $0,  (R12)(DX*1),Z8,  K1,  K3  //;345D0BF3 K3 := K1 & (data_msg==[alt_ptr+rune_index]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  4(R12)(DX*1),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[alt_ptr+rune_index+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  8(R12)(DX*1),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[alt_ptr+rune_index+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  12(R12)(DX*1),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[alt_ptr+rune_index+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

//; advance:
  VPSRLD        $4,  Z9,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z9=data_needle;
  VPERMD        Z21, Z26, Z4              //;68FECBA0 get n_bytes_needle              ;Z4=n_bytes_needle; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z2,  Z2              //;DFE8D20B str_start += n_bytes_data       ;Z2=str_start; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  Z3              //;24E04BE7 str_length -= n_bytes_data      ;Z3=str_length; Z7=n_bytes_data;
  MOVL          X4,  R15                  //;18D7AD2B extract Z4                      ;R15=scratch; Z4=n_bytes_needle;
  ADDQ          R15, R14                  //;B2EF9837 needle_ptr += scratch           ;R14=needle_ptr; R15=scratch;

  ADDL          $16, DX                   //;1F8D79B1 rune_index += 16                ;DX=rune_index;
  DECL          CX                        //;A99E9290 n_runes--                       ;CX=n_runes;
  JNZ           loop                      //;80013DFA jump if not zero (ZF = 0)       ;
next:
  VPTESTNMD     Z3,  Z3,  K1,  K1         //;E555E77C K1 &= (str_length == 0)         ;K1=lane_active; Z3=str_length;
  NEXT()

//; #endregion bcCmpStrEqUTF8Ci

//; #region bcSkip1charLeft
//; skip the first UTF-8 codepoint in Z2:Z3
TEXT bcSkip1charLeft(SB), NOSPLIT|NOFRAME, $0
  VPTESTMD      Z3,  Z3,  K1,  K1         //;B1146BCF update lane mask with non-empty lanes;K1=lane_active; Z3=str_length;
  KTESTW        K1,  K1                   //;69D1CDA2 all lanes empty?                ;K1=lane_active;
  JZ            next                      //;A5924904 yes, then exit; jump if zero (ZF = 1);

  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
  VPADDD        Z7,  Z2,  K1,  Z2         //;45909060 str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
next:
  NEXT()
//; #endregion bcSkip1charLeft

//; #region bcSkip1charRight
//; skip the last UTF-8 codepoint in Z2:Z3
TEXT bcSkip1charRight(SB), NOSPLIT|NOFRAME, $0
  VPTESTMD      Z3,  Z3,  K1,  K1         //;B1146BCF update lane mask with non-empty lanes;K1=lane_active; Z3=str_length;
  KTESTW        K1,  K1                   //;69D1CDA2 all lanes empty?                ;K1=lane_active;
  JZ            next                      //;A5924904 yes, then exit; jump if zero (ZF = 1);

  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z27 //;F6E81301 load constant UTF8 2byte mask   ;Z27=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z28 //;B1E12620 load constant UTF8 3byte mask   ;Z28=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z29 //;D896A9E1 load constant UTF8 4byte mask   ;Z29=UTF8_4byte_mask;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPADDD        Z10, Z10, Z24             //;EDD57CAF load constant 2                 ;Z24=constd_2; Z10=constd_1;
  VPADDD        Z10, Z24, Z25             //;7E7A1CB0 load constant 3                 ;Z25=constd_3; Z24=constd_2; Z10=constd_1;
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPADDD        Z2,  Z3,  Z4              //;5684E300 compute end-of-string ptr       ;Z4=end_of_str; Z3=str_length; Z2=str_start;
  VPGATHERDD    -4(SI)(Z4*1),K3,  Z8      //;573D089A gather data from end            ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=end_of_str;

//; #region count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z27, Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data_msg; Z27=UTF8_2byte_mask;
  VPCMPD        $0,  Z27, Z26, K1,  K3    //;C6890BF4 K3 := K1 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K1=lane_active; Z26=scratch_Z26; Z27=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z28, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data_msg; Z28=UTF8_3byte_mask;
  VPCMPD        $0,  Z28, Z26, K1,  K4    //;14C32DC0 K4 := K1 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=tmp_mask2; K1=lane_active; Z26=scratch_Z26; Z28=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z29, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data_msg; Z29=UTF8_4byte_mask;
  VPCMPD        $0,  Z29, Z26, K1,  K5    //;1AE0A51C K5 := K1 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=tmp_mask3; K1=lane_active; Z26=scratch_Z26; Z29=UTF8_4byte_mask; 0=Eq;
  VMOVDQU32     Z10, Z7                   //;A7640B64 n_bytes_data := 1               ;Z7=n_bytes_data; Z10=constd_1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_data; K3=tmp_mask; Z10=constd_1;
  VPADDD        Z24, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_data; K4=tmp_mask2; Z24=constd_2;
  VPADDD        Z25, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_data; K5=tmp_mask3; Z25=constd_3;
//; #endregion count_bytes_code_point_right; data in Z8; result out Z7

  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
next:
  NEXT()
//; #endregion bcSkip1charRight

//; #region bcSkipNcharLeft
//; skip the first n UTF-8 codepoints in Z2:Z3
TEXT bcSkipNcharLeft(SB), NOSPLIT|NOFRAME, $0
//; #region load from stack-slot: load 16x uint32 into Z6
  LOADARG1Z(Z27, Z26)
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z26=scratch_Z26;
//; #endregion load from stack-slot
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_length>=counter)     ;K1=lane_active; Z3=str_length; Z6=counter; 5=GreaterEq;
  KTESTW        K1,  K1                   //;69D1CDA2                                 ;K1=lane_active;
  JZ            next                      //;A5924904 jump if zero (ZF = 1)           ;

  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
  VPADDD        Z7,  Z2,  K1,  Z2         //;45909060 str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;

  VPSUBD        Z10, Z6,  Z6              //;97723E12 counter--                       ;Z6=counter; Z10=constd_1;
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;DF88A710 K1 &= (0<=str_length); was the codepoint present?;K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
  VPTESTMD      Z6,  Z6,  K1,  K3         //;2E4360D2 any chars left to trim          ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;799F076E                                 ;K3=tmp_mask;
  JZ            next                      //;203DDAE1 any chars left? NO, loop next; jump if zero (ZF = 1);

  VPTERNLOGD.Z  $15, Z3,  Z3,  K1,  Z7    //;5D4D882F negate                          ;Z7=n_bytes_data; K1=lane_active; Z3=str_length;
  VPMOVD2M      Z7,  K3                   //;E1D7C41C                                 ;K3=tmp_mask; Z7=n_bytes_data;
  KANDW         K1,  K3,  K1              //;21163EF3                                 ;K1=lane_active; K3=tmp_mask;
  KTESTW        K1,  K1                   //;218EF478 any string left that are non-empty?;K1=lane_active;
  JNZ           loop                      //;B5466486 any chars left? Yes, loop again; jump if not zero (ZF = 0);
next:
  NEXT()
//; #endregion bcSkipNcharLeft

//; #region bcSkipNcharRight
//; skip the last n UTF-8 codepoints in the Z2:Z3
TEXT bcSkipNcharRight(SB), NOSPLIT|NOFRAME, $0
//; #region load from stack-slot: load 16x uint32 into Z6
  LOADARG1Z(Z27, Z26)
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z26=scratch_Z26;
//; #endregion load from stack-slot
  VPCMPD        $5,  Z6,  Z3,  K1,  K1    //;502E314F K1 &= (str_length>=counter)     ;K1=lane_active; Z3=str_length; Z6=counter; 5=GreaterEq;
  KTESTW        K1,  K1                   //;69D1CDA2                                 ;K1=lane_active;
  JZ            next                      //;A5924904 jump if zero (ZF = 1)           ;

  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z27 //;F6E81301 load constant UTF8 2byte mask   ;Z27=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z28 //;B1E12620 load constant UTF8 3byte mask   ;Z28=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z29 //;D896A9E1 load constant UTF8 4byte mask   ;Z29=UTF8_4byte_mask;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
  VPADDD        Z10, Z10, Z22             //;EDD57CAF load constant 2                 ;Z22=constd_2; Z10=constd_1;
  VPADDD        Z10, Z22, Z23             //;7E7A1CB0 load constant 3                 ;Z23=constd_3; Z22=constd_2; Z10=constd_1;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPADDD        Z2,  Z3,  Z4              //;5684E300 end_of_str := str_length + str_start;Z4=end_of_str; Z3=str_length; Z2=str_start;
  VPGATHERDD    -4(SI)(Z4*1),K3,  Z8      //;573D089A gather data from end            ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=end_of_str;

//; #region count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z27, Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data_msg; Z27=UTF8_2byte_mask;
  VPCMPD        $0,  Z27, Z26, K1,  K3    //;C6890BF4 K3 := K1 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K1=lane_active; Z26=scratch_Z26; Z27=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z28, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data_msg; Z28=UTF8_3byte_mask;
  VPCMPD        $0,  Z28, Z26, K1,  K4    //;14C32DC0 K4 := K1 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=tmp_mask2; K1=lane_active; Z26=scratch_Z26; Z28=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z29, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data_msg; Z29=UTF8_4byte_mask;
  VPCMPD        $0,  Z29, Z26, K1,  K5    //;1AE0A51C K5 := K1 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=tmp_mask3; K1=lane_active; Z26=scratch_Z26; Z29=UTF8_4byte_mask; 0=Eq;
  VMOVDQU32     Z10, Z7                   //;A7640B64 n_bytes_data := 1               ;Z7=n_bytes_data; Z10=constd_1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_data; K3=tmp_mask; Z10=constd_1;
  VPADDD        Z22, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_data; K4=tmp_mask2; Z22=constd_2;
  VPADDD        Z23, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_data; K5=tmp_mask3; Z23=constd_3;
//; #endregion count_bytes_code_point_right; data in Z8; result out Z7
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z10, Z6,  Z6              //;97723E12 counter--                       ;Z6=counter; Z10=constd_1;
  VPCMPD        $2,  Z3,  Z11, K1,  K1    //;DF88A710 K1 &= (0<=str_length); was the codepoint present?;K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
  VPTESTMD      Z6,  Z6,  K1,  K3         //;2E4360D2 any chars left to trim          ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;799F076E                                 ;K3=tmp_mask;
  JZ            next                      //;203DDAE1 any chars left? NO, loop next; jump if zero (ZF = 1);

  VPTERNLOGD.Z  $15, Z3,  Z3,  K1,  Z7    //;5D4D882F negate                          ;Z7=n_bytes_data; K1=lane_active; Z3=str_length;
  VPMOVD2M      Z7,  K3                   //;E1D7C41C                                 ;K3=tmp_mask; Z7=n_bytes_data;
  KANDW         K1,  K3,  K1              //;21163EF3                                 ;K1=lane_active; K3=tmp_mask;
  KTESTW        K1,  K1                   //;218EF478 any string left that are non-empty?;K1=lane_active;
  JNZ           loop                      //;B5466486 any chars left? Yes, loop again; jump if not zero (ZF = 0);
next:
  NEXT()
//; #endregion bcSkipNcharRight

//; #region bcTrimWsLeft
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimWsLeft(SB), NOSPLIT|NOFRAME, $0
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
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
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
  NEXT()
//; #endregion bcTrimWsLeft

//; #region bcTrimWsRight
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimWsRight(SB), NOSPLIT|NOFRAME, $0
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
  VPADDD        Z3,  Z2,  Z14             //;00000000 str_pos_end := str_start + str_length;Z14=str_pos_end; Z2=str_start; Z3=str_length;
  VPSUBD        Z20, Z14, Z14             //;00000000 str_pos_end -= 4                ;Z14=str_pos_end; Z20=constd_4;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z14*1),K3,  Z8       //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z14=str_pos_end;
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
  VPLZCNTD      Z8,  K1,  Z8              //;90920F43 count leading zeros             ;Z8=data_msg; K1=lane_active;
  VPSRLD        $3,  Z8,  K1,  Z8         //;68276EFE divide by 8 yields byte_count   ;Z8=data_msg; K1=lane_active;
  VPMINSD       Z3,  Z8,  K1,  Z8         //;6616691F take minimun of length          ;Z8=data_msg; K1=lane_active; Z3=str_length;
//; #endregion zmm8 = #bytes

  VPSUBD        Z8,  Z14, K1,  Z14        //;40C40F7D str_pos_end -= data_msg         ;Z14=str_pos_end; K1=lane_active; Z8=data_msg;
  VPSUBD        Z8,  Z3,  K1,  Z3         //;63A2C77B str_length -= data_msg          ;Z3=str_length; K1=lane_active; Z8=data_msg;
//; select lanes that have([essential] remaining string length > 0)
  VPCMPD        $2,  Z3,  Z11, K1,  K2    //;94B55922 K2 := K1 & (0<=str_length)      ;K2=scratch_mask1; K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
//; select lanes that have([optimization] number of trimmed chars = 4)
  VPCMPD        $0,  Z20, Z8,  K2,  K2    //;D3BA3C05 K2 &= (data_msg==4)             ;K2=scratch_mask1; Z8=data_msg; Z20=constd_4; 0=Eq;
  KTESTW        K2,  K2                   //;7CB2A200                                 ;K2=scratch_mask1;
  JNZ           loop                      //;00000000 jump if not zero (ZF = 0)       ;

next:
  NEXT()
//; #endregion bcTrimWsRight

//; #region bcTrim4charLeft
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrim4charLeft(SB), NOSPLIT|NOFRAME, $0
//; #region load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
//; #region load 4chars
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;26BB22F5 Load ptr of string              ;R14=chars_ptr; R14=chars_slice;
  MOVL          (R14),R14                 //;B7C25D43 Load first 4 chars              ;R14=chars_ptr;
  VPBROADCASTB  R14, Z9                   //;96085025                                 ;Z9=c_char0; R14=chars_ptr;
  SHRL          $8,  R14                  //;63D19F3B                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z10                  //;FCEBCAA6                                 ;Z10=c_char1; R14=chars_ptr;
  SHRL          $8,  R14                  //;E5627E10                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z12                  //;66A9E2D3                                 ;Z12=c_char2; R14=chars_ptr;
  SHRL          $8,  R14                  //;C5E83B19                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z13                  //;C18E3641                                 ;Z13=c_char3; R14=chars_ptr;
//; #endregion load 4chars
//; #endregion load constants

loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
//; #region trim left/right 4char comparison
  VPCMPB        $0,  Z9,  Z8,  K3         //;D8545E6D K3 := (data_msg==c_char0); is char == char0;K3=tmp_mask; Z8=data_msg; Z9=c_char0; 0=Eq;
  VPCMPB        $0,  Z10, Z8,  K2         //;933CFC19 K2 := (data_msg==c_char1); is char == char1;K2=scratch2_mask; Z8=data_msg; Z10=c_char1; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z12, Z8,  K2         //;D206A939 K2 := (data_msg==c_char2); is char == char2;K2=scratch2_mask; Z8=data_msg; Z12=c_char2; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z13, Z8,  K2         //;AB8B7AAA K2 := (data_msg==c_char3); is char == char3;K2=scratch2_mask; Z8=data_msg; Z13=c_char3; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  KORTESTQ      K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found : no need to update string_start_position; jump if zero (ZF = 1);
//; #endregion

//; #region convert mask to selected byte count
  VPMOVM2B      K3,  Z7                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z7=n_bytes_data; K3=tmp_mask;
  VPTERNLOGQ    $15, Z7,  Z7,  Z7         //;249B4036 negate                          ;Z7=n_bytes_data;
  VPSHUFB       Z22, Z7,  Z7              //;8CF1488E reverse byte order              ;Z7=n_bytes_data; Z22=constant_bswap32;
  VPLZCNTD      Z7,  K1,  Z7              //;90920F43 count leading zeros             ;Z7=n_bytes_data; K1=lane_active;
  VPSRLD        $3,  Z7,  K1,  Z7         //;68276EFE divide by 8 yields byte_count   ;Z7=n_bytes_data; K1=lane_active;
  VPMINSD       Z3,  Z7,  K1,  Z7         //;6616691F take minimun of length          ;Z7=n_bytes_data; K1=lane_active; Z3=str_length;
//; #endregion zmm7 = #bytes

  VPADDD        Z7,  Z2,  K1,  Z2         //;40C40F7D str_start += n_bytes_data       ;Z2=str_start; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;63A2C77B str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
//; select lanes that have([essential] remaining string length > 0)
  VPCMPD        $2,  Z3,  Z11, K1,  K2    //;94B55922 K2 := K1 & (0<=str_length)      ;K2=scratch_mask1; K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
//; select lanes that have([optimization] number of trimmed chars = 4)
  VPCMPD        $0,  Z20, Z8,  K2,  K2    //;D3BA3C05 K2 &= (data_msg==4)             ;K2=scratch_mask1; Z8=data_msg; Z20=constd_4; 0=Eq;
  KTESTW        K2,  K2                   //;7CB2A200                                 ;K2=scratch_mask1;
  JNZ           loop                      //;00000000 jump if not zero (ZF = 0)       ;

next:
  NEXT()
//; #endregion bcTrim4charLeft

//; #region bcTrim4charRight
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrim4charRight(SB), NOSPLIT|NOFRAME, $0
//; #region load constants
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
//; #region load 4chars
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;26BB22F5 Load ptr of string              ;R14=chars_ptr; R14=chars_slice;
  MOVL          (R14),R14                 //;B7C25D43 Load first 4 chars              ;R14=chars_ptr;
  VPBROADCASTB  R14, Z9                   //;96085025                                 ;Z9=c_char0; R14=chars_ptr;
  SHRL          $8,  R14                  //;63D19F3B                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z10                  //;FCEBCAA6                                 ;Z10=c_char1; R14=chars_ptr;
  SHRL          $8,  R14                  //;E5627E10                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z12                  //;66A9E2D3                                 ;Z12=c_char2; R14=chars_ptr;
  SHRL          $8,  R14                  //;C5E83B19                                 ;R14=chars_ptr;
  VPBROADCASTB  R14, Z13                  //;C18E3641                                 ;Z13=c_char3; R14=chars_ptr;
//; #endregion load 4chars
//; #endregion load constants

  VPADDD        Z3,  Z2,  Z14             //;813A5F04 str_pos_end := str_start + str_length;Z14=str_pos_end; Z2=str_start; Z3=str_length;
  VPSUBD        Z20, Z14, Z14             //;EAF06C41 str_pos_end -= 4                ;Z14=str_pos_end; Z20=constd_4;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z14*1),K3,  Z8       //;68B7D88C gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z14=str_pos_end;
//; #region trim left/right 4char comparison
  VPCMPB        $0,  Z9,  Z8,  K3         //;D8545E6D K3 := (data_msg==c_char0); is char == char0;K3=tmp_mask; Z8=data_msg; Z9=c_char0; 0=Eq;
  VPCMPB        $0,  Z10, Z8,  K2         //;933CFC19 K2 := (data_msg==c_char1); is char == char1;K2=scratch2_mask; Z8=data_msg; Z10=c_char1; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z12, Z8,  K2         //;D206A939 K2 := (data_msg==c_char2); is char == char2;K2=scratch2_mask; Z8=data_msg; Z12=c_char2; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  VPCMPB        $0,  Z13, Z8,  K2         //;AB8B7AAA K2 := (data_msg==c_char3); is char == char3;K2=scratch2_mask; Z8=data_msg; Z13=c_char3; 0=Eq;
  KORQ          K2,  K3,  K3              //;00000000                                 ;K3=tmp_mask; K2=scratch2_mask;
  KORTESTQ      K3,  K3                   //;A522D4C2 1 for every whitespace          ;K3=tmp_mask;
  JZ            next                      //;DC07C307 no matching chars found : no need to update string_start_position; jump if zero (ZF = 1);
//; #endregion

//; #region convert mask to selected byte count
  VPMOVM2B      K3,  Z7                   //;B0C4D1C5 promote 64x bit to 64x byte     ;Z7=n_bytes_data; K3=tmp_mask;
  VPTERNLOGQ    $15, Z7,  Z7,  Z7         //;249B4036 negate                          ;Z7=n_bytes_data;
  VPLZCNTD      Z7,  K1,  Z7              //;90920F43 count leading zeros             ;Z7=n_bytes_data; K1=lane_active;
  VPSRLD        $3,  Z7,  K1,  Z7         //;68276EFE divide by 8 yields byte_count   ;Z7=n_bytes_data; K1=lane_active;
  VPMINSD       Z3,  Z7,  K1,  Z7         //;6616691F take minimun of length          ;Z7=n_bytes_data; K1=lane_active; Z3=str_length;
//; #endregion zmm7 = #bytes

  VPSUBD        Z7,  Z14, K1,  Z14        //;40C40F7D str_pos_end -= n_bytes_data     ;Z14=str_pos_end; K1=lane_active; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;63A2C77B str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;
//; select lanes that have([essential] remaining string length > 0)
  VPCMPD        $2,  Z3,  Z11, K1,  K2    //;94B55922 K2 := K1 & (0<=str_length)      ;K2=scratch_mask1; K1=lane_active; Z11=constd_0; Z3=str_length; 2=LessEq;
//; select lanes that have([optimization] number of trimmed chars = 4)
  VPCMPD        $0,  Z20, Z8,  K2,  K2    //;D3BA3C05 K2 &= (data_msg==4)             ;K2=scratch_mask1; Z8=data_msg; Z20=constd_4; 0=Eq;
  KTESTW        K2,  K2                   //;7CB2A200                                 ;K2=scratch_mask1;
  JNZ           loop                      //;00000000 jump if not zero (ZF = 0)       ;

next:
  NEXT()
//; #endregion bcTrim4charRight

//; #region bcTrimPrefixCs
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimPrefixCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z14                //;713DF24F bcst needle_length              ;Z14=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z14, Z3,  K1,  K2    //;502E314F K2 := K1 & (str_length>=needle_length);K2=lanes_local; K1=lane_active; Z3=str_length; Z14=needle_length; 5=GreaterEq;
  KTESTW        K2,  K2                   //;6E50BE85 any lanes eligible?             ;K2=lanes_local;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z14, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z14=needle_length;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z4                   //;6F6F1342 search_base := str_start        ;Z4=search_base; Z2=str_start;

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VPCMPD.BCST   $0,  (R14),Z8,  K2,  K2   //;F0E5B3BD K2 &= (data_msg==Address())     ;K2=lanes_local; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KTESTW        K2,  K2                   //;5746030A any lanes still alive?          ;K2=lanes_local;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
tail:
  VPTESTMD      Z6,  Z6,  K2,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K2=lanes_local; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4)              ;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D 4 or more chars in needle       ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
  VPCMPD        $0,  Z9,  Z8,  K2,  K2    //;474761AE K2 &= (data_msg==data_needle)   ;K2=lanes_local; Z8=data_msg; Z9=data_needle; 0=Eq;
  VPADDD        Z14, Z2,  K2,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K2=lanes_local; Z14=needle_length;
  VPSUBD        Z14, Z3,  K2,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K2=lanes_local; Z14=needle_length;
next:
  NEXT()
//; #endregion bcTrimPrefixCs

//; #region bcTrimPrefixCi
//; Z2 = string offsets. Contains the start position of the strings, which may be updated (increased)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimPrefixCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z14                //;713DF24F bcst needle_length              ;Z14=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z14, Z3,  K1,  K2    //;502E314F K2 := K1 & (str_length>=needle_length);K2=lanes_local; K1=lane_active; Z3=str_length; Z14=needle_length; 5=GreaterEq;
  KTESTW        K2,  K2                   //;6E50BE85 any lanes eligible?             ;K2=lanes_local;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z14, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z14=needle_length;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z4                   //;6F6F1342 search_base := str_start        ;Z4=search_base; Z2=str_start;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD.BCST   $0,  (R14),Z13, K2,  K2   //;F0E5B3BD K2 &= (data_msg_upper==Address());K2=lanes_local; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K2,  K2                   //;5746030A any lanes still alive?          ;K2=lanes_local;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
tail:
  VPTESTMD      Z6,  Z6,  K2,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K2=lanes_local; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4)              ;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D 4 or more chars in needle       ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K2,  K2    //;474761AE K2 &= (data_msg_upper==data_needle);K2=lanes_local; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
  VPADDD        Z14, Z2,  K2,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K2=lanes_local; Z14=needle_length;
  VPSUBD        Z14, Z3,  K2,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K2=lanes_local; Z14=needle_length;
next:
  NEXT()
//; #endregion bcTrimPrefixCi

//; #region bcTrimSuffixCs

//; Z2 = string offsets. Contains the start position of the strings (unchanged)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimSuffixCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z14                //;713DF24F bcst needle_length              ;Z14=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z14, Z3,  K1,  K2    //;502E314F K2 := K1 & (str_length>=needle_length);K2=lanes_local; K1=lane_active; Z3=str_length; Z14=needle_length; 5=GreaterEq;
  KTESTW        K2,  K2                   //;6E50BE85 any lanes eligible?             ;K2=lanes_local;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z14, Z6                   //;6F6F1342 needle_length_idx := needle_length;Z6=needle_length_idx; Z14=needle_length;
  VPSUBD        Z14, Z3,  K2,  Z4         //;4ADB5015 search_base := str_length - needle_length;Z4=search_base; K2=lanes_local; Z3=str_length; Z14=needle_length;
  VPADDD        Z2,  Z4,  Z4              //;3E1762B7 search_base += str_start        ;Z4=search_base; Z2=str_start;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VPCMPD.BCST   $0,  (R14),Z8,  K2,  K2   //;F0E5B3BD K2 &= (data_msg==Address())     ;K2=lanes_local; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KTESTW        K2,  K2                   //;5746030A any lanes still alive?          ;K2=lanes_local;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 needle_length_idx -= 4          ;Z6=needle_length_idx; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
tail:
  VPTESTMD      Z6,  Z6,  K2,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K2=lanes_local; Z6=needle_length_idx;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (needle_length_idx>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=needle_length_idx; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=needle_length_idx; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle and mask            ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

  VPCMPD        $0,  Z9,  Z8,  K2,  K2    //;474761AE K2 &= (data_msg==data_needle)   ;K2=lanes_local; Z8=data_msg; Z9=data_needle; 0=Eq;
update:
  VPSUBD        Z14, Z3,  K2,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K2=lanes_local; Z14=needle_length;
next:
  NEXT()
//; #endregion bcTrimSuffixCs

//; #region bcTrimSuffixCi

//; Z2 = string offsets. Contains the start position of the strings (unchanged)
//; Z3 = string lengths. Contains the length of the strings, which may be updated (decreased)
TEXT bcTrimSuffixCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z14                //;713DF24F bcst needle_length              ;Z14=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z14, Z3,  K1,  K2    //;502E314F K2 := K1 & (str_length>=needle_length);K2=lanes_local; K1=lane_active; Z3=str_length; Z14=needle_length; 5=GreaterEq;
  KTESTW        K2,  K2                   //;6E50BE85 any lanes eligible?             ;K2=lanes_local;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z14, Z6                   //;6F6F1342 needle_length_idx := needle_length;Z6=needle_length_idx; Z14=needle_length;
  VPSUBD        Z14, Z3,  K2,  Z4         //;4ADB5015 search_base := str_length - needle_length;Z4=search_base; K2=lanes_local; Z3=str_length; Z14=needle_length;
  VPADDD        Z2,  Z4,  Z4              //;3E1762B7 search_base += str_start        ;Z4=search_base; Z2=str_start;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD.BCST   $0,  (R14),Z13, K2,  K2   //;F0E5B3BD K2 &= (data_msg_upper==Address());K2=lanes_local; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K2,  K2                   //;5746030A any lanes still alive?          ;K2=lanes_local;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 needle_length_idx -= 4          ;Z6=needle_length_idx; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z4,  Z4              //;D7CC90DD search_base += 4                ;Z4=search_base; Z20=constd_4;
tail:
  VPTESTMD      Z6,  Z6,  K2,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K2=lanes_local; Z6=needle_length_idx;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);

  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (needle_length_idx>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=needle_length_idx; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lanes_local;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=needle_length_idx; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;BF3EB085 load needle and mask            ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K2,  K2    //;474761AE K2 &= (data_msg_upper==data_needle);K2=lanes_local; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
update:
  VPSUBD        Z14, Z3,  K2,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K2=lanes_local; Z14=needle_length;
next:
  NEXT()
//; #endregion bcTrimSuffixCi

//; #region bcContainsSubstrCs

#define TEST_CHAR(c)                        \
    VPALIGNR      c,  Z27, Z26, Z25         \
    VPSRLDQ       c,  Z24, Z29              \
    VPBROADCASTB  X29, Z29                  \
    VPCMPB        $0,  Z25, Z29, K2,  K2    \
    DECL          BX                        \
    JLE           end_of_needle_13          \
    KTESTQ        K2,  K2                   \
    JZ            found_nothing_14          \

#define SHIFT_DATA_TEST_CHAR()              \
    VMOVDQU64     Z26, Z27                  \
    VPERMT2Q      Z28, Z14, Z26             \
    VPERMT2Q      Z26, Z14, Z28             \
    VPERMQ        Z24, Z14, Z24             \
    VPBROADCASTB  X24, Z29                  \
    VPCMPB        $0,  Z27, Z29, K2,  K2    \
    DECL          BX                        \
    JLE           end_of_needle_13          \
    KTESTQ        K2,  K2                   \
    JZ            found_nothing_14          \

TEXT bcContainsSubstrCs(SB), NOSPLIT|NOFRAME, $0
    VMOVQ          R9,  X9                  //;00000000                               ;X9=storage0;
    VMOVQ          R10, X10                 //;00000000                               ;X10=storage1;
    VMOVQ          R11, X11                 //;00000000                               ;X11=storage2;
    VMOVQ          R12, X12                 //;00000000                               ;X12=storage3;
    VMOVQ          DI,  X13                 //;00000000                               ;X13=storage4;
    IMM_FROM_DICT(R9)
    MOVQ          (R9),R11                  //;D2647DF0 load needle ptr               ;R11=needle_base_ptr; R9=needle_slice;
    MOVQ          8(R9),CX                  //;36F86C1A load len(needle): for SliceHeader see https://mmcloughlin.com/posts/golang-asm-slice-arg;CX=needle_length_gpr; R9=needle_slice;
    INCQ          R11                       //;48822F75 remove the length byte        ;R11=needle_base_ptr;
    DECL          CX                        //;789B1946 substract the length byte     ;CX=needle_length_gpr;

//; #region load constants
    VMOVDQU64     c_2lane_shift<>(SB),Z14   //;F4B92304                               ;Z14=c_2lane_shift;
//; #endregion load constants

//; #region fast fail if all string are shorter than the needle we are search for
    VPBROADCASTD  CX,  Z20                  //;15B88E2A                               ;Z20=scratch1; CX=needle_length_gpr;
    VPCMPD        $2,  Z3,  Z20, K1,  K1    //;6C59E4A1 is needle_length < str_length?;K1=lane_mask16; Z20=scratch1; Z3=str_pos_length; 2=LessEq;
    KTESTW        K1,  K1                   //;2C853185 everything is too short?      ;K1=lane_mask16;
    JZ            end_1                     //;D69FB67D yes: found nothing; jump if zero (ZF = 1);
//; #endregion

    VMOVDQU64     (R11),Z20                 //;CFCAA793 load needle 64chars           ;Z20=needle_content; R11=needle_base_ptr;
    KMOVW         K1,  DI                   //;A288B0DD copy initial lane index       ;DI=result_mask_gpr; K1=lane_mask16;
    XORL          R10, R10                  //;45423FAD reset lanes index             ;R10=lane_index (0-15);
lane_loop_0:
    BTL           R10, DI                   //;F312D50D is lane at index alive?       ;DI=result_mask_gpr; R10=lane_index (0-15);
    JNC           next_2                    //;BFC28F0C no, try next lane; jump if not carry (CF = 0);

//; #region load_gpr_from_ZMM
    VPBROADCASTD  R10, Z21                  //;F4920F59                               ;Z21=scratch2; R10=lane_index (0-15);
    VPERMD        Z2,  Z21, Z4              //;E8C67F1E copy posN to all postions     ;Z4=scratch1; Z21=scratch2; Z2=str_pos_start;
    MOVL          X4,  R8                   //;2A86FDF0 extract from pos0             ;R8=str_pos_start_gpr; X4=scratch1;
    VPERMD        Z3,  Z21, Z4              //;6E161E3D copy posN to all postions     ;Z4=scratch1; Z21=scratch2; Z3=str_pos_length;
    MOVL          X4,  R12                  //;5B90F74A extract from pos0             ;R12=str_length_gpr; X4=scratch1;
//; #endregion load_gpr_from_ZMM

    ADDQ          SI,  R8                   //;57F25882                               ;R8=str_pos_start_gpr; SI=raw_ptr;
    MOVQ          R8,  R15                  //;9B22C3FA                               ;R15=data_index; R8=str_pos_start_gpr;
    TESTQ         CX,  CX                   //;4AC1011A is needle empty?              ;CX=needle_length_gpr;
    JZ            found_something_4         //;2328990B yes, empty needle is by definition everywhere present; jump if zero (ZF = 1);

//; #region gen_tail_block_mask
    MOVQ          R12, BX                   //;99E240A3                               ;BX=scratch; R12=str_length_gpr;
    ANDQ          $63, BX                   //;284163CA                               ;BX=scratch;
    JNZ           mixed_block_9             //;9A30F875 jump if not zero (ZF = 0)     ;
    VPTERNLOGQ    $255,Z23, Z23, Z23        //;B34B5A81                               ;Z23=tail_block_mask;
    JMP           end_tail_block_10         //;D71F71DF                               ;
mixed_block_9:
    MOVL          $1,  DX                   //;4EBF722E                               ;DX=scratch1;
    SHLXQ         BX,  DX,  DX              //;AE13A7DF                               ;DX=scratch1; BX=scratch;
    DECQ          DX                        //;2F7AED29                               ;DX=scratch1;
    KMOVQ         DX,  K3                   //;FF5DB827                               ;K3=scratch_mask; DX=scratch1;
    VPMOVM2B      K3,  Z23                  //;22539847                               ;Z23=tail_block_mask; K3=scratch_mask;
end_tail_block_10://; #endregion

    MOVQ          R12, R14                  //;998DBF90                               ;R14=remaining_blocks; R12=str_length_gpr;
//; #region load data0 and data1
    VMOVDQU64     (R15),Z21                 //;83CE2243 load 1ste block of data       ;Z21=data0; R15=data_index;
    DECQ          R14                       //;BE20F9DB                               ;R14=remaining_blocks;
    SHRQ          $6,  R14                  //;F5D45AD6 initial #remaining blocks     ;R14=remaining_blocks;
    JNZ           done_load_block_12        //;ADACFD2E second block present? yes, load it; jump if not zero (ZF = 0);
    VPANDQ        Z23, Z21, Z21             //;3FF83124 first block is mixed          ;Z21=data0; Z23=tail_block_mask;
    VPXORQ        Z22, Z22, Z22             //;FC3F00CB second block is empty         ;Z22=data1;
    JMP           done_load_first_blocks_11 //;00000000                               ;

done_load_block_12:
    VMOVDQU64     64(R15),Z22               //;83CE2243 load 2nd block of data        ;Z22=data1; R15=data_index;
    CMPQ          R14, $1                   //;C7AE9498                               ;R14=remaining_blocks;
    JG            done_load_first_blocks_11 //;CEDA037B next block present? yes, no need for tail_mask; jump if greater ((ZF = 0) and (SF = OF));
    VPANDQ        Z23, Z22, Z22             //;C79CD914 second block is mixed         ;Z22=data1; Z23=tail_block_mask;

done_load_first_blocks_11:
//; #endregion

loop_5:
    VPBROADCASTB  X20, Z24                  //;57BECC93 load needle char0             ;Z24=needle_char; X20=needle_content;
    VPCMPB        $0,  Z21, Z24, K2         //;9E9A1570 update the observed_mask      ;K2=observed_mask; Z24=needle_char; Z21=data0; 0=Eq;
    KTESTQ        K2,  K2                   //;69B36EC7 is char0 present?             ;K2=observed_mask;
    JZ            next_block_6              //;DB125A73 no, try next block; jump if zero (ZF = 1);
    MOVQ          CX,  BX                   //;96DBB4C3 copy needle                   ;BX=needle_index; CX=needle_length_gpr;
    DECL          BX                        //;23AB2920 dec needle length             ;BX=needle_index;
    JLE           found_something_4         //;2ACECDCD at end of needle already?; jump if less or equal ((ZF = 1) or (SF neq OF));
    VMOVDQU64     Z20, Z24                  //;4B77B339                               ;Z24=needle_content_shifted_lane; Z20=needle_content;

    VMOVDQU64     Z21, Z27                  //;E141A815                               ;Z27=data0_block_prev; Z21=data0;
    VMOVDQU64     Z21, Z26                  //;F05F9A8A                               ;Z26=data0_block_curr; Z21=data0;
    VPERMT2Q      Z22, Z14, Z26             //;2A81E188                               ;Z26=data0_block_curr; Z14=c_2lane_shift; Z22=data1;
    VMOVDQU64     Z22, Z28                  //;BB1C4234                               ;Z28=data1_block_curr; Z22=data1;
    VPERMT2Q      Z21, Z14, Z28             //;2A9975DA                               ;Z28=data1_block_curr; Z14=c_2lane_shift; Z21=data0;

//; #region test chars
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)

//; #region test char 63
    VPALIGNR      $15, Z27, Z26, Z25        //;C0E48226 shift data                    ;Z25=data0_shifted_byte; Z26=data0_block_curr; Z27=data0_block_prev;
    VPSRLDQ       $15, Z24, Z29             //;A6DAB775 shift needle                  ;Z29=needle_char; Z24=needle_content_shifted_lane;
    VPBROADCASTB  X29, Z29                  //;6BDA1B78 load char63 from needle       ;Z29=needle_char; X29=needle_char;
    VPCMPB        $0,  Z25, Z29, K2,  K2    //;9E9A1570 update the observed_mask      ;K2=observed_mask; Z29=needle_char; Z25=data0_shifted_byte; 0=Eq;
//; #endregion
end_of_needle_13:
    KTESTQ        K2,  K2                   //;C6DD0CAF is needle present?            ;K2=observed_mask;
    JNZ           found_something_4         //;9ECCEE8C yes, found something!; jump if not zero (ZF = 0);

found_nothing_14:
//; #endregion

next_block_6:
//; #region gen_tail_block_mask
    TESTQ         R14, R14                  //;00000000                               ;R14=remaining_blocks;
    JZ            found_nothing_3           //;00000000 jump if zero (ZF = 1)         ;
    VMOVDQU64     Z22, Z21                  //;00000000                               ;Z21=data0; Z22=data1;
    ADDQ          $64, R15                  //;00000000                               ;R15=data_index;
    DECQ          R14                       //;00000000                               ;R14=remaining_blocks;
    JNZ           label_A_16                //;00000000 jump if not zero (ZF = 0)     ;
    VPXORQ        Z22, Z22, Z22             //;00000000                               ;Z22=data1;
    JMP           done_load_next_block_15   //;00000000                               ;
label_A_16:
    VMOVDQU64     64(R15),Z22               //;00000000                               ;Z22=data1; R15=data_index;
    CMPQ          R14, $1                   //;00000000                               ;R14=remaining_blocks;
    JG            done_load_next_block_15   //;00000000 jump if greater ((ZF = 0) and (SF = OF));
    VPANDQ        Z23, Z22, Z22             //;00000000                               ;Z22=data1; Z23=tail_block_mask;
done_load_next_block_15:
//; #endregion
    JMP           loop_5                    //;BE22EE80                               ;

found_nothing_3:
    BTRL          R10, DI                   //;2E08A28B needle was not present, mark the lane as dead;DI=result_mask_gpr; R10=lane_index (0-15);
found_something_4:
next_2:
    INCL          R10                       //;7307E80D increment lane index          ;R10=lane_index (0-15);
    CMPL          R10, $16                  //;5EF664AF are we at last lane index?    ;R10=lane_index (0-15);
    JNZ           lane_loop_0               //;4CBFD545 no, loop again; jump if not zero (ZF = 0);
    KMOVW         DI,  K1                   //;00000000                               ;K1=lane_mask16; DI=result_mask_gpr;
end_1:
    VMOVQ          X9,  R9                  //;00000000                               ;X9=storage0;
    VMOVQ          X10, R10                 //;00000000                               ;X10=storage1;
    VMOVQ          X11, R11                 //;00000000                               ;X11=storage2;
    VMOVQ          X12, R12                 //;00000000                               ;X12=storage3;
    VMOVQ          X13, DI                  //;00000000                               ;X13=storage4;
    NEXT()

//; #endregion bcContainsSubstrCs

//; #region bcContainsSubstrCi
TEXT bcContainsSubstrCi(SB), NOSPLIT|NOFRAME, $0
    VMOVQ          R9,  X9                  //;00000000                               ;X9=storage0;
    VMOVQ          R10, X10                 //;00000000                               ;X10=storage1;
    VMOVQ          R11, X11                 //;00000000                               ;X11=storage2;
    VMOVQ          R12, X12                 //;00000000                               ;X12=storage3;
    VMOVQ          DI,  X13                 //;00000000                               ;X13=storage4;
    IMM_FROM_DICT(R9)
    MOVQ          (R9),R11                  //;D2647DF0 load needle ptr               ;R11=needle_base_ptr; R9=needle_slice;
    MOVQ          8(R9),CX                  //;36F86C1A load len(needle): for SliceHeader see https://mmcloughlin.com/posts/golang-asm-slice-arg;CX=needle_length_gpr; R9=needle_slice;
    INCQ          R11                       //;48822F75 remove the length byte        ;R11=needle_base_ptr;
    DECL          CX                        //;789B1946 substract the length byte     ;CX=needle_length_gpr;

//; #region load constants
    VMOVDQU64     c_2lane_shift<>(SB),Z14   //;F4B92304                               ;Z14=c_2lane_shift;
//; #region loading to_upper constants
    MOVL          $8020256,R8               //;00000000                               ;R8=tmp_constant;
    VPBROADCASTB  R8,  Z15                  //;00000000                               ;Z15=c_0b00100000; R8=tmp_constant;
    SHRL          $8,  R8                   //;00000000                               ;R8=tmp_constant;
    VPBROADCASTB  R8,  Z16                  //;00000000                               ;Z16=c_char_a; R8=tmp_constant;
    SHRL          $8,  R8                   //;00000000                               ;R8=tmp_constant;
    VPBROADCASTB  R8,  Z17                  //;00000000                               ;Z17=c_char_z; R8=tmp_constant;
//; #endregion
//; #endregion load constants

//; #region fast fail if all string are shorter than the needle we are search for
    VPBROADCASTD  CX,  Z20                  //;15B88E2A                               ;Z20=scratch1; CX=needle_length_gpr;
    VPCMPD        $2,  Z3,  Z20, K1,  K1    //;6C59E4A1 is needle_length < str_length?;K1=lane_mask16; Z20=scratch1; Z3=str_pos_length; 2=LessEq;
    KTESTW        K1,  K1                   //;2C853185 everything is too short?      ;K1=lane_mask16;
    JZ            end_1                     //;D69FB67D yes: found nothing; jump if zero (ZF = 1);
//; #endregion

    VMOVDQU64     (R11),Z20                 //;CFCAA793 load needle 64chars           ;Z20=needle_content; R11=needle_base_ptr;
//; #region str_to_upper
    VPCMPB        $5,  Z16, Z20, K2         //;30E9B9FD larger than a?                ;K2=scratch_mask; Z20=needle_content; Z16=c_char_a; 5=GreaterThen;
    VPCMPB        $2,  Z17, Z20, K3         //;8CE85BA0 smaller than z?           ;K3=scratch_mask2; Z20=needle_content; Z17=c_char_z; 2=LessEq;
    KANDQ         K3,  K2,  K2              //;00000000                               ;K2=scratch_mask; K3=scratch_mask2;
    VPMOVM2B      K2,  Z21                  //;6433A8DD mask with selected chars      ;Z21=scratch; K2=scratch_mask;
    VPTERNLOGQ    $76, Z15, Z20, Z21        //;B1CB1982 magic! see generator doc      ;Z21=scratch; Z20=needle_content; Z15=c_0b00100000;
//; TODO: is the next move really necessary?
    VMOVDQU64     Z21, Z20                  //;E73844C3                               ;Z20=needle_content; Z21=scratch;
//; #endregion str_to_upper
    KMOVW         K1,  DI                   //;A288B0DD copy initial lane index       ;DI=result_mask_gpr; K1=lane_mask16;
    XORL          R10, R10                  //;45423FAD reset lanes index             ;R10=lane_index (0-15);
lane_loop_0:
    BTL           R10, DI                   //;F312D50D is lane at index alive?       ;DI=result_mask_gpr; R10=lane_index (0-15);
    JNC           next_2                    //;BFC28F0C no, try next lane; jump if not carry (CF = 0);

//; #region load_gpr_from_ZMM
    VPBROADCASTD  R10, Z21                  //;F4920F59                               ;Z21=scratch2; R10=lane_index (0-15);
    VPERMD        Z2,  Z21, Z4              //;E8C67F1E copy posN to all postions     ;Z4=scratch1; Z21=scratch2; Z2=str_pos_start;
    MOVL          X4,  R8                   //;2A86FDF0 extract from pos0             ;R8=str_pos_start_gpr; X4=scratch1;
    VPERMD        Z3,  Z21, Z4              //;6E161E3D copy posN to all postions     ;Z4=scratch1; Z21=scratch2; Z3=str_pos_length;
    MOVL          X4,  R12                  //;5B90F74A extract from pos0             ;R12=str_length_gpr; X4=scratch1;
//; #endregion load_gpr_from_ZMM

    ADDQ          SI,  R8                   //;57F25882                               ;R8=str_pos_start_gpr; SI=raw_ptr;
    MOVQ          R8,  R15                  //;9B22C3FA                               ;R15=data_index; R8=str_pos_start_gpr;
    TESTQ         CX,  CX                   //;4AC1011A is needle empty?              ;CX=needle_length_gpr;
    JZ            found_something_4         //;2328990B yes, empty needle is by definition everywhere present; jump if zero (ZF = 1);

//; #region gen_tail_block_mask
    MOVQ          R12, BX                   //;99E240A3                               ;BX=scratch; R12=str_length_gpr;
    ANDQ          $63, BX                   //;284163CA                               ;BX=scratch;
    JNZ           mixed_block_9             //;9A30F875 jump if not zero (ZF = 0)     ;
    VPTERNLOGQ    $255,Z23, Z23, Z23        //;B34B5A81                               ;Z23=tail_block_mask;
    JMP           end_tail_block_10         //;D71F71DF                               ;
mixed_block_9:
    MOVL          $1,  DX                   //;4EBF722E                               ;DX=scratch1;
    SHLXQ         BX,  DX,  DX              //;AE13A7DF                               ;DX=scratch1; BX=scratch;
    DECQ          DX                        //;2F7AED29                               ;DX=scratch1;
    KMOVQ         DX,  K3                   //;FF5DB827                               ;K3=scratch_mask; DX=scratch1;
    VPMOVM2B      K3,  Z23                  //;22539847                               ;Z23=tail_block_mask; K3=scratch_mask;
end_tail_block_10://; #endregion

    MOVQ          R12, R14                  //;998DBF90                               ;R14=remaining_blocks; R12=str_length_gpr;
//; #region load data0 and data1
    VMOVDQU64     (R15),Z21                 //;83CE2243 load 1ste block of data       ;Z21=data0; R15=data_index;
    DECQ          R14                       //;BE20F9DB                               ;R14=remaining_blocks;
    SHRQ          $6,  R14                  //;F5D45AD6 initial #remaining blocks     ;R14=remaining_blocks;
    JNZ           done_load_block_12        //;ADACFD2E second block present? yes, load it; jump if not zero (ZF = 0);
    VPANDQ        Z23, Z21, Z21             //;3FF83124 first block is mixed          ;Z21=data0; Z23=tail_block_mask;
    VPXORQ        Z22, Z22, Z22             //;FC3F00CB second block is empty         ;Z22=data1;
    JMP           done_load_first_blocks_11 //;00000000                               ;

done_load_block_12:
    VMOVDQU64     64(R15),Z22               //;83CE2243 load 2nd block of data        ;Z22=data1; R15=data_index;
    CMPQ          R14, $1                   //;C7AE9498                               ;R14=remaining_blocks;
    JG            done_load_first_blocks_11 //;CEDA037B next block present? yes, no need for tail_mask; jump if greater ((ZF = 0) and (SF = OF));
    VPANDQ        Z23, Z22, Z22             //;C79CD914 second block is mixed         ;Z22=data1; Z23=tail_block_mask;

done_load_first_blocks_11:
//; #endregion

//; #region str_to_upper
    VPCMPB        $5,  Z16, Z21, K3         //;30E9B9FD larger than a?                ;K3=scratch_mask; Z21=data0; Z16=c_char_a; 5=GreaterThen;
    VPCMPB        $2,  Z17, Z21, K4         //;8CE85BA0 smaller than z?           ;K4=scratch_mask2; Z21=data0; Z17=c_char_z; 2=LessEq;
    KANDQ         K4,  K3,  K3              //;00000000                               ;K3=scratch_mask; K4=scratch_mask2;
    VPMOVM2B      K3,  Z24                  //;6433A8DD mask with selected chars      ;Z24=scratch; K3=scratch_mask;
    VPTERNLOGQ    $76, Z15, Z21, Z24        //;B1CB1982 magic! see generator doc      ;Z24=scratch; Z21=data0; Z15=c_0b00100000;
//; TODO: is the next move really necessary?
    VMOVDQU64     Z24, Z21                  //;E73844C3                               ;Z21=data0; Z24=scratch;
//; #endregion str_to_upper
//; #region str_to_upper
    VPCMPB        $5,  Z16, Z22, K3         //;30E9B9FD larger than a?                ;K3=scratch_mask; Z22=data1; Z16=c_char_a; 5=GreaterThen;
    VPCMPB        $2,  Z17, Z22, K4         //;8CE85BA0 smaller than z?           ;K4=scratch_mask2; Z22=data1; Z17=c_char_z; 2=LessEq;
    KANDQ         K4,  K3,  K3              //;00000000                               ;K3=scratch_mask; K4=scratch_mask2;
    VPMOVM2B      K3,  Z24                  //;6433A8DD mask with selected chars      ;Z24=scratch; K3=scratch_mask;
    VPTERNLOGQ    $76, Z15, Z22, Z24        //;B1CB1982 magic! see generator doc      ;Z24=scratch; Z22=data1; Z15=c_0b00100000;
//; TODO: is the next move really necessary?
    VMOVDQU64     Z24, Z22                  //;E73844C3                               ;Z22=data1; Z24=scratch;
//; #endregion str_to_upper
loop_5:
    VPBROADCASTB  X20, Z24                  //;57BECC93 load needle char0             ;Z24=needle_char; X20=needle_content;
    VPCMPB        $0,  Z21, Z24, K2         //;9E9A1570 update the observed_mask      ;K2=observed_mask; Z24=needle_char; Z21=data0; 0=Eq;
    KTESTQ        K2,  K2                   //;69B36EC7 is char0 present?             ;K2=observed_mask;
    JZ            next_block_6              //;DB125A73 no, try next block; jump if zero (ZF = 1);
    MOVQ          CX,  BX                   //;96DBB4C3 copy needle                   ;BX=needle_index; CX=needle_length_gpr;
    DECL          BX                        //;23AB2920 dec needle length             ;BX=needle_index;
    JLE           found_something_4         //;2ACECDCD at end of needle already?; jump if less or equal ((ZF = 1) or (SF neq OF));
    VMOVDQU64     Z20, Z24                  //;4B77B339                               ;Z24=needle_content_shifted_lane; Z20=needle_content;

    VMOVDQU64     Z21, Z27                  //;E141A815                               ;Z27=data0_block_prev; Z21=data0;
    VMOVDQU64     Z21, Z26                  //;F05F9A8A                               ;Z26=data0_block_curr; Z21=data0;
    VPERMT2Q      Z22, Z14, Z26             //;2A81E188                               ;Z26=data0_block_curr; Z14=c_2lane_shift; Z22=data1;
    VMOVDQU64     Z22, Z28                  //;BB1C4234                               ;Z28=data1_block_curr; Z22=data1;
    VPERMT2Q      Z21, Z14, Z28             //;2A9975DA                               ;Z28=data1_block_curr; Z14=c_2lane_shift; Z21=data0;

//; #region test chars
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)
    TEST_CHAR($15)
    SHIFT_DATA_TEST_CHAR()
    TEST_CHAR($1)
    TEST_CHAR($2)
    TEST_CHAR($3)
    TEST_CHAR($4)
    TEST_CHAR($5)
    TEST_CHAR($6)
    TEST_CHAR($7)
    TEST_CHAR($8)
    TEST_CHAR($9)
    TEST_CHAR($10)
    TEST_CHAR($11)
    TEST_CHAR($12)
    TEST_CHAR($13)
    TEST_CHAR($14)

//; #region test char 63
    VPALIGNR      $15, Z27, Z26, Z25        //;C0E48226 shift data                    ;Z25=data0_shifted_byte; Z26=data0_block_curr; Z27=data0_block_prev;
    VPSRLDQ       $15, Z24, Z29             //;A6DAB775 shift needle                  ;Z29=needle_char; Z24=needle_content_shifted_lane;
    VPBROADCASTB  X29, Z29                  //;6BDA1B78 load char63 from needle       ;Z29=needle_char; X29=needle_char;
    VPCMPB        $0,  Z25, Z29, K2,  K2    //;9E9A1570 update the observed_mask      ;K2=observed_mask; Z29=needle_char; Z25=data0_shifted_byte; 0=Eq;
//; #endregion
end_of_needle_13:
    KTESTQ        K2,  K2                   //;C6DD0CAF is needle present?            ;K2=observed_mask;
    JNZ           found_something_4         //;9ECCEE8C yes, found something!; jump if not zero (ZF = 0);

found_nothing_14:
//; #endregion

next_block_6:
//; #region gen_tail_block_mask
    TESTQ         R14, R14                  //;00000000                               ;R14=remaining_blocks;
    JZ            found_nothing_3           //;00000000 jump if zero (ZF = 1)         ;
    VMOVDQU64     Z22, Z21                  //;00000000                               ;Z21=data0; Z22=data1;
    ADDQ          $64, R15                  //;00000000                               ;R15=data_index;
    DECQ          R14                       //;00000000                               ;R14=remaining_blocks;
    JNZ           label_A_16                //;00000000 jump if not zero (ZF = 0)     ;
    VPXORQ        Z22, Z22, Z22             //;00000000                               ;Z22=data1;
    JMP           done_load_next_block_15   //;00000000                               ;
label_A_16:
    VMOVDQU64     64(R15),Z22               //;00000000                               ;Z22=data1; R15=data_index;
    CMPQ          R14, $1                   //;00000000                               ;R14=remaining_blocks;
    JG            done_load_next_block_15   //;00000000 jump if greater ((ZF = 0) and (SF = OF));
    VPANDQ        Z23, Z22, Z22             //;00000000                               ;Z22=data1; Z23=tail_block_mask;
done_load_next_block_15:
//; #endregion
//; #region str_to_upper
    VPCMPB        $5,  Z16, Z22, K3         //;30E9B9FD larger than a?                ;K3=scratch_mask; Z22=data1; Z16=c_char_a; 5=GreaterThen;
    VPCMPB        $2,  Z17, Z22, K4         //;8CE85BA0 smaller than z?           ;K4=scratch_mask2; Z22=data1; Z17=c_char_z; 2=LessEq;
    KANDQ         K4,  K3,  K3              //;00000000                               ;K3=scratch_mask; K4=scratch_mask2;
    VPMOVM2B      K3,  Z24                  //;6433A8DD mask with selected chars      ;Z24=scratch; K3=scratch_mask;
    VPTERNLOGQ    $76, Z15, Z22, Z24        //;B1CB1982 magic! see generator doc      ;Z24=scratch; Z22=data1; Z15=c_0b00100000;
//; TODO: is the next move really necessary?
    VMOVDQU64     Z24, Z22                  //;E73844C3                               ;Z22=data1; Z24=scratch;
//; #endregion str_to_upper
    JMP           loop_5                    //;BE22EE80                               ;

found_nothing_3:
    BTRL          R10, DI                   //;2E08A28B needle was not present, mark the lane as dead;DI=result_mask_gpr; R10=lane_index (0-15);
found_something_4:
next_2:
    INCL          R10                       //;7307E80D increment lane index          ;R10=lane_index (0-15);
    CMPL          R10, $16                  //;5EF664AF are we at last lane index?    ;R10=lane_index (0-15);
    JNZ           lane_loop_0               //;4CBFD545 no, loop again; jump if not zero (ZF = 0);
    KMOVW         DI,  K1                   //;00000000                               ;K1=lane_mask16; DI=result_mask_gpr;
end_1:
    VMOVQ          X9,  R9                  //;00000000                               ;X9=storage0;
    VMOVQ          X10, R10                 //;00000000                               ;X10=storage1;
    VMOVQ          X11, R11                 //;00000000                               ;X11=storage2;
    VMOVQ          X12, R12                 //;00000000                               ;X12=storage3;
    VMOVQ          X13, DI                  //;00000000                               ;X13=storage4;
    NEXT()
//; #endregion bcContainsSubstrCi

//; #region bcContainsSuffixCs
TEXT bcContainsSuffixCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_length              ;Z25=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (str_length>=needle_length);K1=lane_active; Z3=str_length; Z25=needle_length; 5=GreaterEq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; TODO HJ 28-10-21 double check whether this code is correct: R8 seems not displaced with the length of needle
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VMOVDQU32     Z25, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z25=needle_length;

  VPSUBD        Z25, Z3,  K1,  Z24        //;4ADB5015 search_base := str_length - needle_length;Z24=search_base; K1=lane_active; Z3=str_length; Z25=needle_length;
  VPADDD        Z2,  Z24, Z24             //;3E1762B7 search_base += str_start        ;Z24=search_base; Z2=str_start;

  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data_msg==Address())     ;K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD search_base += 4                ;Z24=search_base; Z20=constd_4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data_msg==data_needle)   ;K1=lane_active; Z8=data_msg; Z9=data_needle; 0=Eq;
update:
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K1=lane_active; Z25=needle_length;
next:
  NEXT()
//; #endregion bcContainsSuffixCs

//; #region bcContainsSuffixCi
TEXT bcContainsSuffixCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_length              ;Z25=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (str_length>=needle_length);K1=lane_active; Z3=str_length; Z25=needle_length; 5=GreaterEq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
//; TODO HJ 28-10-21 double check whether this code is correct: R8 seems not displaced with the length of needle
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VMOVDQU32     Z25, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z25=needle_length;

  VPSUBD        Z25, Z3,  K1,  Z24        //;4ADB5015 search_base := str_length - needle_length;Z24=search_base; K1=lane_active; Z3=str_length; Z25=needle_length;
  VPADDD        Z2,  Z24, Z24             //;3E1762B7 search_base += str_start        ;Z24=search_base; Z2=str_start;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==Address());K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD search_base += 4                ;Z24=search_base; Z20=constd_4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            update                    //;4DA2206F no, update results; jump if zero (ZF = 1);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
update:
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K1=lane_active; Z25=needle_length;
next:
  NEXT()
//; #endregion bcContainsSuffixCi

//; #region bcContainsSuffixUTF8Ci
TEXT bcContainsSuffixUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;5B83F09F load number of code-points      ;CX=n_runes; R14=needle_ptr;
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_length != 0); empty data are dead lanes;K1=lane_active; Z3=str_length;

  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=n_runes;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $5,  Z26, Z3,  K1,  K1    //;74222733 K1 &= (str_length>=scratch_Z26) ;K1=lane_active; Z3=str_length; Z26=scratch_Z26; 5=GreaterEq;
  KTESTW        K1,  K1                   //;A808AD8E any lanes still todo?           ;K1=lane_active;
  JZ            next                      //;1CA4B42D no, then exit; jump if zero (ZF = 1);

  MOVL          4(R14),R13                //;00000000                                 ;R13=n_alt; R14=needle_ptr;
  MOVL          8(R14),R12                //;1EEAB85B                                 ;R12=alt_ptr; R14=needle_ptr;
  VPBROADCASTD  12(R14),Z6                //;00000000                                 ;Z6=counter_needle; R14=needle_ptr;
  ADDQ          R14, R12                  //;7B0665F3 alt_ptr += needle_ptr           ;R12=alt_ptr; R14=needle_ptr;
  ADDQ          $16, R14                  //;48EB17D0 needle_ptr += 16                ;R14=needle_ptr;

  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPBROADCASTD  CONSTD_UTF8_2B_MASK(),Z5  //;F6E81301 load constant UTF8 2byte mask   ;Z5=UTF8_2byte_mask;
  VPBROADCASTD  CONSTD_UTF8_3B_MASK(),Z23 //;B1E12620 load constant UTF8 3byte mask   ;Z23=UTF8_3byte_mask;
  VPBROADCASTD  CONSTD_UTF8_4B_MASK(),Z21 //;D896A9E1 load constant UTF8 4byte mask   ;Z21=UTF8_4byte_mask;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z25  //;B323211A load table_n_bytes_utf8         ;Z25=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPADDD        Z10, Z10, Z14             //;EDD57CAF load constant 2                 ;Z14=constd_2; Z10=constd_1;
  VPADDD        Z10, Z14, Z12             //;7E7A1CB0 load constant 3                 ;Z12=constd_3; Z14=constd_2; Z10=constd_1;
  VPADDD        Z10, Z12, Z20             //;9CFA6ADD load constant 4                 ;Z20=constd_4; Z12=constd_3; Z10=constd_1;
  VPADDD        Z2,  Z3,  Z24             //;ADF771FC search_base := str_length + str_start;Z24=search_base; Z3=str_length; Z2=str_start;
  XORL          DX,  DX                   //;CF90D470                                 ;DX=rune_index;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion

loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data_msg := 0                   ;Z8=data_msg;
  VPGATHERDD    -4(SI)(Z24*1),K3,  Z8     //;573D089A gather data from end            ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPBROADCASTD.Z (R14),K1,  Z9            //;B556F1BC load needle data                ;Z9=data_needle; K1=lane_active; R14=needle_ptr;

//; test to distinguish between all-ascii or mixed-ascii
  VPMOVB2M      Z8,  K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z8=data_msg;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);

  VPSHUFB       Z22, Z8,  Z8              //;B77C3AA8 reverse byte order data         ;Z8=data_msg; Z22=constant_bswap32;
//; clear tail from data
  VPMINSD       Z6,  Z20, Z26             //;DEC17BF3 scratch_Z26 := min(4, counter_needle);Z26=scratch_Z26; Z20=constd_4; Z6=counter_needle;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

//; test for equality
  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;BBBDF880 K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPMINSD       Z6,  Z20, Z4              //;92E95537 n_bytes_needle := min(4, counter_needle);Z4=n_bytes_needle; Z20=constd_4; Z6=counter_needle;
//; advance
  VPSUBD        Z4,  Z24, Z24             //;D7CC90DD search_base -= n_bytes_needle   ;Z24=search_base; Z4=n_bytes_needle;
  VPSUBD        Z4,  Z3,  Z3              //;AEDCD850 str_length -= n_bytes_needle    ;Z3=str_length; Z4=n_bytes_needle;
  VPSUBD        Z4,  Z6,  Z6              //;18AA0564 counter_needle -= n_bytes_needle;Z6=counter_needle; Z4=n_bytes_needle;
  ADDQ          $4,  R14                  //;2BC9E208 needle_ptr += 4                 ;R14=needle_ptr;
  ADDL          $48, DX                   //;F0BC3163 rune_index += 48                ;DX=rune_index;
  SUBL          $4,  CX                   //;646B86C9 n_runes -= 4                    ;CX=n_runes;
  JNLE          loop                      //;1EBC2C20 jump if not less or equal ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;
mixed_ascii:
//; #region count_bytes_code_point_right; data in Z8; result out Z7
  VPANDD        Z5,  Z8,  Z26             //;B7541DA7 remove irrelevant bits for 2byte test;Z26=scratch_Z26; Z8=data_msg; Z5=UTF8_2byte_mask;
  VPCMPD        $0,  Z5,  Z26, K1,  K3    //;C6890BF4 K3 := K1 & (scratch_Z26==UTF8_2byte_mask); create 2byte mask;K3=tmp_mask; K1=lane_active; Z26=scratch_Z26; Z5=UTF8_2byte_mask; 0=Eq;
  VPANDD        Z23, Z8,  Z26             //;D14D6426 remove irrelevant bits for 3byte test;Z26=scratch_Z26; Z8=data_msg; Z23=UTF8_3byte_mask;
  VPCMPD        $0,  Z23, Z26, K1,  K4    //;14C32DC0 K4 := K1 & (scratch_Z26==UTF8_3byte_mask); create 3byte mask;K4=alt2_match; K1=lane_active; Z26=scratch_Z26; Z23=UTF8_3byte_mask; 0=Eq;
  VPANDD        Z21, Z8,  Z26             //;C19D386F remove irrelevant bits for 4byte test;Z26=scratch_Z26; Z8=data_msg; Z21=UTF8_4byte_mask;
  VPCMPD        $0,  Z21, Z26, K1,  K5    //;1AE0A51C K5 := K1 & (scratch_Z26==UTF8_4byte_mask); create 4byte mask;K5=alt3_match; K1=lane_active; Z26=scratch_Z26; Z21=UTF8_4byte_mask; 0=Eq;
  VMOVDQU32     Z10, Z7                   //;A7640B64 n_bytes_data := 1               ;Z7=n_bytes_data; Z10=constd_1;
  VPADDD        Z10, Z7,  K3,  Z7         //;684FACB1 2byte UTF-8: add extra 1byte    ;Z7=n_bytes_data; K3=tmp_mask; Z10=constd_1;
  VPADDD        Z14, Z7,  K4,  Z7         //;A542E2E5 3byte UTF-8: add extra 2bytes   ;Z7=n_bytes_data; K4=alt2_match; Z14=constd_2;
  VPADDD        Z12, Z7,  K5,  Z7         //;26F561C2 4byte UTF-8: add extra 3bytes   ;Z7=n_bytes_data; K5=alt3_match; Z12=constd_3;
//; #endregion count_bytes_code_point_right; data in Z8; result out Z7

  VPSUBD        Z7,  Z20, Z26             //;C8ECAA75 scratch_Z26 := 4 - n_bytes_data ;Z26=scratch_Z26; Z20=constd_4; Z7=n_bytes_data;
  VPSLLD        $3,  Z26, Z26             //;5734792E                                 ;Z26=scratch_Z26;
  VPSRLVD       Z26, Z8,  Z8              //;529FFC90                                 ;Z8=data_msg; Z26=scratch_Z26;

  VPCMPD.BCST   $0,  (R12)(DX*1),Z8,  K1,  K3  //;345D0BF3 K3 := K1 & (data_msg==[alt_ptr+rune_index]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  4(R12)(DX*1),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[alt_ptr+rune_index+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  8(R12)(DX*1),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[alt_ptr+rune_index+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  12(R12)(DX*1),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[alt_ptr+rune_index+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSRLD        $4,  Z9,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z9=data_needle;
  VPERMD        Z25, Z26, Z4              //;68FECBA0 get n_bytes_needle              ;Z4=n_bytes_needle; Z26=scratch_Z26; Z25=table_n_bytes_utf8;
//; advance:
  VPSUBD        Z4,  Z24, Z24             //;DFE8D20B search_base -= n_bytes_needle   ;Z24=search_base; Z4=n_bytes_needle;
  VPSUBD        Z7,  Z3,  Z3              //;24E04BE7 str_length -= n_bytes_data      ;Z3=str_length; Z7=n_bytes_data;
  VPSUBD        Z4,  Z6,  Z6              //;A7F99FAC counter_needle -= n_bytes_needle;Z6=counter_needle; Z4=n_bytes_needle;
  MOVL          X4,  R15                  //;18D7AD2B extract Z4                      ;R15=scratch; Z4=n_bytes_needle;
  ADDQ          R15, R14                  //;B2EF9837 needle_ptr += scratch           ;R14=needle_ptr; R15=scratch;
  ADDL          $16, DX                   //;1F8D79B1 rune_index += 16                ;DX=rune_index;
  DECL          CX                        //;A99E9290 n_runes--                       ;CX=n_runes;
  JNZ           loop                      //;80013DFA jump if not zero (ZF = 0)       ;
next:
  NEXT()

//; #endregion bcContainsSuffixUTF8Ci

//; #region bcContainsPrefixCs
TEXT bcContainsPrefixCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_length              ;Z25=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (str_length>=needle_length); cmp len(needle) len(data);K1=lane_active; Z3=str_length; Z25=needle_length; 5=GreaterEq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z25, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z25=needle_length;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z24                  //;6F6F1342 search_base := str_start        ;Z24=search_base; Z2=str_start;

  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VPCMPD.BCST   $0,  (R14),Z8,  K1,  K1   //;F0E5B3BD K1 &= (data_msg==Address()); cmp data with needle;K1=lane_active; Z8=data_msg; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD search_base += 4                ;Z24=search_base; Z20=constd_4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

  VPCMPD        $0,  Z9,  Z8,  K1,  K1    //;474761AE K1 &= (data_msg==data_needle)   ;K1=lane_active; Z8=data_msg; Z9=data_needle; 0=Eq;
  VPADDD        Z25, Z2,  K1,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K1=lane_active; Z25=needle_length;
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K1=lane_active; Z25=needle_length;
next:
  NEXT()
//; #endregion bcContainsPrefixCs

//; #region bcContainsPrefixCi
TEXT bcContainsPrefixCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  VPBROADCASTD  8(R14),Z25                //;713DF24F bcst needle_length              ;Z25=needle_length; R14=needle_slice;
  VPCMPD        $5,  Z25, Z3,  K1,  K1    //;502E314F K1 &= (str_length>=needle_length); cmp len(needle) len(data);K1=lane_active; Z3=str_length; Z25=needle_length; 5=GreaterEq;
  KTESTW        K1,  K1                   //;6E50BE85 any lanes eligible?             ;K1=lane_active;
  JZ            next                      //;BD98C1A8 no, exit; jump if zero (ZF = 1) ;

  VMOVDQU32     Z25, Z6                   //;6F6F1342 counter := needle_length        ;Z6=counter; Z25=needle_length;
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z24                  //;6F6F1342 search_base := str_start        ;Z24=search_base; Z2=str_start;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  JMP           tail                      //;F2A3982D                                 ;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD.BCST   $0,  (R14),Z13, K1,  K1   //;F0E5B3BD K1 &= (data_msg_upper==Address()); cmp data with needle;K1=lane_active; Z13=data_msg_upper; R14=needle_ptr; 0=Eq;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  VPSUBD        Z20, Z6,  Z6              //;AEDCD850 counter -= 4                    ;Z6=counter; Z20=constd_4;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPADDD        Z20, Z24, Z24             //;D7CC90DD search_base += 4                ;Z24=search_base; Z20=constd_4;
tail:
  VPCMPD        $5,  Z20, Z6,  K3         //;C28D3832 K3 := (counter>=4); 4 or more chars in needle?;K3=tmp_mask; Z6=counter; Z20=constd_4; 5=GreaterEq;
  KTESTW        K3,  K3                   //;77067C8D                                 ;K3=tmp_mask;
  JNZ           loop                      //;B678BE90 no, loop again; jump if not zero (ZF = 0);

  VPTESTMD      Z6,  Z6,  K1,  K3         //;E0E548E4 any chars left in needle?       ;K3=tmp_mask; K1=lane_active; Z6=counter;
  KTESTW        K3,  K3                   //;C28D3832                                 ;K3=tmp_mask;
  JZ            next                      //;4DA2206F no, update results; jump if zero (ZF = 1);

  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z24*1),K3,  Z8       //;36FEA5FE gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z24=search_base;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VPERMD        Z18, Z6,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z6=counter; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;
  VPANDD.BCST   (R14),Z19, Z9             //;EE8B32D9 load needle with mask           ;Z9=data_needle; Z19=tail_mask; R14=needle_ptr;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K1,  K1    //;474761AE K1 &= (data_msg_upper==data_needle);K1=lane_active; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
  VPADDD        Z25, Z2,  K1,  Z2         //;8A3B8A20 str_start += needle_length      ;Z2=str_start; K1=lane_active; Z25=needle_length;
  VPSUBD        Z25, Z3,  K1,  Z3         //;B5FDDA17 str_length -= needle_length     ;Z3=str_length; K1=lane_active; Z25=needle_length;
next:
  NEXT()
//; #endregion bcContainsPrefixCi

//; #region bcContainsPrefixUTF8Ci
//; case-insensitive UTF-8 string compare in slice in Z2:Z3, with stack[imm]
//; empty needles or empty data always result in a dead lane
TEXT bcContainsPrefixUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  MOVL          (R14),CX                  //;5B83F09F load number of code-points      ;CX=n_runes; R14=needle_ptr;
  VPTESTMD      Z3,  Z3,  K1,  K1         //;790C4E82 K1 &= (str_length != 0); empty data are dead lanes;K1=lane_active; Z3=str_length;

  VPBROADCASTD  CX,  Z26                  //;485C8362 bcst number of code-points      ;Z26=scratch_Z26; CX=n_runes;
  VPTESTMD      Z26, Z26, K1,  K1         //;CD49D8A5 K1 &= (scratch_Z26 != 0); empty needles are dead lanes;K1=lane_active; Z26=scratch_Z26;
  VPCMPD        $5,  Z26, Z3,  K1,  K1    //;74222733 K1 &= (str_length>=scratch_Z26) ;K1=lane_active; Z3=str_length; Z26=scratch_Z26; 5=GreaterEq;
  KTESTW        K1,  K1                   //;A808AD8E any lanes still todo?           ;K1=lane_active;
  JZ            next                      //;1CA4B42D no, then exit; jump if zero (ZF = 1);

  MOVL          4(R14),R13                //;00000000                                 ;R13=n_alt; R14=needle_ptr;
  MOVL          8(R14),R12                //;1EEAB85B                                 ;R12=alt_ptr; R14=needle_ptr;
  ADDQ          R14, R12                  //;7B0665F3 alt_ptr += needle_ptr           ;R12=alt_ptr; R14=needle_ptr;
  ADDQ          $16, R14                  //;48EB17D0 needle_ptr += 16                ;R14=needle_ptr;

  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  XORL          DX,  DX                   //;CF90D470                                 ;DX=rune_index;
//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion

loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data_msg := 0                   ;Z8=data_msg;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;
  VPBROADCASTD.Z (R14),K1,  Z9            //;B556F1BC load needle data                ;Z9=data_needle; K1=lane_active; R14=needle_ptr;

//; clear tail from data
  VPMINSD       Z3,  Z20, Z7              //;DEC17BF3 n_bytes_data := min(4, str_length);Z7=n_bytes_data; Z20=constd_4; Z3=str_length;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;64208067 mask data from msg              ;Z8=data_msg; Z19=tail_mask;

//; test to distinguish between all-ascii or mixed-ascii
  VPMOVB2M      Z8,  K3                   //;5303B427 get 64 sign-bits                ;K3=tmp_mask; Z8=data_msg;
  KTESTQ        K3,  K3                   //;A2B0951C all sign-bits zero?             ;K3=tmp_mask;
  JNZ           mixed_ascii               //;303EFD4D no, found a non-ascii char; jump if not zero (ZF = 0);

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K3         //;30E9B9FD K3 := (data_msg>=c_char_a)      ;K3=tmp_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K3,  K3    //;8CE85BA0 K3 &= (data_msg<=c_char_z)      ;K3=tmp_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K3,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K3=tmp_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPB        $4,  Z13, Z9,  K3         //;BBBDF880 K3 := (data_needle!=data_msg_upper);K3=tmp_mask; Z9=data_needle; Z13=data_msg_upper; 4=NotEqual;
  VPMOVM2B      K3,  Z26                  //;F3452970 promote 64x bit to 64x byte     ;Z26=scratch_Z26; K3=tmp_mask;
  VPTESTNMD     Z26, Z26, K1,  K1         //;E2969ED8 K1 &= (scratch_Z26 == 0); non zero means does not match;K1=lane_active; Z26=scratch_Z26;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

//; advance:
  VPADDD        Z7,  Z2,  Z2              //;302348A4 str_start += n_bytes_data       ;Z2=str_start; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  Z3              //;6569898C str_length -= n_bytes_data      ;Z3=str_length; Z7=n_bytes_data;
  ADDQ          $4,  R14                  //;2BC9E208 needle_ptr += 4                 ;R14=needle_ptr;
  ADDL          $48, DX                   //;F0BC3163 rune_index += 48                ;DX=rune_index;
  SUBL          $4,  CX                   //;646B86C9 n_runes -= 4                    ;CX=n_runes;
  JNLE          loop                      //;1EBC2C20 jump if not less or equal ((ZF = 0) and (SF = OF));
  JMP           next                      //;2230EE05                                 ;
mixed_ascii:
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;FC6636EA mask data from msg              ;Z8=data_msg; Z19=tail_mask;

  VPCMPD.BCST   $0,  (R12)(DX*1),Z8,  K1,  K3  //;345D0BF3 K3 := K1 & (data_msg==[alt_ptr+rune_index]);K3=tmp_mask; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  4(R12)(DX*1),Z8,  K1,  K4  //;EFD0A9A3 K4 := K1 & (data_msg==[alt_ptr+rune_index+4]);K4=alt2_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  8(R12)(DX*1),Z8,  K1,  K5  //;CAC0FAC6 K5 := K1 & (data_msg==[alt_ptr+rune_index+8]);K5=alt3_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  VPCMPD.BCST   $0,  12(R12)(DX*1),Z8,  K1,  K6  //;50C70740 K6 := K1 & (data_msg==[alt_ptr+rune_index+12]);K6=alt4_match; K1=lane_active; Z8=data_msg; R12=alt_ptr; DX=rune_index;
  KORW          K3,  K4,  K3              //;58E49245 tmp_mask |= alt2_match          ;K3=tmp_mask; K4=alt2_match;
  KORW          K3,  K5,  K3              //;BDCB8940 tmp_mask |= alt3_match          ;K3=tmp_mask; K5=alt3_match;
  KORW          K6,  K3,  K1              //;AAF6ED91 lane_active := tmp_mask | alt4_match;K1=lane_active; K3=tmp_mask; K6=alt4_match;
  KTESTW        K1,  K1                   //;5746030A any lanes still alive?          ;K1=lane_active;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

//; advance:
  VPSRLD        $4,  Z9,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z9=data_needle;
  VPERMD        Z21, Z26, Z4              //;68FECBA0 get n_bytes_needle              ;Z4=n_bytes_needle; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z2,  Z2              //;DFE8D20B str_start += n_bytes_data       ;Z2=str_start; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  Z3              //;24E04BE7 str_length -= n_bytes_data      ;Z3=str_length; Z7=n_bytes_data;
  MOVL          X4,  R15                  //;18D7AD2B extract Z4                      ;R15=scratch; Z4=n_bytes_needle;
  ADDQ          R15, R14                  //;B2EF9837 needle_ptr += scratch           ;R14=needle_ptr; R15=scratch;

  ADDL          $16, DX                   //;1F8D79B1 rune_index += 16                ;DX=rune_index;
  DECL          CX                        //;A99E9290 n_runes--                       ;CX=n_runes;
  JNZ           loop                      //;80013DFA jump if not zero (ZF = 0)       ;
next:
  NEXT()

//; #endregion bcContainsPrefixUTF8Ci

//; #region bcLengthStr
//; count number of UTF-8 code-points in Z2:Z3 (str interpretation); store the result in Z2:Z3 (int64 interpretation)
TEXT bcLengthStr(SB), NOSPLIT|NOFRAME, $0
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VPBROADCASTB  CONSTD_0x80(),Z27         //;96E41B4F load constant 80808080          ;Z27=constd_80808080;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
  VPXORD        Z6,  Z6,  Z6              //;F292B105 counter := 0                    ;Z6=counter;
  JMP           test                      //;4CAF1B53                                 ;
loop:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 data_msg := 0                   ;Z8=data_msg;
  VPGATHERDD    (SI)(Z2*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z2=str_start;

  VPMINSD       Z20, Z3,  Z7              //;DDF0DB53 n_bytes_data := min(str_length, 4);Z7=n_bytes_data; Z3=str_length; Z20=constd_4;
  VPERMD        Z18, Z7,  Z19             //;8F3EBC09 get tail_mask                   ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;EF91B1F3 remove tail from data           ;Z8=data_msg; Z19=tail_mask;
  VPMOVB2M      Z8,  K3                   //;F22D958D get 64 sign-bits                ;K3=tmp_mask; Z8=data_msg;
  KTESTQ        K3,  K3                   //;F2C8F6C8 all sign-bits zero?             ;K3=tmp_mask;
  JNZ           non_ascii                 //;71B77ACE no: non-ascii present; jump if not zero (ZF = 0);
  VPADDD        Z7,  Z6,  K2,  Z6         //;978F956A counter += n_bytes_data         ;Z6=counter; K2=lane2_mask; Z7=n_bytes_data;

update:
  VPSUBD        Z7,  Z3,  K2,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K2=lane2_mask; Z7=n_bytes_data;
  VPADDD        Z7,  Z2,  K2,  Z2         //;45909060 str_start += n_bytes_data       ;Z2=str_start; K2=lane2_mask; Z7=n_bytes_data;

test:
//; We could compare Z2 > end_of_str, and remove the above sub Z3, but the min(4, Z3) prevents that
  VPCMPD        $6,  Z11, Z3,  K1,  K2    //;DA211F9B K2 := K1 & (str_length>0)       ;K2=lane2_mask; K1=lane_active; Z3=str_length; Z11=constd_0; 6=Greater;
  KTESTW        K2,  K2                   //;799F076E all lanes done? 0 means lane is done;K2=lane2_mask;
  JNZ           loop                      //;203DDAE1 if some lanes alive then loop; jump if not zero (ZF = 0);

  VPMOVZXDQ     Y6,  Z2                   //;9CA47A78 cast 8 x int32 to 8 x int64     ;Z2=str_start; Z6=counter;
  VEXTRACTI32X8 $1,  Z6,  Y6              //;DC597720 256-bits to lower lane          ;Z6=counter;
  VPMOVZXDQ     Y6,  Z3                   //;C24D656F cast 8 x int32 to 8 x int64     ;Z3=str_length; Z6=counter;
  NEXT()

non_ascii:  //; NOTE: this is the assumed to be a somewhat unlikely branch
  VPTESTNMD     Z27, Z8,  K2,  K3         //;85E34261 K3 is all-ascii lanes           ;K3=tmp_mask; K2=lane2_mask; Z8=data_msg; Z27=constd_80808080;
  VPADDD        Z7,  Z6,  K3,  Z6         //;D765BB59 for all ascii lanes             ;Z6=counter; K3=tmp_mask; Z7=n_bytes_data;
  KANDNW        K2,  K3,  K3              //;5A982E07 K3 is mixed-ascii lanes         ;K3=tmp_mask; K2=lane2_mask;
  VPADDD        Z10, Z6,  K3,  Z6         //;8E335D11 for mixed-ascii lanes           ;Z6=counter; K3=tmp_mask; Z10=constd_1;
  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, K3,  Z7         //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; K3=tmp_mask; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  JMP           update                    //;A596F5F6                                 ;
//; #endregion bcLengthStr

//; #region bcSubstr
//; Get a substring of UTF-8 code-points in Z2:Z3 (str interpretation). The substring starts
//; from the specified start-index and ends at the specified length or at the last character
//; of the string (which ever is first). The start-index is 1-based! The first index of the
//; string starts at 1. The substring is stored in Z2 : Z3(str interpretation)
TEXT bcSubstr(SB), NOSPLIT|NOFRAME, $0
//; #region load from stack-slot: load 16x uint32 into Z6
  LOADARG1Z(Z27, Z28)
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z28, Y28                  //;8F762E8E truncate uint64 to uint32       ;Z28=scratch_Z28;
  VINSERTI64X4  $1,  Y28, Z27, Z6         //;3944001B merge into 16x uint32           ;Z6=counter; Z27=scratch_Z27; Z28=scratch_Z28;
//; #endregion load from stack-slot
//; #region load from stack-slot: load 16x uint32 into Z12
  LOADARG1Z(Z27, Z28)
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z28, Y28                  //;8F762E8E truncate uint64 to uint32       ;Z28=scratch_Z28;
  VINSERTI64X4  $1,  Y28, Z27, Z12        //;3944001B merge into 16x uint32           ;Z12=substr_length; Z27=scratch_Z27; Z28=scratch_Z28;
//; #endregion load from stack-slot
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VMOVDQU32     Z2,  Z4                   //;CFB0D832 current_offset := str_start     ;Z4=current_offset; Z2=str_start;
  VPSUBD        Z10, Z6,  Z6              //;34951830 1-based to 0-based indices      ;Z6=counter; Z10=constd_1;
//; #region find start of substring
  JMP           test1                     //;4CAF1B53                                 ;
loop1:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;FC80CF41 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=current_offset;
  VPSUBD        Z10, Z6,  Z6              //;19C9DC47 counter--                       ;Z6=counter; Z10=constd_1;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z4,  K2,  Z4         //;45909060 current_offset += n_bytes_data  ;Z4=current_offset; K2=lane2_mask; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;

test1:
  VPTESTMD      Z6,  Z6,  K1,  K2         //;2E4360D2 any chars left to skip?         ;K2=lane2_mask; K1=lane_active; Z6=counter;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;DA211F9B K2 &= (str_length>0)            ;K2=lane2_mask; Z3=str_length; Z11=constd_0; 6=Greater;
  KTESTW        K2,  K2                   //;799F076E all lanes done? 0 means lane is done;K2=lane2_mask;
  JNZ           loop1                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
//; #endregion find start of substring

  VMOVDQU32     Z4,  Z2                   //;60EBBEED str_start := current_offset     ;Z2=str_start; Z4=current_offset;
//; #region find end of substring
  JMP           test2                     //;4CAF1B53                                 ;
loop2:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;5A704AF6 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=current_offset;
  VPSUBD        Z10, Z12, Z12             //;61D287CD substr_length--                 ;Z12=substr_length; Z10=constd_1;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z4,  K2,  Z4         //;45909060 current_offset += n_bytes_data  ;Z4=current_offset; K2=lane2_mask; Z7=n_bytes_data;
  VPSUBD        Z7,  Z3,  K1,  Z3         //;B69EBA11 str_length -= n_bytes_data      ;Z3=str_length; K1=lane_active; Z7=n_bytes_data;

test2:
  VPTESTMD      Z12, Z12, K1,  K2         //;2E4360D2 any chars left to trim          ;K2=lane2_mask; K1=lane_active; Z12=substr_length;
  VPCMPD        $6,  Z11, Z3,  K2,  K2    //;DA211F9B K2 &= (str_length>0); all lanes done?;K2=lane2_mask; Z3=str_length; Z11=constd_0; 6=Greater;
  KTESTW        K2,  K2                   //;799F076E 0 means lane is done            ;K2=lane2_mask;
  JNZ           loop2                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
//; #endregion find end of substring
  VPSUBD        Z2,  Z4,  Z3              //;E24AE85F str_length := current_offset - str_start;Z3=str_length; Z4=current_offset; Z2=str_start;
  NEXT()
//; #endregion bcSubstr

//; #region bcSplitPart
//; NOTE: the delimiter cannot be byte 0
TEXT bcSplitPart(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 Load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;FEE415A0                                 ;R14=split_info;
  VPBROADCASTB  (R14),Z21                 //;B4B43F80 bcst delimiter                  ;Z21=delimiter; R14=split_info;

//; #region load from stack-slot: load 16x uint32 into Z7
  LOADARG1Z(Z27, Z26)
  VPMOVQD       Z27, Y27                  //;17FCB103 truncate uint64 to uint32       ;Z27=scratch_Z27;
  VPMOVQD       Z26, Y26                  //;8F762E8E truncate uint64 to uint32       ;Z26=scratch_Z26;
  VINSERTI64X4  $1,  Y26, Z27, Z7         //;3944001B merge into 16x uint32           ;Z7=counter_delim; Z27=scratch_Z27; Z26=scratch_Z26;
//; #endregion load from stack-slot
  VPCMPD        $5,  Z7,  Z3,  K1,  K1    //;502E314F K1 &= (str_length>=counter_delim);K1=lane_active; Z3=str_length; Z7=counter_delim; 5=GreaterEq;
  KTESTW        K1,  K1                   //;1C6F0B57                                 ;K1=lane_active;
  JZ            next                      //;F22A6A94 jump if zero (ZF = 1)           ;

  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;

  KMOVW         K1,  K2                   //;FE3838B3 lane2_mask := lane_active       ;K2=lane2_mask; K1=lane_active;
  VMOVDQU32     Z2,  Z4                   //;CFB0D832 search_base := str_start        ;Z4=search_base; Z2=str_start;
  VPADDD        Z2,  Z3,  Z5              //;E5429114 o_data_end := str_length + str_start;Z5=o_data_end; Z3=str_length; Z2=str_start;
  VPSUBD        Z10, Z7, Z7               // index-- (1-based indexing)
//; #region find n-th delimiter
  JMP           tail1                     //;9DD42F87                                 ;
loop1:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 clear data_msg                  ;Z8=data_msg;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;FC80CF41 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;

  VPCMPB        $0,  Z21, Z8,  K3         //;8E3317B0 K3 := (data_msg==delimiter)     ;K3=tmp_mask; Z8=data_msg; Z21=delimiter; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count;

//; advance
  VPADDD        Z14, Z4,  K2,  Z4         //;5034DEA0 search_base += skip_count       ;Z4=search_base; K2=lane2_mask; Z14=skip_count;

//; did we encounter a delimiter?
  VPCMPD        $4,  Z20, Z14, K2,  K3    //;80B9AEA2 K3 := K2 & (skip_count!=4); active lanes where skip != 4;K3=tmp_mask; K2=lane2_mask; Z14=skip_count; Z20=constd_4; 4=NotEqual;
  VPSUBD        Z10, Z7,  K3,  Z7         //;35E75E57 counter_delim--                 ;Z7=counter_delim; K3=tmp_mask; Z10=constd_1;
  VPADDD        Z10, Z4,  K3,  Z4         //;D5281D43 search_base++                   ;Z4=search_base; K3=tmp_mask; Z10=constd_1;

tail1:
//; still a lane todo?
  VPCMPD        $1,  Z7,  Z11, K2,  K2    //;50E6D99D K2 &= (0<counter_delim)         ;K2=lane2_mask; Z11=constd_0; Z7=counter_delim; 1=LessThen;
  VPCMPD        $1,  Z5,  Z4,  K2,  K3    //;A052FCB6 K3 := K2 & (search_base<o_data_end);K3=tmp_mask; K2=lane2_mask; Z4=search_base; Z5=o_data_end; 1=LessThen;
  KTESTW        K3,  K3                   //;799F076E all lanes done? 0 means lane is done;K3=tmp_mask;
  JNZ           loop1                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);

  VPCMPD        $0,  Z7,  Z11, K1,  K1    //;A0ABF51F K1 &= (0==counter_delim)        ;K1=lane_active; Z11=constd_0; Z7=counter_delim; 0=Eq;
//; #endregion find n-th delimiter

  VMOVDQU32     Z4,  K1,  Z2              //;B69A81FE str_start := search_base        ;Z2=str_start; K1=lane_active; Z4=search_base;

//; #region find next delimiter
  KMOVW         K1,  K2                   //;A543DE2E lane2_mask := lane_active       ;K2=lane2_mask; K1=lane_active;
loop2:
  KMOVW         K2,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K2=lane2_mask;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 clear data_msg                  ;Z8=data_msg;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;5A704AF6 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;

  VPCMPB        $0,  Z21, Z8,  K3         //;E8DC9CCA K3 := (data_msg==delimiter)     ;K3=tmp_mask; Z8=data_msg; Z21=delimiter; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count;

//; advance
  VPADDD        Z14, Z4,  K2,  Z4         //;5034DEA0 search_base += skip_count       ;Z4=search_base; K2=lane2_mask; Z14=skip_count;

//; did we encounter a delimiter?
  VPCMPD        $0,  Z20, Z14, K2,  K2    //;80B9AEA2 K2 &= (skip_count==4); active lanes where skip != 4;K2=lane2_mask; Z14=skip_count; Z20=constd_4; 0=Eq;
  VPCMPD        $1,  Z5,  Z4,  K2,  K3    //;E2BEF075 K3 := K2 & (search_base<o_data_end);K3=tmp_mask; K2=lane2_mask; Z4=search_base; Z5=o_data_end; 1=LessThen;
  KTESTW        K3,  K3                   //;799F076E all lanes still todo?           ;K3=tmp_mask;
  JNZ           loop2                     //;203DDAE1 any lanes todo? yes, then loop; jump if not zero (ZF = 0);
//; #endregion find next delimiter

  VPMINSD       Z5,  Z4,  Z4              //;C62A5921 search_base := min(search_base, o_data_end);Z4=search_base; Z5=o_data_end;
  VPSUBD        Z2,  Z4,  K1,  Z3         //;E24AE85F str_length := search_base - str_start;Z3=str_length; K1=lane_active; Z4=search_base; Z2=str_start;
next:
  NEXT()
//; #endregion bcSplitPart

//; #region bcMatchpatCs
//; string @ (SI)(Z2:Z3) matches dict[imm] ?
//;  each string segment length is incoded directly in dict[imm], and the segments directly in dict[imm], and the segments operation
TEXT bcMatchpatCs(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R8)                      //;05667C35 Load *[]byte with the provided str into R8
  MOVQ          (R8),DX                   //;E6E1D839                                 ;DX=seg_begin_ptr; R8=pattern_begin_ptr;
  KMOVW         K1,  K2                   //;ECF269E6 lane_matched := lane_active     ;K2=lane_matched; K1=lane_active;
  KMOVW         K0,  K1                   //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VMOVDQU32     Z2,  Z25                  //;3FC39C85 o_data_outer_loop := str_start  ;Z25=o_data_outer_loop; Z2=str_start;
  VPADDD        Z2,  Z3,  Z5              //;E5429114 o_data_end := str_length + str_start;Z5=o_data_end; Z3=str_length; Z2=str_start;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;

  VPBROADCASTB  1(DX),Z24                 //;3DD84A29 load first code-point needle    ;Z24=data_needle2; DX=seg_begin_ptr;
  JMP           outer_tail                //;ECD5FF70                                 ;

outer_loop:
//; try to match against the first byte of the needle and advance up to 4 bytes at a time while that byte isn't present in the input
  KMOVW         K2,  K3                   //;6979316F copy eligible lanes             ;K3=tmp_mask; K2=lane_matched;
  VPGATHERDD    (SI)(Z25*1),K3,  Z8       //;D040E340 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z25=o_data_outer_loop;
  VPCMPB        $0,  Z24, Z8,  K3         //;FB45C48A K3 := (data_msg==data_needle2); select matching bytes;K3=tmp_mask; Z8=data_msg; Z24=data_needle2; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count_data; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count_data; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count_data;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count_data;

  VPADDD        Z14, Z25, K2,  Z25        //;5034DEA0 o_data_outer_loop += skip_count_data;Z25=o_data_outer_loop; K2=lane_matched; Z14=skip_count_data;
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;DBB2E31C K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end; 1=LessThen;
  KTESTW        K2,  K2                   //;EB60CC5C any lanes still alive?          ;K2=lane_matched;
  JZ            next                      //;FF07EB20 no, then exit; jump if zero (ZF = 1);

  VPCMPD        $4,  Z20, Z14, K2,  K4    //;80B9AEA2 K4 := K2 & (skip_count_data!=4); active lanes where skip != 4;K4=active_lanes; K2=lane_matched; Z14=skip_count_data; Z20=constd_4; 4=NotEqual;
  KTESTW        K4,  K4                   //;BC448E1A                                 ;K4=active_lanes;
  JZ            outer_loop                //;B6E89A28 keep looping if we skipped 4 bytes everywhere; jump if zero (ZF = 1);
  KMOVW         K2,  K4                   //;C1B09128 may as well search everything active;K4=active_lanes; K2=lane_matched;

  MOVBLZX       (DX),CX                   //;B285DE84 load seg_length                 ;CX=seg_index; DX=seg_begin_ptr;
  VPBROADCASTD  CX,  Z23                  //;40103F07 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  XORL          R14, R14                  //;5B762374 reset bytes_consumed            ;R14=bytes_consumed;
  LEAQ          1(DX),R13                 //;3724357C init seg_ptr (skip seg_length char);R13=seg_ptr; DX=seg_begin_ptr;
  VMOVDQU32     Z25, Z6                   //;C5082E4B o_data_inner_loop := o_data_outer_loop;Z6=o_data_inner_loop; Z25=o_data_outer_loop;

//; #region inner_loop
  JMP           inner_tail                //;E826EBC7                                 ;
inner_loop:
  VPSUBD        Z6,  Z5,  Z26             //;A584C7CC compute n_remaining_bytes_data  ;Z26=scratch_Z26; Z5=o_data_end; Z6=o_data_inner_loop;
  VPCMPD        $2,  Z26, Z23, K4,  K4    //;6F2BA80B K4 &= (seg_length<=scratch_Z26) ;K4=active_lanes; Z23=seg_length; Z26=scratch_Z26; 2=LessEq;
  KTESTW        K4,  K4                   //;FB6F192A any lanes still alive?          ;K4=active_lanes;
  JZ            no_update                 //;815AFE30 no, then break out of inner_loop; jump if zero (ZF = 1);

  LEAQ          (SI)(R14*4),R15           //;D11AE9A2 R15 = haystack base             ;R15=scratch; SI=msg_ptr; R14=bytes_consumed;
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPGATHERDD    (R15)(Z6*1),K3,  Z8       //;2CF4C294 gather data                     ;Z8=data_msg; K3=tmp_mask; R15=scratch; Z6=o_data_inner_loop;

  VPCMPD.BCST   $0,  (R13)(R14*4),Z8,  K4,  K4  //;7EA5C5FB K4 &= (data_msg==Address());K4=active_lanes; Z8=data_msg; R13=seg_ptr; R14=bytes_consumed;
  KTESTW        K4,  K4                   //;FB6F192A any lanes still alive?          ;K4=active_lanes;
  JZ            no_update                 //;815AFE30 no, break out of inner_loop; jump if zero (ZF = 1);
  SUBL          $4,  CX                   //;BDFA8BC7 seg_index -= 4                  ;CX=seg_index;
  INCL          R14                       //;1FA30197 bytes_consumed++                ;R14=bytes_consumed;
inner_tail:
//; NOTE: entered from inner_skipchar, as well as the code above ^^^
  CMPL          CX,  $4                   //;A91F11D1 more than 4chars in segment?    ;CX=seg_index;
  JG            inner_loop                //;21A93561 yes, do the inner_loop; jump if greater ((ZF = 0) and (SF = OF));
  TESTL         CX,  CX                   //;55C7CFAD any chars left in needle?       ;CX=seg_index;
  JZ            update                    //;B0CE3FB0 no, then the inner-code is done; jump if zero (ZF = 1);
//; #endregion inner_loop

//; #region load msg_data and seg_data
  KMOVW         K4,  K3                   //;14DDEED3 copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  LEAQ          (SI)(R14*4),R15           //;6C167139                                 ;R15=scratch; SI=msg_ptr; R14=bytes_consumed;
  VPGATHERDD    (R15)(Z6*1),K3,  Z8       //;53C5314C gather data                     ;Z8=data_msg; K3=tmp_mask; R15=scratch; Z6=o_data_inner_loop;

  VPBROADCASTD  CX,  Z26                  //;63080F73 This is a bugfix. Somehow, using Z23 does not work;Z26=scratch_Z26; CX=seg_index;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask (needle)          ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD.BCST.Z (R13)(R14*4),Z19, K4,  Z9  //;950C4AB8 load segment with mask         ;Z9=data_needle; K4=active_lanes; Z19=tail_mask; R13=seg_ptr; R14=bytes_consumed;

  VPSUBD        Z6,  Z5,  Z26             //;9D333DC0 compute n_remaining_bytes_data  ;Z26=scratch_Z26; Z5=o_data_end; Z6=o_data_inner_loop;
  VPMINSD       Z26, Z20, Z26             //;B13B2440 scratch_Z26 := min(4, scratch_Z26);Z26=scratch_Z26; Z20=constd_4;
  VPERMD        Z18, Z26, Z26             //;B7D1A978 get tail_mask (data)            ;Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z26, Z19, Z19             //;70311526 combine data and segment mask   ;Z19=tail_mask; Z26=scratch_Z26;

  VPANDD        Z8,  Z19, Z8              //;AF7967AF final haystack &= mask          ;Z8=data_msg; Z19=tail_mask;
//; #endregion load msg_data and seg_data

  VPCMPD        $0,  Z9,  Z8,  K4,  K4    //;A8770CE8 K4 &= (data_msg==data_needle)   ;K4=active_lanes; Z8=data_msg; Z9=data_needle; 0=Eq;
  KTESTW        K4,  K4                   //;B58B42F2 any matches?                    ;K4=active_lanes;
  JZ            no_update                 //;A1F36466 no, then postinc; jump if zero (ZF = 1);
  VPADDD        Z23, Z6,  Z6              //;30ECF5E0 o_data_inner_loop += seg_length ;Z6=o_data_inner_loop; Z23=seg_length;

//; see if we've reached the end of the pattern, or if there's another segment to match
  VMOVD         X23, CX                   //;59065A37 restore seg_length              ;CX=seg_index; Z23=seg_length;
  ADDQ          R13, CX                   //;291DBAF6 seg_index += seg_ptr            ;CX=seg_index; R13=seg_ptr;
  MOVQ          8(R8),R13                 //;9A4A2B75 load pattern_length             ;R13=seg_ptr; R8=pattern_begin_ptr;
  LEAQ          (DX)(R13*1),R13           //;953CDD7D load end-of-pattern pointer     ;R13=seg_ptr; DX=seg_begin_ptr;
  CMPQ          CX,  R13                  //;B85D3F03                                 ;R13=seg_ptr; CX=seg_index;
  JNE           skipchar                  //;69250FD4 test if we are *actually done*; jump if not equal (ZF = 0);

update:
  VPSUBD        Z2,  Z6,  Z26             //;C6E4E202 scratch_Z26 := o_data_inner_loop - str_start;Z26=scratch_Z26; Z6=o_data_inner_loop; Z2=str_start;
  VPSUBD        Z26, Z3,  K4,  Z3         //;92C20EC9 str_length -= scratch_Z26       ;Z3=str_length; K4=active_lanes; Z26=scratch_Z26;
  VMOVDQU32     Z6,  K4,  Z2              //;BBD0D6BD str_start := o_data_inner_loop  ;Z2=str_start; K4=active_lanes; Z6=o_data_inner_loop;
  KORW          K4,  K1,  K1              //;13B24E89 add to lane_active              ;K1=lane_active; K4=active_lanes;
  KANDNW        K2,  K4,  K2              //;6577B2E7 remove from lane_matched        ;K2=lane_matched; K4=active_lanes;
no_update:
  VPADDD        Z10, Z25, K2,  Z25        //;1361241C o_data_outer_loop++             ;Z25=o_data_outer_loop; K2=lane_matched; Z10=constd_1;
outer_tail:
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;A511EAB5 K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end;
  KTESTW        K2,  K2                   //;2427BAAC any lanes still alive?          ;K2=lane_matched;
  JNZ           outer_loop                //;2385D85E yes, then jump; jump if not zero (ZF = 0);
next:
  NEXT()

//; #region skipchar
skipchar:
//; at this point Z6 = end-of-last-match,
//; DX = string pointer, R13 = end-of-string pointer,
//; CX = end-of-segment (so, start of next segment)
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D98831F7 K4 &= (o_data_inner_loop<o_data_end);K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end;
  KTESTW        K4,  K4                   //;DC4B6D58                                 ;K4=active_lanes;
  JZ            no_update                 //;B6BD72EA all inner matches failed; jump if zero (ZF = 1);

  KMOVW         K4,  K3                   //;86D47D0E copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 clear data_msg                  ;Z8=data_msg;
  VPGATHERDD    (SI)(Z6*1),K3,  Z8        //;F8AFC558 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z6=o_data_inner_loop;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z6,  K4,  Z6         //;8E6EFFC6 o_data_inner_loop += n_bytes_data;Z6=o_data_inner_loop; K4=active_lanes; Z7=n_bytes_data;
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D69EC427 K4 &= (o_data_inner_loop<o_data_end); unset lanes we have used up;K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end;

//; set up registers as if we were entering 'inner_tail' from the header of 'inner_loop'
  LEAQ          1(CX),R13                 //;F5CF92D4 init seg_ptr (skip seg_length char);R13=seg_ptr; CX=seg_index;
  MOVBLZX       (CX),CX                   //;72E449D6 load seg_length                 ;CX=seg_index;
  XORL          R14, R14                  //;51BB9559 reset bytes_consumed            ;R14=bytes_consumed;
  TESTL         CX,  CX                   //;113BD3AB any bytes left in segment?      ;CX=seg_index;
  JZ            reset_and_skip            //;8DE787B9 yes, keep skipping; jump if zero (ZF = 1);
  VPBROADCASTD  CX,  Z23                  //;E14BC512 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  JMP           inner_tail                //;D3658066                                 ;
reset_and_skip:
  ADDQ          R13, CX                   //;357B2138 seg_index += seg_ptr            ;CX=seg_index; R13=seg_ptr;
  JMP           skipchar                  //;43A5A433                                 ;
//; #endregion skipchar
//; #endregion bcMatchpatCs

//; #region bcMatchpatCi
//; string @ (SI)(Z2:Z3) matches dict[imm] ?
//;  each string segment length is incoded directly in dict[imm], and the segments directly in dict[imm], and the segments operation
TEXT bcMatchpatCi(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R8)                      //;05667C35 Load *[]byte with the provided str into R8
  MOVQ          (R8),DX                   //;E6E1D839                                 ;DX=seg_begin_ptr; R8=pattern_begin_ptr;
  KMOVW         K1,  K2                   //;ECF269E6 lane_matched := lane_active     ;K2=lane_matched; K1=lane_active;
  KMOVW         K0,  K1                   //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VMOVDQU32     Z2,  Z25                  //;3FC39C85 o_data_outer_loop := str_start  ;Z25=o_data_outer_loop; Z2=str_start;
  VPADDD        Z2,  Z3,  Z5              //;E5429114 o_data_end := str_length + str_start;Z5=o_data_end; Z3=str_length; Z2=str_start;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;

//; #region loading to_upper constants
  MOVL          $0x7A6120,R15             //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z15                  //;00000000                                 ;Z15=c_0b00100000; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z16                  //;00000000                                 ;Z16=c_char_a; R15=tmp_constant;
  SHRL          $8,  R15                  //;00000000                                 ;R15=tmp_constant;
  VPBROADCASTB  R15, Z17                  //;00000000                                 ;Z17=c_char_z; R15=tmp_constant;
//; #endregion
  VPBROADCASTB  1(DX),Z24                 //;3DD84A29 load first code-point needle    ;Z24=data_needle2; DX=seg_begin_ptr;
  JMP           outer_tail                //;ECD5FF70                                 ;

outer_loop:
//; try to match against the first byte of the needle and advance up to 4 bytes at a time while that byte isn't present in the input
  KMOVW         K2,  K3                   //;6979316F copy eligible lanes             ;K3=tmp_mask; K2=lane_matched;
  VPGATHERDD    (SI)(Z25*1),K3,  Z8       //;D040E340 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z25=o_data_outer_loop;
//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K5         //;30E9B9FD K5 := (data_msg>=c_char_a)      ;K5=scratch_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K5,  K5    //;8CE85BA0 K5 &= (data_msg<=c_char_z)      ;K5=scratch_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K5,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K5=scratch_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPB        $0,  Z24, Z13, K3         //;7CA38894 K3 := (data_msg_upper==data_needle2); select matching bytes;K3=tmp_mask; Z13=data_msg_upper; Z24=data_needle2; 0=Eq;
  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=skip_count_data; K3=tmp_mask;
  VPSHUFB       Z22, Z14, Z14             //;4F265F03 reverse byte order              ;Z14=skip_count_data; Z22=constant_bswap32;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=skip_count_data;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields skip_count   ;Z14=skip_count_data;

  VPADDD        Z14, Z25, K2,  Z25        //;5034DEA0 o_data_outer_loop += skip_count_data;Z25=o_data_outer_loop; K2=lane_matched; Z14=skip_count_data;
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;DBB2E31C K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end; 1=LessThen;
  KTESTW        K2,  K2                   //;EB60CC5C any lanes still alive?          ;K2=lane_matched;
  JZ            next                      //;FF07EB20 no, then exit; jump if zero (ZF = 1);

  VPCMPD        $4,  Z20, Z14, K2,  K4    //;80B9AEA2 K4 := K2 & (skip_count_data!=4); active lanes where skip != 4;K4=active_lanes; K2=lane_matched; Z14=skip_count_data; Z20=constd_4; 4=NotEqual;
  KTESTW        K4,  K4                   //;BC448E1A                                 ;K4=active_lanes;
  JZ            outer_loop                //;B6E89A28 keep looping if we skipped 4 bytes everywhere; jump if zero (ZF = 1);
  KMOVW         K2,  K4                   //;C1B09128 may as well search everything active;K4=active_lanes; K2=lane_matched;

  MOVBLZX       (DX),CX                   //;B285DE84 load seg_length                 ;CX=seg_index; DX=seg_begin_ptr;
  VPBROADCASTD  CX,  Z23                  //;40103F07 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  XORL          R14, R14                  //;5B762374 reset bytes_consumed            ;R14=bytes_consumed;
  LEAQ          1(DX),R13                 //;3724357C init seg_ptr (skip seg_length char);R13=seg_ptr; DX=seg_begin_ptr;
  VMOVDQU32     Z25, Z6                   //;C5082E4B o_data_inner_loop := o_data_outer_loop;Z6=o_data_inner_loop; Z25=o_data_outer_loop;

//; #region inner_loop
  JMP           inner_tail                //;E826EBC7                                 ;
inner_loop:
  VPSUBD        Z6,  Z5,  Z26             //;A584C7CC compute n_remaining_bytes_data  ;Z26=scratch_Z26; Z5=o_data_end; Z6=o_data_inner_loop;
  VPCMPD        $2,  Z26, Z23, K4,  K4    //;6F2BA80B K4 &= (seg_length<=scratch_Z26) ;K4=active_lanes; Z23=seg_length; Z26=scratch_Z26; 2=LessEq;
  KTESTW        K4,  K4                   //;FB6F192A any lanes still alive?          ;K4=active_lanes;
  JZ            no_update                 //;815AFE30 no, then break out of inner_loop; jump if zero (ZF = 1);

  LEAQ          (SI)(R14*4),R15           //;D11AE9A2 R15 = haystack base             ;R15=scratch; SI=msg_ptr; R14=bytes_consumed;
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPGATHERDD    (R15)(Z6*1),K3,  Z8       //;2CF4C294 gather data                     ;Z8=data_msg; K3=tmp_mask; R15=scratch; Z6=o_data_inner_loop;

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K5         //;30E9B9FD K5 := (data_msg>=c_char_a)      ;K5=scratch_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K5,  K5    //;8CE85BA0 K5 &= (data_msg<=c_char_z)      ;K5=scratch_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K5,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K5=scratch_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD.BCST   $0,  (R13)(R14*4),Z13, K4,  K4  //;480D68BD K4 &= (data_msg_upper==Address());K4=active_lanes; Z13=data_msg_upper; R13=seg_ptr; R14=bytes_consumed;
  KTESTW        K4,  K4                   //;FB6F192A any lanes still alive?          ;K4=active_lanes;
  JZ            no_update                 //;815AFE30 no, break out of inner_loop; jump if zero (ZF = 1);
  SUBL          $4,  CX                   //;BDFA8BC7 seg_index -= 4                  ;CX=seg_index;
  INCL          R14                       //;1FA30197 bytes_consumed++                ;R14=bytes_consumed;
inner_tail:
//; NOTE: entered from inner_skipchar, as well as the code above ^^^
  CMPL          CX,  $4                   //;A91F11D1 more than 4chars in segment?    ;CX=seg_index;
  JG            inner_loop                //;21A93561 yes, do the inner_loop; jump if greater ((ZF = 0) and (SF = OF));
  TESTL         CX,  CX                   //;55C7CFAD any chars left in needle?       ;CX=seg_index;
  JZ            update                    //;B0CE3FB0 no, then the inner-code is done; jump if zero (ZF = 1);
//; #endregion inner_loop

//; #region load msg_data and seg_data
  KMOVW         K4,  K3                   //;14DDEED3 copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  LEAQ          (SI)(R14*4),R15           //;6C167139                                 ;R15=scratch; SI=msg_ptr; R14=bytes_consumed;
  VPGATHERDD    (R15)(Z6*1),K3,  Z8       //;53C5314C gather data                     ;Z8=data_msg; K3=tmp_mask; R15=scratch; Z6=o_data_inner_loop;

  VPBROADCASTD  CX,  Z26                  //;63080F73 This is a bugfix. Somehow, using Z23 does not work;Z26=scratch_Z26; CX=seg_index;
  VPERMD        Z18, Z26, Z19             //;E5886CFE get tail_mask (needle)          ;Z19=tail_mask; Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD.BCST.Z (R13)(R14*4),Z19, K4,  Z9  //;950C4AB8 load segment with mask         ;Z9=data_needle; K4=active_lanes; Z19=tail_mask; R13=seg_ptr; R14=bytes_consumed;

  VPSUBD        Z6,  Z5,  Z26             //;9D333DC0 compute n_remaining_bytes_data  ;Z26=scratch_Z26; Z5=o_data_end; Z6=o_data_inner_loop;
  VPMINSD       Z26, Z20, Z26             //;B13B2440 scratch_Z26 := min(4, scratch_Z26);Z26=scratch_Z26; Z20=constd_4;
  VPERMD        Z18, Z26, Z26             //;B7D1A978 get tail_mask (data)            ;Z26=scratch_Z26; Z18=tail_mask_data;
  VPANDD        Z26, Z19, Z19             //;70311526 combine data and segment mask   ;Z19=tail_mask; Z26=scratch_Z26;

  VPANDD        Z8,  Z19, Z8              //;AF7967AF final haystack &= mask          ;Z8=data_msg; Z19=tail_mask;
//; #endregion load msg_data and seg_data

//; #region str_to_upper
  VPCMPB        $5,  Z16, Z8,  K5         //;30E9B9FD K5 := (data_msg>=c_char_a)      ;K5=scratch_mask; Z8=data_msg; Z16=c_char_a; 5=GreaterEq;
  VPCMPB        $2,  Z17, Z8,  K5,  K5    //;8CE85BA0 K5 &= (data_msg<=c_char_z)      ;K5=scratch_mask; Z8=data_msg; Z17=c_char_z; 2=LessEq;
  VPMOVM2B      K5,  Z13                  //;ADC21F45 mask with selected chars        ;Z13=data_msg_upper; K5=scratch_mask;
  VPTERNLOGQ    $76, Z15, Z8,  Z13        //;1BB96D97 see stringext.md                ;Z13=data_msg_upper; Z8=data_msg; Z15=c_0b00100000;
//; #endregion str_to_upper

  VPCMPD        $0,  Z9,  Z13, K4,  K4    //;3FCC2424 K4 &= (data_msg_upper==data_needle);K4=active_lanes; Z13=data_msg_upper; Z9=data_needle; 0=Eq;
  KTESTW        K4,  K4                   //;B58B42F2 any matches?                    ;K4=active_lanes;
  JZ            no_update                 //;A1F36466 no, then postinc; jump if zero (ZF = 1);
  VPADDD        Z23, Z6,  Z6              //;30ECF5E0 o_data_inner_loop += seg_length ;Z6=o_data_inner_loop; Z23=seg_length;

//; see if we've reached the end of the pattern, or if there's another segment to match
  VMOVD         X23, CX                   //;59065A37 restore seg_length              ;CX=seg_index; Z23=seg_length;
  ADDQ          R13, CX                   //;291DBAF6 seg_index += seg_ptr            ;CX=seg_index; R13=seg_ptr;
  MOVQ          8(R8),R13                 //;9A4A2B75 load pattern_length             ;R13=seg_ptr; R8=pattern_begin_ptr;
  LEAQ          (DX)(R13*1),R13           //;953CDD7D load end-of-pattern pointer     ;R13=seg_ptr; DX=seg_begin_ptr;
  CMPQ          CX,  R13                  //;B85D3F03                                 ;R13=seg_ptr; CX=seg_index;
  JNE           skipchar                  //;69250FD4 test if we are *actually done*; jump if not equal (ZF = 0);

update:
  VPSUBD        Z2,  Z6,  Z26             //;C6E4E202 scratch_Z26 := o_data_inner_loop - str_start;Z26=scratch_Z26; Z6=o_data_inner_loop; Z2=str_start;
  VPSUBD        Z26, Z3,  K4,  Z3         //;92C20EC9 str_length -= scratch_Z26       ;Z3=str_length; K4=active_lanes; Z26=scratch_Z26;
  VMOVDQU32     Z6,  K4,  Z2              //;BBD0D6BD str_start := o_data_inner_loop  ;Z2=str_start; K4=active_lanes; Z6=o_data_inner_loop;
  KORW          K4,  K1,  K1              //;13B24E89 add to lane_active              ;K1=lane_active; K4=active_lanes;
  KANDNW        K2,  K4,  K2              //;6577B2E7 remove from lane_matched        ;K2=lane_matched; K4=active_lanes;
no_update:
  VPADDD        Z10, Z25, K2,  Z25        //;1361241C o_data_outer_loop++             ;Z25=o_data_outer_loop; K2=lane_matched; Z10=constd_1;
outer_tail:
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;A511EAB5 K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end;
  KTESTW        K2,  K2                   //;2427BAAC any lanes still alive?          ;K2=lane_matched;
  JNZ           outer_loop                //;2385D85E yes, then jump; jump if not zero (ZF = 0);
next:
  NEXT()

//; #region skipchar
skipchar:
//; at this point Z6 = end-of-last-match,
//; DX = string pointer, R13 = end-of-string pointer,
//; CX = end-of-segment (so, start of next segment)
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D98831F7 K4 &= (o_data_inner_loop<o_data_end);K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end;
  KTESTW        K4,  K4                   //;DC4B6D58                                 ;K4=active_lanes;
  JZ            no_update                 //;B6BD72EA all inner matches failed; jump if zero (ZF = 1);

  KMOVW         K4,  K3                   //;86D47D0E copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPXORD        Z8,  Z8,  Z8              //;CED5BB69 clear data_msg                  ;Z8=data_msg;
  VPGATHERDD    (SI)(Z6*1),K3,  Z8        //;F8AFC558 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z6=o_data_inner_loop;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z6,  K4,  Z6         //;8E6EFFC6 o_data_inner_loop += n_bytes_data;Z6=o_data_inner_loop; K4=active_lanes; Z7=n_bytes_data;
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D69EC427 K4 &= (o_data_inner_loop<o_data_end); unset lanes we have used up;K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end;

//; set up registers as if we were entering 'inner_tail' from the header of 'inner_loop'
  LEAQ          1(CX),R13                 //;F5CF92D4 init seg_ptr (skip seg_length char);R13=seg_ptr; CX=seg_index;
  MOVBLZX       (CX),CX                   //;72E449D6 load seg_length                 ;CX=seg_index;
  XORL          R14, R14                  //;51BB9559 reset bytes_consumed            ;R14=bytes_consumed;
  TESTL         CX,  CX                   //;113BD3AB any bytes left in segment?      ;CX=seg_index;
  JZ            reset_and_skip            //;8DE787B9 yes, keep skipping; jump if zero (ZF = 1);
  VPBROADCASTD  CX,  Z23                  //;E14BC512 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  JMP           inner_tail                //;D3658066                                 ;
reset_and_skip:
  ADDQ          R13, CX                   //;357B2138 seg_index += seg_ptr            ;CX=seg_index; R13=seg_ptr;
  JMP           skipchar                  //;43A5A433                                 ;
//; #endregion skipchar
//; #endregion bcMatchpatCi

//; #region bcMatchpatUTF8Ci
//; string @ (SI)(Z2:Z3) matches dict[imm] ?
//;  each string segment length is incoded directly in dict[imm], and the segments directly in dict[imm], and the segments operation
TEXT bcMatchpatUTF8Ci(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R8)                      //;05667C35 load *[]byte with the provided str into R8
  MOVQ          (R8),DX                   //;E6E1D839                                 ;DX=needle_begin_ptr; R8=tmp;
  KMOVW         K1,  K2                   //;ECF269E6 lane_matched := lane_active     ;K2=lane_matched; K1=lane_active;
  KMOVW         K0,  K1                   //;6F6437B4 lane_active := 0                ;K1=lane_active;
  VMOVDQU32     Z2,  Z25                  //;3FC39C85 o_data_outer_loop := str_start  ;Z25=o_data_outer_loop; Z2=str_start;
  VPADDD        Z2,  Z3,  Z5              //;E5429114 compute string end position     ;Z5=o_data_end; Z3=str_length; Z2=str_start;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
  VMOVDQU32     CONST_TAIL_MASK(),Z18     //;7DB21CB0 load tail_mask_data             ;Z18=tail_mask_data;
  VMOVDQU32     CONST_N_BYTES_UTF8(),Z21  //;B323211A load table_n_bytes_utf8         ;Z21=table_n_bytes_utf8;
  VPXORD        Z11, Z11, Z11             //;81C90120 load constant 0                 ;Z11=constd_0;
  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;

  MOVQ          8(R8),R8                  //;272B5640 load pattern_length             ;R8=tmp;
  LEAQ          (DX)(R8*1),BX             //;74E897E8 needle_end_ptr := needle_begin_ptr + tmp;BX=needle_end_ptr; DX=needle_begin_ptr; R8=tmp;

  JMP           outer_tail                //;ECD5FF70                                 ;

outer_loop:
//; keep looping in outer_loop till we find a matching start code-point

  KMOVW         K2,  K3                   //;6979316F copy eligible lanes             ;K3=tmp_mask; K2=lane_matched;
  VPGATHERDD    (SI)(Z25*1),K3,  Z8       //;D040E340 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z25=o_data_outer_loop;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask (data)            ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;BF3EB085 mask data                       ;Z8=data_msg; Z19=tail_mask;

  VPCMPD.BCST   $0,  4(DX),Z8,  K3        //;345D0BF3 K3 := (data_msg==[needle_begin_ptr+4]);K3=tmp_mask; Z8=data_msg; DX=needle_begin_ptr; 0=Eq;
  VPCMPD.BCST   $0,  8(DX),Z8,  K5        //;EFD0A9A3 K5 := (data_msg==[needle_begin_ptr+8]);K5=scratch_mask1; Z8=data_msg; DX=needle_begin_ptr; 0=Eq;
  VPCMPD.BCST   $0,  12(DX),Z8,  K6       //;CAC0FAC6 K6 := (data_msg==[needle_begin_ptr+12]);K6=scratch_mask2; Z8=data_msg; DX=needle_begin_ptr; 0=Eq;
  VPCMPD.BCST   $0,  16(DX),Z8,  K7       //;50C70740 K7 := (data_msg==[needle_begin_ptr+16]);K7=scratch_mask3; Z8=data_msg; DX=needle_begin_ptr; 0=Eq;
  KORW          K3,  K5,  K3              //;58E49245 tmp_mask |= scratch_mask1       ;K3=tmp_mask; K5=scratch_mask1;
  KORW          K3,  K6,  K3              //;BDCB8940 tmp_mask |= scratch_mask2       ;K3=tmp_mask; K6=scratch_mask2;
  KORW          K7,  K3,  K4              //;AAF6ED91 active_lanes := tmp_mask | scratch_mask3;K4=active_lanes; K3=tmp_mask; K7=scratch_mask3;
  KNOTW         K4,  K3                   //;2C3A5B12                                 ;K3=tmp_mask; K4=active_lanes;

  VPADDD        Z7,  Z25, K3,  Z25        //;5034DEA0 o_data_outer_loop += n_bytes_data;Z25=o_data_outer_loop; K3=tmp_mask; Z7=n_bytes_data;
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;DBB2E31C K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end; 1=LessThen;
  KTESTW        K2,  K2                   //;EB60CC5C any lanes still alive?          ;K2=lane_matched;
  JZ            next                      //;FF07EB20 no, then exit; jump if zero (ZF = 1);

  KNOTW         K3,  K4                   //;EA2AB365 negate                          ;K4=active_lanes; K3=tmp_mask;
  KTESTW        K4,  K4                   //;BC448E1A ZF := (K4==0); CF := 1          ;K4=active_lanes;
  JZ            outer_loop                //;B6E89A28 keep looping if we skipped everywhere; jump if zero (ZF = 1);
  KMOVW         K2,  K4                   //;C1B09128 may as well search everything active;K4=active_lanes; K2=lane_matched;

  MOVL          (DX),CX                   //;B285DE84 load seg_length                 ;CX=seg_index; DX=needle_begin_ptr;
  VPBROADCASTD  CX,  Z23                  //;40103F07 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  XORL          R14, R14                  //;5B762374 reset bytes_consumed            ;R14=bytes_consumed;
  LEAQ          4(DX),R13                 //;3724357C init seg_ptr (skip seg_length char);R13=seg_start_ptr; DX=needle_begin_ptr;
  VMOVDQU32     Z25, Z6                   //;C5082E4B o_data_inner_loop := o_data_outer_loop;Z6=o_data_inner_loop; Z25=o_data_outer_loop;

//; #region inner_loop
  JMP           inner_tail                //;E826EBC7                                 ;
inner_loop:
  KMOVW         K4,  K3                   //;F271B5DF copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPGATHERDD    (SI)(Z6*1),K3,  Z8        //;2CF4C294 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z6=o_data_inner_loop;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPERMD        Z18, Z7,  Z19             //;E5886CFE get tail_mask (data)            ;Z19=tail_mask; Z7=n_bytes_data; Z18=tail_mask_data;
  VPANDD        Z8,  Z19, Z8              //;BF3EB085 mask data                       ;Z8=data_msg; Z19=tail_mask;

  VPCMPD.BCST   $0,  (R13)(R14*1),Z8,  K4,  K3  //;345D0BF3 K3 := K4 & (data_msg==[seg_start_ptr+bytes_consumed]);K3=tmp_mask; K4=active_lanes; Z8=data_msg; R13=seg_start_ptr; R14=bytes_consumed;
  VPCMPD.BCST   $0,  4(R13)(R14*1),Z8,  K4,  K5  //;EFD0A9A3 K5 := K4 & (data_msg==[seg_start_ptr+bytes_consumed+4]);K5=scratch_mask1; K4=active_lanes; Z8=data_msg; R13=seg_start_ptr; R14=bytes_consumed;
  VPCMPD.BCST   $0,  8(R13)(R14*1),Z8,  K4,  K6  //;CAC0FAC6 K6 := K4 & (data_msg==[seg_start_ptr+bytes_consumed+8]);K6=scratch_mask2; K4=active_lanes; Z8=data_msg; R13=seg_start_ptr; R14=bytes_consumed;
  VPCMPD.BCST   $0,  12(R13)(R14*1),Z8,  K4,  K7  //;50C70740 K7 := K4 & (data_msg==[seg_start_ptr+bytes_consumed+12]);K7=scratch_mask3; K4=active_lanes; Z8=data_msg; R13=seg_start_ptr; R14=bytes_consumed;
  KORW          K3,  K5,  K3              //;58E49245 tmp_mask |= scratch_mask1       ;K3=tmp_mask; K5=scratch_mask1;
  KORW          K3,  K6,  K3              //;BDCB8940 tmp_mask |= scratch_mask2       ;K3=tmp_mask; K6=scratch_mask2;
  KORW          K7,  K3,  K4              //;AAF6ED91 active_lanes := tmp_mask | scratch_mask3;K4=active_lanes; K3=tmp_mask; K7=scratch_mask3;
  ADDL          $16, R14                  //;4D85A22A bytes_consumed += 16            ;R14=bytes_consumed;

  KTESTW        K4,  K4                   //;FB6F192A any lanes still alive?          ;K4=active_lanes;
  JZ            no_update                 //;815AFE30 no, then break out of inner_loop; jump if zero (ZF = 1);

  VPADDD        Z6,  Z7,  Z6              //;BC3C6510 o_data_inner_loop += n_bytes_data;Z6=o_data_inner_loop; Z7=n_bytes_data;
  DECL          CX                        //;466E8A52 seg_index--                     ;CX=seg_index;
inner_tail:
  TESTL         CX,  CX                   //;55C7CFAD any chars left in needle?       ;CX=seg_index;
  JNZ           inner_loop                //;B0CE3FB0 no, then the inner-code is done; jump if not zero (ZF = 0);

//; see if we've reached the end of the pattern, or if there's another segment to match
  VMOVD         X23, CX                   //;59065A37 restore seg_length              ;CX=seg_end_ptr; Z23=seg_length;
  SHLQ          $4,  CX                   //;8790EEE2 seg_end_ptr <<= 4               ;CX=seg_end_ptr;
  ADDQ          R13, CX                   //;291DBAF6 seg_end_ptr += seg_start_ptr    ;CX=seg_end_ptr; R13=seg_start_ptr;

  LEAQ          (R13)(R14*1),R8           //;736B9EFF tmp := seg_start_ptr + bytes_consumed;R8=tmp; R13=seg_start_ptr; R14=bytes_consumed;
  CMPQ          R8,  BX                   //;6DF8AA3C at end of needle?               ;BX=needle_end_ptr; R8=tmp;
  JNE           skipchar                  //;69250FD4 no, then skip a char; jump if not equal (ZF = 0);

//; #endregion inner_loop

update:
  VPSUBD        Z2,  Z6,  Z26             //;C6E4E202 scratch_Z26 := o_data_inner_loop - str_start;Z26=scratch_Z26; Z6=o_data_inner_loop; Z2=str_start;
  VPSUBD        Z26, Z3,  K4,  Z3         //;92C20EC9 str_length -= scratch_Z26       ;Z3=str_length; K4=active_lanes; Z26=scratch_Z26;
  VMOVDQU32     Z6,  K4,  Z2              //;BBD0D6BD str_start := end-of-match       ;Z2=str_start; K4=active_lanes; Z6=o_data_inner_loop;
  KORW          K4,  K1,  K1              //;13B24E89 add to lane_active              ;K1=lane_active; K4=active_lanes;
  KANDNW        K2,  K4,  K2              //;6577B2E7 remove from lane_matched        ;K2=lane_matched; K4=active_lanes;
no_update:
  VPADDD        Z7,  Z25, K2,  Z25        //;1361241C o_data_outer_loop += n_bytes_data;Z25=o_data_outer_loop; K2=lane_matched; Z7=n_bytes_data;
outer_tail:
  VPCMPD        $1,  Z5,  Z25, K2,  K2    //;A511EAB5 K2 &= (o_data_outer_loop<o_data_end); restrict to valid offsets;K2=lane_matched; Z25=o_data_outer_loop; Z5=o_data_end; 1=LessThen;
  KTESTW        K2,  K2                   //;2427BAAC any lanes still alive?          ;K2=lane_matched;
  JNZ           outer_loop                //;2385D85E yes, then jump; jump if not zero (ZF = 0);
next:
  NEXT()

//; #region skipchar
skipchar:
//; at this point Z6 = end-of-last-match,
//; DX = string pointer, R13 = end-of-string pointer,
//; R11 = end-of-segment (so, start of next segment)
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D98831F7 K4 &= (o_data_inner_loop<o_data_end);K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end; 1=LessThen;
  KTESTW        K4,  K4                   //;DC4B6D58 ZF := (K4==0); CF := 1          ;K4=active_lanes;
  JZ            no_update                 //;B6BD72EA all inner matches failed; jump if zero (ZF = 1);

  KMOVW         K4,  K3                   //;86D47D0E copy eligible lanes             ;K3=tmp_mask; K4=active_lanes;
  VPGATHERDD    (SI)(Z6*1),K3,  Z8        //;F8AFC558 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z6=o_data_inner_loop;

  VPSRLD        $4,  Z8,  Z26             //;FE5F1413 shift 4 bits to right           ;Z26=scratch_Z26; Z8=data_msg;
  VPERMD        Z21, Z26, Z7              //;68FECBA0 get n_bytes_data                ;Z7=n_bytes_data; Z26=scratch_Z26; Z21=table_n_bytes_utf8;
  VPADDD        Z7,  Z6,  K4,  Z6         //;8E6EFFC6 o_data_inner_loop += n_bytes_data;Z6=o_data_inner_loop; K4=active_lanes; Z7=n_bytes_data;
  VPCMPD        $1,  Z5,  Z6,  K4,  K4    //;D69EC427 K4 &= (o_data_inner_loop<o_data_end); unset lanes we have used up;K4=active_lanes; Z6=o_data_inner_loop; Z5=o_data_end; 1=LessThen;

//; set up registers as if we were entering 'inner_tail' from the header of 'inner_loop'
  LEAQ          4(CX),R13                 //;F5CF92D4 init seg_ptr (skip seg_length char);R13=seg_start_ptr; CX=seg_end_ptr;
  MOVL          (CX),CX                   //;72E449D6 load seg_length                 ;CX=seg_index; CX=seg_end_ptr;
  XORL          R14, R14                  //;51BB9559 reset bytes_consumed            ;R14=bytes_consumed;
  TESTL         CX,  CX                   //;113BD3AB any bytes left in segment?      ;CX=seg_index;
  JZ            reset_and_skip            //;8DE787B9 yes, keep skipping; jump if zero (ZF = 1);
  VPBROADCASTD  CX,  Z23                  //;E14BC512 bcst seg_length                 ;Z23=seg_length; CX=seg_index;
  JMP           inner_tail                //;D3658066                                 ;
reset_and_skip:
//;MOVQ         CX,  CX                   //;979A1F89                                 ;CX=seg_end_ptr; CX=seg_index;
  SHLQ          $4,  CX                   //;39BAB93A seg_end_ptr <<= 4               ;CX=seg_end_ptr;
  ADDQ          R13, CX                   //;E03E8BC7 seg_end_ptr += seg_start_ptr    ;CX=seg_end_ptr; R13=seg_start_ptr;

  JMP           skipchar                  //;43A5A433                                 ;
//; #endregion skipchar
//; #endregion bcMatchpatUTF8Ci

//; #region bcIsSubnetOfIP4
//; Determine whether the string at Z2:Z3 is an IP address in the range of the provided IP address range
TEXT bcIsSubnetOfIP4(SB), NOSPLIT|NOFRAME, $0
  IMM_FROM_DICT(R14)                      //;05667C35 load *[]byte with the provided str into R14
  MOVQ          (R14),R14                 //;D2647DF0 load needle_ptr                 ;R14=needle_ptr; R14=needle_slice;
  VMOVDQU32     Z2,  Z4                   //;6F6F1342 search_base := str_start        ;Z4=search_base; Z2=str_start;

  VPBROADCASTD  CONSTD_1(),Z10            //;6F57EE92 load constant 1                 ;Z10=constd_1;
  VPBROADCASTD  CONSTD_4(),Z20            //;C8AFBE50 load constant 4                 ;Z20=constd_4;
  VPBROADCASTB  CONSTD_0x2E(),Z21         //;487A092B load constant char_dot          ;Z21=char_dot;
  VPBROADCASTB  CONSTD_0x0F(),Z19         //;7E33FF0D load constant 0b00001111        ;Z19=bcd_mask;
  VMOVDQU32     bswap32<>(SB),Z22         //;2510A88F load constant_bswap32           ;Z22=constant_bswap32;
//; first 3 numbers in IP address (that end with a dot)
  MOVL          $3,  CX                   //;97E4B0BB compare the first 3 ints of IP  ;CX=counter;
  KMOVW         K1,  K4                   //;E40C8014 lane_todo_min := lane_active    ;K4=lane_todo_min; K1=lane_active;
  KMOVW         K1,  K5                   //;C82AE9DA lane_todo_max := lane_active    ;K5=lane_todo_max; K1=lane_active;
  KMOVW         K1,  K6                   //;CA9B839F lane_active_min := lane_active  ;K6=lane_active_min; K1=lane_active;
  KMOVW         K1,  K7                   //;7159F950 lane_active_max := lane_active  ;K7=lane_active_max; K1=lane_active;
loop:
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;

  VPADDD        Z10, Z4,  Z4              //;E66940CD search_base++                   ;Z4=search_base; Z10=constd_1;

  VPBROADCASTD  (R14),Z26                 //;85FE2A68 load ip_range                   ;Z26=ip_min; R14=needle_ptr;
  ADDQ          $4,  R14                  //;B2EF9837 needle_ptr += 4                 ;R14=needle_ptr;
  VPSRLD        $4,  Z26, Z27             //;7D831D80                                 ;Z27=ip_max; Z26=ip_min;
  VPANDD        Z26, Z19, Z26             //;C8F73FDE ip_min &= bcd_mask              ;Z26=ip_min; Z19=bcd_mask;
  VPANDD        Z27, Z19, Z27             //;E5C42B44 ip_max &= bcd_mask              ;Z27=ip_max; Z19=bcd_mask;

  VPSHUFB       Z22, Z8,  Z8              //;4F265F03 reverse byte order              ;Z8=data_msg; Z22=constant_bswap32;
  VPCMPB        $0,  Z21, Z8,  K3         //;FDA19C68 K3 := (data_msg==char_dot)      ;K3=tmp_mask; Z8=data_msg; Z21=char_dot; 0=Eq;
  VPANDD        Z8,  Z19, Z8              //;C318FD02 data_msg &= bcd_mask            ;Z8=data_msg; Z19=bcd_mask;

  VPMOVM2B      K3,  Z14                  //;E74FDEBD promote 64x bit to 64x byte     ;Z14=dot_pos; K3=tmp_mask;
  VPLZCNTD      Z14, Z14                  //;72202F9A count leading zeros             ;Z14=dot_pos;
  VPSRLD        $3,  Z14, Z14             //;6DC91432 divide by 8 yields dot_pos      ;Z14=dot_pos;
  VPSUBD        Z14, Z20, Z28             //;BC43621D scratch_Z28 := 4 - dot_pos      ;Z28=scratch_Z28; Z20=constd_4; Z14=dot_pos;
  VPADDD        Z14, Z4,  Z4              //;9077E42E search_base += dot_pos          ;Z4=search_base; Z14=dot_pos;
  VPSLLD        $3,  Z28, Z28             //;B533D91C times 8 gives bytes to shift    ;Z28=scratch_Z28;
  VPSRLVD       Z28, Z8,  Z8              //;6D4B355C adjust data                     ;Z8=data_msg; Z28=scratch_Z28;

  VPCMPD        $5,  Z26, Z8,  K4,  K3    //;982B35DE K3 := K4 & (data_msg>=ip_min)   ;K3=tmp_mask; K4=lane_todo_min; Z8=data_msg; Z26=ip_min; 5=GreaterEq;
  KANDNW        K6,  K4,  K2              //;27235B6C scratch_K2 := ~lane_todo_min & lane_active_min;K2=scratch_K2; K4=lane_todo_min; K6=lane_active_min;
  VPCMPD        $0,  Z26, Z8,  K4,  K4    //;7347068C K4 &= (data_msg==ip_min)        ;K4=lane_todo_min; Z8=data_msg; Z26=ip_min; 0=Eq;
  KORW          K3,  K2,  K6              //;5A29F035 lane_active_min := scratch_K2 | tmp_mask;K6=lane_active_min; K2=scratch_K2; K3=tmp_mask;

  VPCMPD        $2,  Z27, Z8,  K5,  K3    //;27BFCA91 K3 := K5 & (data_msg<=ip_max)   ;K3=tmp_mask; K5=lane_todo_max; Z8=data_msg; Z27=ip_max; 2=LessEq;
  KANDNW        K7,  K5,  K2              //;C52B6681 scratch_K2 := ~lane_todo_max & lane_active_max;K2=scratch_K2; K5=lane_todo_max; K7=lane_active_max;
  VPCMPD        $0,  Z27, Z8,  K5,  K5    //;A70DC3C3 K5 &= (data_msg==ip_max)        ;K5=lane_todo_max; Z8=data_msg; Z27=ip_max; 0=Eq;
  KORW          K3,  K2,  K7              //;E588CF91 lane_active_max := scratch_K2 | tmp_mask;K7=lane_active_max; K2=scratch_K2; K3=tmp_mask;

  KORTESTW      K4,  K5                   //;2BFBF8CE any lanes still todo?           ;K5=lane_todo_max; K4=lane_todo_min;
  JZ            next                      //;B763A908 no, exit; jump if zero (ZF = 1) ;

  DECL          CX                        //;18ACCC03 counter--                       ;CX=counter;
  JNZ           loop                      //;6929AA0C another number in IP present?; jump if not zero (ZF = 0);

//; load last numbers in IP address
  KMOVW         K1,  K3                   //;723D04C9 copy eligible lanes             ;K3=tmp_mask; K1=lane_active;
  VPGATHERDD    (SI)(Z4*1),K3,  Z8        //;E4967C89 gather data                     ;Z8=data_msg; K3=tmp_mask; SI=msg_ptr; Z4=search_base;

  VPSHUFB       Z22, Z8,  Z8              //;4F265F03 reverse byte order              ;Z8=data_msg; Z22=constant_bswap32;
//; calculate the number of remaining bytes and use that instead of finding a dot.
  VPSUBD        Z2,  Z4,  Z14             //;800D09BC dot_pos := search_base - str_start;Z14=dot_pos; Z4=search_base; Z2=str_start;
  VPSUBD        Z14, Z3,  Z14             //;52D7FB45 dot_pos := str_length - dot_pos ;Z14=dot_pos; Z3=str_length;
  VPANDD        Z8,  Z19, Z8              //;C318FD02 data_msg &= bcd_mask            ;Z8=data_msg; Z19=bcd_mask;

  VPSUBD        Z14, Z20, Z28             //;BC43621D scratch_Z28 := 4 - dot_pos      ;Z28=scratch_Z28; Z20=constd_4; Z14=dot_pos;
  VPSLLD        $3,  Z28, Z28             //;B533D91C times 8 gives bytes to shift    ;Z28=scratch_Z28;
  VPSRLVD       Z28, Z8,  Z8              //;6D4B355C adjust data                     ;Z8=data_msg; Z28=scratch_Z28;

  VPBROADCASTD  (R14),Z26                 //;85FE2A68 load ip_range                   ;Z26=ip_min; R14=needle_ptr;
  VPSRLD        $4,  Z26, Z27             //;7D831D80                                 ;Z27=ip_max; Z26=ip_min;
  VPANDD        Z26, Z19, Z26             //;C8F73FDE ip_min &= bcd_mask              ;Z26=ip_min; Z19=bcd_mask;
  VPANDD        Z27, Z19, Z27             //;E5C42B44 ip_max &= bcd_mask              ;Z27=ip_max; Z19=bcd_mask;

  VPCMPD        $5,  Z26, Z8,  K4,  K3    //;982B35DE K3 := K4 & (data_msg>=ip_min)   ;K3=tmp_mask; K4=lane_todo_min; Z8=data_msg; Z26=ip_min; 5=GreaterEq;
  KANDNW        K6,  K4,  K2              //;6F7C6F6E scratch_K2 := ~lane_todo_min & lane_active_min;K2=scratch_K2; K4=lane_todo_min; K6=lane_active_min;
  KORW          K3,  K2,  K6              //;7B1A3448 lane_active_min := scratch_K2 | tmp_mask;K6=lane_active_min; K2=scratch_K2; K3=tmp_mask;

  VPCMPD        $2,  Z27, Z8,  K5,  K3    //;327EA9E2 K3 := K5 & (data_msg<=ip_max)   ;K3=tmp_mask; K5=lane_todo_max; Z8=data_msg; Z27=ip_max; 2=LessEq;
  KANDNW        K7,  K5,  K2              //;85D2E03D scratch_K2 := ~lane_todo_max & lane_active_max;K2=scratch_K2; K5=lane_todo_max; K7=lane_active_max;
  KORW          K3,  K2,  K7              //;CB00427A lane_active_max := scratch_K2 | tmp_mask;K7=lane_active_max; K2=scratch_K2; K3=tmp_mask;

next:
  KANDW         K6,  K7,  K1              //;5F783BA8 lane_active := lane_active_max & lane_active_min;K1=lane_active; K7=lane_active_max; K6=lane_active_min;
  NEXT()
//; #endregion bcIsSubnetOfIP4

//; #endregion string methods

// this is the 'unimplemented!' op
TEXT bctrap(SB), NOSPLIT|NOFRAME, $0
  BYTE $0xCC
  RET

// chacha8 random initialization vector
DATA  chachaiv<>+0(SB)/4, $0x9722F977  // XOR'd with length for real IV
DATA  chachaiv<>+4(SB)/4, $0x3320646e
DATA  chachaiv<>+8(SB)/4, $0x79622d32
DATA  chachaiv<>+12(SB)/4, $0x6b206574
DATA  chachaiv<>+16(SB)/4, $0x058A60F5
DATA  chachaiv<>+20(SB)/4, $0xB25F6FB1
DATA  chachaiv<>+24(SB)/4, $0x1FEFA3D9
DATA  chachaiv<>+28(SB)/4, $0xB9D8F520
DATA  chachaiv<>+32(SB)/4, $0xB415DBCC
DATA  chachaiv<>+36(SB)/4, $0x34B70366
DATA  chachaiv<>+40(SB)/4, $0x3F4DBB4D
DATA  chachaiv<>+44(SB)/4, $0xCBB67392
DATA  chachaiv<>+48(SB)/4, $0x61707865
DATA  chachaiv<>+52(SB)/4, $0x143BE9F6
DATA  chachaiv<>+56(SB)/4, $0xDA97A1A8
DATA  chachaiv<>+60(SB)/4, $0x6F0E9495
GLOBL chachaiv<>(SB), RODATA|NOPTR, $64

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
