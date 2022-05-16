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

// Macros used by ByteCode Instructions
// ====================================

#define BC_MACROS_H_DEFINED

#ifndef BC_CONSTANT_H_DEFINED
#include "bc_constant.h"
#endif

// In | Out | Clobber
// ------------------

// These are informative macros that should make it easier to follow code
// that uses highler level constructs implemented via macros.

#define IN(Reg) Reg
#define OUT(Reg) Reg
#define IN_OUT(Reg) Reg
#define CLOBBER(Reg) Reg

// Math Functions for Inlining
// ---------------------------

// These math functions were designed to by inlinable in other bytecode instructions. They are
// not as precise as our bytecode instructions, however, they are much faster and suitable for
// calculations where the highest precision is not required (like GEO tiling, for example).

// BC_FAST_SIN_4ULP() - lower precision sin(x) that works for input range (-15, 15)
// and has a maximum error of 3.5 ULPs. No special number handling, no -0 handling, ...
CONST_DATA_U64(const_sin_u35,  0, $0xc00921fb54442d18) // f64(-3.1415926535897931)
CONST_DATA_U64(const_sin_u35,  8, $0x3fd45f306dc9c883) // f64(0.31830988618379069)
CONST_DATA_U64(const_sin_u35, 16, $0xbca1a62633145c07) // f64(-1.2246467991473532E-16)
CONST_DATA_U64(const_sin_u35, 24, $0xbc62622b22d526be) // f64(-7.9725595500903787E-18)
CONST_DATA_U64(const_sin_u35, 32, $0xbd6ae7ea531357bf) // f64(-7.6471221911815883E-13)
CONST_DATA_U64(const_sin_u35, 40, $0x3ce94fa618796592) // f64(2.810099727108632E-15)
CONST_DATA_U64(const_sin_u35, 48, $0xbe5ae64567cb5786) // f64(-2.5052108376350205E-8)
CONST_DATA_U64(const_sin_u35, 56, $0x3ec71de3a5568a50) // f64(2.7557319223919875E-6)
CONST_DATA_U64(const_sin_u35, 64, $0xbf2a01a01a019fc7) // f64(-1.9841269841269616E-4)
CONST_DATA_U64(const_sin_u35, 72, $0x3f8111111111110f) // f64(0.0083333333333333297)
CONST_DATA_U64(const_sin_u35, 80, $0xbfc5555555555555) // f64(-0.16666666666666666)
CONST_GLOBAL(const_sin_u35, $88)

// Clobbers Z16..Z27
#define BC_FAST_SIN_4ULP(DST_A, DST_B, SRC_A, SRC_B)                                           \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 0), Z20                                            \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 8), Z21                                            \
  VMULPD Z21, SRC_A, Z16                                                                       \
  VMULPD Z21, SRC_B, Z17                                                                       \
  VRNDSCALEPD $8, Z16, Z16                                                                     \
  VRNDSCALEPD $8, Z17, Z17                                                                     \
  VCVTPD2DQ.RN_SAE Z16, Y22                                                                    \
  VCVTPD2DQ.RN_SAE Z17, Y23                                                                    \
  VINSERTI32X8 $1, Y23, Z22, Z22                                                               \
  VMOVAPD Z20, Z21                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 16), Z23                                           \
  VFMADD213PD SRC_A, Z16, Z20                                 /* Z20 = (Z16 * Z20) + SRC_A */  \
  VFMADD213PD SRC_B, Z17, Z21                                 /* Z21 = (Z17 * Z21) + SRC_B */  \
  VFMADD231PD Z23, Z16, Z20                                   /* Z20 = (Z16 * Z23) + Z20   */  \
  VFMADD231PD Z23, Z17, Z21                                   /* Z21 = (Z17 * Z23) + Z21   */  \
  VPSLLD $31, Z22, Z22                                                                         \
  VMULPD Z20, Z20, Z16                                                                         \
  VMULPD Z21, Z21, Z17                                                                         \
  VPBROADCASTQ CONSTF64_SIGN_BIT(), Z19                                                        \
  VPMOVD2M Z22, K3                                                                             \
  KSHIFTRW $8, K3, K4                                                                          \
  VMOVDQA64.Z Z19, K3, Z18                                                                     \
  VMOVDQA64.Z Z19, K4, Z19                                                                     \
  VXORPD Z20, Z18, Z18                                                                         \
  VXORPD Z21, Z19, Z19                                                                         \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 24), Z20                                           \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 32), Z22                                           \
  VMOVAPD Z20, Z21                                                                             \
  VMOVAPD Z22, Z23                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 40), Z26                                           \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 40), Z27                                           \
  VFMADD213PD Z26, Z16, Z20                                   /* Z20 = (Z16 * Z20) + Z26   */  \
  VFMADD213PD Z26, Z17, Z21                                   /* Z21 = (Z17 * Z21) + Z26   */  \
  VFMADD213PD Z27, Z16, Z22                                   /* Z22 = (Z16 * Z22) + Z27   */  \
  VFMADD213PD Z27, Z17, Z23                                   /* Z23 = (Z17 * Z23) + Z27   */  \
  VMULPD Z16, Z16, Z24                                                                         \
  VMULPD Z17, Z17, Z25                                                                         \
  VFMADD231PD Z20, Z24, Z22                                   /* Z22 = (Z24 * Z20) + Z22   */  \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 48), Z20                                           \
  VFMADD231PD Z21, Z25, Z23                                   /* Z23 = (Z25 * Z21) + Z23   */  \
  VMOVAPD Z20, Z21                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 56), Z27                                           \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 64), Z26                                           \
  VFMADD213PD Z27, Z16, Z20                                   /* Z20 = (Z16 * Z20) + Z27   */  \
  VFMADD213PD Z27, Z17, Z21                                   /* Z21 = (Z17 * Z21) + Z27   */  \
  VMOVAPD Z26, Z27                                                                             \
  VMULPD Z24, Z24, DST_A                                                                       \
  VMULPD Z25, Z25, DST_B                                                                       \
  VFMADD213PD.BCST CONST_GET_PTR(const_sin_u35, 72), Z16, Z26 /* Z26 = (Z16 * Z26) + mem   */  \
  VFMADD213PD.BCST CONST_GET_PTR(const_sin_u35, 72), Z17, Z27 /* Z27 = (Z17 * Z27) + mem   */  \
  VFMADD231PD Z20, Z24, Z26                                   /* Z26 = (Z24 * Z20) + Z26   */  \
  VFMADD231PD Z21, Z25, Z27                                   /* Z27 = (Z25 * Z21) + Z27   */  \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 80), Z24                                           \
  VFMADD231PD Z22, DST_A, Z26                                 /* Z26 = (DST_A * Z22) + Z26 */  \
  VFMADD231PD Z23, DST_B, Z27                                 /* Z27 = (DST_B * Z23) + Z27 */  \
  VFMADD213PD Z24, Z16, Z26                                   /* Z26 = (Z16 * Z26) + Z24   */  \
  VFMADD213PD Z24, Z17, Z27                                   /* Z27 = (Z17 * Z27) + Z24   */  \
  VMULPD Z18, Z26, DST_A                                                                       \
  VMULPD Z19, Z27, DST_B                                                                       \
  VFMADD213PD Z18, Z16, DST_A                                 /* DST_A = (Z16*DST_A) + Z18 */  \
  VFMADD213PD Z19, Z17, DST_B                                 /* DST_B = (Z17*DST_B) + Z19 */

// BC_FAST_LN_4ULP() - lower precision ln(x) that has a maximum error of 3.5 ULPs.
CONST_DATA_U64(const_ln_u35,  0, $0x3ff5555555555555) // f64(1.3333333333333333)
CONST_DATA_U64(const_ln_u35,  8, $0xbff0000000000000) // f64(-1)
CONST_DATA_U64(const_ln_u35, 16, $0x3fc385c5cbc3f50d) // f64(0.15251991700635195)
CONST_DATA_U64(const_ln_u35, 24, $0x3fc7474ba672b05f) // f64(0.18186326625198299)
CONST_DATA_U64(const_ln_u35, 32, $0x3fc3a5791d95db39) // f64(0.15348733849142507)
CONST_DATA_U64(const_ln_u35, 40, $0x3fcc71bfeed5d419) // f64(0.22222136651876737)
CONST_DATA_U64(const_ln_u35, 48, $0x3fd249249bfbe987) // f64(0.28571429474654803)
CONST_DATA_U64(const_ln_u35, 56, $0x3fd99999998c136e) // f64(0.3999999999507996)
CONST_DATA_U64(const_ln_u35, 64, $0x3fe555555555593f) // f64(0.66666666666677787)
CONST_DATA_U64(const_ln_u35, 72, $0x3fe62e42fefa39ef) // f64(0.69314718055994529)
CONST_DATA_U64(const_ln_u35, 80, $0x40862e42fefa39ef) // f64(709.78271289338397)
CONST_DATA_U64(const_ln_u35, 88, $0x4000000000000000) // f64(2)
CONST_DATA_U64(const_ln_u35, 96, $0x0050000000500000) // i64(22517998142095360)
CONST_GLOBAL(const_ln_u35, $104)

// Clobbers Z14..Z27
#define BC_FAST_LN_4ULP(DST_A, DST_B, SRC_A, SRC_B)                                            \
  VMULPD.BCST CONST_GET_PTR(const_ln_u35, 0), SRC_A, DST_A                                     \
  VMULPD.BCST CONST_GET_PTR(const_ln_u35, 0), SRC_B, DST_B                                     \
  VGETEXPPD DST_A, DST_A                                                                       \
  VGETEXPPD DST_B, DST_B                                                                       \
  VCMPPD.BCST $VCMP_IMM_EQ_OQ, CONSTF64_POSITIVE_INF(), DST_A, K3                              \
  VCMPPD.BCST $VCMP_IMM_EQ_OQ, CONSTF64_POSITIVE_INF(), DST_B, K4                              \
  VGETMANTPD $11, SRC_A, Z14                                                                   \
  VGETMANTPD $11, SRC_B, Z15                                                                   \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 8), Z20                                             \
  VBROADCASTSD CONSTF64_1(), Z21                                                               \
  VADDPD Z20, Z14, Z16                                                                         \
  VADDPD Z20, Z15, Z17                                                                         \
  VADDPD Z21, Z14, Z14                                                                         \
  VADDPD Z21, Z15, Z15                                                                         \
  VDIVPD Z14, Z16, Z14                                                                         \
  VDIVPD Z15, Z17, Z15                                                                         \
  VMULPD Z14, Z14, Z16                                                                         \
  VMULPD Z15, Z15, Z17                                                                         \
  VMULPD Z16, Z16, Z18                                                                         \
  VMULPD Z17, Z17, Z19                                                                         \
  VMULPD Z18, Z18, Z20                                                                         \
  VMULPD Z19, Z19, Z21                                                                         \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 16), Z22                                            \
  VMOVAPD Z22, Z23                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 24), Z24                                            \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 32), Z25                                            \
  VFMADD213PD Z24, Z16, Z22                                  /* Z22 = (Z16 * Z22) + Z24 */     \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 40), Z24                                            \
  VFMADD213PD Z24, Z17, Z23                                  /* Z23 = (Z17 * Z23) + Z24 */     \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 56), Z26                                            \
  VFMADD231PD Z25, Z18, Z22                                  /* Z22 = (Z18 * Z25) + Z22 */     \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 48), Z27                                            \
  VFMADD231PD Z25, Z19, Z23                                  /* Z23 = (Z19 * Z25) + Z23 */     \
  VMOVAPD Z24, Z25                                                                             \
  VFMADD213PD Z27, Z16, Z24                                  /* Z24 = (Z16 * Z24) + Z27 */     \
  VFMADD213PD Z27, Z17, Z25                                  /* Z25 = (Z17 * Z25) + Z27 */     \
  VMOVAPD Z26, Z27                                                                             \
  VFMADD213PD.BCST CONST_GET_PTR(const_ln_u35, 64), Z16, Z26 /* Z26 = (Z16 * Z26) + mem */     \
  VFMADD213PD.BCST CONST_GET_PTR(const_ln_u35, 64), Z17, Z27 /* Z27 = (Z17 * Z27) + mem */     \
  VMULPD Z16, Z14, Z16                                                                         \
  VMULPD Z17, Z15, Z17                                                                         \
  VFMADD231PD Z24, Z18, Z26                                  /* Z26 = (Z18 * Z24) + Z26 */     \
  VFMADD231PD Z25, Z19, Z27                                  /* Z27 = (Z19 * Z25) + Z27 */     \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 72), Z24                                            \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 80), Z25                                            \
  VMULPD Z24, DST_A, DST_A                                                                     \
  VMULPD Z24, DST_B, DST_B                                                                     \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 88), Z24                                            \
  VMOVAPD Z25, K3, DST_A                                                                       \
  VMOVAPD Z25, K4, DST_B                                                                       \
  VFMADD231PD Z24, Z14, DST_A                                /* DST_A = (Z14 * Z24) + DST_A */ \
  VFMADD231PD Z24, Z15, DST_B                                /* DST_B = (Z15 * Z24) + DST_B */ \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 96), Z24                                            \
  VFMADD231PD Z22, Z20, Z26                                  /* Z26 = (Z20 * Z22) + Z26     */ \
  VFMADD231PD Z23, Z21, Z27                                  /* Z27 = (Z21 * Z23) + Z27     */ \
  VFMADD231PD Z26, Z16, DST_A                                /* DST_A = (Z16 * Z26) + DST_A */ \
  VFMADD231PD Z27, Z17, DST_B                                /* DST_B = (Z17 * Z27) + DST_B */ \
  VFIXUPIMMPD $0, Z24, SRC_A, DST_A                                                            \
  VFIXUPIMMPD $0, Z24, SRC_B, DST_B

// UInt to String
// --------------

// Macros that provide steps that are commonly used by our integer to
// string conversion.

#define BC_UINT_TO_STR_STEP_10000_PREPARE(ConstOut1, ConstOut2) \
  VPBROADCASTQ CONSTQ_3518437209(), ConstOut1 \
  VPBROADCASTQ CONSTQ_10000(), ConstOut2

#define BC_UINT_TO_STR_STEP_10000_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  VPMULUDQ ConstIn1, InOut1, Tmp1 \
  VPMULUDQ ConstIn1, InOut2, Tmp2 \
  VPSRLQ $45, Tmp1, Tmp1           \
  VPSRLQ $45, Tmp2, Tmp2           \
  VPMULUDQ ConstIn2, Tmp1, Tmp3   \
  VPMULUDQ ConstIn2, Tmp2, Tmp4   \
  VPSUBD Tmp3, InOut1, InOut1      \
  VPSUBD Tmp4, InOut2, InOut2      \
  VPSLLQ $32, InOut1, InOut1       \
  VPSLLQ $32, InOut2, InOut2       \
  VPORQ Tmp1, InOut1, InOut1       \
  VPORQ Tmp2, InOut2, InOut2

#define BC_UINT_TO_STR_STEP_10000_4X(InOut1, InOut2, InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10000_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10000_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)

#define BC_UINT_TO_STR_STEP_100_PREPARE(ConstOut1, ConstOut2) \
  VPBROADCASTD CONSTD_5243(), ConstOut1 \
  VPBROADCASTD CONSTD_100(), ConstOut2

#define BC_UINT_TO_STR_STEP_100_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  VPMULHUW ConstIn1, InOut1, Tmp1 \
  VPMULHUW ConstIn1, InOut2, Tmp2 \
  VPSRLW $3, Tmp1, Tmp1            \
  VPSRLW $3, Tmp2, Tmp2            \
  VPMULLW ConstIn2, Tmp1, Tmp3    \
  VPMULLW ConstIn2, Tmp2, Tmp4    \
  VPSUBW Tmp3, InOut1, InOut1      \
  VPSUBW Tmp4, InOut2, InOut2      \
  VPSLLD $16, InOut1, InOut1       \
  VPSLLD $16, InOut2, InOut2       \
  VPORD Tmp1, InOut1, InOut1       \
  VPORD Tmp2, InOut2, InOut2

#define BC_UINT_TO_STR_STEP_100_4X(InOut1, InOut2, InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_100_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_100_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)

#define BC_UINT_TO_STR_STEP_10_PREPARE(ConstOut1, ConstOut2) \
  VPBROADCASTW CONSTD_6554(), ConstOut1 \
  VPBROADCASTW CONSTD_10(), ConstOut2

#define BC_UINT_TO_STR_STEP_10_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  VPMULHUW ConstIn1, InOut1, Tmp1 \
  VPMULHUW ConstIn1, InOut2, Tmp2 \
  VPMULLW ConstIn2, Tmp1, Tmp3    \
  VPMULLW ConstIn2, Tmp2, Tmp4    \
  VPSUBW Tmp3, InOut1, InOut1      \
  VPSUBW Tmp4, InOut2, InOut2      \
  VPSLLD $8, InOut1, InOut1        \
  VPSLLD $8, InOut2, InOut2        \
  VPORD Tmp1, InOut1, InOut1       \
  VPORD Tmp2, InOut2, InOut2

#define BC_UINT_TO_STR_STEP_10_4X(InOut1, InOut2, InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)

#define BC_UINT_TO_STR_STEP_10_6X(InOut1, InOut2, InOut3, InOut4, InOut5, InOut6, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut5, InOut6, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)
