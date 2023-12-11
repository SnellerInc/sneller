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

// Integer Utilities for Inlining
// ------------------------------

#define BC_DIV_U64VEC_BY_U64VEC(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_Z0, SRC_B_Z1, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_Z7, TMP_Z8, TMP_Z9, TMP_K0, TMP_K1) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate the reciprocal of SRC_B_IMM, rounded down */   \
  VCVTUQQ2PD.RU_SAE SRC_B_Z0, TMP_Z8                          \
  VCVTUQQ2PD.RU_SAE SRC_B_Z1, TMP_Z9                          \
  VDIVPD.RD_SAE TMP_Z8, TMP_Z0, TMP_Z8                        \
  VDIVPD.RD_SAE TMP_Z9, TMP_Z0, TMP_Z9                        \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE SRC_A_Z0, TMP_Z2                          \
  VCVTUQQ2PD.RD_SAE SRC_A_Z1, TMP_Z3                          \
  VMULPD.RD_SAE TMP_Z8, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z9, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, SRC_B_Z0, TMP_Z2                            \
  VPMULLQ DST_Z1, SRC_B_Z1, TMP_Z3                            \
  VPSUBQ TMP_Z2, SRC_A_Z0, TMP_Z0                             \
  VPSUBQ TMP_Z3, SRC_A_Z1, TMP_Z1                             \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z8, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z9, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPXORQ TMP_Z9, TMP_Z9, TMP_Z9                               \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, SRC_B_Z0, TMP_Z2                            \
  VPMULLQ DST_Z1, SRC_B_Z1, TMP_Z3                            \
  VPSUBQ TMP_Z2, SRC_A_Z0, TMP_Z4                             \
  VPSUBQ TMP_Z3, SRC_A_Z1, TMP_Z5                             \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_Z0, TMP_Z4, SRC_K0, TMP_K0     \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_Z1, TMP_Z5, SRC_K1, TMP_K1     \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z0, TMP_K0, DST_Z0            \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z1, TMP_K1, DST_Z1            \
                                                              \
  /* Lanes that must yield either zero or a negative value */ \
  VPXORQ SRC_A_Z0, SRC_B_Z0, TMP_Z4                           \
  VPXORQ SRC_A_Z1, SRC_B_Z1, TMP_Z5                           \
  VPMOVQ2M TMP_Z4, TMP_K0                                     \
  VPMOVQ2M TMP_Z5, TMP_K1                                     \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPSUBQ DST_Z0, TMP_Z9, TMP_K0, DST_Z0                       \
  VPSUBQ DST_Z1, TMP_Z9, TMP_K1, DST_Z1

#define BC_DIV_TRUNC_I64VEC_BY_I64VEC(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_Z0, SRC_B_Z1, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_Z7, TMP_Z8, TMP_Z9, TMP_K0, TMP_K1) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate the reciprocal of SRC_B, rounded down */       \
  VPABSQ SRC_B_Z0, TMP_Z6                                     \
  VPABSQ SRC_B_Z1, TMP_Z7                                     \
  VCVTUQQ2PD.RU_SAE TMP_Z6, TMP_Z8                            \
  VCVTUQQ2PD.RU_SAE TMP_Z7, TMP_Z9                            \
  VDIVPD.RD_SAE TMP_Z8, TMP_Z0, TMP_Z8                        \
  VDIVPD.RD_SAE TMP_Z9, TMP_Z0, TMP_Z9                        \
                                                              \
  /* Convert dividends to absolute values */                  \
  VPABSQ SRC_A_Z0, TMP_Z4                                     \
  VPABSQ SRC_A_Z1, TMP_Z5                                     \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE TMP_Z4, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z5, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z8, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z9, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, TMP_Z6, TMP_Z2                              \
  VPMULLQ DST_Z1, TMP_Z7, TMP_Z3                              \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z1                               \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z8, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z9, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPXORQ TMP_Z9, TMP_Z9, TMP_Z9                               \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, TMP_Z6, TMP_Z2                              \
  VPMULLQ DST_Z1, TMP_Z7, TMP_Z3                              \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z4                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z5                               \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, TMP_Z6, TMP_Z4, SRC_K0, TMP_K0       \
  VPCMPUQ $VPCMP_IMM_GE, TMP_Z7, TMP_Z5, SRC_K1, TMP_K1       \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z0, TMP_K0, DST_Z0            \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z1, TMP_K1, DST_Z1            \
                                                              \
  /* Lanes that must yield either zero or a negative value */ \
  VPXORQ SRC_A_Z0, SRC_B_Z0, TMP_Z4                           \
  VPXORQ SRC_A_Z1, SRC_B_Z1, TMP_Z5                           \
  VPMOVQ2M TMP_Z4, TMP_K0                                     \
  VPMOVQ2M TMP_Z5, TMP_K1                                     \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPSUBQ DST_Z0, TMP_Z9, TMP_K0, DST_Z0                       \
  VPSUBQ DST_Z1, TMP_Z9, TMP_K1, DST_Z1

#define BC_DIV_TRUNC_I64VEC_BY_I64IMM(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_IMM, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_Z7, TMP_K0, TMP_K1) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate the reciprocal of SRC_B_IMM, rounded down */   \
  VPABSQ SRC_B_IMM, TMP_Z7                                    \
  VCVTUQQ2PD.RU_SAE TMP_Z7, TMP_Z6                            \
  VDIVPD.RD_SAE TMP_Z6, TMP_Z0, TMP_Z6                        \
                                                              \
  /* Convert dividends to absolute values */                  \
  VPABSQ SRC_A_Z0, TMP_Z4                                     \
  VPABSQ SRC_A_Z1, TMP_Z5                                     \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE TMP_Z4, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z5, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, TMP_Z7, TMP_Z2                              \
  VPMULLQ DST_Z1, TMP_Z7, TMP_Z3                              \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z1                               \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPXORQ TMP_Z6, TMP_Z6, TMP_Z6                               \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, TMP_Z7, TMP_Z2                              \
  VPMULLQ DST_Z1, TMP_Z7, TMP_Z3                              \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z4                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z5                               \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, TMP_Z7, TMP_Z4, SRC_K0, TMP_K0       \
  VPCMPUQ $VPCMP_IMM_GE, TMP_Z7, TMP_Z5, SRC_K1, TMP_K1       \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z0, TMP_K0, DST_Z0            \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z1, TMP_K1, DST_Z1            \
                                                              \
  /* Lanes that must yield either zero or a negative value */ \
  VPXORQ SRC_A_Z0, SRC_B_IMM, TMP_Z4                          \
  VPXORQ SRC_A_Z1, SRC_B_IMM, TMP_Z5                          \
  VPMOVQ2M TMP_Z4, TMP_K0                                     \
  VPMOVQ2M TMP_Z5, TMP_K1                                     \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPSUBQ DST_Z0, TMP_Z6, TMP_K0, DST_Z0                       \
  VPSUBQ DST_Z1, TMP_Z6, TMP_K1, DST_Z1

#define BC_DIV_FLOOR_I64VEC_BY_U64IMM(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_IMM, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_K0, TMP_K1, TMP_K2, TMP_K3) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate reciprocal of SRC_B_IMM, rounded down */       \
  VCVTUQQ2PD.RU_SAE SRC_B_IMM, TMP_Z6                         \
  VDIVPD.RD_SAE TMP_Z6, TMP_Z0, TMP_Z6                        \
                                                              \
  /* Lanes containing negative values */                      \
  VPMOVQ2M SRC_A_Z0, TMP_K0                                   \
  VPMOVQ2M SRC_A_Z1, TMP_K1                                   \
                                                              \
  /* Convert inputs to absolute values */                     \
  VPABSQ SRC_A_Z0, TMP_Z4                                     \
  VPABSQ SRC_A_Z1, TMP_Z5                                     \
                                                              \
  /* Modify dividents to get floor semantics when negative */ \
  VPADDQ.Z SRC_B_IMM, SRC_I64_MINUS_ONE, TMP_K0, TMP_Z2       \
  VPADDQ.Z SRC_B_IMM, SRC_I64_MINUS_ONE, TMP_K1, TMP_Z3       \
  VPADDQ TMP_Z2, TMP_Z4, TMP_Z4                               \
  VPADDQ TMP_Z3, TMP_Z5, TMP_Z5                               \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE TMP_Z4, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z5, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, SRC_B_IMM, TMP_Z2                           \
  VPMULLQ DST_Z1, SRC_B_IMM, TMP_Z3                           \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z1                               \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPXORQ TMP_Z6, TMP_Z6, TMP_Z6                               \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, SRC_B_IMM, TMP_Z2                           \
  VPMULLQ DST_Z1, SRC_B_IMM, TMP_Z3                           \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z4                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z5                               \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_IMM, TMP_Z4, SRC_K0, TMP_K2    \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_IMM, TMP_Z5, SRC_K1, TMP_K3    \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z0, TMP_K2, DST_Z0            \
  VPSUBQ SRC_I64_MINUS_ONE, DST_Z1, TMP_K3, DST_Z1            \
                                                              \
  /* Negate the result, if the result must be negative */     \
  VPSUBQ DST_Z0, TMP_Z6, TMP_K0, DST_Z0                       \
  VPSUBQ DST_Z1, TMP_Z6, TMP_K1, DST_Z1

#define BC_MOD_FLOOR_I64VEC_BY_U64VEC(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_Z0, SRC_B_Z1, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_Z7, TMP_K0, TMP_K1) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate reciprocal of SRC_B_IMM, rounded down */       \
  VCVTUQQ2PD.RU_SAE SRC_B_Z0, TMP_Z6                          \
  VCVTUQQ2PD.RU_SAE SRC_B_Z1, TMP_Z7                          \
  VDIVPD.RD_SAE TMP_Z6, TMP_Z0, TMP_Z6                        \
  VDIVPD.RD_SAE TMP_Z7, TMP_Z0, TMP_Z7                        \
                                                              \
  /* Convert inputs to absolute values */                     \
  VPABSQ SRC_A_Z0, TMP_Z4                                     \
  VPABSQ SRC_A_Z1, TMP_Z5                                     \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE TMP_Z4, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z5, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z7, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, SRC_B_Z0, TMP_Z2                            \
  VPMULLQ DST_Z1, SRC_B_Z1, TMP_Z3                            \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z1                               \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z7, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, SRC_B_Z0, TMP_Z2                            \
  VPMULLQ DST_Z1, SRC_B_Z1, TMP_Z3                            \
  VPSUBQ TMP_Z2, TMP_Z4, DST_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, DST_Z1                               \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_Z0, DST_Z0, SRC_K0, TMP_K0     \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_Z1, DST_Z1, SRC_K1, TMP_K1     \
  VPSUBQ SRC_B_Z0, DST_Z0, TMP_K0, DST_Z0                     \
  VPSUBQ SRC_B_Z1, DST_Z1, TMP_K1, DST_Z1                     \
                                                              \
  /* Lanes containing negative SRC and have non-zero DST */   \
  VPMOVQ2M SRC_A_Z0, TMP_K0                                   \
  VPMOVQ2M SRC_A_Z1, TMP_K1                                   \
  VPTESTMQ DST_Z0, DST_Z0, TMP_K0, TMP_K0                     \
  VPTESTMQ DST_Z1, DST_Z1, TMP_K1, TMP_K1                     \
                                                              \
  /* Fixup the results to get a floor semantics */            \
  VPSUBQ DST_Z0, SRC_B_Z0, TMP_K0, DST_Z0                     \
  VPSUBQ DST_Z1, SRC_B_Z1, TMP_K1, DST_Z1

#define BC_MOD_FLOOR_I64VEC_BY_U64IMM(DST_Z0, DST_Z1, SRC_A_Z0, SRC_A_Z1, SRC_B_IMM, SRC_I64_MINUS_ONE, SRC_K0, SRC_K1, TMP_Z0, TMP_Z1, TMP_Z2, TMP_Z3, TMP_Z4, TMP_Z5, TMP_Z6, TMP_K0, TMP_K1) \
  /* Prepare constants */                                     \
  VBROADCASTSD CONSTF64_1(), TMP_Z0                           \
                                                              \
  /* Calculate reciprocal of SRC_B_IMM, rounded down */       \
  VCVTUQQ2PD.RU_SAE SRC_B_IMM, TMP_Z6                         \
  VDIVPD.RD_SAE TMP_Z6, TMP_Z0, TMP_Z6                        \
                                                              \
  /* Convert inputs to absolute values */                     \
  VPABSQ SRC_A_Z0, TMP_Z4                                     \
  VPABSQ SRC_A_Z1, TMP_Z5                                     \
                                                              \
  /* Perform the first division step (estimate) */            \
  VCVTUQQ2PD.RD_SAE TMP_Z4, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z5, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, DST_Z0                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, DST_Z1                  \
                                                              \
  /* Decrease the dividend by the estimate */                 \
  VPMULLQ DST_Z0, SRC_B_IMM, TMP_Z2                           \
  VPMULLQ DST_Z1, SRC_B_IMM, TMP_Z3                           \
  VPSUBQ TMP_Z2, TMP_Z4, TMP_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, TMP_Z1                               \
                                                              \
  /* Perform the second division step (correction) */         \
  VCVTUQQ2PD.RD_SAE TMP_Z0, TMP_Z2                            \
  VCVTUQQ2PD.RD_SAE TMP_Z1, TMP_Z3                            \
  VMULPD.RD_SAE TMP_Z6, TMP_Z2, TMP_Z2                        \
  VMULPD.RD_SAE TMP_Z6, TMP_Z3, TMP_Z3                        \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z2, SRC_K0, TMP_Z2                  \
  VCVTPD2UQQ.RD_SAE.Z TMP_Z3, SRC_K1, TMP_Z3                  \
                                                              \
  /* Add the correction to the result estimate */             \
  VPADDQ.Z TMP_Z2, DST_Z0, SRC_K0, DST_Z0                     \
  VPADDQ.Z TMP_Z3, DST_Z1, SRC_K1, DST_Z1                     \
                                                              \
  /* We can still be off by one, so correct it */             \
  VPMULLQ DST_Z0, SRC_B_IMM, TMP_Z2                           \
  VPMULLQ DST_Z1, SRC_B_IMM, TMP_Z3                           \
  VPSUBQ TMP_Z2, TMP_Z4, DST_Z0                               \
  VPSUBQ TMP_Z3, TMP_Z5, DST_Z1                               \
                                                              \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_IMM, DST_Z0, SRC_K0, TMP_K0    \
  VPCMPUQ $VPCMP_IMM_GE, SRC_B_IMM, DST_Z1, SRC_K1, TMP_K1    \
  VPSUBQ SRC_B_IMM, DST_Z0, TMP_K0, DST_Z0                    \
  VPSUBQ SRC_B_IMM, DST_Z1, TMP_K1, DST_Z1                    \
                                                              \
  /* Lanes containing negative SRC and have non-zero DST */   \
  VPMOVQ2M SRC_A_Z0, TMP_K0                                   \
  VPMOVQ2M SRC_A_Z1, TMP_K1                                   \
  VPTESTMQ DST_Z0, DST_Z0, TMP_K0, TMP_K0                     \
  VPTESTMQ DST_Z1, DST_Z1, TMP_K1, TMP_K1                     \
                                                              \
  /* Fixup the results to get a floor semantics */            \
  VPSUBQ DST_Z0, SRC_B_IMM, TMP_K0, DST_Z0                    \
  VPSUBQ DST_Z1, SRC_B_IMM, TMP_K1, DST_Z1

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
  /* Calculate the mask of resulting negative values */       \
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

// Math Functions for Inlining
// ---------------------------

// Dst = Left - RoundTrunc(Left / Right) * Right
#define BC_MOD_TRUNC_F64(DST_Z0, DST_Z1, SRC_L_Z0, SRC_L_Z1, SRC_R_Z0, SRC_R_Z1, MASK_0, MASK_1) \
  VDIVPD.RZ_SAE SRC_R_Z0, SRC_L_Z0, DST_Z0          \
  VDIVPD.RZ_SAE SRC_R_Z1, SRC_L_Z1, DST_Z1          \
  VRNDSCALEPD $VROUND_IMM_TRUNC_SAE, DST_Z0, DST_Z0 \
  VRNDSCALEPD $VROUND_IMM_TRUNC_SAE, DST_Z1, DST_Z1 \
  VFNMADD132PD.Z SRC_R_Z0, SRC_L_Z0, MASK_0, DST_Z0 \
  VFNMADD132PD.Z SRC_R_Z1, SRC_L_Z1, MASK_1, DST_Z1

// Dst = Left - RoundFloor(Left / Right) * Right
#define BC_MOD_FLOOR_F64(DST_Z0, DST_Z1, SRC_L_Z0, SRC_L_Z1, SRC_R_Z0, SRC_R_Z1, MASK_0, MASK_1) \
  VDIVPD.RD_SAE SRC_R_Z0, SRC_L_Z0, DST_Z0          \
  VDIVPD.RD_SAE SRC_R_Z1, SRC_L_Z1, DST_Z1          \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, DST_Z0, DST_Z0  \
  VRNDSCALEPD $VROUND_IMM_DOWN_SAE, DST_Z1, DST_Z1  \
  VFNMADD132PD.Z SRC_R_Z0, SRC_L_Z0, MASK_0, DST_Z0 \
  VFNMADD132PD.Z SRC_R_Z1, SRC_L_Z1, MASK_1, DST_Z1

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

// Clobbers Z16..Z27, K3..K4
#define BC_FAST_SIN_4ULP(OutA, OutB, InA, InB)                                                 \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 0), Z20                                            \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 8), Z21                                            \
  VMULPD Z21, InA, Z16                                                                         \
  VMULPD Z21, InB, Z17                                                                         \
  VRNDSCALEPD $8, Z16, Z16                                                                     \
  VRNDSCALEPD $8, Z17, Z17                                                                     \
  VCVTPD2DQ.RN_SAE Z16, Y22                                                                    \
  VCVTPD2DQ.RN_SAE Z17, Y23                                                                    \
  VINSERTI32X8 $1, Y23, Z22, Z22                                                               \
  VMOVAPD Z20, Z21                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 16), Z23                                           \
  VFMADD213PD InA, Z16, Z20                                    /* Z20 = (Z16 * Z20) + InA */   \
  VFMADD213PD InB, Z17, Z21                                    /* Z21 = (Z17 * Z21) + InB */   \
  VFMADD231PD Z23, Z16, Z20                                    /* Z20 = (Z16 * Z23) + Z20 */   \
  VFMADD231PD Z23, Z17, Z21                                    /* Z21 = (Z17 * Z23) + Z21 */   \
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
  VFMADD213PD Z26, Z16, Z20                                    /* Z20 = (Z16 * Z20) + Z26 */   \
  VFMADD213PD Z26, Z17, Z21                                    /* Z21 = (Z17 * Z21) + Z26 */   \
  VFMADD213PD Z26, Z16, Z22                                    /* Z22 = (Z16 * Z22) + Z26 */   \
  VFMADD213PD Z26, Z17, Z23                                    /* Z23 = (Z17 * Z23) + Z26 */   \
  VMULPD Z16, Z16, Z24                                                                         \
  VMULPD Z17, Z17, Z25                                                                         \
  VFMADD231PD Z20, Z24, Z22                                    /* Z22 = (Z24 * Z20) + Z22 */   \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 48), Z20                                           \
  VFMADD231PD Z21, Z25, Z23                                    /* Z23 = (Z25 * Z21) + Z23 */   \
  VMOVAPD Z20, Z21                                                                             \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 56), Z27                                           \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 64), Z26                                           \
  VFMADD213PD Z27, Z16, Z20                                    /* Z20 = (Z16 * Z20) + Z27 */   \
  VFMADD213PD Z27, Z17, Z21                                    /* Z21 = (Z17 * Z21) + Z27 */   \
  VMOVAPD Z26, Z27                                                                             \
  VMULPD Z24, Z24, OutA                                                                        \
  VMULPD Z25, Z25, OutB                                                                        \
  VFMADD213PD.BCST CONST_GET_PTR(const_sin_u35, 72), Z16, Z26  /* Z26 = (Z16 * Z26) + mem */   \
  VFMADD213PD.BCST CONST_GET_PTR(const_sin_u35, 72), Z17, Z27  /* Z27 = (Z17 * Z27) + mem */   \
  VFMADD231PD Z20, Z24, Z26                                    /* Z26 = (Z24 * Z20) + Z26 */   \
  VFMADD231PD Z21, Z25, Z27                                    /* Z27 = (Z25 * Z21) + Z27 */   \
  VBROADCASTSD CONST_GET_PTR(const_sin_u35, 80), Z24                                           \
  VFMADD231PD Z22, OutA, Z26                                   /* Z26 = (OutA * Z22) + Z26 */  \
  VFMADD231PD Z23, OutB, Z27                                   /* Z27 = (OutB * Z23) + Z27 */  \
  VFMADD213PD Z24, Z16, Z26                                    /* Z26 = (Z16 * Z26) + Z24 */   \
  VFMADD213PD Z24, Z17, Z27                                    /* Z27 = (Z17 * Z27) + Z24 */   \
  VMULPD Z18, Z26, OutA                                                                        \
  VMULPD Z19, Z27, OutB                                                                        \
  VFMADD213PD Z18, Z16, OutA                                   /* OutA = (Z16*OutA) + Z18 */   \
  VFMADD213PD Z19, Z17, OutB                                   /* OutB = (Z17*OutB) + Z19 */

// BC_FAST_COS_4ULP() - lower precision cos(x) that works for input range (-15, 15)
// and has a maximum error of 3.5 ULPs. No special number handling, no -0 handling, ...
CONST_DATA_U64(const_cos_u35,   0, $0x3fd45f306dc9c883) // f64(0.31830988618379069)
CONST_DATA_U64(const_cos_u35,   8, $0x4000000000000000) // f64(2)
CONST_DATA_U64(const_cos_u35,  16, $0xbfe0000000000000) // f64(-0.5)
CONST_DATA_U64(const_cos_u35,  24, $0xbff921fb54442d18) // i64(-4613618979930100456)
CONST_DATA_U64(const_cos_u35,  32, $0xbc91a62633145c07) // i64(-4858919839960114169)
CONST_DATA_U64(const_cos_u35,  40, $0x0000000200000002) // i64(8589934594)
CONST_DATA_U64(const_cos_u35,  48, $0xbc62622b22d526be) // f64(-7.9725595500903787E-18)
CONST_DATA_U64(const_cos_u35,  56, $0xbd6ae7ea531357bf) // f64(-7.6471221911815883E-13)
CONST_DATA_U64(const_cos_u35,  64, $0x3ce94fa618796592) // f64(2.810099727108632E-15)
CONST_DATA_U64(const_cos_u35,  72, $0x3de6124601c23966) // f64(1.605904306056645E-10)
CONST_DATA_U64(const_cos_u35,  80, $0xbe5ae64567cb5786) // f64(-2.5052108376350205E-8)
CONST_DATA_U64(const_cos_u35,  88, $0xbf2a01a01a019fc7) // f64(-1.9841269841269616E-4)
CONST_DATA_U64(const_cos_u35,  96, $0x3ec71de3a5568a50) // f64(2.7557319223919875E-6)
CONST_DATA_U64(const_cos_u35, 104, $0x3f8111111111110f) // f64(0.0083333333333333297)
CONST_DATA_U64(const_cos_u35, 112, $0xbfc5555555555555) // f64(-0.16666666666666666)
CONST_GLOBAL(const_cos_u35, $120)

// Clobbers Z14..Z27
#define BC_FAST_COS_4ULP(OutA, OutB, InA, InB)                                                 \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 0), Z14                                            \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 8), Z16                                            \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 16), Z17                                           \
  VMOVAPD Z14, Z15                                                                             \
  VFMADD213PD Z17, InA, Z14                                    /* Z14 = (InA * Z14) + Z17 */   \
  VFMADD213PD Z17, InB, Z15                                    /* Z15 = (InB * Z15) + Z17 */   \
  VRNDSCALEPD $8, Z14, Z14                                                                     \
  VRNDSCALEPD $8, Z15, Z15                                                                     \
  VBROADCASTSD CONSTF64_1(), Z18                                                               \
  VMOVAPD Z16, Z17                                                                             \
  VFMADD213PD Z18, Z14, Z16                                    /* Z16 = (Z14 * Z16) + Z18 */   \
  VFMADD213PD Z18, Z15, Z17                                    /* Z17 = (Z15 * Z17) + Z18 */   \
  VCVTPD2DQ.RN_SAE Z16, Y18                                                                    \
  VCVTPD2DQ.RN_SAE Z17, Y19                                                                    \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 24), Z20                                           \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 32), Z21                                           \
  VINSERTI32X8 $1, Y19, Z18, Z18                                                               \
  VFMADD231PD Z20, Z16, InA                                    /* InA = (Z16 * Z20) + InA */   \
  VFMADD231PD Z20, Z17, InB                                    /* InB = (Z17 * Z20) + InB */   \
  VPANDD.BCST CONST_GET_PTR(const_cos_u35, 40), Z18, Z14                                       \
  VFMADD231PD Z21, Z16, InA                                    /* InA = (Z16 * Z21) + InA */   \
  VFMADD231PD Z21, Z17, InB                                    /* InB = (Z17 * Z21) + InB */   \
  VPTESTNMD Z14, Z14, K6                                                                       \
  VMULPD InA, InA, Z16                                                                         \
  VMULPD InB, InB, Z17                                                                         \
  VMULPD Z16, Z16, Z18                                                                         \
  VMULPD Z17, Z17, Z19                                                                         \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 48), Z20                                           \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 56), Z22                                           \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 64), Z26                                           \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 72), Z27                                           \
  VMOVAPD Z20, Z21                                                                             \
  VMOVAPD Z22, Z23                                                                             \
  VFMADD213PD Z26, Z16, Z20                                    /* Z20 = (Z16 * Z20) + Z26 */   \
  VFMADD213PD Z26, Z17, Z21                                    /* Z21 = (Z17 * Z21) + Z26 */   \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 80), Z24                                           \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 88), Z26                                           \
  VFMADD213PD Z27, Z16, Z22                                    /* Z22 = (Z16 * Z22) + Z27 */   \
  VFMADD213PD Z27, Z17, Z23                                    /* Z23 = (Z17 * Z23) + Z27 */   \
  VBROADCASTSD CONST_GET_PTR(const_cos_u35, 96), Z27                                           \
  VMOVAPD Z24, Z25                                                                             \
  VFMADD213PD Z27, Z16, Z24                                    /* Z24 = (Z16 * Z24) + Z27 */   \
  VFMADD213PD Z27, Z17, Z25                                    /* Z25 = (Z17 * Z25) + Z27 */   \
  VFMADD231PD Z20, Z18, Z22                                    /* Z22 = (Z18 * Z20) + Z22 */   \
  VFMADD231PD Z21, Z19, Z23                                    /* Z23 = (Z19 * Z21) + Z23 */   \
  VMOVAPD Z26, Z27                                                                             \
  VFMADD213PD.BCST CONST_GET_PTR(const_cos_u35, 104), Z16, Z26 /* Z26 = (Z16 * Z26) + mem */   \
  VFMADD213PD.BCST CONST_GET_PTR(const_cos_u35, 104), Z17, Z27 /* Z27 = (Z17 * Z27) + mem */   \
  VFMADD231PD Z24, Z18, Z26                                    /* Z26 = (Z18 * Z24) + Z26 */   \
  VFMADD231PD Z25, Z19, Z27                                    /* Z27 = (Z19 * Z25) + Z27 */   \
  VMULPD Z18, Z18, Z18                                                                         \
  VMULPD Z19, Z19, Z19                                                                         \
  VFMADD231PD Z22, Z18, Z26                                    /* Z26 = (Z18 * Z22) + Z26 */   \
  VFMADD231PD Z23, Z19, Z27                                    /* Z27 = (Z19 * Z23) + Z27 */   \
  VFMADD213PD.BCST CONST_GET_PTR(const_cos_u35, 112), Z16, Z26 /* Z26 = (Z16 * Z26) + mem */   \
  VFMADD213PD.BCST CONST_GET_PTR(const_cos_u35, 112), Z17, Z27 /* Z27 = (Z17 * Z27) + mem */   \
  VPBROADCASTQ.Z CONSTF64_SIGN_BIT(), K6, Z14                                                  \
  KSHIFTRW $8, K6, K6                                                                          \
  VPBROADCASTQ.Z CONSTF64_SIGN_BIT(), K6, Z15                                                  \
  VXORPD InA, Z14, Z24                                                                         \
  VXORPD InB, Z15, Z25                                                                         \
  VMULPD Z24, Z26, Z14                                                                         \
  VMULPD Z25, Z27, Z15                                                                         \
  VMULPD Z14, Z16, Z14                                                                         \
  VMULPD Z15, Z17, Z15                                                                         \
  VADDPD Z24, Z14, OutA                                                                        \
  VADDPD Z25, Z15, OutB

// BC_FAST_ASIN_4ULP() - lower precision asin(x) that has a maximum error of 3.5 ULPs.
CONST_DATA_U64(const_asin_u35,   0, $0x3fa02ff4c7428a47) // f64(0.031615876506539346)
CONST_DATA_U64(const_asin_u35,   8, $0xbf9032e75ccd4ae8) // f64(-0.015819182433299966)
CONST_DATA_U64(const_asin_u35,  16, $0x3f93c0e0817e9742) // f64(0.019290454772679107)
CONST_DATA_U64(const_asin_u35,  24, $0x3f7b0ef96b727e7e) // f64(0.0066060774762771706)
CONST_DATA_U64(const_asin_u35,  32, $0x3f88e3fd48d0fb6f) // f64(0.012153605255773773)
CONST_DATA_U64(const_asin_u35,  40, $0x3f8c70ddf81249fc) // f64(0.013887151845016092)
CONST_DATA_U64(const_asin_u35,  48, $0x3f91c6b5042ec6b2) // f64(0.017359569912236146)
CONST_DATA_U64(const_asin_u35,  56, $0x3f96e89f8578b64e) // f64(0.022371761819320483)
CONST_DATA_U64(const_asin_u35,  64, $0x3f9f1c72c5fd95ba) // f64(0.030381959280381322)
CONST_DATA_U64(const_asin_u35,  72, $0x3fa6db6db407c2b3) // f64(0.044642856813771024)
CONST_DATA_U64(const_asin_u35,  80, $0x3fb3333333375cd0) // f64(0.075000000003785816)
CONST_DATA_U64(const_asin_u35,  88, $0x3fc55555555552f4) // f64(0.16666666666664975)
CONST_DATA_U64(const_asin_u35,  96, $0xc000000000000000) // f64(-2)
CONST_DATA_U64(const_asin_u35, 104, $0x3ff921fb54442d18) // f64(1.5707963267948966)
CONST_GLOBAL(const_asin_u35, $112)

// Clobbers Z10..Z27, K3..K6
#define BC_FAST_ASIN_4ULP(OutA, OutB, InA, InB)                                                \
  VBROADCASTSD CONSTF64_ABS_BITS(), Z11                                                        \
  VPMOVQ2M InA, K5                                                                             \
  VPMOVQ2M InB, K6                                                                             \
  VANDPD Z11, InA, Z10                                                                         \
  VANDPD Z11, InB, Z11                                                                         \
  VBROADCASTSD CONSTF64_HALF(), Z13                                                            \
  VBROADCASTSD CONSTF64_1(), Z15                                                               \
  VCMPPD $VCMP_IMM_LT_OS, Z13, Z10, K3                                                         \
  VCMPPD $VCMP_IMM_LT_OS, Z13, Z11, K4                                                         \
  VSUBPD Z10, Z15, Z14                                                                         \
  VSUBPD Z11, Z15, Z15                                                                         \
  VMULPD Z13, Z14, Z12                                                                         \
  VMULPD Z13, Z15, Z13                                                                         \
  VMULPD InA, InA, K3, Z12                                                                     \
  VMULPD InB, InB, K4, Z13                                                                     \
  VSQRTPD Z12, Z14                                                                             \
  VSQRTPD Z13, Z15                                                                             \
  VMOVAPD Z10, K3, Z14                                                                         \
  VMOVAPD Z11, K4, Z15                                                                         \
  VMULPD Z12, Z12, Z10                                                                         \
  VMULPD Z13, Z13, Z11                                                                         \
  VMULPD Z10, Z10, Z16                                                                         \
  VMULPD Z11, Z11, Z17                                                                         \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 0), Z18                                           \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 16), Z20                                          \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 8), Z22                                           \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 24), Z23                                          \
  VMOVAPD Z18, Z19                                                                             \
  VFMADD213PD Z22, Z12, Z18                                     /* Z18 = (Z12 * Z18) + Z22 */  \
  VFMADD213PD Z22, Z13, Z19                                     /* Z19 = (Z13 * Z19) + Z22 */  \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 32), Z22                                          \
  VMOVAPD Z20, Z21                                                                             \
  VFMADD213PD Z23, Z12, Z20                                     /* Z20 = (Z12 * Z20) + Z23 */  \
  VFMADD213PD Z23, Z13, Z21                                     /* Z21 = (Z13 * Z21) + Z23 */  \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 48), Z24                                          \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 40), Z26                                          \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 56), Z27                                          \
  VMOVAPD Z22, Z23                                                                             \
  VMOVAPD Z24, Z25                                                                             \
  VFMADD213PD Z26, Z12, Z22                                     /* Z22 = (Z12 * Z22) + Z26 */  \
  VFMADD213PD Z26, Z13, Z23                                     /* Z23 = (Z13 * Z23) + Z26 */  \
  VFMADD213PD Z27, Z12, Z24                                     /* Z24 = (Z12 * Z24) + Z27 */  \
  VFMADD213PD Z27, Z13, Z25                                     /* Z25 = (Z13 * Z25) + Z27 */  \
  VFMADD231PD Z18, Z10, Z20                                     /* Z20 = (Z10 * Z18) + Z20 */  \
  VFMADD231PD Z19, Z11, Z21                                     /* Z21 = (Z11 * Z19) + Z21 */  \
  VFMADD231PD Z22, Z10, Z24                                     /* Z24 = (Z10 * Z22) + Z24 */  \
  VFMADD231PD Z23, Z11, Z25                                     /* Z25 = (Z11 * Z23) + Z25 */  \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 64), Z18                                          \
  VMOVAPD Z18, Z19                                                                             \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 72), Z12, Z18  /* Z18 = (Z12 * Z18) + mem */  \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 72), Z13, Z19  /* Z19 = (Z13 * Z19) + mem */  \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 80), Z22                                          \
  VMOVAPD Z22, Z23                                                                             \
  VMULPD Z16, Z16, Z26                                                                         \
  VMULPD Z17, Z17, Z27                                                                         \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 88), Z12, Z22  /* Z22 = (Z12 * Z22) + mem */  \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 88), Z13, Z23  /* Z23 = (Z13 * Z23) + mem */  \
  VFMADD231PD Z18, Z10, Z22                                     /* Z22 = (Z10 * Z18) + Z22 */  \
  VFMADD231PD Z19, Z11, Z23                                     /* Z23 = (Z11 * Z19) + Z23 */  \
  VFMADD231PD Z24, Z16, Z22                                     /* Z22 = (Z16 * Z24) + Z22 */  \
  VFMADD231PD Z25, Z17, Z23                                     /* Z23 = (Z17 * Z25) + Z23 */  \
  VFMADD231PD Z20, Z26, Z22                                     /* Z22 = (Z26 * Z20) + Z22 */  \
  VFMADD231PD Z21, Z27, Z23                                     /* Z23 = (Z27 * Z21) + Z23 */  \
  VMULPD Z14, Z12, Z10                                                                         \
  VMULPD Z15, Z13, Z11                                                                         \
  VFMADD213PD Z14, Z22, Z10                                     /* Z10 = (Z22 * Z10) + Z14 */  \
  VFMADD213PD Z15, Z23, Z11                                     /* Z11 = (Z23 * Z11) + Z15 */  \
  VBROADCASTSD CONST_GET_PTR(const_asin_u35, 96), Z12                                          \
  VMOVAPD Z12, Z13                                                                             \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 104), Z10, Z12 /* Z12 = (Z10 * Z12) + mem */  \
  VFMADD213PD.BCST CONST_GET_PTR(const_asin_u35, 104), Z11, Z13 /* Z13 = (Z11 * Z13) + mem */  \
  VMOVAPD Z10, K3, Z12                                                                         \
  VMOVAPD Z11, K4, Z13                                                                         \
  VPMOVM2Q K5, OutA                                                                            \
  VPMOVM2Q K6, OutB                                                                            \
  VPTERNLOGQ.BCST $108, CONSTF64_SIGN_BIT(), Z12, OutA                                         \
  VPTERNLOGQ.BCST $108, CONSTF64_SIGN_BIT(), Z13, OutB

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
#define BC_FAST_LN_4ULP(OutA, OutB, InA, InB)                                                  \
  VMULPD.BCST CONST_GET_PTR(const_ln_u35, 0), InA, OutA                                        \
  VMULPD.BCST CONST_GET_PTR(const_ln_u35, 0), InB, OutB                                        \
  VGETEXPPD OutA, OutA                                                                         \
  VGETEXPPD OutB, OutB                                                                         \
  VCMPPD.BCST $VCMP_IMM_EQ_OQ, CONSTF64_POSITIVE_INF(), OutA, K3                               \
  VCMPPD.BCST $VCMP_IMM_EQ_OQ, CONSTF64_POSITIVE_INF(), OutB, K4                               \
  VGETMANTPD $11, InA, Z14                                                                     \
  VGETMANTPD $11, InB, Z15                                                                     \
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
  VFMADD213PD Z24, Z16, Z22                                    /* Z22 = (Z16 * Z22) + Z24 */   \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 40), Z24                                            \
  VFMADD213PD Z24, Z17, Z23                                    /* Z23 = (Z17 * Z23) + Z24 */   \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 56), Z26                                            \
  VFMADD231PD Z25, Z18, Z22                                    /* Z22 = (Z18 * Z25) + Z22 */   \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 48), Z27                                            \
  VFMADD231PD Z25, Z19, Z23                                    /* Z23 = (Z19 * Z25) + Z23 */   \
  VMOVAPD Z24, Z25                                                                             \
  VFMADD213PD Z27, Z16, Z24                                    /* Z24 = (Z16 * Z24) + Z27 */   \
  VFMADD213PD Z27, Z17, Z25                                    /* Z25 = (Z17 * Z25) + Z27 */   \
  VMOVAPD Z26, Z27                                                                             \
  VFMADD213PD.BCST CONST_GET_PTR(const_ln_u35, 64), Z16, Z26   /* Z26 = (Z16 * Z26) + mem */   \
  VFMADD213PD.BCST CONST_GET_PTR(const_ln_u35, 64), Z17, Z27   /* Z27 = (Z17 * Z27) + mem */   \
  VMULPD Z16, Z14, Z16                                                                         \
  VMULPD Z17, Z15, Z17                                                                         \
  VFMADD231PD Z24, Z18, Z26                                    /* Z26 = (Z18 * Z24) + Z26 */   \
  VFMADD231PD Z25, Z19, Z27                                    /* Z27 = (Z19 * Z25) + Z27 */   \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 72), Z24                                            \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 80), Z25                                            \
  VMULPD Z24, OutA, OutA                                                                       \
  VMULPD Z24, OutB, OutB                                                                       \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 88), Z24                                            \
  VMOVAPD Z25, K3, OutA                                                                        \
  VMOVAPD Z25, K4, OutB                                                                        \
  VFMADD231PD Z24, Z14, OutA                                   /* OutA = (Z14 * Z24) + OutA */ \
  VFMADD231PD Z24, Z15, OutB                                   /* OutB = (Z15 * Z24) + OutB */ \
  VBROADCASTSD CONST_GET_PTR(const_ln_u35, 96), Z24                                            \
  VFMADD231PD Z22, Z20, Z26                                    /* Z26 = (Z20 * Z22) + Z26 */   \
  VFMADD231PD Z23, Z21, Z27                                    /* Z27 = (Z21 * Z23) + Z27 */   \
  VFMADD231PD Z26, Z16, OutA                                   /* OutA = (Z16 * Z26) + OutA */ \
  VFMADD231PD Z27, Z17, OutB                                   /* OutB = (Z17 * Z27) + OutB */ \
  VFIXUPIMMPD $0, Z24, InA, OutA                                                               \
  VFIXUPIMMPD $0, Z24, InB, OutB

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
  VPSRLQ $45, Tmp1, Tmp1          \
  VPSRLQ $45, Tmp2, Tmp2          \
  VPMULUDQ ConstIn2, Tmp1, Tmp3   \
  VPMULUDQ ConstIn2, Tmp2, Tmp4   \
  VPSUBD Tmp3, InOut1, InOut1     \
  VPSUBD Tmp4, InOut2, InOut2     \
  VPSLLQ $32, InOut1, InOut1      \
  VPSLLQ $32, InOut2, InOut2      \
  VPORQ Tmp1, InOut1, InOut1      \
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
  VPSRLW $3, Tmp1, Tmp1           \
  VPSRLW $3, Tmp2, Tmp2           \
  VPMULLW ConstIn2, Tmp1, Tmp3    \
  VPMULLW ConstIn2, Tmp2, Tmp4    \
  VPSUBW Tmp3, InOut1, InOut1     \
  VPSUBW Tmp4, InOut2, InOut2     \
  VPSLLD $16, InOut1, InOut1      \
  VPSLLD $16, InOut2, InOut2      \
  VPORD Tmp1, InOut1, InOut1      \
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
  VPSUBW Tmp3, InOut1, InOut1     \
  VPSUBW Tmp4, InOut2, InOut2     \
  VPSLLD $8, InOut1, InOut1       \
  VPSLLD $8, InOut2, InOut2       \
  VPORD Tmp1, InOut1, InOut1      \
  VPORD Tmp2, InOut2, InOut2

#define BC_UINT_TO_STR_STEP_10_4X(InOut1, InOut2, InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)

#define BC_UINT_TO_STR_STEP_10_6X(InOut1, InOut2, InOut3, InOut4, InOut5, InOut6, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut1, InOut2, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut3, InOut4, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4) \
  BC_UINT_TO_STR_STEP_10_2X(InOut5, InOut6, ConstIn1, ConstIn2, Tmp1, Tmp2, Tmp3, Tmp4)
