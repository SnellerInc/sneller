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


// The macros used to deal with Ion TV fields and varuint lengths.
// They expects some other defines to be present prior use. Please
// see impementation of bcobjectsize to see how the macros are
// expected to be used.

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
