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

// LOWER/UPPER functions
// --------------------------------------------------

#include "bc_constant_tolower.h"
#include "bc_constant_toupper.h"

#define TERN_MERGE  $0xd8       // merge op: VPTERNLOGD mask, true, false

// BC_STR_CHANGE_CASE contants
#define QWORD_LO_VEC        Z10
#define QWORD_HI_VEC        Z11
#define CONSTD_0x00000001   Z12
#define CONSTD_0x00000004   Z13
#define CONSTD_0x00000080   Z14
#define CONSTD_0x000000ff   Z15
#define CONSTD_0x20202020   Z16
#define CONSTD_0x3f3f3f3f   Z17

// BC_UTF8_TO_UTF32 constants
#define utf8_to_utf32_merge_step1   Z18
#define utf8_to_utf32_merge_step2   Z19
#define utf8_to_utf32_shl_lookup    Z20
#define utf8_to_utf32_shr_lookup    Z21

// BC_UTF8_LENGTH_AUX constants
#define utf8_length_lookup          Z22


#define BC_STR_SET_CONSTD(const, reg)    \
    MOVQ $const, R11                     \
    VPBROADCASTD R11, reg

#define BC_STR_SET_VPSHUFB(lane, reg)              \
    VBROADCASTI32X4 CONST_GET_PTR(lane, 0), reg

#define BC_STR_DEF_CONSTD(name, val)    \
    CONST_DATA_U32(name, 0, val)        \
    CONST_GLOBAL(name, $4)

// BC_STR_DEF_VPSHUFB_LANE_CONST defines constants for VPSHUFB
// for **a single** lane. Such constants has to be loaded
// with VBROADCASTI32X4.
#define BC_STR_DEF_VPSHUFB_LANE_CONST(name, b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15) \
    CONST_DATA_U8(name, 0, $b0) \
    CONST_DATA_U8(name, 1, $b1) \
    CONST_DATA_U8(name, 2, $b2) \
    CONST_DATA_U8(name, 3, $b3) \
    CONST_DATA_U8(name, 4, $b4) \
    CONST_DATA_U8(name, 5, $b5) \
    CONST_DATA_U8(name, 6, $b6) \
    CONST_DATA_U8(name, 7, $b7) \
    CONST_DATA_U8(name, 8, $b8) \
    CONST_DATA_U8(name, 9, $b9) \
    CONST_DATA_U8(name, 10, $b10) \
    CONST_DATA_U8(name, 11, $b11) \
    CONST_DATA_U8(name, 12, $b12) \
    CONST_DATA_U8(name, 13, $b13) \
    CONST_DATA_U8(name, 14, $b14) \
    CONST_DATA_U8(name, 15, $b15) \
    CONST_GLOBAL(name, $16)

// BC_STR_CHANGE_CASE generates body of string lower/upper functions.
// They differ only in the way of transforming ASCII bytes.
#define BC_STR_CHANGE_CASE(QWORD_LO, QWORD_HI, displacement, delta)            \
    BC_UNPACK_2xSLOT(BC_SLOT_SIZE*2, OUT(BX), OUT(R8))                         \
                                                                               \
    BC_LOAD_K1_FROM_SLOT(OUT(K1), IN(R8))                                      \
    BC_LOAD_SLICE_FROM_SLOT_MASKED(OUT(Z23), OUT(Z24), IN(BX), IN(K1))         \
                                                                               \
    KTESTW K1, K1                                                              \
    JZ next                                                                    \
                                                                               \
    /* calculate the worst case output length */                               \
    VPBROADCASTD CONSTD_4(), Z4                                                \
    VPSRLD $1, Z24, Z5                                                         \
    VPADDD Z24, Z5, Z5                                                         \
    VPADDD.Z Z4, Z5, K1, Z5                                                    \
                                                                               \
    BC_HORIZONTAL_LENGTH_SUM(OUT(R15), OUT(Z5), OUT(Z6), OUT(Z7), OUT(K1), IN(Z5), IN(K1), X10, K2) \
    BC_ALLOC_SLICE(OUT(Z2), IN(R15), CX, R8)                                   \
    VPADDD.Z Z5, Z2, K1, Z2                                                    \
                                                                               \
    KMOVW K1, BX                                                               \
    VPXORD X3, X3, X3                                                          \
                                                                               \
    /* Calculate offsets of output strings */                                  \
    VMOVDQU32.Z Z2, K1, Z29                                                    \
                                                                               \
    KMOVW K1, K3                                                               \
                                                                               \
utf8:                                                                          \
    MOVQ            QWORD_LO, BX                                               \
    VPBROADCASTQ    BX, QWORD_LO_VEC                                           \
    MOVQ            QWORD_HI, BX                                               \
    VPBROADCASTQ    BX, QWORD_HI_VEC                                           \
    BC_STR_SET_CONSTD(0x00000001, CONSTD_0x00000001)                           \
    BC_STR_SET_CONSTD(0x00000004, CONSTD_0x00000004)                           \
    BC_STR_SET_CONSTD(0x00000080, CONSTD_0x00000080)                           \
    BC_STR_SET_CONSTD(0x000000ff, CONSTD_0x000000ff)                           \
    BC_STR_SET_CONSTD(0x20202020, CONSTD_0x20202020)                           \
    BC_STR_SET_CONSTD(0x3f3f3f3f, CONSTD_0x3f3f3f3f)                           \
                                                                               \
    BC_STR_SET_CONSTD(0x01400140, utf8_to_utf32_merge_step1)                   \
    BC_STR_SET_CONSTD(0x00011000, utf8_to_utf32_merge_step2)                   \
    BC_STR_SET_VPSHUFB(utf8_to_utf32_shift_left, utf8_to_utf32_shl_lookup)     \
    BC_STR_SET_VPSHUFB(utf8_to_utf32_shift_right, utf8_to_utf32_shr_lookup)    \
                                                                               \
    BC_STR_SET_VPSHUFB(utf8_length, utf8_length_lookup)                        \
                                                                               \
    VPADDD      Z23, Z24, Z24   /* End pointer */                              \
utf8_loop:                                                                     \
    VPCMPD      $VPCMP_IMM_LT, Z24, Z23, K3, K3                                \
    KTESTW      K3, K3                                                         \
    JZ          utf8_end                                                       \
                                                                               \
    /* Load 4 bytes from the input */                                          \
    KMOVQ       K3, K5                                                         \
    VPXORD      Z5, Z5, Z5                                                     \
    VPGATHERDD  0(SI)(Z23*1), K5, Z5                                           \
                                                                               \
    /* Check if all bytes are ASCII (the fastest path) */                      \
    VPMOVB2M    Z5, K5                                                         \
    KTESTQ      K5, K5                                                         \
    JZ          utf8_all_ascii                                                 \
                                                                               \
    /* Handle ASCII bytes */                                                   \
    VPTESTNMD   CONSTD_0x00000080, Z5, K3, K5                                  \
    VPADDD      QWORD_LO_VEC, Z5, Z6                                           \
    VPADDD      QWORD_HI_VEC, Z5, Z7                                           \
    VPANDND     Z7, Z6, Z6                                                     \
    VPTESTMD    CONSTD_0x00000080, Z6, K5, K6                                  \
                                                                               \
    VMOVDQA32   Z5, Z25                                                        \
    VPXORD      CONSTD_0x20202020, Z25, K6, Z25                                \
    VMOVDQA32.Z CONSTD_0x00000001, K5, Z26                                     \
    VMOVDQA32.Z CONSTD_0x00000001, K5, Z27                                     \
                                                                               \
    KANDNW      K3, K5, K6                                                     \
    KTESTW      K6, K6                                                         \
    JZ          utf8_skip                                                      \
                                                                               \
    /* Handle non-ASCII characters */                                          \
    VMOVDQA32   Z25, Z4                                                        \
    BC_UTF8_TO_UTF32                                                           \
    BC_UTF8_LENGTH_AUX(Z5, Z6)                                                 \
    VPADDD      Z6, Z26, K6, Z26 /* update input length */                     \
    BC_UTF32_CHANGE_CASE(displacement, delta)                                  \
                                                                               \
    VMOVDQA32   Z4, Z5                                                         \
    BC_UTF8_LENGTH(Z5, Z6)                                                     \
    VPADDD      Z6, Z27, K6, Z27 /* update output length */                    \
                                                                               \
    /* merge with ASCII chars */                                               \
    VMOVDQA32   Z4, K6, Z25                                                    \
                                                                               \
utf8_skip:                                                                     \
    KMOVW       K3, K2                                                         \
    VPSCATTERDD Z25, K2, 0(SI)(Z29*1)                                          \
    VPADDD      Z26, Z23, Z23                                                  \
    VPADDD      Z27, Z29, Z29                                                  \
    JMP         utf8_loop                                                      \
                                                                               \
utf8_all_ascii:                                                                \
    VPADDD      QWORD_LO_VEC, Z5, Z6                                           \
    VPADDD      QWORD_HI_VEC, Z5, Z7                                           \
    VPANDND     Z7, Z6, Z6                                                     \
    VPSRLD      $2, Z6, Z6                                                     \
    VPANDD      CONSTD_0x20202020, Z6, Z6                                      \
    VPXORD      Z5, Z6, Z5                                                     \
    KMOVQ       K3, K5                                                         \
    VPSCATTERDD Z5, K5, 0(SI)(Z29*1)                                           \
    VPSUBD      Z23, Z24, Z8 /* remaining bytes */                             \
    VPMINUD.Z   CONSTD_0x00000004, Z8, K3, Z8                                  \
    VPADDD      Z8, Z23, Z23                                                   \
    VPADDD      Z8, Z29, Z29                                                   \
                                                                               \
    JMP     utf8_loop                                                          \
utf8_end:                                                                      \
    /* At the end adjust output lengths, as there are cases when upper */      \
    /* or lower UTF-8 characters contain different number of bytes than */     \
    /* the transformed character. */                                           \
    VPADDD      Z2, Z3, Z28      /* Z28 = initial Z29 value */                 \
    VPSUBD      Z28, Z29, Z27    /* Z27 = bytes written */                     \
    VPADDD      Z27, Z3, K1, Z3                                                \
                                                                               \
next:                                                                          \
    BC_UNPACK_2xSLOT(0, OUT(DX), OUT(R8))                                      \
    BC_STORE_SLICE_TO_SLOT(IN(Z2), IN(Z3), IN(DX))                             \
    BC_STORE_K_TO_SLOT(IN(K1), IN(R8))                                         \
                                                                               \
    NEXT_ADVANCE(BC_SLOT_SIZE*4)


// BC_UTF8_TO_UTF32 converts **non-ASCII** UTF-8 characters into
// UTF-32 numbers.
//
// Inputs:
// - Z4
// Outpus
// - Z4 - UTF32
// - Z5 - 4 MSBs of UTF8 leading bytes, shifted right
// Clobbers:
// - Z6
// - Z7
#define BC_UTF8_TO_UTF32                                                            \
    /* Z5: four MSB of UTF-8 leading byte */                                        \
    VPANDD  CONSTD_0x000000ff, Z4, Z5                                               \
    VPSRLD  $4, Z5, Z5                                                              \
                                                                                    \
    /* Possible inputs */                                                           \
    /*  byte 2                      byte 0 -- leading byte */                       \
    /* [10dddddd|10cccccc|10bbbbbb|11110aaa] - 4-byte char */                       \
    /* [????????|10cccccc|10bbbbbb|1110aaaa] - 3-byte char */                       \
    /* [????????|???????c|10bbbbbb|110aaaaa] - 2-byte char */                       \
                                                                                    \
    /* 1. Mask out two MSBs from each byte. */                                      \
    /* It will left some garbage bits in bytes #0, */                               \
    /* but it's not a problem. */                                                   \
    /* */                                                                           \
    /*  byte 2                      byte 0 */                                       \
    /* [00dddddd|00cccccc|00bbbbbb|00110aaa] - 4-byte char */                       \
    /* [00??????|00cccccc|00bbbbbb|0010aaaa] - 3-byte char */                       \
    /* [00??????|00?????c|00bbbbbb|000aaaaa] - 2-byte char */                       \
    VPANDD CONSTD_0x3f3f3f3f, Z4, Z4                                                \
                                                                                    \
    /* 2. Merge and reverse fields d & c and b & a */                               \
    /*  byte 2                      byte 0 */                                       \
    /* [0000cccc|ccdddddd|0000110a|aabbbbbb] - 4-byte char */                       \
    /* [0000cccc|cc??????|000010aa|aabbbbbb] - 3-byte char */                       \
    /* [0000????|?c??????|00000aaa|aabbbbbb] - 2-byte char */                       \
    VPMADDUBSW utf8_to_utf32_merge_step1, Z4, Z4                                    \
                                                                                    \
    /* 3. Merge and reverse fields c-d & a-b */                                     \
    /*  byte 2                      byte 0 */                                       \
    /* [00000000|110aaabb|bbbbcccc|ccdddddd] - 4-byte char */                       \
    /* [00000000|10aaaabb|bbbbcccc|cc??????] - 3-byte char */                       \
    /* [00000000|0aaaaabb|bbbb????|?c??????] - 2-byte char */                       \
    VPMADDWD utf8_to_utf32_merge_step2, Z4, Z4                                      \
                                                                                    \
    /* 4. Reset leading bytes leftovers as well as garbage bits. */                 \
    /*    Based on the char type, shift left and then right */                      \
    /*    to clear out the unnedeed bits and, as a result, produce */               \
    /*    a proper character number. */                                             \
    VPSHUFB Z5, utf8_to_utf32_shl_lookup, Z6                                        \
    VPANDD  CONSTD_0x000000ff, Z6, Z6                                               \
                                                                                    \
    /* [aaabbbbb|bccccccd|ddddd000|00000000] - 4-byte char (shift by 11 bits) */    \
    /* [aaaabbbb|bbcccccc|??????00|00000000] - 3-byte char (shift by 10 bits) */    \
    /* [aaaaabbb|bbb?????|c??????0|00000000] - 2-byte char (shift by 9 bits) */     \
    VPSLLVD Z6, Z4, Z4                                                              \
                                                                                    \
    /* [00000000|000aaabb|bbbbcccc|ccdddddd] - 4-byte char (shift by 11 bits) */    \
    /* [00000000|00000000|aaaabbbb|bbcccccc] - 3-byte char (shift by 16 bits) */    \
    /* [00000000|00000000|00000aaa|aabbbbbb] - 2-byte char (shift by 21 bits) */    \
    VPSHUFB Z5, utf8_to_utf32_shr_lookup, Z7                                        \
    VPANDD  CONSTD_0x000000ff, Z7, Z7                                               \
    VPSRLVD Z7, Z4, Z4


/*  0000:  0  // ASCII char
    0001:  0  // ...
    0010:  0  // ...
    0011:  0  // ...
    0100:  0  // ...
    0101:  0  // ...
    0110:  0  // ...
    0111:  0  // ...
    1000:  0  // continuation byte
    1001:  0  // ...
    1010:  0  // ...
    1011:  0  // ...
    1100:  9  // 2-byte char
    1101:  9  // 2-byte char
    1110: 10  // 3-byte char
    1111: 11  // 4-byte char */
BC_STR_DEF_VPSHUFB_LANE_CONST(utf8_to_utf32_shift_left, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 9, 10, 11)

/*  0000:  0  // ASCII char
    0001:  0  // ...
    0010:  0  // ...
    0011:  0  // ...
    0100:  0  // ...
    0101:  0  // ...
    0110:  0  // ...
    0111:  0  // ...
    1000:  0  // continuation byte
    1001:  0  // ...
    1010:  0  // ...
    1011:  0  // ...
    1100: 21  // 2-byte char
    1101: 21  // 2-byte char
    1110: 16  // 3-byte char
    1111: 11  // 4-byte char */
BC_STR_DEF_VPSHUFB_LANE_CONST(utf8_to_utf32_shift_right, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 21, 16, 11)

// BC_UTF32_CHANGE_CASE changes case of UTF32 characters using
// two auxiliary tables. The alogrithm is shown in `_generate/strcase.go`;
// the only difference is that in assembly version we keep precalculated
// UTF-8 strings instead of charcode differences.
#define BC_UTF32_CHANGE_CASE(displacement, delta)                                              \
    VPSRLD  $8, Z4, Z5                                                                         \
    VPCMPD.BCST  $VPCMP_IMM_LE, CONST_GET_PTR(utf32_change_case_lookup_mask, 0), Z5, K3, K4    \
    KTESTW K4, K4                                                                              \
    JZ nochange                                                                                \
                                                                                               \
    LEAQ        CONST_GET_PTR(displacement, 0), BX                                             \
    KMOVQ       K4, K2                                                                         \
    VPXORQ      Z6, Z6, Z6                                                                     \
    VPGATHERDD  (BX)(Z5*4), K2, Z6                                                             \
                                                                                               \
    VPANDD      CONSTD_0x000000ff, Z6, Z5  /* Z5 = lo */                                       \
    VPSRLD      $8, Z6, Z7                                                                     \
    VPANDD      CONSTD_0x000000ff, Z7, Z7  /* Z7 = hi */                                       \
    VPSRLD      $16, Z6, Z6 /* Z6 = offset */                                                  \
                                                                                               \
    VPANDD      CONSTD_0x000000ff, Z4, Z8 /* Z8 = col */                                       \
                                                                                               \
    VPCMPD      $VPCMP_IMM_GE, Z5, Z8, K4, K2   /* K2 = col >= lo */                           \
    VPCMPD      $VPCMP_IMM_LE, Z7, Z8, K2, K2   /* K2 &= col <= hi */                          \
                                                                                               \
    VPSUBD      Z5, Z8, Z8 /* Z8 = col - lo */                                                 \
    VPADDD      Z6, Z8, Z8 /* Z8 = offset + col - lo */                                        \
                                                                                               \
    LEAQ        CONST_GET_PTR(delta, 0), BX                                                    \
    VMOVDQU32   Z25, Z4 /* Z25 contains the input -- update UTF-8 codes directly */            \
    VPGATHERDD  (BX)(Z8*4), K2, Z4                                                             \
nochange:

BC_STR_DEF_CONSTD(utf32_change_case_lookup_mask, $0x1ff)

// BC_UTF8_LENGTH_AUX calculates the number of bytes
// of **valid** UTF8 characters, based on the four
// bits from leading bytes stored in `in`.
//
// Clobbers utf8
#define BC_UTF8_LENGTH(utf8, out)                         \
    VPANDD  CONSTD_0x000000ff, utf8, utf8                 \
    VPSRLD  $4, utf8, utf8                                \
    BC_UTF8_LENGTH_AUX(utf8, out)

// BC_UTF8_LENGTH_AUX calculates the number of bytes
// of **valid** UTF8 characters, based on the four
// bits from leading bytes stored in `in`.
//
// See BC_UTF8_TO_UTF32 to see how Z5 is constructed.
#define BC_UTF8_LENGTH_AUX(in, out)        \
    VPSHUFB in, utf8_length_lookup, out    \
    VPANDD  CONSTD_0x000000ff, out, out

/*  0000:  1  // ASCII char
    0001:  1  // ...
    0010:  1  // ...
    0011:  1  // ...
    0100:  1  // ...
    0101:  1  // ...
    0110:  1  // ...
    0111:  1  // ...
    1000:  0  // continuation byte
    1001:  0  // ...
    1010:  0  // ...
    1011:  0  // ...
    1100:  2  // 2-byte char
    1101:  2  // 2-byte char
    1110:  3  // 3-byte char
    1111:  4  // 4-byte char */
BC_STR_DEF_VPSHUFB_LANE_CONST(utf8_length, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 2, 2, 3, 4)


TEXT bcslower(SB), NOSPLIT|NOFRAME, $0
    // 0x3f = 128 - ord('A')
    // 0x25 = 128 - ord('Z') - 1
    BC_STR_CHANGE_CASE($0x2525252525252525, $0x3f3f3f3f3f3f3f3f, str_tolower_lookup, str_tolower_data)

    _BC_ERROR_HANDLER_MORE_SCRATCH()

TEXT bcsupper(SB), NOSPLIT|NOFRAME, $0
    // 0x1f = 128 - ord('a')
    // 0x05 = 128 - ord('z') - 1
    BC_STR_CHANGE_CASE($0x0505050505050505, $0x1f1f1f1f1f1f1f1f, str_toupper_lookup, str_toupper_data)

    _BC_ERROR_HANDLER_MORE_SCRATCH()


#undef TERN_MERGE
#undef QWORD_LO_VEC
#undef QWORD_HI_VEC
#undef CONSTD_0x00000001
#undef CONSTD_0x00000004
#undef CONSTD_0x00000080
#undef CONSTD_0x000000ff
#undef CONSTD_0x20202020
#undef CONSTD_0x3f3f3f3f
#undef utf8_to_utf32_merge_step1
#undef utf8_to_utf32_merge_step2
#undef utf8_to_utf32_shl_lookup
#undef utf8_to_utf32_shr_lookup
#undef utf8_length_lookup
#undef BC_STR_SET_CONSTD
#undef BC_STR_SET_VPSHUFB
#undef BC_STR_DEF_CONSTD
#undef BC_STR_DEF_VPSHUFB_LANE_CONST
#undef BC_STR_CHANGE_CASE
#undef BC_UTF8_TO_UTF32
#undef BC_UTF32_CHANGE_CASE
#undef BC_UTF8_LENGTH
#undef BC_UTF8_LENGTH_AUX
