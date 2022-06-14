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

#define BC_CONSTANT_H_DEFINED

// Constants Machinery
// -------------------

#ifndef BC_CONSTANTS_CONSUME_ONLY
  #define CONST_DATA_U32(name, offset, value) DATA name<>+(offset)(SB)/4, value
  #define CONST_DATA_U64(name, offset, value) DATA name<>+(offset)(SB)/8, value
  #define CONST_GLOBAL(name, size) GLOBL name<>(SB), RODATA|NOPTR, size
#else
  #define CONST_DATA_U32(name, offset, value)
  #define CONST_DATA_U64(name, offset, value)
  #define CONST_GLOBAL(name, size)
#endif

#define CONST_GET_PTR(name, offset) name<>+(offset)(SB)


// Common 32-Bit and 64-Bit Constants
// ----------------------------------

#define CONSTD_1() CONST_GET_PTR(constpool, 0)
#define CONSTQ_1() CONST_GET_PTR(constpool, 0)
CONST_DATA_U64(constpool, 0, $1)

#define CONSTD_2() CONST_GET_PTR(constpool, 8)
#define CONSTQ_2() CONST_GET_PTR(constpool, 8)
CONST_DATA_U64(constpool, 8, $2)

#define CONSTD_3() CONST_GET_PTR(constpool, 16)
#define CONSTQ_3() CONST_GET_PTR(constpool, 16)
CONST_DATA_U64(constpool, 16, $3)

#define CONSTD_4() CONST_GET_PTR(constpool, 24)
#define CONSTQ_4() CONST_GET_PTR(constpool, 24)
CONST_DATA_U64(constpool, 24, $4)

#define CONSTD_5() CONST_GET_PTR(constpool, 32)
#define CONSTQ_5() CONST_GET_PTR(constpool, 32)
CONST_DATA_U64(constpool, 32, $5)

#define CONSTD_6() CONST_GET_PTR(constpool, 40)
#define CONSTQ_6() CONST_GET_PTR(constpool, 40)
CONST_DATA_U64(constpool, 40, $6)

#define CONSTD_7() CONST_GET_PTR(constpool, 48)
#define CONSTQ_7() CONST_GET_PTR(constpool, 48)
CONST_DATA_U64(constpool, 48, $7)

#define CONSTD_8() CONST_GET_PTR(constpool, 56)
#define CONSTQ_8() CONST_GET_PTR(constpool, 56)
CONST_DATA_U64(constpool, 56, $8)

#define CONSTD_9() CONST_GET_PTR(constpool, 64)
#define CONSTQ_9() CONST_GET_PTR(constpool, 64)
CONST_DATA_U64(constpool, 64, $9)

#define CONSTD_10() CONST_GET_PTR(constpool, 72)
#define CONSTQ_10() CONST_GET_PTR(constpool, 72)
#define CONSTD_0x0A() CONST_GET_PTR(constpool, 72)
#define CONSTQ_0x0A() CONST_GET_PTR(constpool, 72)
CONST_DATA_U64(constpool, 72, $10)

#define CONSTD_11() CONST_GET_PTR(constpool, 80)
#define CONSTQ_11() CONST_GET_PTR(constpool, 80)
#define CONSTD_0x0B() CONST_GET_PTR(constpool, 80)
#define CONSTQ_0x0B() CONST_GET_PTR(constpool, 80)
CONST_DATA_U64(constpool, 80, $11)

#define CONSTD_12() CONST_GET_PTR(constpool, 88)
#define CONSTQ_12() CONST_GET_PTR(constpool, 88)
#define CONSTD_0x0C() CONST_GET_PTR(constpool, 88)
#define CONSTQ_0x0C() CONST_GET_PTR(constpool, 88)
CONST_DATA_U64(constpool, 88, $12)

#define CONSTD_13() CONST_GET_PTR(constpool, 96)
#define CONSTQ_13() CONST_GET_PTR(constpool, 96)
#define CONSTD_0x0D() CONST_GET_PTR(constpool, 96)
#define CONSTQ_0x0D() CONST_GET_PTR(constpool, 96)
CONST_DATA_U64(constpool, 96, $13)

#define CONSTD_14() CONST_GET_PTR(constpool, 104)
#define CONSTQ_14() CONST_GET_PTR(constpool, 104)
#define CONSTD_0x0E() CONST_GET_PTR(constpool, 104)
#define CONSTQ_0x0E() CONST_GET_PTR(constpool, 104)
CONST_DATA_U64(constpool, 104, $14)

#define CONSTD_15() CONST_GET_PTR(constpool, 112)
#define CONSTQ_15() CONST_GET_PTR(constpool, 112)
#define CONSTD_0x0F() CONST_GET_PTR(constpool, 112)
#define CONSTQ_0x0F() CONST_GET_PTR(constpool, 112)
CONST_DATA_U64(constpool, 112, $15)

#define CONSTD_16() CONST_GET_PTR(constpool, 120)
#define CONSTQ_16() CONST_GET_PTR(constpool, 120)
#define CONSTD_0x10() CONST_GET_PTR(constpool, 120)
#define CONSTQ_0x10() CONST_GET_PTR(constpool, 120)
#define CONSTD_FALSE_BYTE() CONST_GET_PTR(constpool, 120)
CONST_DATA_U64(constpool, 120, $16)

#define CONSTD_17() CONST_GET_PTR(constpool, 128)
#define CONSTQ_17() CONST_GET_PTR(constpool, 128)
#define CONSTD_0x11() CONST_GET_PTR(constpool, 128)
#define CONSTQ_0x11() CONST_GET_PTR(constpool, 128)
#define CONSTD_TRUE_BYTE() CONST_GET_PTR(constpool, 128)
CONST_DATA_U64(constpool, 128, $17)

#define CONSTD_24() CONST_GET_PTR(constpool, 136)
#define CONSTQ_24() CONST_GET_PTR(constpool, 136)
CONST_DATA_U64(constpool, 136, $24)

#define CONSTD_31() CONST_GET_PTR(constpool, 144)
#define CONSTQ_31() CONST_GET_PTR(constpool, 144)
#define CONSTD_0x1F() CONST_GET_PTR(constpool, 144)
#define CONSTQ_0x1F() CONST_GET_PTR(constpool, 144)
CONST_DATA_U64(constpool, 144, $31)

#define CONSTD_32() CONST_GET_PTR(constpool, 152)
#define CONSTQ_32() CONST_GET_PTR(constpool, 152)
#define CONSTD_0x20() CONST_GET_PTR(constpool, 152)
#define CONSTQ_0x20() CONST_GET_PTR(constpool, 152)
CONST_DATA_U64(constpool, 152, $32)

#define CONSTD_48() CONST_GET_PTR(constpool, 160)
#define CONSTQ_48() CONST_GET_PTR(constpool, 160)
CONST_DATA_U64(constpool, 160, $48)

#define CONSTD_60() CONST_GET_PTR(constpool, 168)
#define CONSTQ_60() CONST_GET_PTR(constpool, 168)
CONST_DATA_U64(constpool, 168, $60)

#define CONSTD_63() CONST_GET_PTR(constpool, 176)
#define CONSTQ_63() CONST_GET_PTR(constpool, 176)
#define CONSTD_0x3F() CONST_GET_PTR(constpool, 176)
#define CONSTQ_0x3F() CONST_GET_PTR(constpool, 176)
CONST_DATA_U64(constpool, 176, $63)

#define CONSTD_64() CONST_GET_PTR(constpool, 184)
#define CONSTQ_64() CONST_GET_PTR(constpool, 184)
CONST_DATA_U64(constpool, 184, $64)

#define CONSTD_100() CONST_GET_PTR(constpool, 192)
#define CONSTQ_100() CONST_GET_PTR(constpool, 192)
CONST_DATA_U64(constpool, 192, $100)

#define CONSTD_127() CONST_GET_PTR(constpool, 200)
#define CONSTQ_127() CONST_GET_PTR(constpool, 200)
#define CONSTD_0x7F() CONST_GET_PTR(constpool, 200)
#define CONSTQ_0x7F() CONST_GET_PTR(constpool, 200)
CONST_DATA_U64(constpool, 200, $127)

#define CONSTD_128() CONST_GET_PTR(constpool, 208)
#define CONSTQ_128() CONST_GET_PTR(constpool, 208)
#define CONSTD_0x80() CONST_GET_PTR(constpool, 208)
#define CONSTQ_0x80() CONST_GET_PTR(constpool, 208)
CONST_DATA_U64(constpool, 208, $128)

#define CONSTD_255() CONST_GET_PTR(constpool, 216)
#define CONSTQ_255() CONST_GET_PTR(constpool, 216)
#define CONSTD_0xFF() CONST_GET_PTR(constpool, 216)
#define CONSTQ_0xFF() CONST_GET_PTR(constpool, 216)
CONST_DATA_U64(constpool, 216, $255)

#define CONSTD_256() CONST_GET_PTR(constpool, 224)
#define CONSTQ_256() CONST_GET_PTR(constpool, 224)
CONST_DATA_U64(constpool, 224, $256)

#define CONSTD_365() CONST_GET_PTR(constpool, 236)
#define CONSTQ_365() CONST_GET_PTR(constpool, 236)
CONST_DATA_U64(constpool, 236, $365)

#define CONSTD_400() CONST_GET_PTR(constpool, 244)
#define CONSTQ_400() CONST_GET_PTR(constpool, 244)
CONST_DATA_U64(constpool, 244, $400)

#define CONSTD_1461() CONST_GET_PTR(constpool, 256)
#define CONSTQ_1461() CONST_GET_PTR(constpool, 256)
CONST_DATA_U64(constpool, 256, $1461)

#define CONSTD_3600() CONST_GET_PTR(constpool, 264)
#define CONSTQ_3600() CONST_GET_PTR(constpool, 264)
CONST_DATA_U64(constpool, 264, $3600)

#define CONSTD_8191() CONST_GET_PTR(constpool, 272)
#define CONSTQ_8191() CONST_GET_PTR(constpool, 272)
#define CONSTD_0x1FFF() CONST_GET_PTR(constpool, 272)
#define CONSTQ_0x1FFF() CONST_GET_PTR(constpool, 272)
CONST_DATA_U64(constpool, 272, $0x1FFF)

#define CONSTD_16256() CONST_GET_PTR(constpool, 280)
#define CONSTQ_16256() CONST_GET_PTR(constpool, 280)
#define CONSTD_0x3F80() CONST_GET_PTR(constpool, 280)
#define CONSTQ_0x3F80() CONST_GET_PTR(constpool, 280)
CONST_DATA_U64(constpool, 280, $0x3F80)

#define CONSTD_36524() CONST_GET_PTR(constpool, 288)
#define CONSTQ_36524() CONST_GET_PTR(constpool, 288)
CONST_DATA_U64(constpool, 288, $36524)

#define CONSTD_65535() CONST_GET_PTR(constpool, 296)
#define CONSTQ_65535() CONST_GET_PTR(constpool, 296)
#define CONSTD_0xFFFF() CONST_GET_PTR(constpool, 296)
#define CONSTQ_0xFFFF() CONST_GET_PTR(constpool, 296)
CONST_DATA_U64(constpool, 296, $0xFFFF)

#define CONSTD_86400() CONST_GET_PTR(constpool, 304)
#define CONSTQ_86400() CONST_GET_PTR(constpool, 304)
CONST_DATA_U64(constpool, 304, $86400)

#define CONSTQ_146097() CONST_GET_PTR(constpool, 312)
CONST_DATA_U64(constpool, 312, $146097)

#define CONSTD_1000000() CONST_GET_PTR(constpool, 320)
#define CONSTQ_1000000() CONST_GET_PTR(constpool, 320)
CONST_DATA_U64(constpool, 320, $1000000)

#define CONSTD_16777215() CONST_GET_PTR(constpool, 328)
#define CONSTQ_16777215() CONST_GET_PTR(constpool, 328)
#define CONSTD_0xFFFFFF() CONST_GET_PTR(constpool, 328)
#define CONSTQ_0xFFFFFF() CONST_GET_PTR(constpool, 328)
CONST_DATA_U64(constpool, 328, $0xFFFFFF)

#define CONSTD_60000000() CONST_GET_PTR(constpool, 336)
#define CONSTQ_60000000() CONST_GET_PTR(constpool, 336)
CONST_DATA_U64(constpool, 336, $60000000)

#define CONSTD_1000000000() CONST_GET_PTR(constpool, 344)
#define CONSTQ_1000000000() CONST_GET_PTR(constpool, 344)
CONST_DATA_U64(constpool, 344, $1000000000)

#define CONSTD_3600000000() CONST_GET_PTR(constpool, 352)
#define CONSTQ_3600000000() CONST_GET_PTR(constpool, 352)
CONST_DATA_U64(constpool, 352, $3600000000)

#define CONSTQ_86400000000() CONST_GET_PTR(constpool, 360)
CONST_DATA_U64(constpool, 360, $86400000000)

#define CONSTD_0x007F007F() CONST_GET_PTR(constpool, 368)
#define CONSTQ_0x007F007F007F007F() CONST_GET_PTR(constpool, 368)
CONST_DATA_U64(constpool, 368, $0x007F007F007F007F)

#define CONSTD_0x01000000() CONST_GET_PTR(constpool, 376)
#define CONSTQ_0x0000000101000000() CONST_GET_PTR(constpool, 376)
CONST_DATA_U64(constpool, 376, $0x0000000101000000)

#define CONSTD_0x01010101() CONST_GET_PTR(constpool, 384)
#define CONSTQ_0x0101010101010101() CONST_GET_PTR(constpool, 384)
CONST_DATA_U64(constpool, 384, $0x0101010101010101)

#define CONSTD_0x0F0F0F0F() CONST_GET_PTR(constpool, 392)
#define CONSTQ_0x0F0F0F0F0F0F0F0F() CONST_GET_PTR(constpool, 392)
CONST_DATA_U64(constpool, 392, $0x0F0F0F0F0F0F0F0F)

#define CONSTQ_0x7FFFFFFFFFFFFFFF() CONST_GET_PTR(constpool, 400)
#define CONSTF64_ABS_BITS() CONST_GET_PTR(constpool, 400)
CONST_DATA_U64(constpool, 400, $0x7FFFFFFFFFFFFFFF)

#define CONSTD_0x7F7F7F7F() CONST_GET_PTR(constpool, 408)
#define CONSTQ_0x7F7F7F7F7F7F7F7F() CONST_GET_PTR(constpool, 408)
CONST_DATA_U64(constpool, 408, $0x7F7F7F7F7F7F7F7F)

#define CONSTQ_0x8000000000000000() CONST_GET_PTR(constpool, 416)
#define CONSTF64_SIGN_BIT() CONST_GET_PTR(constpool, 416)
CONST_DATA_U64(constpool, 416, $0x8000000000000000)

#define CONSTD_0x80800080() CONST_GET_PTR(constpool, 424)
#define CONSTQ_0x8080808080800080() CONST_GET_PTR(constpool, 424)
CONST_DATA_U64(constpool, 424, $0x8080808080800080)

#define CONSTD_0xFFFF0000() CONST_GET_PTR(constpool, 432)
#define CONSTQ_0xFFFF0000FFFF0000() CONST_GET_PTR(constpool, 432)
CONST_DATA_U64(constpool, 432, $0xFFFF0000FFFF0000)

#define CONSTD_NEG_1() CONST_GET_PTR(constpool, 440)
#define CONSTQ_NEG_1() CONST_GET_PTR(constpool, 440)
#define CONSTD_0xFFFFFFFF() CONST_GET_PTR(constpool, 440)
#define CONSTQ_0xFFFFFFFFFFFFFFFF() CONST_GET_PTR(constpool, 440)
CONST_DATA_U64(constpool, 440, $0xFFFFFFFFFFFFFFFF)

#define CONSTQ_0x00008060() CONST_GET_PTR(constpool, 448)
#define CONSTQ_0x0000000000008060() CONST_GET_PTR(constpool, 448)
CONST_DATA_U64(constpool, 448, $0x0000000000008060)

#define CONSTD_0x00808080() CONST_GET_PTR(constpool, 456)
#define CONSTQ_0x0000000000808080() CONST_GET_PTR(constpool, 456)
CONST_DATA_U64(constpool, 456, $0x0000000000808080)

#define CONSTD_0xC6808080() CONST_GET_PTR(constpool, 464)
#define CONSTQ_0x00000000C6808080() CONST_GET_PTR(constpool, 464)
CONST_DATA_U64(constpool, 464, $0x00000000C6808080)

// Unix microseconds from 1st January 1970 to 1st March 0000:
//   ((146097 * 5 - 11017) * 86400000000) == 62162035200000000

// ((146097 * 5 - 11017) * 86400000000)
#define CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET() CONST_GET_PTR(constpool, 472)
CONST_DATA_U64(constpool, 472, $62162035200000000)

// ((146097 * 5 - 11017) * 86400000000) >> 13
#define CONSTQ_1970_01_01_TO_0000_03_01_US_OFFSET_SHR_13() CONST_GET_PTR(constpool, 480)
CONST_DATA_U64(constpool, 480, $7588139062500)

#define CONSTQ_0x0001013C() CONST_GET_PTR(constpool, 488)
CONST_DATA_U64(constpool, 488, $0x0001013C)

#define CONSTQ_0x0001003C() CONST_GET_PTR(constpool, 496)
CONST_DATA_U64(constpool, 496, $0x0001003C)

#define CONSTQ_0x0000007F7F7F7F7F() CONST_GET_PTR(constpool, 504)
CONST_DATA_U64(constpool, 504, $0x0000007F7F7F7F7F)

#define CONSTD_306() CONST_GET_PTR(constpool, 512)
#define CONSTQ_306() CONST_GET_PTR(constpool, 512)
CONST_DATA_U64(constpool, 512, $306)

#define CONSTD_1000() CONST_GET_PTR(constpool, 520)
#define CONSTQ_1000() CONST_GET_PTR(constpool, 520)
CONST_DATA_U64(constpool, 520, $1000)

#define CONSTD_15625() CONST_GET_PTR(constpool, 528)
#define CONSTQ_15625() CONST_GET_PTR(constpool, 528)
CONST_DATA_U64(constpool, 528, $15625)

#define CONSTD_999999() CONST_GET_PTR(constpool, 536)
#define CONSTQ_999999() CONST_GET_PTR(constpool, 536)
CONST_DATA_U64(constpool, 536, $999999)

#define CONSTQ_0x1000000000000000() CONST_GET_PTR(constpool, 544)
CONST_DATA_U64(constpool, 544, $0x1000000000000000)

// 2.5579538487363607E-12 <- 90 / 35184372088832
#define CONSTQ_0x3D86800000000000() CONST_GET_PTR(constpool, 552)
CONST_DATA_U64(constpool, 552, $0x3D86800000000000)

// 5.1159076974727213E-12 <- 180 / 35184372088832
#define CONSTQ_0x3D96800000000000() CONST_GET_PTR(constpool, 560)
CONST_DATA_U64(constpool, 560, $0x3D96800000000000)

#define CONSTQ_35184372088832() CONST_GET_PTR(constpool, 568)
CONST_DATA_U64(constpool, 568, $35184372088832)

#define CONSTQ_46() CONST_GET_PTR(constpool, 576)
CONST_DATA_U64(constpool, 576, $46)

#define CONSTD_0x2E() CONST_GET_PTR(constpool, 584)
CONST_DATA_U32(constpool, 584, $0x2E)

#define CONSTD_5243() CONST_GET_PTR(constpool, 588)
CONST_DATA_U32(constpool, 588, $5243)

#define CONSTD_6554() CONST_GET_PTR(constpool, 592)
CONST_DATA_U32(constpool, 592, $6554)

#define CONSTQ_10000() CONST_GET_PTR(constpool, 596)
CONST_DATA_U64(constpool, 596, $10000)

#define CONSTQ_20() CONST_GET_PTR(constpool, 608)
#define CONSTD_20() CONST_GET_PTR(constpool, 608)
CONST_DATA_U64(constpool, 608, $20)

#define CONSTQ_29() CONST_GET_PTR(constpool, 616)
#define CONSTD_29() CONST_GET_PTR(constpool, 616)
CONST_DATA_U64(constpool, 616, $29)

#define CONSTD_0b11001110_01110011_10011100_11100111() CONST_GET_PTR(constpool, 624)
CONST_DATA_U32(constpool, 624, $0b11001110_01110011_10011100_11100111)

#define CONSTQ_0b00000000_00000000_00000000_00000000_00011111_00000000_00000000_00011111() CONST_GET_PTR(constpool, 632)
CONST_DATA_U64(constpool, 632, $0b0000000000000000000000000000000000011111000000000000000000011111)

#define CONSTQ_0x0000FFFFFFFFFFFF() CONST_GET_PTR(constpool, 640)
CONST_DATA_U64(constpool, 640, $0x0000FFFFFFFFFFFF)

#define CONSTD_100000000() CONST_GET_PTR(constpool, 648)
#define CONSTQ_100000000() CONST_GET_PTR(constpool, 648)
CONST_DATA_U64(constpool, 648, $100000000)

#define CONSTD_134217727() CONST_GET_PTR(constpool, 656)
#define CONSTQ_134217727() CONST_GET_PTR(constpool, 656)
CONST_DATA_U64(constpool, 656, $134217727)

// Integer Division Reciprocals
// ----------------------------

#define CONSTPOOL_RECIPROCALS_INDEX 680

// Unsigned 32-bit division by 25: (Value * 1374389535) >> 35.
// Unsigned 32-bit division by 50: (Value * 1374389535) >> 36.
// Unsigned 32-bit division by 100: (Value * 1374389535) >> 37.
// Unsigned 32-bit division by 200: (Value * 1374389535) >> 38.
// Unsigned 32-bit division by 400: (Value * 1374389535) >> 39, and so on...
#define CONSTQ_1374389535() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 0)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 0, $1374389535)

// Unsigned 32-bit division by 153 => Result = Value * 3593175255 >> 39.
#define CONSTQ_3593175255() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 8)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 8, $3593175255)

// Unsigned 32-bit division by 365 => Result = Value * 45965 >> 24.
#define CONSTQ_45965() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 16)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 16, $45965)

// Unsigned 32-bit division by 1000 => Result = Value * 274877907 >> 38.
#define CONSTQ_274877907() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 24)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 24, $274877907)

// Unsigned 32-bit division by 1461 => Result = Value * 376287347 >> 39.
#define CONSTQ_376287347() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 32)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 32, $376287347)

// Unsigned 32-bit division by 10000 => Result = (Value * 3518437209) >> 45
#define CONSTQ_3518437209() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 40)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 40, $3518437209)

// Unsigned 32-bit division by 36524 => Result = (Value >> 2) * 963321983 >> 43.
#define CONSTQ_963321983() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 48)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 48, $963321983)

// Unsigned 32-bit division by 146097 => Result = Value * 963315389 >> 47.
#define CONSTQ_963315389() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 56)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 56, $963315389)

// Unsigned 37-bit division by 1000000 => Result = Value * 1125899907 >> 50.
#define CONSTQ_1125899907() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 64)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 64, $1125899907)

// Unsigned 37-bit division by 60000000 => Result = (Value >> 8) * 18764999 >> 42.
#define CONSTQ_18764999() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 72)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 72, $18764999)

// Unsigned 32-bit division by 100000000 => Result = (uint64(Value) * 1441151881) >> 57
#define CONSTQ_1441151881() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 80)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 80, $1441151881)

// Unsigned 44-bit division by 3600000000 => Result = (Value >> 12) * 2562048517 >> 51
#define CONSTQ_2562048517() CONST_GET_PTR(constpool, CONSTPOOL_RECIPROCALS_INDEX + 88)
CONST_DATA_U64(constpool, CONSTPOOL_RECIPROCALS_INDEX + 88, $2562048517)


// 64-Bit Floating Point Constants
// -------------------------------

#define CONSTPOOL_F64_INDEX (CONSTPOOL_RECIPROCALS_INDEX + 128)

#define CONSTF64_HALF() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 0)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 0, $0x3FE0000000000000)

#define CONSTF64_1() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 8)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 8, $0x3FF0000000000000)

#define CONSTF64_11() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 16)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 16, $0x4026000000000000)

#define CONSTF64_12() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 24)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 24, $0x4028000000000000)

#define CONSTF64_399() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 32)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX +  32, $0x4078F00000000000)

#define CONSTF64_400() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 40)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 40, $0x4079000000000000)

#define CONSTF64_NAN() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 48)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 48, $0x7FF8000000000000)

#define CONSTF64_POSITIVE_INF() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 56)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 56, $0x7FF0000000000000)

#define CONSTF64_NEGATIVE_INF() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 64)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 64, $0xFFF0000000000000)

// float64((60 * 60 * 24 * 1000000) >> 13)
#define CONSTF64_MICROSECONDS_IN_1_DAY_SHR_13() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 72)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 72, $0x41641DD760000000)

#define CONSTF64_100000000() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 80)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 80, $0x4197D78400000000)

#define CONSTF64_152587890625() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 88)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 88, $0x4241C37937E08000)

#define CONSTF64_65536() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 96)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 96, $0x40F0000000000000)

#define CONSTF64_180() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 104)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 104, $0x4066800000000000)

// (1 << 48) / 360.0
#define CONSTF64_281474976710656_DIV_360() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 112)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 112, $0x4266c16c16c16c17)

// (1 << 48) / 2.0
#define CONSTF64_140737488355328() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 120)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 120, $0x42e0000000000000)

// PI / 180
#define CONSTF64_PI_DIV_180() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 128)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 128, $0x3f91df46a2529d39)

// (1 << 48) / (3.1415926535897931 * 4);
#define CONSTF64_281474976710656_DIV_4PI() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 136)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 136, $0x42b45f306dc9c883)

#define CONSTF64_0p9999() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 144)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 144, $0x3fefff2e48e8a71e)

#define CONSTF64_MINUS_0p9999() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 152)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 152, $0xbfefff2e48e8a71e)

// Earth radius multiplied by 2 for geo distance calculation
#define CONSTF64_12742000() CONST_GET_PTR(constpool, CONSTPOOL_F64_INDEX + 160)
CONST_DATA_U64(constpool, CONSTPOOL_F64_INDEX + 160, $0x41684dae00000000)


// Other Constants
// ---------------
//
// Add new or uncategorized constants here...

#define CONSTPOOL_OTHER_INDEX (CONSTPOOL_F64_INDEX + 200)

#define CONSTD_0x0000C080() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 0)
#define CONSTD_UTF8_R2() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 0)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 0, $0x0000C080)

#define CONSTD_0x00E08080() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 4)
#define CONSTD_UTF8_R3() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 4)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 4, $0x00E08080)

#define CONSTD_1681() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 8) // minimum year (for timestamp conversion)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 8, $1681)

#define CONSTD_2262() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 12) // maximum year (for timestamp conversion)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 12, $2262)

#define CONSTD_0xC28F5C29() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 16)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 16, $0xC28F5C29) // (-1030792151)

#define CONSTD_10737418() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 20)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 20, $10737418) // (<= 10737418 --> %400 == 0)

#define CONSTD_42949672() CONST_GET_PTR(constpool, CONSTPOOL_OTHER_INDEX + 24)
CONST_DATA_U32(constpool, CONSTPOOL_OTHER_INDEX + 24, $42949672) // ( > 42949672 --> %100 != 0)

CONST_GLOBAL(constpool, $(CONSTPOOL_OTHER_INDEX + 32))


// UTF-8 constants.
#define CONSTD_UTF8_2B_MASK() CONST_GET_PTR(constants_utf8, 0)
CONST_DATA_U32(constants_utf8, 0, $0b10000000110000000000000000000000)
#define CONSTD_UTF8_3B_MASK() CONST_GET_PTR(constants_utf8, 4)
CONST_DATA_U32(constants_utf8, 4, $0b10000000100000001110000000000000)
#define CONSTD_UTF8_4B_MASK() CONST_GET_PTR(constants_utf8, 8)
CONST_DATA_U32(constants_utf8, 8, $0b10000000100000001000000011110000)
#define CONSTD_0b11000000() CONST_GET_PTR(constants_utf8, 12)
CONST_DATA_U32(constants_utf8, 12, $0b11000000)
#define CONSTD_0b11100000() CONST_GET_PTR(constants_utf8, 16)
CONST_DATA_U32(constants_utf8, 16, $0b11100000)
#define CONSTD_0b11110000() CONST_GET_PTR(constants_utf8, 20)
CONST_DATA_U32(constants_utf8, 20, $0b11110000)
#define CONSTD_0b11111000() CONST_GET_PTR(constants_utf8, 24)
CONST_DATA_U32(constants_utf8, 24, $0b11111000)
CONST_GLOBAL(constants_utf8, $28)

//swap byte 0 and 2, and leave byte 1 and 3 untouched
#define BSWAP_UTF8_3BYTE() CONST_GET_PTR(bswapUTF_3byte, 0)
CONST_DATA_U64(bswapUTF_3byte, 0, $0x0704050603000102)
CONST_DATA_U64(bswapUTF_3byte, 8, $0x0F0C0D0E0B08090A)
CONST_GLOBAL(bswapUTF_3byte, $16)

//swap byte 0 and 1, and leave byte 2 and 3 untouched
#define BSWAP_UTF8_2BYTE() CONST_GET_PTR(bswapUTF_2byte, 0)
CONST_DATA_U64(bswapUTF_2byte, 0, $0x0706040503020001)
CONST_DATA_U64(bswapUTF_2byte, 8, $0x0F0E0C0D0B0A0809)
CONST_GLOBAL(bswapUTF_2byte, $16)

CONST_DATA_U64(c_2lane_shift, 0, $2)
CONST_DATA_U64(c_2lane_shift, 8, $3)
CONST_DATA_U64(c_2lane_shift, 16, $4)
CONST_DATA_U64(c_2lane_shift, 24, $5)
CONST_DATA_U64(c_2lane_shift, 32, $6)
CONST_DATA_U64(c_2lane_shift, 40, $7)
CONST_DATA_U64(c_2lane_shift, 48, $8)
CONST_DATA_U64(c_2lane_shift, 56, $9)
CONST_GLOBAL(c_2lane_shift, $64)

// const for VPERMD to select the tail mask for the remaining bytes with the following code:
// VBROADCASTI64X2 CONST_TAIL_MASK(), Z9
// VPERMD        Z9, Z6, Z9   ; Z6 contains remaining number of bytes [0-3]; after: Z9 contains the tail mask
#define CONST_TAIL_MASK() CONST_GET_PTR(tail_mask_map, 0)
CONST_DATA_U32(tail_mask_map, 0, $0x00000000)
CONST_DATA_U32(tail_mask_map, 4, $0x000000FF)
CONST_DATA_U32(tail_mask_map, 8, $0x0000FFFF)
CONST_DATA_U32(tail_mask_map, 12, $0x00FFFFFF)
CONST_DATA_U32(tail_mask_map, 16, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 20, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 24, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 28, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 32, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 36, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 40, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 44, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 48, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 52, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 56, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_map, 60, $0xFFFFFFFF)
CONST_GLOBAL(tail_mask_map, $64)

#define CONST_N_BYTES_UTF8() CONST_GET_PTR(n_bytes_utf8, 0)
CONST_DATA_U32(n_bytes_utf8, 0, $1)
CONST_DATA_U32(n_bytes_utf8, 4, $1)
CONST_DATA_U32(n_bytes_utf8, 8, $1)
CONST_DATA_U32(n_bytes_utf8, 12, $1)
CONST_DATA_U32(n_bytes_utf8, 16, $1)
CONST_DATA_U32(n_bytes_utf8, 20, $1)
CONST_DATA_U32(n_bytes_utf8, 24, $1)
CONST_DATA_U32(n_bytes_utf8, 28, $1)
CONST_DATA_U32(n_bytes_utf8, 32, $1)
CONST_DATA_U32(n_bytes_utf8, 36, $1)
CONST_DATA_U32(n_bytes_utf8, 40, $1)
CONST_DATA_U32(n_bytes_utf8, 44, $1)
CONST_DATA_U32(n_bytes_utf8, 48, $2)
CONST_DATA_U32(n_bytes_utf8, 52, $2)
CONST_DATA_U32(n_bytes_utf8, 56, $3)
CONST_DATA_U32(n_bytes_utf8, 60, $4)
CONST_GLOBAL(n_bytes_utf8, $64)

// Byteswap predicates applicable to VPSHUFB
// -----------------------------------------

CONST_DATA_U32(bswap24_zero_last_byte, 0, $0xFF010203)
CONST_DATA_U32(bswap24_zero_last_byte, 4, $0xFF050607)
CONST_DATA_U32(bswap24_zero_last_byte, 8, $0xFF090A0B)
CONST_DATA_U32(bswap24_zero_last_byte, 12, $0xFF0D0E0F)
CONST_DATA_U32(bswap24_zero_last_byte, 16, $0xFF010203)
CONST_DATA_U32(bswap24_zero_last_byte, 20, $0xFF050607)
CONST_DATA_U32(bswap24_zero_last_byte, 24, $0xFF090A0B)
CONST_DATA_U32(bswap24_zero_last_byte, 28, $0xFF0D0E0F)
CONST_DATA_U32(bswap24_zero_last_byte, 32, $0xFF010203)
CONST_DATA_U32(bswap24_zero_last_byte, 36, $0xFF050607)
CONST_DATA_U32(bswap24_zero_last_byte, 40, $0xFF090A0B)
CONST_DATA_U32(bswap24_zero_last_byte, 44, $0xFF0D0E0F)
CONST_DATA_U32(bswap24_zero_last_byte, 48, $0xFF010203)
CONST_DATA_U32(bswap24_zero_last_byte, 52, $0xFF050607)
CONST_DATA_U32(bswap24_zero_last_byte, 56, $0xFF090A0B)
CONST_DATA_U32(bswap24_zero_last_byte, 60, $0xFF0D0E0F)
CONST_GLOBAL(bswap24_zero_last_byte, $64)

CONST_DATA_U32(bswap32, 0, $0x00010203)
CONST_DATA_U32(bswap32, 4, $0x04050607)
CONST_DATA_U32(bswap32, 8, $0x08090A0B)
CONST_DATA_U32(bswap32, 12, $0x0C0D0E0F)
CONST_DATA_U32(bswap32, 16, $0x00010203)
CONST_DATA_U32(bswap32, 20, $0x04050607)
CONST_DATA_U32(bswap32, 24, $0x08090A0B)
CONST_DATA_U32(bswap32, 28, $0x0C0D0E0F)
CONST_DATA_U32(bswap32, 32, $0x00010203)
CONST_DATA_U32(bswap32, 36, $0x04050607)
CONST_DATA_U32(bswap32, 40, $0x08090A0B)
CONST_DATA_U32(bswap32, 44, $0x0C0D0E0F)
CONST_DATA_U32(bswap32, 48, $0x00010203)
CONST_DATA_U32(bswap32, 52, $0x04050607)
CONST_DATA_U32(bswap32, 56, $0x08090A0B)
CONST_DATA_U32(bswap32, 60, $0x0C0D0E0F)
CONST_GLOBAL(bswap32, $64)

CONST_DATA_U64(bswap64, 0, $0x0001020304050607)
CONST_DATA_U64(bswap64, 8, $0x08090A0B0C0D0E0F)
CONST_DATA_U64(bswap64, 16, $0x0001020304050607)
CONST_DATA_U64(bswap64, 24, $0x08090A0B0C0D0E0F)
CONST_DATA_U64(bswap64, 32, $0x0001020304050607)
CONST_DATA_U64(bswap64, 40, $0x08090A0B0C0D0E0F)
CONST_DATA_U64(bswap64, 48, $0x0001020304050607)
CONST_DATA_U64(bswap64, 56, $0x08090A0B0C0D0E0F)
CONST_GLOBAL(bswap64, $64)

CONST_DATA_U64(popcnt_nibble,  0, $0x0302020102010100) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble,  8, $0x0403030203020201) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_DATA_U64(popcnt_nibble, 16, $0x0302020102010100) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble, 24, $0x0403030203020201) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_DATA_U64(popcnt_nibble, 32, $0x0302020102010100) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble, 40, $0x0403030203020201) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_DATA_U64(popcnt_nibble, 48, $0x0302020102010100) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble, 56, $0x0403030203020201) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_GLOBAL(popcnt_nibble, $64)

// LUT to convert 5 GEO_HASH bits into characters:
//   - original: 0123456789bcdefghjkmnpqrstuvwxyz
//   - sneller : 0145hjnp2367kmqr89destwxbcfguvyz
// NOTE: The result is the same, we just uses a different bit permutation as an input.

CONST_DATA_U64(geohash_chars_lut, 0, $0x706E6A6835343130)
CONST_DATA_U64(geohash_chars_lut, 8, $0x72716D6B37363332)
CONST_DATA_U64(geohash_chars_lut, 16, $0x7877747365643938)
CONST_DATA_U64(geohash_chars_lut, 24, $0x7A79767567666362)
CONST_GLOBAL(geohash_chars_lut, $32)

CONST_DATA_U64(geohash_chars_swap,  0, $0x0405060708090A0B)
CONST_DATA_U64(geohash_chars_swap,  8, $0xFFFFFFFF00010203)
CONST_GLOBAL(geohash_chars_swap, $16)

CONST_DATA_U64(aggregate_conflictdq_mask,  0, $0x000000FF000000FF)
CONST_DATA_U64(aggregate_conflictdq_mask,  8, $0x000000FF000000FF)
CONST_DATA_U64(aggregate_conflictdq_mask, 16, $0x000000FF000000FF)
CONST_DATA_U64(aggregate_conflictdq_mask, 24, $0x000000FF000000FF)
CONST_DATA_U64(aggregate_conflictdq_mask, 32, $0x0000FF000000FF00)
CONST_DATA_U64(aggregate_conflictdq_mask, 40, $0x0000FF000000FF00)
CONST_DATA_U64(aggregate_conflictdq_mask, 48, $0x0000FF000000FF00)
CONST_DATA_U64(aggregate_conflictdq_mask, 56, $0x0000FF000000FF00)
CONST_GLOBAL(aggregate_conflictdq_mask, $64)

CONST_DATA_U32(aggregate_conflictdq_norm,  0, $63)
CONST_DATA_U32(aggregate_conflictdq_norm,  4, $63)
CONST_DATA_U32(aggregate_conflictdq_norm,  8, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 12, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 16, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 20, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 24, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 28, $63)
CONST_DATA_U32(aggregate_conflictdq_norm, 32, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 36, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 40, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 44, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 48, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 52, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 56, $55)
CONST_DATA_U32(aggregate_conflictdq_norm, 60, $55)
CONST_GLOBAL(aggregate_conflictdq_norm, $64)

// Consecutive DWORD offsets for 1 ZMM register incremented by 16, for each lane.
CONST_DATA_U32(consts_offsets_d_16,  0, $(0  * 16))
CONST_DATA_U32(consts_offsets_d_16,  4, $(1  * 16))
CONST_DATA_U32(consts_offsets_d_16,  8, $(2  * 16))
CONST_DATA_U32(consts_offsets_d_16, 12, $(3  * 16))
CONST_DATA_U32(consts_offsets_d_16, 16, $(4  * 16))
CONST_DATA_U32(consts_offsets_d_16, 20, $(5  * 16))
CONST_DATA_U32(consts_offsets_d_16, 24, $(6  * 16))
CONST_DATA_U32(consts_offsets_d_16, 28, $(7  * 16))
CONST_DATA_U32(consts_offsets_d_16, 32, $(8  * 16))
CONST_DATA_U32(consts_offsets_d_16, 36, $(9  * 16))
CONST_DATA_U32(consts_offsets_d_16, 40, $(10 * 16))
CONST_DATA_U32(consts_offsets_d_16, 44, $(11 * 16))
CONST_DATA_U32(consts_offsets_d_16, 48, $(12 * 16))
CONST_DATA_U32(consts_offsets_d_16, 52, $(13 * 16))
CONST_DATA_U32(consts_offsets_d_16, 56, $(14 * 16))
CONST_DATA_U32(consts_offsets_d_16, 60, $(15 * 16))
CONST_GLOBAL(consts_offsets_d_16, $64)

// Consecutive DWORD offsets for 1 ZMM register incremented by 16, for each lane.
//
// This is for interleaved lanes that were formed by using unpacking low and high
// 128 bit words - it's easier to just store interleaved and correct offsets than
// to shuffle vectors before storing (we don't need offsets to be consecutive).
CONST_DATA_U32(consts_offsets_interleaved_d_16,  0, $(0  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16,  4, $(4  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16,  8, $(1  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 12, $(5  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 16, $(2  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 20, $(6  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 24, $(3  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 28, $(7  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 32, $(8  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 36, $(12 * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 40, $(9  * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 44, $(13 * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 48, $(10 * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 52, $(14 * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 56, $(11 * 16))
CONST_DATA_U32(consts_offsets_interleaved_d_16, 60, $(15 * 16))
CONST_GLOBAL(consts_offsets_interleaved_d_16, $64)

// Consecutive DWORD offsets for 1 ZMM register incremented by 20, for each lane.
CONST_DATA_U32(consts_offsets_d_20,  0, $(0  * 20))
CONST_DATA_U32(consts_offsets_d_20,  4, $(1  * 20))
CONST_DATA_U32(consts_offsets_d_20,  8, $(2  * 20))
CONST_DATA_U32(consts_offsets_d_20, 12, $(3  * 20))
CONST_DATA_U32(consts_offsets_d_20, 16, $(4  * 20))
CONST_DATA_U32(consts_offsets_d_20, 20, $(5  * 20))
CONST_DATA_U32(consts_offsets_d_20, 24, $(6  * 20))
CONST_DATA_U32(consts_offsets_d_20, 28, $(7  * 20))
CONST_DATA_U32(consts_offsets_d_20, 32, $(8  * 20))
CONST_DATA_U32(consts_offsets_d_20, 36, $(9  * 20))
CONST_DATA_U32(consts_offsets_d_20, 40, $(10 * 20))
CONST_DATA_U32(consts_offsets_d_20, 44, $(11 * 20))
CONST_DATA_U32(consts_offsets_d_20, 48, $(12 * 20))
CONST_DATA_U32(consts_offsets_d_20, 52, $(13 * 20))
CONST_DATA_U32(consts_offsets_d_20, 56, $(14 * 20))
CONST_DATA_U32(consts_offsets_d_20, 60, $(15 * 20))
CONST_DATA_U32(consts_offsets_d_20, 64, $(16 * 20))
CONST_GLOBAL(consts_offsets_d_20, $68)

// Consecutive DWORD offsets for 1 ZMM register incremented by 32, for each lane.
CONST_DATA_U32(consts_offsets_d_32,  0, $(0  * 32))
CONST_DATA_U32(consts_offsets_d_32,  4, $(1  * 32))
CONST_DATA_U32(consts_offsets_d_32,  8, $(2  * 32))
CONST_DATA_U32(consts_offsets_d_32, 12, $(3  * 32))
CONST_DATA_U32(consts_offsets_d_32, 16, $(4  * 32))
CONST_DATA_U32(consts_offsets_d_32, 20, $(5  * 32))
CONST_DATA_U32(consts_offsets_d_32, 24, $(6  * 32))
CONST_DATA_U32(consts_offsets_d_32, 28, $(7  * 32))
CONST_DATA_U32(consts_offsets_d_32, 32, $(8  * 32))
CONST_DATA_U32(consts_offsets_d_32, 36, $(9  * 32))
CONST_DATA_U32(consts_offsets_d_32, 40, $(10 * 32))
CONST_DATA_U32(consts_offsets_d_32, 44, $(11 * 32))
CONST_DATA_U32(consts_offsets_d_32, 48, $(12 * 32))
CONST_DATA_U32(consts_offsets_d_32, 52, $(13 * 32))
CONST_DATA_U32(consts_offsets_d_32, 56, $(14 * 32))
CONST_DATA_U32(consts_offsets_d_32, 60, $(15 * 32))
CONST_DATA_U32(consts_offsets_d_32, 64, $(16 * 32))
CONST_GLOBAL(consts_offsets_d_32, $68)

// Consecutive QWORD offsets for 2 ZMM registers incremented by 16, for each lane.
CONST_DATA_U64(consts_offsets_q_16,  0, $0)
CONST_DATA_U64(consts_offsets_q_16,  8, $16)
CONST_DATA_U64(consts_offsets_q_16, 16, $32)
CONST_DATA_U64(consts_offsets_q_16, 24, $48)
CONST_DATA_U64(consts_offsets_q_16, 32, $64)
CONST_DATA_U64(consts_offsets_q_16, 40, $80)
CONST_DATA_U64(consts_offsets_q_16, 48, $96)
CONST_DATA_U64(consts_offsets_q_16, 56, $112)
CONST_DATA_U64(consts_offsets_q_16, 64, $128)
CONST_DATA_U64(consts_offsets_q_16, 72, $144)
CONST_DATA_U64(consts_offsets_q_16, 80, $160)
CONST_DATA_U64(consts_offsets_q_16, 88, $176)
CONST_DATA_U64(consts_offsets_q_16, 96, $192)
CONST_DATA_U64(consts_offsets_q_16, 104, $208)
CONST_DATA_U64(consts_offsets_q_16, 112, $224)
CONST_DATA_U64(consts_offsets_q_16, 120, $240)
CONST_GLOBAL(consts_offsets_q_16, $128)

// Consecutive QWORD offsets for 2 ZMM registers incremented by 32, for each lane.
CONST_DATA_U64(consts_offsets_q_32,   0, $(0  * 32))
CONST_DATA_U64(consts_offsets_q_32,   8, $(1  * 32))
CONST_DATA_U64(consts_offsets_q_32,  16, $(2  * 32))
CONST_DATA_U64(consts_offsets_q_32,  24, $(3  * 32))
CONST_DATA_U64(consts_offsets_q_32,  32, $(4  * 32))
CONST_DATA_U64(consts_offsets_q_32,  40, $(5  * 32))
CONST_DATA_U64(consts_offsets_q_32,  48, $(6  * 32))
CONST_DATA_U64(consts_offsets_q_32,  56, $(7  * 32))
CONST_DATA_U64(consts_offsets_q_32,  64, $(8  * 32))
CONST_DATA_U64(consts_offsets_q_32,  72, $(9  * 32))
CONST_DATA_U64(consts_offsets_q_32,  80, $(10 * 32))
CONST_DATA_U64(consts_offsets_q_32,  88, $(11 * 32))
CONST_DATA_U64(consts_offsets_q_32,  96, $(12 * 32))
CONST_DATA_U64(consts_offsets_q_32, 104, $(13 * 32))
CONST_DATA_U64(consts_offsets_q_32, 112, $(14 * 32))
CONST_DATA_U64(consts_offsets_q_32, 120, $(15 * 32))
CONST_DATA_U64(consts_offsets_q_32, 128, $(16 * 32))
CONST_GLOBAL(consts_offsets_q_32, $136)

// A DWORD table designed for VPERMD that can be used to map months of the year into the number
// of days preceeding the month, where the index 0 represents March (months start from March).
//
// Months list and values explanation:
//
//   [Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, Jan, Feb] <- Months, indexed from 0 (March)
//   [  0,  31,  61,  92, 122, 153, 184, 214, 245, 275, 306, 337] <- Number of days preceeding the month
CONST_DATA_U32(consts_days_until_month_from_march,  0, $0)
CONST_DATA_U32(consts_days_until_month_from_march,  4, $31)
CONST_DATA_U32(consts_days_until_month_from_march,  8, $61)
CONST_DATA_U32(consts_days_until_month_from_march, 12, $92)
CONST_DATA_U32(consts_days_until_month_from_march, 16, $122)
CONST_DATA_U32(consts_days_until_month_from_march, 20, $153)
CONST_DATA_U32(consts_days_until_month_from_march, 24, $184)
CONST_DATA_U32(consts_days_until_month_from_march, 28, $214)
CONST_DATA_U32(consts_days_until_month_from_march, 32, $245)
CONST_DATA_U32(consts_days_until_month_from_march, 36, $275)
CONST_DATA_U32(consts_days_until_month_from_march, 40, $306)
CONST_DATA_U32(consts_days_until_month_from_march, 44, $337)
CONST_DATA_U32(consts_days_until_month_from_march, 48, $0)
CONST_DATA_U32(consts_days_until_month_from_march, 52, $0)
CONST_DATA_U32(consts_days_until_month_from_march, 56, $0)
CONST_DATA_U32(consts_days_until_month_from_march, 60, $0)
CONST_GLOBAL(consts_days_until_month_from_march, $64)

// The final value of DATE_DIFF(MONTH|YEAR, interval, timestamp) is calculated as:
//
//   Result = Month * consts_datediff_month_year_div_rcp[imm] >> 35
CONST_DATA_U64(consts_datediff_month_year_div_rcp, 0, $34359738368)
CONST_DATA_U64(consts_datediff_month_year_div_rcp, 8, $2863311531)
CONST_GLOBAL(consts_datediff_month_year_div_rcp, $16)

// A predicate for VPSHUFB that is use to byteswap microseconds - used by timestamp boxing.
CONST_DATA_U64(consts_boxts_microsecond_swap, 0, $0xFF000102FFFFFFFF)
CONST_DATA_U64(consts_boxts_microsecond_swap, 8, $0xFF08090AFFFFFFFF)
CONST_GLOBAL(consts_boxts_microsecond_swap, $16)

// A byte mask for QWORD bytes.
//
// Can be used to mask out bytes that are not part of ION encoded data after QWORD gather.
CONST_DATA_U64(consts_byte_mask_q,  0, $0x00000000000000FF)
CONST_DATA_U64(consts_byte_mask_q,  8, $0x000000000000FFFF)
CONST_DATA_U64(consts_byte_mask_q, 16, $0x0000000000FFFFFF)
CONST_DATA_U64(consts_byte_mask_q, 24, $0x00000000FFFFFFFF)
CONST_DATA_U64(consts_byte_mask_q, 32, $0x000000FFFFFFFFFF)
CONST_DATA_U64(consts_byte_mask_q, 40, $0x0000FFFFFFFFFFFF)
CONST_DATA_U64(consts_byte_mask_q, 48, $0x00FFFFFFFFFFFFFF)
CONST_DATA_U64(consts_byte_mask_q, 56, $0xFFFFFFFFFFFFFFFF)
CONST_GLOBAL(consts_byte_mask_q, $64)
