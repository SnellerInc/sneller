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

// Globals Affecting BC Instructions Flow
// --------------------------------------

// Number of lanes to trigger scalar loop instead of vector loop consisting of GATHERs.
#define BC_SCALAR_PROCESSING_LANE_COUNT 6

// Constants Machinery
// -------------------

#ifndef BC_CONSTANTS_CONSUME_ONLY
  #define CONST_DATA_U8(name, offset, value) DATA name<>+(offset)(SB)/1, value
  #define CONST_DATA_U16(name, offset, value) DATA name<>+(offset)(SB)/2, value
  #define CONST_DATA_U32(name, offset, value) DATA name<>+(offset)(SB)/4, value
  #define CONST_DATA_U64(name, offset, value) DATA name<>+(offset)(SB)/8, value
  #define CONST_GLOBAL(name, size) GLOBL name<>(SB), RODATA|NOPTR, size
#else
  #define CONST_DATA_U8(name, offset, value)
  #define CONST_DATA_U16(name, offset, value)
  #define CONST_DATA_U32(name, offset, value)
  #define CONST_DATA_U64(name, offset, value)
  #define CONST_GLOBAL(name, size)
#endif

#define CONST_GET_PTR(name, offset) name<>+(offset)(SB)

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

#define FUZZY_ADV_MAP() CONST_GET_PTR(fuzzy_adv_map, 0)
CONST_DATA_U32(fuzzy_adv_map, 0*4, $0x0101) // substitution: adv_data=1; adv_needle=1
CONST_DATA_U32(fuzzy_adv_map, 1*4, $0x0102) // deletion: adv_data=1; adv_needle=2
CONST_DATA_U32(fuzzy_adv_map, 2*4, $0x0201) // insertion: adv_data=2; adv_needle=1
CONST_DATA_U32(fuzzy_adv_map, 3*4, $0x0202) // transposition: adv_data=2; adv_needle=2
CONST_DATA_U32(fuzzy_adv_map, 4*4, $0x0101) // equality: adv_data=1; adv_needle=1
CONST_DATA_U32(fuzzy_adv_map, 5*4, $0x0101) // equality: adv_data=1; adv_needle=1
CONST_DATA_U32(fuzzy_adv_map, 6*4, $0x0101) // equality: adv_data=1; adv_needle=1
CONST_DATA_U32(fuzzy_adv_map, 7*4, $0x0101) // equality: adv_data=1; adv_needle=1
CONST_GLOBAL(fuzzy_adv_map, $32)

#define FUZZY_ROR_APPROX3() CONST_GET_PTR(fuzzy_ror_approx3, 0)
CONST_DATA_U32(fuzzy_ror_approx3,  0, $0x03010002)
CONST_DATA_U32(fuzzy_ror_approx3,  4, $0x07050406)
CONST_DATA_U32(fuzzy_ror_approx3,  8, $0x0B09080A)
CONST_DATA_U32(fuzzy_ror_approx3, 12, $0x0F0D0C0E)
CONST_DATA_U32(fuzzy_ror_approx3, 16, $0x03010002)
CONST_DATA_U32(fuzzy_ror_approx3, 20, $0x07050406)
CONST_DATA_U32(fuzzy_ror_approx3, 24, $0x0B09080A)
CONST_DATA_U32(fuzzy_ror_approx3, 28, $0x0F0D0C0E)
CONST_DATA_U32(fuzzy_ror_approx3, 32, $0x03010002)
CONST_DATA_U32(fuzzy_ror_approx3, 36, $0x07050406)
CONST_DATA_U32(fuzzy_ror_approx3, 40, $0x0B09080A)
CONST_DATA_U32(fuzzy_ror_approx3, 44, $0x0F0D0C0E)
CONST_DATA_U32(fuzzy_ror_approx3, 48, $0x03010002)
CONST_DATA_U32(fuzzy_ror_approx3, 52, $0x07050406)
CONST_DATA_U32(fuzzy_ror_approx3, 56, $0x0B09080A)
CONST_DATA_U32(fuzzy_ror_approx3, 60, $0x0F0D0C0E)
CONST_GLOBAL(fuzzy_ror_approx3, $64)

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

#define CONST_TAIL_INV_MASK() CONST_GET_PTR(tail_mask_inv_map, 0)
CONST_DATA_U32(tail_mask_inv_map, 0, $0xFFFFFFFF)
CONST_DATA_U32(tail_mask_inv_map, 4, $0xFFFFFF00)
CONST_DATA_U32(tail_mask_inv_map, 8, $0xFFFF0000)
CONST_DATA_U32(tail_mask_inv_map, 12, $0xFF000000)
CONST_DATA_U32(tail_mask_inv_map, 16, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 20, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 24, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 28, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 32, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 36, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 40, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 44, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 48, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 52, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 56, $0x00000000)
CONST_DATA_U32(tail_mask_inv_map, 60, $0x00000000)
CONST_GLOBAL(tail_mask_inv_map, $64)

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

CONST_DATA_U64(popcnt_nibble, 0, $0x0302020102010100) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble, 8, $0x0403030203020201) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_GLOBAL(popcnt_nibble, $16)

CONST_DATA_U64(popcnt_nibble_vpsadbw_pos, 0, $0x8382828182818180) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble_vpsadbw_pos, 8, $0x8483838283828281) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_GLOBAL(popcnt_nibble_vpsadbw_pos, $16)

CONST_DATA_U64(popcnt_nibble_vpsadbw_neg, 0, $0x7D7E7E7F7E7F7F80) // [0111 0110 0101 0100 0011 0010 0001 0000]
CONST_DATA_U64(popcnt_nibble_vpsadbw_neg, 8, $0x7C7D7D7E7D7E7E7F) // [1111 1110 1101 1100 1011 1010 1001 1000]
CONST_GLOBAL(popcnt_nibble_vpsadbw_neg, $16)

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

// Consecutive DWORD offsets for 1 ZMM register incremented by 8, for each lane.
CONST_DATA_U32(consts_offsets_d_8,  0, $(0  * 8))
CONST_DATA_U32(consts_offsets_d_8,  4, $(1  * 8))
CONST_DATA_U32(consts_offsets_d_8,  8, $(2  * 8))
CONST_DATA_U32(consts_offsets_d_8, 12, $(3  * 8))
CONST_DATA_U32(consts_offsets_d_8, 16, $(4  * 8))
CONST_DATA_U32(consts_offsets_d_8, 20, $(5  * 8))
CONST_DATA_U32(consts_offsets_d_8, 24, $(6  * 8))
CONST_DATA_U32(consts_offsets_d_8, 28, $(7  * 8))
CONST_DATA_U32(consts_offsets_d_8, 32, $(8  * 8))
CONST_DATA_U32(consts_offsets_d_8, 36, $(9  * 8))
CONST_DATA_U32(consts_offsets_d_8, 40, $(10 * 8))
CONST_DATA_U32(consts_offsets_d_8, 44, $(11 * 8))
CONST_DATA_U32(consts_offsets_d_8, 48, $(12 * 8))
CONST_DATA_U32(consts_offsets_d_8, 52, $(13 * 8))
CONST_DATA_U32(consts_offsets_d_8, 56, $(14 * 8))
CONST_DATA_U32(consts_offsets_d_8, 60, $(15 * 8))
CONST_GLOBAL(consts_offsets_d_8, $64)

// Consecutive DWORD offsets for 1 ZMM register incremented by 9, for each lane.
CONST_DATA_U32(consts_offsets_d_9,  0, $0)
CONST_DATA_U32(consts_offsets_d_9,  4, $9)
CONST_DATA_U32(consts_offsets_d_9,  8, $18)
CONST_DATA_U32(consts_offsets_d_9, 12, $27)
CONST_DATA_U32(consts_offsets_d_9, 16, $36)
CONST_DATA_U32(consts_offsets_d_9, 20, $45)
CONST_DATA_U32(consts_offsets_d_9, 24, $54)
CONST_DATA_U32(consts_offsets_d_9, 28, $63)
CONST_DATA_U32(consts_offsets_d_9, 32, $72)
CONST_DATA_U32(consts_offsets_d_9, 36, $81)
CONST_DATA_U32(consts_offsets_d_9, 40, $90)
CONST_DATA_U32(consts_offsets_d_9, 44, $99)
CONST_DATA_U32(consts_offsets_d_9, 48, $108)
CONST_DATA_U32(consts_offsets_d_9, 52, $117)
CONST_DATA_U32(consts_offsets_d_9, 56, $126)
CONST_DATA_U32(consts_offsets_d_9, 60, $135)
CONST_GLOBAL(consts_offsets_d_9, $64)

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

// Consecutive DWORD offsets for 1 ZMM register incremented by 2, for each lane.
CONST_DATA_U32(consts_offsets_d_2,  0, $(0  * 2))
CONST_DATA_U32(consts_offsets_d_2,  4, $(1  * 2))
CONST_DATA_U32(consts_offsets_d_2,  8, $(2  * 2))
CONST_DATA_U32(consts_offsets_d_2, 12, $(3  * 2))
CONST_DATA_U32(consts_offsets_d_2, 16, $(4  * 2))
CONST_DATA_U32(consts_offsets_d_2, 20, $(5  * 2))
CONST_DATA_U32(consts_offsets_d_2, 24, $(6  * 2))
CONST_DATA_U32(consts_offsets_d_2, 28, $(7  * 2))
CONST_DATA_U32(consts_offsets_d_2, 32, $(8  * 2))
CONST_DATA_U32(consts_offsets_d_2, 36, $(9  * 2))
CONST_DATA_U32(consts_offsets_d_2, 40, $(10 * 2))
CONST_DATA_U32(consts_offsets_d_2, 44, $(11 * 2))
CONST_DATA_U32(consts_offsets_d_2, 48, $(12 * 2))
CONST_DATA_U32(consts_offsets_d_2, 52, $(13 * 2))
CONST_DATA_U32(consts_offsets_d_2, 56, $(14 * 2))
CONST_DATA_U32(consts_offsets_d_2, 60, $(15 * 2))
CONST_GLOBAL(consts_offsets_d_2, $64)

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
// of days preceeding the month, where the index 0 represents March (internal month indexing).
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

// VPSHUFB predicate that calculates a quarter [1, 4] from a month in a [1, 12] range, where [1] represents March
CONST_DATA_U64(consts_quarter_from_month_1_is_march, 0, $0x0303030202020100)
CONST_DATA_U64(consts_quarter_from_month_1_is_march, 8, $0x0000000101040404)
CONST_GLOBAL(consts_quarter_from_month_1_is_march, $16)

// The final value of DATE_DIFF(MONTH|YEAR, interval, timestamp) is calculated as:
//
//   Result = Month * consts_datediff_month_year_div_rcp[imm] >> 35
CONST_DATA_U64(consts_datediff_month_year_div_rcp, 0, $34359738368)
CONST_DATA_U64(consts_datediff_month_year_div_rcp, 8, $11453246124)
CONST_DATA_U64(consts_datediff_month_year_div_rcp, 16, $2863311531)
CONST_GLOBAL(consts_datediff_month_year_div_rcp, $24)

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

// UTF-8 related constants

// Identity mapping for 8-bit vector chunks

CONST_DATA_U64(consts_identity_b_8,   0, $0x0706050403020100)
CONST_DATA_U64(consts_identity_b_8,   8, $0x0f0e0d0c0b0a0908)
CONST_DATA_U64(consts_identity_b_8,  16, $0x1716151413121110)
CONST_DATA_U64(consts_identity_b_8,  24, $0x1f1e1d1c1b1a1918)
CONST_DATA_U64(consts_identity_b_8,  32, $0x2726252423222120)
CONST_DATA_U64(consts_identity_b_8,  40, $0x2f2e2d2c2b2a2928)
CONST_DATA_U64(consts_identity_b_8,  48, $0x3736353433323130)
CONST_DATA_U64(consts_identity_b_8,  56, $0x3f3e3d3c3b3a3938)
CONST_DATA_U64(consts_identity_b_8,  64, $0x4746454443424140)
CONST_DATA_U64(consts_identity_b_8,  72, $0x4f4e4d4c4b4a4948)
CONST_DATA_U64(consts_identity_b_8,  80, $0x5756555453525150)
CONST_DATA_U64(consts_identity_b_8,  88, $0x5f5e5d5c5b5a5958)
CONST_DATA_U64(consts_identity_b_8,  96, $0x6766656463626160)
CONST_DATA_U64(consts_identity_b_8, 104, $0x6f6e6d6c6b6a6968)
CONST_DATA_U64(consts_identity_b_8, 112, $0x7776757473727170)
CONST_DATA_U64(consts_identity_b_8, 120, $0x7f7e7d7c7b7a7978)
CONST_DATA_U64(consts_identity_b_8, 128, $0x8786858483828180)
CONST_DATA_U64(consts_identity_b_8, 136, $0x8f8e8d8c8b8a8988)
CONST_DATA_U64(consts_identity_b_8, 144, $0x9796959493929190)
CONST_DATA_U64(consts_identity_b_8, 152, $0x9f9e9d9c9b9a9998)
CONST_DATA_U64(consts_identity_b_8, 160, $0xa7a6a5a4a3a2a1a0)
CONST_DATA_U64(consts_identity_b_8, 168, $0xafaeadacabaaa9a8)
CONST_DATA_U64(consts_identity_b_8, 176, $0xb7b6b5b4b3b2b1b0)
CONST_DATA_U64(consts_identity_b_8, 184, $0xbfbebdbcbbbab9b8)
CONST_DATA_U64(consts_identity_b_8, 192, $0xc7c6c5c4c3c2c1c0)
CONST_DATA_U64(consts_identity_b_8, 200, $0xcfcecdcccbcac9c8)
CONST_DATA_U64(consts_identity_b_8, 208, $0xd7d6d5d4d3d2d1d0)
CONST_DATA_U64(consts_identity_b_8, 216, $0xdfdedddcdbdad9d8)
CONST_DATA_U64(consts_identity_b_8, 224, $0xe7e6e5e4e3e2e1e0)
CONST_DATA_U64(consts_identity_b_8, 232, $0xefeeedecebeae9e8)
CONST_DATA_U64(consts_identity_b_8, 240, $0xf7f6f5f4f3f2f1f0)
CONST_DATA_U64(consts_identity_b_8, 248, $0xfffefdfcfbfaf9f8)
CONST_GLOBAL(consts_identity_b_8, $256)
