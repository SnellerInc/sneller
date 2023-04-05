// Copyright (C) 2023 Sneller, Inc.
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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"
#include "../../../internal/asmutils/bc_imm_amd64.h"
#include "../../../internal/asmutils/bc_constant.h"

// -------------------------------------------

#define short_literal_stride        32
#define short_literal_register      Y2
#define long_literal_stride         32
#define long_literal_register       Y2
#define short_match_stride          32
#define short_match_register        Y2
#define long_match_stride           64
#define long_match_register         Z2

// -------------------------------------------

#define COPY_SINGLE_ITEM(slot_id, lbl_litcpy, lbl_litcpy_completed, lbl_matchcpy, lbl_matchcpy_completed)                                           \
    /* cycle 0 */                                                                                                                                   \
    VMOVDQU8        (SI), short_literal_register                    /* SIMDREG2 := the first short_literal_stride bytes of the literal */           \
    VPEXTRD         $(slot_id), X15, AX                             /* AX  := token[1].offset */                                                    \
                                                                                                                                                    \
    /* cycle 1 */                                                                                                                                   \
    VMOVDQU8        short_literal_register, (DI)                    /* Store the first short_literal_stride bytes of the literal payload */         \
    VPEXTRD         $(slot_id), X16, BX                             /* BX  := token[1].litlen */                                                    \
    NEGQ            AX                                              /* AX := -match_offset */                                                       \
                                                                                                                                                    \
    /* cycle 2 */                                                                                                                                   \
    CMOVQNE         AX, R9                                          /* lastOffs := -match_offset for non-zero offsets */                            \
    ADDQ            BX, DI                                          /* Adjust the dst.Data cursor, optimistically assuming the copying is over */   \
    ADDQ            BX, SI                                          /* Adjust the literals cursor, optimistically assuming the copying is over */   \
    CMPL            BX, $short_literal_stride                       /* Check if len(literals) > short_literal_stride */                             \
    JA              lbl_litcpy                                      /* Handle the long literal case */                                              \
                                                                                                                                                    \
lbl_litcpy_completed:                                                                                                                               \
    /* cycle 3 */                                                                                                                                   \
    VMOVDQU8        (DI)(R9*1), short_match_register                /* SIMDREG2 := the first short_match_stride bytes of the match */               \
    VPEXTRD         $(slot_id), X17, CX                             /* CX  := token[1].matchlen */                                                  \
                                                                                                                                                    \
    /* cycle 4 */                                                                                                                                   \
    VMOVDQU8        short_match_register, (DI)                      /* Store the first match_copy_stride bytes of the match payload */              \
    ADDQ            CX, DI                                          /* Optimistically assume the entire match has been copied */                    \
    CMPL            CX, $short_match_stride                         /* Check if len(match) > short_match_stride */                                  \
    JA              lbl_matchcpy                                    /* Handle the long match case */                                                \
                                                                                                                                                    \
lbl_matchcpy_completed:

// -------------------------------------------

#define COPY_SINGLE_ITEM_COMPLETERS(lbl_litcpy, lbl_litcpy_completed, lbl_matchcpy, lbl_matchcpy_completed)                                         \
lbl_litcpy:                                                                                                                                         \
    CALL    copySingleLongLiteral<>(SB)                                                                                                             \
    JMP     lbl_litcpy_completed                                                                                                                    \
                                                                                                                                                    \
lbl_matchcpy:                                                                                                                                       \
    CALL    copySingleLongMatch<>(SB)                                                                                                               \
    JMP     lbl_matchcpy_completed

// -------------------------------------------
//
// func decompressIguanaVBMI2(dst []byte, streams *streamPack, lastOffs *int64) ([]byte, errorCode)
TEXT ·decompressIguanaVBMI2(SB), NOSPLIT | NOFRAME, $0-40
    MOVQ            streams+24(FP), BX
    VPTERNLOGQ      $0xff, Z1, Z1, Z1               // Z1  := {-1*}
    VPXORQ          Z0, Z0, Z0                      // Z0  := {0*}
    MOVQ            (stream__size*const_stridTokens+stream_data + const_offsSliceHeaderData)(BX), R11    // R11 := Tokens.Data cursor
    MOVQ            (stream__size*const_stridTokens+stream_data + const_offsSliceHeaderLen)(BX), R10     // R10 := token_count
    VPABSB          Z1, Z2                          // Z2  := uint8{0x01*}
    MOVQ            (stream__size*const_stridOffset16+stream_data + const_offsSliceHeaderData)(BX), R14 // R14 := Offsets16.Data
    MOVQ            (stream__size*const_stridOffset24+stream_data + const_offsSliceHeaderData)(BX), R15 // R15 := Offsets24.Data
    VPSLLD          $3, Z2, Z29                     // Z29 := uint8{0x08*}
    VPADDD          Z2, Z2, Z27                     // Z27 := uint8{0x02*}
    MOVQ            (stream__size*const_stridVarLitLen+stream_data + const_offsSliceHeaderData)(BX), R12 // R12 := VarLitLen.Data
    MOVQ            (stream__size*const_stridVarMatchLen+stream_data + const_offsSliceHeaderData)(BX), R13 // R13 := VarMatchLen.Data
    VPSUBB          Z27, Z1, Z25                    // Z25 := uint8{0xfd*}
    VPADDD          Z29, Z29, Z30                   // Z30 := uint8{0x10*}
    VPADDD          Z27, Z27, Z28                   // Z28 := uint8{0x04*}
    MOVQ            (stream__size*const_stridLiterals+stream_data + const_offsSliceHeaderData)(BX), SI // SI := Literals.Data
    MOVQ            lastOffs+32(FP), R9             // R9  := &lastOffs
    VMOVDQU8        CONST_GET_PTR(consts_uint24_expander, 0), Z21
    VMOVDQU8        CONST_GET_PTR(consts_identity_b_8, 1), Z26
    VPADDD          Z30, Z30, Z31                   // Z31 := uint8{0x20*}
    VPADDB          Z1, Z29, Z24                    // Z24 := uint8{0x07*}
    MOVQ            dst_base+0(FP), DI              // DI  := dst.Data cursor
    MOVQ            dst_len+8(FP), DX               // DX  := dst.Len
    VPADDB          Z1, Z30, Z23                    // Z23 := uint8{0x0f*}
    VPADDB          Z1, Z31, Z22                    // Z22 := uint8{0x1f*}
    MOVQ            dst_cap+16(FP), CX              // CX  := dst.Cap
    MOVQ            DI, ret_base+40(FP)             // Set the result base address
    ADDQ            DX, DI                          // DI  := Move to the dst.Data end as required by append mode
    MOVQ            CX, ret_cap+56(FP)              // Set the result capacity
    MOVQ            (R9), R9                        // R9  := lastOffs

predecoded_tokens_exhausted:
    SUBL            $64, R10                        // token_count -= 64
    JLT             fetch_last_tokens

    // There are still at least 64 tokens available

    VMOVDQU8        (R11), Z2                       // Z2 := uint8{token[i]} for i in 63..0
    MOVL            $0b1_0_0111_1_1_0111_1_1_0111_1_1_0111_1, R8
    ADDQ            $64, R11                        // Move the tokens cursor to the next group of 64 tokens

tokens_fetched:
    // Decode the fetched tokens
    //
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z2  := uint8{token[i]} for i in 63..0
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{0x01*}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPMINUB         Z31, Z2, Z3                     // Z3  := uint8{32 for token[i] >= 32, token[i] otherwise} for i in 63..0
    VPANDD          Z24, Z2, Z19                    // Z19 := uint8{token[i].LLL for token[i] >= 32, garbage otherwise} for i in 63..0
    VPSUBB          Z31, Z3, Z3                     // Z3  := uint8{0 for token[i] >= 32, negative otherwise} for i in 63..0
    VPSRLD          $3, Z2, Z18                     // Z18 := uint8{token[i].GGG_S_MMMM for token[i] >= 32, GGG_S_GGGG otherwise} for i in 63..0
    VPMAXSB         Z1, Z3, Z3                      // Z3  := uint8{0 for token[i] >= 32, 0xff otherwise} for i in 63..0
    VPANDD          Z23, Z18, Z20                   // Z20 := uint8{token[i].MMMM for token[i] >= 32, garbage otherwise} for i in 63..0
    VPMINUB         Z22, Z2, Z4                     // Z4  := uint8{31 for token[i] >= 31, token[i] otherwise} for i in 63..0
    VPANDND         Z19, Z3, Z19                    // Z19 := uint8{token[i].LLL for token[i] >= 32, 0 otherwise} for i in 63..0
    VPSUBB          Z22, Z4, Z4                     // Z4  := uint8{0 for token[i] >= 31, negative otherwise} for i in 63..0
    VPANDND         Z20, Z3, Z20                    // Z20 := uint8{token[i].MMMM for token[i] >= 32, 0 otherwise} for i in 63..0
    VPMAXSB         Z1, Z4, Z4                      // Z4  := uint8{0 for token[i] >= 31, 0xff otherwise} for i in 63..0
    VPSUBB          Z24, Z19, Z5                    // Z5  := uint8{0 for (token[i] >= 32) && (token[i].LLL == 7), negative otherwise} for i in 63..0
    VPSUBB          Z23, Z20, Z6                    // Z6  := uint8{0 for (token[i] >= 32) && (token[i].MMMM == 15), negative otherwise} for i in 63..0
    VPMAXSB         Z1, Z5, Z5                      // Z5  := uint8{0 for (token[i] >= 32) && (token[i].LLL == 7), 0xff otherwise} for i in 63..0
    VPTERNLOGD      $0b0000_0010, Z30, Z3, Z18      // Z18 := uint8{(flOffset16)_0000} for i in 63..0
    VPMAXSB         Z1, Z6, Z6                      // Z6  := uint8{0 for (token[i] >= 32) && (token[i].MMMM == 15), 0xff otherwise} for i in 63..0
    VPTERNLOGD      $0b1101_1000, Z29, Z3, Z18      // Z18 := uint8{(flOffset16)_(flOffset24)_000} for i in 63..0
    VPADDB          Z30, Z2, Z2                     // Z2  := uint8{token[i] + 16} for i in 63..0
    VPTERNLOGD      $0b0111_0010, Z27, Z5, Z18      // Z18 := uint8{(flOffset16)_(flOffset24)_0_(flVarLitLen)_0} for i in 63..0
    VPTERNLOGD      $0b0010_0010, Z3, Z4, Z5        // Z5  := uint8{0xff for token[i] == 31, 0 otherwise} for i in 63..0
    VPTERNLOGD      $0b0111_0010, Z28, Z6, Z18      // Z18 := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0
    VPTERNLOGD      $0b1011_1000, Z2, Z3, Z20       // Z20 := uint8{set match_length[i] for token[i] < 32} for i in 63..0
    VPTERNLOGD      $0b1111_1000, Z28, Z5, Z18      // Z18 := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0

    // Z18 := flags := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0
    // Z19 := literal length offsets
    // Z20 := match length offsets

predecoded_tokens_available:

    // Arm up to 16 predecoded tokens with parameters.
    //
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := Tokens.Data cursor
    // R12 := VarLitLen.Data
    // R13 := VarMatchLen.Data cursor
    // R14 := Offsets16.Data
    // R15 := Offsets24.Data
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z18 := flags := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0
    // Z19 := literal length offsets
    // Z20 := match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPTESTMB        X29, X18, K5                    // K5  := {token[i] needs an Offset24 parameter} for i in 15..0
    VMOVDQU8        (R12), Z2                       // Z2  := uint8{VarLitLen.Data[i]} for i in 63..0
    VPTESTMB        X30, X18, K6                    // K6  := {token[i] needs an Offset16 parameter} for i in 15..0
    VMOVDQU8        (R13), Z3                       // Z3  := uint8{VarMatchLen.Data[i]} for i in 63..0
    VPCMPUB         $VPCMP_IMM_LE, Z25, Z2, K2      // K2  := {VarLitLen.Data[i] <= 0xfd} for i in 63..0
    VMOVDQU16       (R14), Y15                      // Y15 := uint16{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    VPTESTMB        X27, X18, K4                    // K4  := {token[i] needs a VarLitLen parameter} for i in 15..0
    KMOVW           K5, AX                          // AX  := {token[i] needs an Offset24 parameter}
    VMOVDQU8        (R15), Z14                      // Z14 := uint24{Offsets24.Data[(i*3+2)..(i*3)]}
    VPMOVZXBD       X2, Z4                          // Z4  := uint32{VarLitLen.Data[i]} for i in 15..0
    KMOVW           K6, BX                          // BX  := {token[i] needs an Offset16 parameter}
    POPCNTL         AX, AX                          // AX  := the number of tokens requesting Offset24 parameters
    VPMOVZXBD       X19, Z16                        // Z16 := uint32{litlen_token[i].offset} for i in 15..0
    LEAQ            (AX)(AX*2), AX                  // AX  := AX * 3
    POPCNTL         BX, BX                          // BX  := the number of tokens requesting Offset16 parameters
    VPERMB          Z14, Z21, Z14                   // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)] << 8} for i in 15..0
    KORTESTW        K2, K2                          // EFLAGS.CF==0 <=> there are VarLitLen.Data[i] values greater than 253
    LEAQ            (R15)(AX*1), R15                // Skip the consumed bytes from the Offsets24 stream
    VPMOVZXWD       Y15, Z15                        // Z15 := uint32{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    KMOVW           K4, DX                          // DX  := {token[i] needs a VarLitLen parameter} for i in 15..0
    LEAQ            (R14)(BX*2), R14                // Skip the consumed bytes from the Offsets16 stream
    JCC             decode_wide_varlitlen

    // {VarLitLen.Data[i] <= 0xfd} for i in 15..0, so the bytes can be simply skipped by adding the number of consumed ones

    POPCNTL         DX, DX                          // DX := the number of consumed VarLitLen bytes

varlitlen_decoded:

    // DX := the number of consumed VarLitLen bytes
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := Tokens.Data cursor
    // R12 := VarLitLen.Data
    // R13 := VarMatchLen.Data cursor
    // R14 := adjusted Offsets16.Data cursor
    // R15 := adjusted Offsets24.Data cursor
    // K4  := {token[i] needs a VarLitLen parameter} for i in 15..0
    // K5  := {token[i] needs an Offset24 parameter} for i in 15..0
    // K6  := {token[i] needs an Offset16 parameter} for i in 15..0
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z3  := uint8{VarMatchLen.Data[i]} for i in 63..0
    // Z4  := uint32{VarLitLen.Data[i]} for i in 15..0
    // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)] << 8} for i in 15..0
    // Z15 := uint32{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    // Z16 := uint32{litlen_token[i].offset} for i in 15..0
    // Z18 := flags := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0
    // Z19 := literal length offsets
    // Z20 := match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPEXPANDD.Z     Z4, K4, Z4                      // Scatter the subsequent varlitlen values to the requesting token slots
    ADDQ            DX, R12                         // Skip the bytes consumed from the VarLitLen stream
    VPCMPUB         $VPCMP_IMM_LE, Z25, Z3, K2      // K2  := {VarMatchLen.Data[i] <= 0xfd} for i in 63..0
    VPSRLD          $8, Z14, Z14                    // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)]} for i in 15..0
    VPTESTMB        X28, X18, K4                    // K4  := {token[i] needs a VarMatchLen parameter} for i in 15..0
    //p0
    VPMOVZXBD       X20, Z17                        // Z17 := uint32{token[i].matchlen} for i in 15..0
    VPADDD          Z4, Z16, Z16                    // Z16 := uint32{token[i].litlen} for i in 15..0
    VPMOVZXBD       X3, Z4                          // Z4  := uint32{VarMatchLen.Data[i]} for i in 15..0
    KORTESTW        K2, K2                          // EFLAGS.CF==0 <=> there are VarMatchLen.Data[i] values greater than 253
    VALIGND         $4, Z18, Z0, Z18                // Skip the first 16 predecoded token flags entries
    KMOVW           K4, DX                          // DX  := {token[i] needs a VarMatchLen parameter} for i in 15..0
    JCC             decode_wide_varmatchlen

    // {VarMatchLen.Data[i] <= 0xfd} for i in 15..0, so the bytes can be simply skipped by adding the number of consumed ones

    POPCNTL         DX, DX                          // DX := the number of consumed VarMatchLen bytes

varmatchlen_decoded:

    // DX := the number of consumed VarMatchLen bytes
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := Tokens.Data cursor
    // R12 := adjusted VarLitLen.Data cursor
    // R13 := VarMatchLen.Data cursor
    // R14 := adjusted Offsets16.Data cursor
    // R15 := adjusted Offsets24.Data cursor
    // K4  := {token[i] needs a VarMatchLen parameter} for i in 15..0
    // K5  := {token[i] needs an Offset24 parameter} for i in 15..0
    // K6  := {token[i] needs an Offset16 parameter} for i in 15..0
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z4  := uint32{VarMatchLen.Data[i]} for i in 15..0
    // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)]} for i in 15..0
    // Z15 := uint32{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    // Z16 := uint32{token[i].litlen} for i in 15..0
    // Z17 := uint32{token[i].matchlen} for i in 15..0
    // Z18 := adjusted flags := uint8{0 times 16, (flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 47..0
    // Z19 := literal length offsets
    // Z20 := match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPEXPANDD.Z     Z4, K4, Z4                      // Scatter the subsequent varmatchlen values to the requesting token slots
    ADDQ            DX, R13                         // Skip the bytes consumed from the VarMatchLen stream
    VPEXPANDD.Z     Z15, K6, Z15                    // Scatter the subsequent Offset16 values to the requesting token slots
    //p0
    VPEXPANDD.Z     Z14, K5, Z14                    // Scatter the subsequent Offset24 values to the requesting token slots
    SHRL            $1, R8                          // Sequencer: EFLAGS.CF==1 <=> loop_4x should be entered
    VALIGND         $4, Z19, Z0, Z19                // Skip the first 16 predecoded token literal length entries
    VPADDD          Z4, Z17, Z17                    // Z17 := uint32{matchlen[i]} for i in 15..0
    //p05
    VALIGND         $4, Z20, Z0, Z20                // Skip the first 16 predecoded token match length entries
    VPORD           Z14, Z15, Z15                   // Z15 := uint32{Offset24 or Offset16 value for token[i]} for i in 15..0
    JCC             check_loop_1x                   // There remain 0..3 tokens to handle

    // There are at least 4 opcodes available for processing
    //
    // Z16 contains token[i].litlen for i in 15..0
    // Z17 contains matchlen[i] for i in 15..0
    // Z18 contains token[i].offset for i in 15..0
    //
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := adjusted the Tokens.Data cursor cursor
    // R12 := adjusted VarLitLen.Data cursor
    // R13 := adjusted VarMatchLen.Data cursor
    // R14 := adjusted Offsets16.Data cursor
    // R15 := adjusted Offsets24.Data cursor
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z15 := uint32{token[i].offset} for i in 15..0
    // Z16 := uint32{token[i].litlen} for i in 15..0
    // Z17 := uint32{token[i].matchlen} for i in 15..0
    // Z18 := adjusted flags := uint8{0 times 16, (flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 47..0
    // Z19 := adjusted literal length offsets
    // Z20 := adjusted match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

loop_4x:
    COPY_SINGLE_ITEM(0, copy_long_literal0, copy_long_literal0_completed, copy_long_match0, copy_long_match0_completed)
    COPY_SINGLE_ITEM(1, copy_long_literal1, copy_long_literal1_completed, copy_long_match1, copy_long_match1_completed)
    COPY_SINGLE_ITEM(2, copy_long_literal2, copy_long_literal2_completed, copy_long_match2, copy_long_match2_completed)
    COPY_SINGLE_ITEM(3, copy_long_literal3, copy_long_literal3_completed, copy_long_match3, copy_long_match3_completed)

    // Rewind the opcode queue
    VALIGND         $4, Z15, Z0, Z15                // Skip the first 4 entries, part 1
    SHRL            $1, R8                          // Sequencer: EFLAGS.CF==1 <=> loop_4x should be repeated
    VALIGND         $4, Z16, Z0, Z16                // Skip the first 4 entries, part 2
    VALIGND         $4, Z17, Z0, Z17                // Skip the first 4 entries, part 3
    JCS             loop_4x
    SHRL            $1, R8                          // Sequencer: EFLAGS.CF==1 <=> predecoded tokens are present and should be armed with parameters
    JCS             predecoded_tokens_available
    SHRL            $1, R8                          // Sequencer: EFLAGS.CF==1 <=> a new batch of tokens should be predecoded
    JCS             predecoded_tokens_exhausted

    // There still are 0..3 tokens not processed by loop_4x

check_loop_1x:
    TESTL           R8, R8
    JZ              no_more_tokens

loop_1x:
    COPY_SINGLE_ITEM(0, copy_long_literal_f0, copy_long_literal_f0_completed, copy_long_match_f0, copy_long_match_f0_completed)

    // Rewind the opcode queue
    VALIGND         $1, Z15, Z0, Z15                // Skip the first entry, part 1
    VALIGND         $1, Z16, Z0, Z16                // Skip the first entry, part 2
    VALIGND         $1, Z17, Z0, Z17                // Skip the first entry, part 3
    SUBL            $1, R8
    JNZ             loop_1x

no_more_tokens:
    MOVQ            streams+24(FP), BX
    MOVQ            lastOffs+32(FP), AX             // AX := &lastOffs
    MOVQ            (stream__size*const_stridLiterals+stream_data + const_offsSliceHeaderData)(BX), DX // DX := Literals.Data
    MOVQ            (stream__size*const_stridLiterals+stream_data + const_offsSliceHeaderLen)(BX), CX // CX := Literals.Len
    SUBQ            SI, DX                          // DX := -consumed_literals_bytes
    MOVQ            R9, (AX)                        // Store the lastOffs value
    ADDQ            DX, CX                          // CX := the number of the remaining literals bytes
    LEAQ            (DI)(CX*1), DX                  // DX  := just past the dst.Data
    SUBQ            dst_base+0(FP), DX              // DX  := the number of written bytes

    // Append the remaining literals payload bytes

    REP; MOVSB      // TODO: is there a real need for being excessively smart here?
    MOVQ            DX, ret_len+48(FP)
    MOVL            $const_ecOK, ret1+64(FP)
    RET


fetch_last_tokens:

    // R9  := lastOffs
    // R10 := token_count
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{0x01*}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    LEAL            64(R10), AX
    LEAQ            CONST_GET_PTR(consts_composite_remainder, 0), BX
    MOVQ            $-1, DX
    CMPL            R10, $-64
    JLE             no_more_tokens
    MOVL            (BX)(AX*4), R8              // R8 := sequencer for the (R10 & 0x3f) remaining tokens (63..1)
    SHLXQ           R10, DX, DX                 // DX := uint64{-1 >> (R10 & 0x3f)}
    NOTQ            DX
    KMOVQ           DX, K1
    VMOVDQU8.Z      (R11), K1, Z2               // Z2 := uint8{token[i]} for i in K1 range
    JMP             tokens_fetched


decode_wide_varlitlen:

    // DX  := {token[i] needs a VarLitLen parameter} for i in 15..0
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := adjusted the Tokens.Data cursor
    // R12 := VarLitLen.Data
    // R13 := VarMatchLen.Data cursor
    // R14 := adjusted Offsets16.Data cursor
    // R15 := adjusted Offsets24.Data cursor
    // K2  := {VarLitLen.Data[i] <= 0xfd} for i in 63..0
    // K4  := {token[i] needs a VarLitLen parameter} for i in 15..0
    // K5  := {token[i] needs an Offset24 parameter} for i in 15..0
    // K6  := {token[i] needs an Offset16 parameter} for i in 15..0
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z2  := uint8{VarLitLen.Data[i]} for i in 63..0
    // Z3  := uint8{VarMatchLen.Data[i]} for i in 63..0
    // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)] << 8} for i in 15..0
    // Z15 := uint32{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    // Z16 := uint32{litlen_token[i].offset} for i in 15..0
    // Z18 := flags := uint8{(flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 63..0
    // Z19 := literal length offsets
    // Z20 := match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPCMPUB         $VPCMP_IMM_EQ, Z1, Z2, K1       // K1  := {VarLitLen.Data[i] == 0xff} for i in 63..0
    KMOVQ           K2, AX                          // AX  := {VarLitLen.Data[i] <= 0xfd} for i in 63..0
    POPCNTL         DX, DX                          // DX  := the number of tokens needing VarLitLen values
    VPCOMPRESSB     Z2, K2, Z2                      // Z2  := uint8{the sequence of the payload bytes only}
    VPADDB          Z27, Z26, Z5                    // Z5  := uint8{varuint_lengths[i] + 2} for i in 63..0
    DECL            DX                              // DX  := DX - 1 to correctly handle the zero requested tokens case
    VPSUBB          Z27, Z5, K2, Z5                 // Z5  := uint8{restore varuint_lengths[i] for VarLitLen.Data[i] <= 0xfd} for i in 63..0
    VPINSRB         $0, DX, X1, X6                  // X6  := uint8{[0]: the number of tokens needing VarLitLen values; [15..1]: 0xff}
    LEAQ            1(AX*2), CX                     // CX  := (AX << 1) | 0b0001
    LEAQ            3(AX*4), BX                     // BX  := (AX << 2) | 0b0011
    KMOVQ           K1, DX                          // DX  := {VarLitLen.Data[i] == 0xff} for i in 63..0
    NOTQ            AX                              // AX  := {VarLitLen.Data[i] > 0xfd} for i in 63..0
    ANDQ            BX, CX                          // CX  := {0b00 for the two payload bytes following 0xfe or 0xff, 1 otherwise}
    VPSUBB          Z1, Z5, K1, Z5                  // Z5  := uint8{varuint_lengths[i] + 3 for VarLitLen.Data[i] == 0xff} for i in 63..0
    LEAQ            (DX*8), BX                      // BX  := (DX << 3)
    ANDNQ           CX, BX, CX                      // CX  := {0b000 for the three payload bytes following 0xff; 0b00 for bytes following 0xfe; 1 otherwise}
    KMOVQ           CX, K1                          // K1  := {0b000 for the three payload bytes following 0xff; 0b00 for bytes following 0xfe; 1 otherwise}
    PEXTQ           CX, AX, AX                      // AX  :=
    VPCOMPRESSB     Z5, K1, Z5                      // Z5 := uint8{the offset where the VarLitLen.Data[i+1] value begins} for i in 63..0
    PEXTQ           CX, DX, DX
    MOVQ            $0x1111_1111_1111_1111, CX
    PDEPQ           CX, AX, AX
    PDEPQ           CX, DX, DX
    LEAQ            (AX)(DX*2), AX
    LEAQ            (CX)(AX*2), AX                  // AX  := {[(i*4-1)..(i*4)]: 0b0111 for the 0xff prefix, 0b0011 for 0xfe, 0b0001 otherwise} for i in 15..0
    KMOVQ           AX, K1
    VPEXPANDB.Z     Z2, K1, Z4                      // Z4  := uint32{misencoded varuint_256[i]} for i in 15..0
    VPSHUFB         X6, X5, X5                      // X5  := uint8{[0]: the skip value, [15:1]: 0}
    VPEXTRB         $0, X5, DX                      // DX  := the skip value
    VPSRLD          $8, Z4, Z2                      // Z2  := 256*a2 + a1
    //p5
    VPSRLD          $16, Z4, Z5                     // Z5  := a2
    VPADDD          Z2, Z2, Z2                      // Z2  := 512*a2 + 2*a1
    VPSLLD          $9, Z5, Z6                      // Z6  := 512*a2
    VPSUBD          Z2, Z4, Z4                      // Z4  := varuint_256 - 512*a2 - 2*a1
    VPSLLD          $2, Z5, Z2                      // Z2  := 4*a2
    VPSUBD          Z6, Z4, Z4                      // Z4  := varuint_256 - 1024*a2 - 2*a1
    VPADDD          Z2, Z4, Z4                      // Z4  := uint32{corrected varuint_254[i]} for i in 15..0
    JMP             varlitlen_decoded


decode_wide_varmatchlen:

    // DX  := {token[i] needs a VarMatchLen parameter} for i in 15..0
    // DI  := dst.Data cursor
    // SI  := Literals.Data cursor
    // R8  := sequencer
    // R9  := lastOffs
    // R10 := token_count
    // R11 := adjusted the Tokens.Data cursor
    // R12 := adjusted VarLitLen.Data cursor
    // R13 := VarMatchLen.Data cursor
    // R14 := adjusted Offsets16.Data cursor
    // R15 := adjusted Offsets24.Data cursor
    // K2  := {VarMatchLen.Data[i] <= 0xfd} for i in 63..0
    // K4  := {token[i] needs a VarMatchLen parameter} for i in 15..0
    // K5  := {token[i] needs an Offset24 parameter} for i in 15..0
    // K6  := {token[i] needs an Offset16 parameter} for i in 15..0
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z3  := uint8{VarMatchLen.Data[i]} for i in 63..0
    // Z14 := uint32{Offsets24.Data[(i*3+2)..(i*3)]} for i in 15..0
    // Z15 := uint32{Offsets16.Data[(i*2+1)..(i*2)} for i in 15..0
    // Z16 := uint32{token[i].litlen} for i in 15..0
    // Z17 := uint32{token[i].matchlen} for i in 15..0
    // Z18 := adjusted flags := uint8{0 times 16, (flOffset16)_(flOffset24)_(fmVarMatchLen)_(flVarLitLen)_0} for i in 47..0
    // Z19 := literal length offsets
    // Z20 := match length offsets
    // Z21 := uint8{consts_uint24_expander}
    // Z22 := uint8{0x1f*}
    // Z23 := uint8{0x0f*}
    // Z24 := uint8{0x07*}
    // Z25 := uint8{0xfd*}
    // Z26 := uint8{varuint_lengths}
    // Z27 := uint8{0x02*}
    // Z28 := uint8{0x04*}
    // Z29 := uint8{0x08*}
    // Z30 := uint8{0x10*}
    // Z31 := uint8{0x20*}

    VPCMPUB         $VPCMP_IMM_EQ, Z1, Z3, K1       // K1  := {VarMatchLen.Data[i] == 0xff} for i in 63..0
    KMOVQ           K2, AX                          // AX  := {VarMatchLen.Data[i] <= 0xfd} for i in 63..0
    POPCNTL         DX, DX                          // DX  := the number of tokens needing VarMatchLen values
    VPCOMPRESSB     Z3, K2, Z2                      // Z2  := uint8{the sequence of the payload bytes only}
    VPADDB          Z27, Z26, Z5                    // Z5  := uint8{varuint_lengths[i] + 2} for i in 63..0
    DECL            DX                              // DX  := DX - 1 to correctly handle the zero requested tokens case
    VPSUBB          Z27, Z5, K2, Z5                 // Z5  := uint8{restore varuint_lengths[i] for VarMatchLen.Data[i] <= 0xfd} for i in 63..0
    VPINSRB         $0, DX, X1, X6                  // X6  := uint8{[0]: the number of tokens needing VarMatchLen values; [15..1]: 0xff}
    LEAQ            1(AX*2), CX                     // CX  := (AX << 1) | 0b0001
    LEAQ            3(AX*4), BX                     // BX  := (AX << 2) | 0b0011
    KMOVQ           K1, DX                          // DX  := {VarMatchLen.Data[i] == 0xff} for i in 63..0
    NOTQ            AX                              // AX  := {VarMatchLen.Data[i] > 0xfd} for i in 63..0
    ANDQ            BX, CX                          // CX  := {0b00 for the two payload bytes following 0xfe or 0xff, 1 otherwise}
    VPSUBB          Z1, Z5, K1, Z5                  // Z5  := uint8{varuint_lengths[i] + 3 for VarMatchLen.Data[i] == 0xff} for i in 63..0
    LEAQ            (DX*8), BX                      // BX  := (DX << 3)
    ANDNQ           CX, BX, CX                      // CX  := {0b000 for the three payload bytes following 0xff; 0b00 for bytes following 0xfe; 1 otherwise}
    KMOVQ           CX, K1                          // K1  := {0b000 for the three payload bytes following 0xff; 0b00 for bytes following 0xfe; 1 otherwise}
    PEXTQ           CX, AX, AX                      // AX  :=
    VPCOMPRESSB     Z5, K1, Z5                      // Z5 := uint8{the offset where the VarMatchLen.Data[i+1] value begins} for i in 63..0
    PEXTQ           CX, DX, DX
    MOVQ            $0x1111_1111_1111_1111, CX
    PDEPQ           CX, AX, AX
    PDEPQ           CX, DX, DX
    LEAQ            (AX)(DX*2), AX
    LEAQ            (CX)(AX*2), AX                  // AX  := {[(i*4-1)..(i*4)]: 0b0111 for the 0xff prefix, 0b0011 for 0xfe, 0b0001 otherwise} for i in 15..0
    KMOVQ           AX, K1
    VPEXPANDB.Z     Z2, K1, Z4                      // Z4  := uint32{misencoded varuint_256[i]} for i in 15..0
    VPSHUFB         X6, X5, X5                      // X5  := uint8{[0]: the skip value, [15:1]: 0}
    VPEXTRB         $0, X5, DX                      // DX  := the skip value
    VPSRLD          $8, Z4, Z2                      // Z2  := 256*a2 + a1
    //p5
    VPSRLD          $16, Z4, Z5                     // Z5  := a2
    VPADDD          Z2, Z2, Z2                      // Z2  := 512*a2 + 2*a1
    VPSLLD          $9, Z5, Z6                      // Z6  := 512*a2
    VPSUBD          Z2, Z4, Z4                      // Z4  := varuint_256 - 512*a2 - 2*a1
    VPSLLD          $2, Z5, Z2                      // Z2  := 4*a2
    VPSUBD          Z6, Z4, Z4                      // Z4  := varuint_256 - 1024*a2 - 2*a1
    VPADDD          Z2, Z4, Z4                      // Z4  := uint32{corrected varuint_254[i]} for i in 15..0
    JMP             varmatchlen_decoded

    // Copy completers

    COPY_SINGLE_ITEM_COMPLETERS(copy_long_literal0, copy_long_literal0_completed, copy_long_match0, copy_long_match0_completed)
    COPY_SINGLE_ITEM_COMPLETERS(copy_long_literal1, copy_long_literal1_completed, copy_long_match1, copy_long_match1_completed)
    COPY_SINGLE_ITEM_COMPLETERS(copy_long_literal2, copy_long_literal2_completed, copy_long_match2, copy_long_match2_completed)
    COPY_SINGLE_ITEM_COMPLETERS(copy_long_literal3, copy_long_literal3_completed, copy_long_match3, copy_long_match3_completed)
    COPY_SINGLE_ITEM_COMPLETERS(copy_long_literal_f0, copy_long_literal_f0_completed, copy_long_match_f0, copy_long_match_f0_completed)


// -------------------------------------------

TEXT copySingleLongLiteral<>(SB), NOSPLIT | NOFRAME, $0-0
    // cycle 0
    SUBQ            BX, SI                                          // Restore the original SI value, spoiled by the optimistic path
    SUBQ            BX, DI                                          // Restore the original DI value, spoiled by the optimistic path
    SUBQ            $short_literal_stride, BX                       // Adjust the numbers of literals to be copied to account for the already copied ones

loop:
    // cycle 1
    VMOVDQU8        short_literal_stride(SI), long_literal_register   // SIMDREG2 := the next short_literal_stride bytes of literals
    ADDQ            $long_literal_stride, SI                          // Adjust the literals cursor

    // cycle 2
    VMOVDQU8        long_literal_register, short_literal_stride(DI)   // Store the next short_literal_stride bytes of literals
    ADDQ            $long_literal_stride, DI                          // Adjust the dst.Data cursor
    SUBQ            $long_literal_stride, BX                          // Adjust the numbers of literal bytes to be copied
    JA              loop

    // cycle 3
    LEAQ            short_literal_stride(SI)(BX*1), SI                // Out of bounds, correct literals cursor
    LEAQ            short_literal_stride(DI)(BX*1), DI                // Out of bounds, correct dst.Data cursor
    RET

// -------------------------------------------

TEXT copySingleLongMatch<>(SB), NOSPLIT | NOFRAME, $0-0
    // cycle 0
    SUBQ            CX, DI                                              // Restore the original DI value, spoiled by the optimistic path
    SUBQ            $short_match_stride, CX                             // Adjust the numbers of match bytes to be copied

loop:
    // cycle 1
    VMOVDQU8        short_match_stride(DI)(R9*1), long_match_register   // SIMDREG2 := the next long_match_stride bytes of the match

    // cycle 2
    VMOVDQU8        long_match_register, short_match_stride(DI)         // Store the next long_match_stride bytes of the match
    ADDQ            $long_match_stride, DI                              // Adjust the dst.Data cursor
    SUBQ            $long_match_stride, CX                              // Adjust the numbers of match bytes to be copied
    JA              loop

    // cycle 3
    LEAQ            short_match_stride(DI)(CX*1), DI                    // Out of bounds, correct dst.Data cursor
    RET

// -------------------------------------------

CONST_DATA_U32(consts_uint24_expander,  (0*4),  $0x020100ff)
CONST_DATA_U32(consts_uint24_expander,  (1*4),  $0x050403ff)
CONST_DATA_U32(consts_uint24_expander,  (2*4),  $0x080706ff)
CONST_DATA_U32(consts_uint24_expander,  (3*4),  $0x0b0a09ff)
CONST_DATA_U32(consts_uint24_expander,  (4*4),  $0x0e0d0cff)
CONST_DATA_U32(consts_uint24_expander,  (5*4),  $0x11100fff)
CONST_DATA_U32(consts_uint24_expander,  (6*4),  $0x141312ff)
CONST_DATA_U32(consts_uint24_expander,  (7*4),  $0x171615ff)
CONST_DATA_U32(consts_uint24_expander,  (8*4),  $0x1a1918ff)
CONST_DATA_U32(consts_uint24_expander,  (9*4),  $0x1d1c1bff)
CONST_DATA_U32(consts_uint24_expander, (10*4),  $0x201f1eff)
CONST_DATA_U32(consts_uint24_expander, (11*4),  $0x232221ff)
CONST_DATA_U32(consts_uint24_expander, (12*4),  $0x262524ff)
CONST_DATA_U32(consts_uint24_expander, (13*4),  $0x292827ff)
CONST_DATA_U32(consts_uint24_expander, (14*4),  $0x2c2b2aff)
CONST_DATA_U32(consts_uint24_expander, (15*4),  $0x2f2e2dff)
CONST_GLOBAL(consts_uint24_expander, $64)

CONST_DATA_U32(consts_composite_remainder,  (0*4), $0b000_0)
CONST_DATA_U32(consts_composite_remainder,  (1*4), $0b001_0)
CONST_DATA_U32(consts_composite_remainder,  (2*4), $0b010_0)
CONST_DATA_U32(consts_composite_remainder,  (3*4), $0b011_0)
CONST_DATA_U32(consts_composite_remainder,  (4*4), $0b000_0_0_0_1)
CONST_DATA_U32(consts_composite_remainder,  (5*4), $0b001_0_0_0_1)
CONST_DATA_U32(consts_composite_remainder,  (6*4), $0b010_0_0_0_1)
CONST_DATA_U32(consts_composite_remainder,  (7*4), $0b011_0_0_0_1)
CONST_DATA_U32(consts_composite_remainder,  (8*4), $0b000_0_0_01_1)
CONST_DATA_U32(consts_composite_remainder,  (9*4), $0b001_0_0_01_1)
CONST_DATA_U32(consts_composite_remainder,  (10*4), $0b010_0_0_01_1)
CONST_DATA_U32(consts_composite_remainder,  (11*4), $0b011_0_0_01_1)
CONST_DATA_U32(consts_composite_remainder,  (12*4), $0b000_0_0_011_1)
CONST_DATA_U32(consts_composite_remainder,  (13*4), $0b001_0_0_011_1)
CONST_DATA_U32(consts_composite_remainder,  (14*4), $0b010_0_0_011_1)
CONST_DATA_U32(consts_composite_remainder,  (15*4), $0b011_0_0_011_1)
CONST_DATA_U32(consts_composite_remainder,  (16*4), $0b000_0_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (17*4), $0b001_0_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (18*4), $0b010_0_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (19*4), $0b011_0_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (20*4), $0b000_0_0_0_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (21*4), $0b001_0_0_0_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (22*4), $0b010_0_0_0_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (23*4), $0b011_0_0_0_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (24*4), $0b000_0_0_01_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (25*4), $0b001_0_0_01_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (26*4), $0b010_0_0_01_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (27*4), $0b011_0_0_01_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (28*4), $0b000_0_0_011_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (29*4), $0b001_0_0_011_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (30*4), $0b010_0_0_011_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (31*4), $0b011_0_0_011_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (32*4), $0b000_0_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (33*4), $0b001_0_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (34*4), $0b010_0_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (35*4), $0b011_0_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (36*4), $0b000_0_0_0_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (37*4), $0b001_0_0_0_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (38*4), $0b010_0_0_0_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (39*4), $0b011_0_0_0_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (40*4), $0b000_0_0_01_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (41*4), $0b001_0_0_01_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (42*4), $0b010_0_0_01_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (43*4), $0b011_0_0_01_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (44*4), $0b000_0_0_011_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (45*4), $0b001_0_0_011_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (46*4), $0b010_0_0_011_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (47*4), $0b011_0_0_011_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (48*4), $0b000_0_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (49*4), $0b001_0_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (50*4), $0b010_0_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (51*4), $0b011_0_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (52*4), $0b000_0_0_0_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (53*4), $0b001_0_0_0_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (54*4), $0b010_0_0_0_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (55*4), $0b011_0_0_0_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (56*4), $0b000_0_0_01_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (57*4), $0b001_0_0_01_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (58*4), $0b010_0_0_01_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (59*4), $0b011_0_0_01_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (60*4), $0b000_0_0_011_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (61*4), $0b001_0_0_011_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (62*4), $0b010_0_0_011_1_1_0111_1_1_0111_1_1_0111_1)
CONST_DATA_U32(consts_composite_remainder,  (63*4), $0b011_0_0_011_1_1_0111_1_1_0111_1_1_0111_1)
CONST_GLOBAL(consts_composite_remainder, $256)
