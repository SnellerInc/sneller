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


// func ansCompressCoreAVX512Generic(enc *ANSEncoder) ansCoreFlags
TEXT ·ansCompressCoreAVX512Generic(SB), NOSPLIT | NOFRAME, $0-8
    MOVQ            enc+0(FP), R15                                  // R15 := uint64{enc}
    VBROADCASTI32X4 CONST_GET_PTR(consts_enc_byte_in_word_inverter, 0), Y24 // Y24 := uint8{consts_enc_byte_in_word_inverter}
    VPXORD          Y0, Y0, Y0                                      // Z0  := {0*}
    VMOVDQU32       CONST_GET_PTR(consts_enc_composite_transformer, 0), Z25
    VPTERNLOGD      $0xff, Z1, Z1, Z1                               // Z1  := {-1*}
    MOVQ            (ANSEncoder_src+const_offsSliceHeaderLen)(R15), R14 // R14 := uint64{src.Len}
    MOVQ            (ANSEncoder_src+const_offsSliceHeaderData)(R15), SI // SI  := uint64{src.Data}
    VMOVDQU32       ANSEncoder_state+0(R15), Z26            // Z26 := uint32{state[15..0]}
    VPSRLD          $20, Z1, Z23                                    // Z23 := uint32{0x0fff times 16}
    VMOVDQU32       ANSEncoder_state+64(R15), Z27           // Z27 := uint32{state[31..16]}
    MOVQ            ANSEncoder_stats(R15), BX               // BX  := uint64{enc.stats}
    MOVQ            (ANSEncoder_bufFwd+const_offsSliceHeaderData)(R15), R12
    MOVQ            (ANSEncoder_bufRev+const_offsSliceHeaderData)(R15), R13
    MOVQ            (ANSEncoder_bufFwd+const_offsSliceHeaderLen)(R15), AX
    MOVQ            (ANSEncoder_bufRev+const_offsSliceHeaderLen)(R15), DX
    MOVQ            (ANSEncoder_bufFwd+const_offsSliceHeaderCap)(R15), R10
    MOVQ            (ANSEncoder_bufRev+const_offsSliceHeaderCap)(R15), R11
    ADDQ            AX, R12                                         // R12 := uint64{fwdCursor}
    ADDQ            DX, R13                                         // R13 := uint64{revCursor}
    SUBQ            AX, R10                                         // R10 := uint64{#bufFwd bytes available for store}
    SUBQ            DX, R11                                         // R11 := uint64{#bufRev bytes available for store}
    TESTB           $31, R14                                        // EFLAGS.ZF==1 <=> there is no remainder
    JNZ             fetch_partial                                   // there is a non-empty remainder

loop:
    SUBQ            $32, R14                                        // Adjust srcCursor
    JB              done                                            // No more input data to process
    VPMOVZXBD       (SI)(R14*1), Z2                                 // Z2  := uint32{chunk[15..0]}
    KXNORW          K6, K6, K6                                      // K6  := uint16{0xffff}
    VPMOVZXBD       16(SI)(R14*1), Z3                               // Z3  := uint32{chunk[31..16]}
    KXNORW          K7, K7, K7                                      // K7  := uint16{0xffff}

input_fetched:
    KMOVW           K6, K1                                          // for gather
    VMOVDQA32       Z1, Z4                                          // Z4  := uint32{0xffff_ffff times 16}
    VPGATHERDD      (BX)(Z2*4), K1, Z4                              // Z4  := uint32{(start[i] << ansStatisticsCumulativeFrequencyBits) | freq[i]} for i in 15..0
    KMOVW           K7, K1                                          // for gather
    VMOVDQA32       Z1, Z5                                          // Z5  := uint32{0xffff_ffff times 16}
    VPGATHERDD      (BX)(Z3*4), K1, Z5                              // Z5  := uint32{(start[i] << ansStatisticsCumulativeFrequencyBits) | freq[i]} for i in 31..16
    VPSRLD          $const_ansStatisticsCumulativeFrequencyBits, Z4, Z6  // Z6  := uint32{start[15..0]}
    VPANDD          Z23, Z4, Z4                                     // Z4  := uint32{freq[15..0]}
    VPSLLD          $(2*const_ansWordLBits-const_ansWordMBits), Z4, Z2 // Z2  := uint32{freq[15..0] << (2*ansWordLBits-ansWordMBits)}
    VEXTRACTI32X8   $1, Z4, Y9                                      // Y9  := uint32{freq[15..8]}
    VPCMPUD         $VPCMP_IMM_GE, Z2, Z26, K6, K1                  // K1  := uint16{state[i] >= (freq[i] << (2*ansWordLBits-ansWordMBits))} for i in 15..0
    MOVL            $0xffff_0000, AX                                // AX  := uint32{0xffff_0000}
    VCVTUDQ2PD      Y4, Z8                                          // Z8  := float64{freq[7..0]}
    VCVTUDQ2PD      Y9, Z9                                          // Z9  := float64{freq[15..8]}
    KMOVW           K1, CX                                          // CX  := uint32{state[i] >= (freq[i] << (2*ansWordLBits-ansWordMBits))} for i in 15..0
    VPCOMPRESSD.Z   Z26, K1, Z2                                     // Z2  := uint32{state_to_store[i]} for i in 15..0
    VPSRLD          $const_ansStatisticsCumulativeFrequencyBits, Z5, Z7 // Z7  := uint32{start[31..16]}
    VPANDD          Z23, Z5, Z5                                     // Z5  := uint32{freq[15..0]}
    VPSLLD          $(2*const_ansWordLBits-const_ansWordMBits), Z5, Z3 // Z3  := uint32{freq[31..16] << (2*ansWordLBits-ansWordMBits)}
    VPCMPUD         $VPCMP_IMM_GE, Z3, Z27, K7, K2                  // K2  := uint16{state[i] >= (freq[i] << (2*ansWordLBits-ansWordMBits))} for i in 31..16
    //p0
    VEXTRACTI32X8   $1, Z5, Y11                                     // Y11 := uint32{freq[31..24]}
    POPCNTL         CX, CX                                          // CX  := uint32{#words_to_store[15..0]}
    VCVTUDQ2PD      Y5, Z10                                         // Z10 := float64{freq[23..16]}
    SHRXL           CX, AX, DI                                      // DI  := uint32{garbage[31:16], permute_mask[15..0]}
    ADDL            CX, CX                                          // CX  := uint64{#bytes_to_store[15..0]}
    VPCOMPRESSD.Z   Z27, K2, Z3                                     // Z3  := uint32{state_to_store[i]} for i in 31..16
    KMOVW           K2, DX                                          // DX  := uint32{state[i] >= (freq[i] << (2*ansWordLBits-ansWordMBits))} for i in 31..16
    VCVTUDQ2PD      Y11, Z11                                        // Z11 := float64{freq[31..24]}
    KMOVW           DI, K3                                          // K3  := uint16{permute_mask[15..0]}
    MOVWLZX         DI, DI                                          // DI  := uint32{0x0000, permute_mask[15..0]}
    POPCNTL         DX, DX                                          // DX  := uint32{#words_to_store[31..16]}
    PEXTL           DI, DI, DI                                      // DI  := uint32{store_mask[15..0]}
    SHRXL           DX, AX, AX                                      // AX  := uint32{garbage[31:16], permute_mask[31..16]}
    ADDL            DX, DX                                          // DX  := uint64{#bytes_to_store[31..16]}
    SUBQ            CX, R10                                         // EFLAGS.CF==1 <=> there is not enough space in bufFwd
    JB              out_of_fwd_buffer                               // There is not enough space in bufFwd
    MOVWLZX         AX, AX                                          // AX  := uint32{0x0000, permute_mask[31..16]}
    SUBQ            DX, R11                                         // EFLAGS.CF==1 <=> there is not enough space in bufRev
    JB              out_of_rev_buffer                               // There is not enough space in bufRev
    VPSRLD          $const_ansWordLBits, Z26, K1, Z26               // Z26 := uint32{state[i] >> ansWordLBits where normalization is required, state[i] otherwise} for i in 15..0
    KMOVW           AX, K4                                          // K4  := uint16{permute_mask[31..16]}
    VCVTUDQ2PD      Y26, Z12                                        // Z12 := float64{state[7..0]}
    VPCOMPRESSD.Z   Z25, K3, Z16                                    // Z16 := uint32{store_permutation[15..0]}
    VPSRLD          $const_ansWordLBits, Z27, K2, Z27               // Z27 := uint32{state[i] >> ansWordLBits where normalization is required, state[i] otherwise} for i in 31..16
    PEXTL           AX, AX, AX                                      // AX  := uint32{store_mask[31..16]}
    VCVTUDQ2PD      Y27, Z14                                        // Z14 := float64{state[23..16]}
    VEXTRACTI32X8   $1, Z26, Y13                                    // Y13 := uint32{state[15..8]}
    VPADDD          Z6, Z26, Z6                                     // Z6  := uint32{state[i] + start[i]} for i in 15..0
    VPERMD          Z2, Z16, Z2                                     // Z2  := uint32{dwords_to_store[15..0]}
    VPADDD          Z7, Z27, Z7                                     // Z7  := uint32{state[i] + start[i]} for i in 31..16
    VEXTRACTI32X8   $1, Z27, Y15                                    // Y15 := uint32{state[31..24]}
    //p0
    VCVTUDQ2PD      Y13, Z13                                        // Z13 := float64{state[15..8]}
    VDIVPD.RZ_SAE   Z8, Z12, Z12                                    // Z12 := float64{floor(state[i]/freq[i])} for i in 7..0
    VPMOVDW         Z2, Y2                                          // Y2  := uint16{words_to_store[15..0]}
    //p0+p0
    KMOVW           DI, K3                                          // K3  := uint16{store_mask[15..0]}
    //p0
    VPCOMPRESSD.Z   Z25, K4, Z16                                    // Z16 := uint32{store_permutation[31..16]}
    //p0+p0
    VCVTUDQ2PD      Y15, Z15                                        // Z15 := float64{state[31..24]}
    VDIVPD.RZ_SAE   Z9, Z13, Z13                                    // Z13 := float64{floor(state[i]/freq[i])} for i in 15..8
    VPSHUFB         Y24, Y2, Y2                                     // Y2  := uint16{words_to_store_reverse_byte_order[15..0]}
    //p0
    VPERMD          Z3, Z16, Z3                                     // Z3  := uint32{dwords_to_store[31..16]}
    VMOVDQU16       Y2, K3, (R12)                                   // Store words_to_store_reverse_byte_order[15..0] under store_mask[15..0]
    KMOVW           AX, K4                                          // K4  := uint16{store_mask[31..16]}
    ADDQ            CX, R12                                         // Adjust fwdCursor
    VCVTTPD2UDQ     Z12, Y12                                        // Y12 := uint32{floor(state[i]/freq[i])} for i in 7..0
    VDIVPD.RZ_SAE   Z10, Z14, Z14                                   // Z14 := float64{floor(state[i]/freq[i])} for i in 23..16
    VCVTTPD2UDQ     Z13, Y13                                        // Y13 := uint32{state[i]/freq[i]} for i in 15..8
    VPMOVDW         Z3, Y3                                          // Y3  := uint16{words_to_store[31..16]}
    //p0+p0
    VDIVPD.RZ_SAE   Z11, Z15, Z15                                   // Z15 := float64{floor(state[i]/freq[i])} for i in 31..24
    VINSERTI32X8    $1, Y13, Z12, Z12                               // Z12 := uint32{state[i]/freq[i]} for i in 15..0
    VMOVDQU16       Y3, K4, (R13)                                   // Store words_to_store[31..16] under store_mask[31..16]
    ADDQ            DX, R13                                         // Adjust revCursor
    VCVTTPD2UDQ     Z14, Y14                                        // Y14 := uint32{state[i]/freq[i]} for i in 23..16
    VCVTTPD2UDQ     Z15, Y15                                        // Y15 := uint32{state[i]/freq[i]} for i in 31..24
    VPMULLD         Z12, Z4, Z13                                    // Z13 := uint32{(state[i]/freq[i])*freq[i]} for i in 15..0
    VINSERTI32X8    $1, Y15, Z14, Z14                               // Z14 := uint32{state[i]/freq[i]} for i in 31..0
    VPSLLD          $const_ansWordMBits, Z12, Z12                   // Z12 := uint32{(state[i]/freq[i]) << ansWordMBits} for i in 15..0
    VPADDD          Z6, Z12, Z12                                    // Z12 := uint32{((state[i]/freq[i]) << ansWordMBits) + state[i] + start[i]} for i in 15..0
    VPMULLD         Z14, Z5, Z15                                    // Z15 := uint32{(state[i]/freq[i])*freq[i]} for i in 31..16
    VPSLLD          $const_ansWordMBits, Z14, Z14                   // Z14 := uint32{(state[i]/freq[i]) << ansWordMBits} for i in 31..16
    VPSUBD          Z13, Z12, K6, Z26                               // Z26 := uint32{((state[i]/freq[i]) << ansWordMBits) + (state[i] % freq[i]) + start[i]} for i in 15..0
    VPADDD          Z7, Z14, Z14                                    // Z14 := uint32{((state[i]/freq[i]) << ansWordMBits) + state[i] + start[i]} for i in 31..16
    //p05
    VPSUBD          Z15, Z14, K7, Z27                               // Z27 := uint32{((state[i]/freq[i]) << ansWordMBits) + (state[i] % freq[i]) + start[i]} for i in 31..0
    JMP             loop

done:
    // Flush enc.state
    VPERMD          Z26, Z25, Z2                                    // Z2  := uint32{state[15-i]} for i in 15..0
    SUBQ            $64, R10                                        // EFLAGS.CF==1 <=> there is not enough space in bufFwd
    JB              out_of_fwd_buffer                               // There is not enough space in bufFwd
    VBROADCASTI32X4 CONST_GET_PTR(consts_enc_byte_in_dword_inverter, 0), Z3 // Z3 := uint8{consts_enc_byte_in_dword_inverter}
    SUBQ            $64, R11                                        // EFLAGS.CF==1 <=> there is not enough space in bufRev
    JB              out_of_rev_buffer                               // There is not enough space in bufRev
    XORL            AX, AX
    VMOVDQU32       Z26, ANSEncoder_state+0(R15)            // Update ANSEncoder.state[15..0]
    VMOVDQU32       Z27, ANSEncoder_state+64(R15)           // Update ANSEncoder.state[31..16]
    MOVQ            AX, (ANSEncoder_src+const_offsSliceHeaderLen)(R15) // Update enc.src.Len
    MOVL            AX, ret+8(FP)                                   // Indicate the processing has completed and no restart is necessary
    VPSHUFB         Z3, Z2, Z2                                      // Z2 := uint32{byte_reversed_state[15-i]} for i in 15..0
    VMOVDQU32       Z27, (R13)                                      // Flush state[31..16]
    ADDQ            $64, R13
    VMOVDQU32       Z2, (R12)                                       // Flush byte_reversed_state[0..15]
    ADDQ            $64, R12
    SUBQ            (ANSEncoder_bufRev+const_offsSliceHeaderData)(R15), R13 // R13 := uint64{new len(bufRev)}
    SUBQ            (ANSEncoder_bufFwd+const_offsSliceHeaderData)(R15), R12 // R12 := uint64{new len(bufFwd)}
    MOVQ            R13, (ANSEncoder_bufRev+const_offsSliceHeaderLen)(R15)  // Update enc.bufRev.Len
    MOVQ            R12, (ANSEncoder_bufFwd+const_offsSliceHeaderLen)(R15)  // Update enc.bufFwd.Len
    RET


fetch_partial:
    // Start with 1..31 bytes of the input data
    MOVL            $-1, AX                                         // AX  := uint32{0xffff_ffff}
    MOVL            R14, DX                                         // DX  := uint32{src.Len}
    SHLXL           R14, AX, AX                                     // AX  := uint32{^fetch_mask[31..0]}
    ANDL            $31, DX                                         // DX  := uint64{src.Len % 32}
    NOTL            AX                                              // AX  := uint32{fetch_mask[31..0]}
    SUBQ            DX, R14                                         // Adjust the srcCursor to the preceding aligned bundary
    KMOVW           AX, K6                                          // K6  := uint16{fetch_mask[15..0]}
    SHRL            $16, AX                                         // AX  := uint32{fetch_mask[31..16]}
    VPMOVZXBD.Z     (SI)(R14*1), K6, Z2                             // Z2  := uint32{chunk[15..0]}
    KMOVW           AX, K7                                          // K7  := uint16{fetch_mask[31..16]}
    VPMOVZXBD.Z     16(SI)(R14*1), K7, Z3                           // Z3  := uint32{chunk[31..16]}
    JMP             input_fetched                                   // Continue processing
    UD2                                                             // Kill decoding, error handlers ahead


out_of_fwd_buffer:
    // bufFwd has insufficient capacity to absorb the new batch of data. The status of bufRev remains unknown.
    MOVL            $const_ansCoreFlagExpandForward, ret+8(FP)
    SUBQ            DX, R11                                         // EFLAGS.CF==1 <=> there is not enough space in bufRev
    JAE             out_of_buffer_common                            // There is not enough space in bufRev
    MOVL            $(const_ansCoreFlagExpandForward | const_ansCoreFlagExpandReverse), ret+8(FP)
    JMP             out_of_buffer_common


out_of_rev_buffer:
    // bufRev has insufficient capacity to absorb the new batch of data. The bufFwd capacity
    // must have already been validated before checking bufRev, so ansCoreFlagExpandForward
    // cannot be set per the sequence of events.
    MOVL            $const_ansCoreFlagExpandReverse, ret+8(FP)
    // fallback

out_of_buffer_common:
    VMOVDQU32       Z26, ANSEncoder_state+0(R15)            // Update ANSEncoder.state[15..0]
    MOVQ            (ANSEncoder_src+const_offsSliceHeaderLen)(R15), AX // AX := uint64{src.Len}
    VMOVDQU32       Z27, ANSEncoder_state+64(R15)           // Update ANSEncoder.state[31..16]
    SUBQ            (ANSEncoder_bufFwd+const_offsSliceHeaderData)(R15), R12 // R12 := uint64{new len(bufFwd)}
    SUBQ            (ANSEncoder_bufRev+const_offsSliceHeaderData)(R15), R13 // R13 := uint64{new len(bufRev)}
    MOVQ            R12, (ANSEncoder_bufFwd+const_offsSliceHeaderLen)(R15)  // Update enc.bufFwd.Len
    SUBQ            R14, AX             // AX := uint64{the number of consumed enc.src bytes}
    LEAQ            32(R14), DX         // DX := uint64{the last valid input offset if arrived at this point through the "loop" path}
    MOVQ            R13, (ANSEncoder_bufRev+const_offsSliceHeaderLen)(R15)  // Update enc.bufRev.Len
    LEAQ            (R14)(AX*1), DI     // DI := uint64{the last valid input offset if arrived at this point through the "fetch_partial" path}
    CMPQ            AX, $32             // EFLAGS.CF==1 <=> arrived at this point through the "fetch_partial" path
    CMOVQCS         DI, DX              // DX := uint64{the last valid input offset}
    MOVQ            DX, (ANSEncoder_src+const_offsSliceHeaderLen)(R15) // update src.Len
    RET


// func ansDecompressAVX512VBMI(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode)
TEXT ·ansDecompressAVX512VBMI(SB), NOSPLIT | NOFRAME, $0-64
    MOVL            $(const_ansWordM - 1), AX                       // AX  := uint32{ansWordM - 1}
    MOVQ            src_base+32(FP), SI                             // SI  := uint64{src.Data}
    MOVQ            src_len+40(FP), DX                              // DX  := uint64{src.Len}
    VPBROADCASTD    AX, Z18                                         // Z18 := uint32{(ansWordM - 1) times 16}
    MOVQ            dst_base+0(FP), DI                              // DI  := uint64{dst.Data}
    MOVQ            tab+56(FP), R8                                  // R8  := uint64{tab base}
    MOVQ            DI, ret_base+64(FP)                             // Set ret_base of the output slice
    MOVQ            dst_len+8(FP), AX                               // AX  := uint64{dst.Len}
    MOVQ            dstLen+24(FP), CX                               // CX  := uint64{dstLen}
    ADDQ            AX, DI                                          // Skip the already existing dst.Data block in preparation for an append
    ADDQ            CX, AX                                          // AX := the new length when the decoding is done
    LEAQ            -64(SI)(DX*1), BX                               // BX  := uint64{cursorRev}
    VMOVDQU32       (SI), Z16                                       // Z16 := uint32{state[i]} for i in 15..0
    MOVL            $0x55555555, DX                                 // DX  := uint32{0x55555555}
    MOVQ            AX, ret_len+72(FP)                              // Set ret_len of the output slice
    MOVQ            dst_cap+16(FP), AX                              // AX  := uint64{dst.Cap}
    KMOVD           DX, K5                                          // K5  := 0x55555555
    VMOVDQU32       (BX), Z17                                       // Z17 := uint32{state[i]} for i in 31..16
    LEAQ            -32(DI)(CX*1), DX                               // DX  := loop terminator
    VMOVDQU32       CONST_GET_PTR(const_result_extractor, 0), Z20   // Z20 := const_result_extractor
    MOVQ            AX, ret_cap+80(FP)                              // Set ret_cap of the output slice
    MOVL            $const_ansWordL, AX                             // AX  := uint32{ansWordL}
    KXNORW          K6, K6, K6                                      // K6  := 0x0000ffff
    VMOVDQU16       CONST_GET_PTR(const_invert_zxwd, 0), Z21 // Z21 := const_invert_zxwd
    ADDQ            $64, SI                                         // SI  := uint64{cursorFwd}
    VPBROADCASTD    AX, Z19                                         // Z19 := uint32{ansWordL times 16}
    CMPQ            DX, DI
    JBE             handle_short_input                              // if dstLen <= ansSIMDLaneCount

    // The input contains more bytes than the number of lanes
    //
    // BX  := uint64{cursorRev}
    // DX  := loop terminator
    // SI  := uint64{cursorFwd}
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z16 := uint32{state[i]} for i in 15..0
    // Z17 := uint32{state[i]} for i in 31..16
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z19 := uint32{ansWordL times 16}
    // Z20 := const_result_extractor
    // Z21 := const_invert_zxwd
    // K5  := 0x55555555
    // K6  := 0x0000ffff

loop:
    VPANDD          Z18, Z16, Z4                                    // Z4  := uint32{slot[i]} for in 15..0
    KMOVW           K6, K1                                          // for gather
    VPGATHERDD      (R8)(Z4*4), K1, Z2                              // Z2  := uint32{tab[slot[i]]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z16, Z16               // Z16 := uint32{state[i] >> ansWordMBits} for in 15..0
    VPANDD          Z18, Z17, Z4                                    // Z4  := uint32{slot[i]} for in 31..16
    KMOVW           K6, K1                                          // for gather
    VPMOVZXWD       (SI), Z11                                       // Z11 := uint32{src_fwd[i]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z17, Z17               // Z17 := uint32{state[i] >> ansWordMBits} for in 31..16
    VMOVDQU16       -32(BX), Y12                                    // Z12 := uint16{0 times 16, src_rev[i]} for i in 15..0
    VPGATHERDD      (R8)(Z4*4), K1, Z3                              // Z3  := uint32{tab[slot[i]]} for i in 31..16
    VPERMW          Z12, Z21, Z12                                   // Z12 := uint32{src_rev[15-i]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z2, Z5                 // Z5  := uint32{bits 11:0: bias[i], garbage otherwise} for i in 15..0
    VPANDD          Z18, Z2, Z4                                     // Z4  := uint32{freq[i]} for i in 15..0
    VPERMB          Z2, Z20, Z2                                     // Z2  := uint8{garbage times 48, s[i]} for i in 15..0
    VPANDD          Z18, Z5, Z5                                     // Z5  := uint32{bias[i]} for i in 15..0
    VPMULLD         Z4, Z16, Z16                                    // Z16 := uint32{(state[i] >> ansWordMBits) * freq[i]} for i in 15..0
    VPANDD          Z18, Z3, Z4                                     // Z4  := uint32{freq[i]} for i in 31..16
    VPSRLD          $const_ansWordMBits, Z3, Z8                 // Z8  := uint32{bits 11:0: bias[i], garbage otherwise} for i in 31..16
    VPERMB          Z3, Z20, Z3                                     // Z3  := uint8{garbage times 48, s[i]} for i in 31..16
    VPANDD          Z18, Z8, Z8                                     // Z8  := uint32{bias[i]} for i in 31..16
    VPMULLD         Z4, Z17, Z17                                    // Z17 := uint32{(state[i] >> ansWordMBits) * freq[i]} for i in 31..16
    VPADDD          Z5, Z16, Z16                                    // Z16 := uint32{denormalized_state[i]} for i in 15..0
    VMOVDQU8        X2, (DI)                                        // Store the result bytes 15..0
    VPCMPUD         $VPCMP_IMM_LT, Z19, Z16, K1                     // K1  := {denormalized_state[i] < ansWordL} for i in 15..0
    VPSLLD          $const_ansWordLBits, Z16, Z6                    // Z6  := uint32{denormalized_state[i] << ansWordLBits} for i in 15..0
    VMOVDQU8        X3, 16(DI)                                      // Store the result bytes 31..16
    ADDQ            $32, DI                                         // Advance output_cursor
    VPEXPANDD.Z     Z11, K1, Z4                                     // Z4  := uint32{src_fwd[k] for denormalized_state[i] < ansWordL; 0 otherwise} for i in 15..0
    KMOVW           K1, AX                                          // AX  := {denormalized_state[i] < ansWordL} for i in 15..0
    VPADDD          Z8, Z17, Z17                                    // Z17 := uint32{denormalized_state[i]} for i in 31..16
    POPCNTL         AX, AX                                          // AX  := uint64{#consumed src_fwd items}
    VPCMPUD         $VPCMP_IMM_LT, Z19, Z17, K2                     // K2  := {denormalized_state[i] < ansWordL} for i in 31..16
    VPSLLD          $const_ansWordLBits, Z17, Z7                    // Z7  := uint32{denormalized_state[i] << ansWordLBits} for i in 31..16
    VPORD           Z6, Z4, K1, Z16                                 // Z16 := uint32{normalized_state[i]} for i in 15..0
    LEAQ            (SI)(AX*2), SI                                  // Skip the consumed src_fwd items
    VPEXPANDD.Z     Z12, K2, Z4                                     // Z4  := uint32{src_rev[k] for denormalized_state[i] < ansWordL; 0 otherwise} for i in 31..16
    KMOVW           K2, AX                                          // AX  := {denormalized_state[i] < ansWordL} for i in 31..16
    POPCNTL         AX, AX                                          // AX  := uint64{#consumed src_rev items}
    SUBQ            AX, BX                                          // Skip the consumed src_rev items, part #1
    VPORD           Z7, Z4, K2, Z17                                 // Z17 := uint32{normalized_state[i]} for i in 31..16
    SUBQ            AX, BX                                          // Skip the consumed src_rev items, part #2
    CMPQ            DX, DI
    JA              loop

process_last_chunk:

    // 1..32 bytes remains for processing
    //
    // DX  := loop terminator
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z16 := uint32{state[i]} for i in 15..0
    // Z17 := uint32{state[i]} for i in 31..16
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z20 := const_result_extractor
    // K2  := remaining_lanes_mask
    // K6  := 0x0000ffff

    KMOVW           K6, K1                                          // for gather
    VPANDD          Z18, Z16, Z4                                    // Z4  := uint32{slot[i]} for in 15..0
    SUBQ            DI, DX                                          // DX  := uint64{remaining-32}
    VPGATHERDD      (R8)(Z4*4), K1, Z2                              // Z2  := uint32{tab[slot[i]]} for i in 15..0
    MOVL            $-1, AX                                         // AX  := uint32{-1}
    ADDL            $32, DX                                         // DX  := uint64{remaining}
    VPANDD          Z18, Z17, Z4                                    // Z4  := uint32{slot[i]} for in 31..16
    SHLXQ           DX, AX, AX                                      // AX  := uint32{^lane_mask}
    VPGATHERDD      (R8)(Z4*4), K6, Z3                              // Z3  := uint32{tab[slot[i]]} for i in 31..16
    NOTL            AX                                              // AX  := uint32{lane_mask}
    VPERMT2B        Z3, Z20, Z2                                     // Z2  := uint8{s[i]} for i in (remainder_len-1)..0
    KMOVD           AX, K2                                          // K2  := {lane_mask[i]} for i in 31..0
    VMOVDQU8        Y2, K2, (DI)
    MOVL            $const_ecOK, ret1+88(FP)                        // Set the error code
    RET

handle_short_input:

    // Process 0..32 input bytes
    //
    // BX  := uint64{cursorRev}
    // CX  := uint64{dstLen}
    // DX  := loop terminator
    // SI  := uint64{cursorFwd}
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z19 := uint32{ansWordL times 16}
    // Z20 := const_result_extractor
    // K5  := 0x55555555
    // K6  := 0x0000ffff

    TESTQ           CX, CX                                          // EFLAGS.ZF==1 <=> the content is empty
    JNZ             process_last_chunk                              // there are some bytes for processing
    MOVL            $const_ecOK, ret1+88(FP)                        // Set the error code
    RET


// func ansDecompressAVX512Generic(dst []byte, dstLen int, src []byte, tab *ANSDenseTable) ([]byte, errorCode)
TEXT ·ansDecompressAVX512Generic(SB), NOSPLIT | NOFRAME, $0-64
    MOVL            $(const_ansWordM - 1), AX                       // AX  := uint32{ansWordM - 1}
    MOVQ            src_base+32(FP), SI                             // SI  := uint64{src.Data}
    MOVQ            src_len+40(FP), DX                              // DX  := uint64{src.Len}
    VPBROADCASTD    AX, Z18                                         // Z18 := uint32{(ansWordM - 1) times 16}
    MOVQ            dst_base+0(FP), DI                              // DI  := uint64{dst.Data}
    MOVQ            tab+56(FP), R8                                  // R8  := uint64{tab base}
    MOVQ            DI, ret_base+64(FP)                             // Set ret_base of the output slice
    MOVQ            dst_len+8(FP), AX                               // AX  := uint64{dst.Len}
    MOVQ            dstLen+24(FP), CX                               // CX  := uint64{dstLen}
    ADDQ            AX, DI                                          // Skip the already existing dst.Data block in preparation for an append
    ADDQ            CX, AX                                          // AX := the new length when the decoding is done
    LEAQ            -64(SI)(DX*1), BX                               // BX  := uint64{cursorRev}
    VMOVDQU32       (SI), Z16                                       // Z16 := uint32{state[i]} for i in 15..0
    MOVL            $0x55555555, DX                                 // DX  := uint32{0x55555555}
    MOVQ            AX, ret_len+72(FP)                              // Set ret_len of the output slice
    MOVQ            dst_cap+16(FP), AX                              // AX  := uint64{dst.Cap}
    KMOVD           DX, K5                                          // K5  := 0x55555555
    VMOVDQU32       (BX), Z17                                       // Z17 := uint32{state[i]} for i in 31..16
    LEAQ            -32(DI)(CX*1), DX                               // DX  := loop terminator
    VMOVDQU32       CONST_GET_PTR(const_result_extractor, 0), Z20   // Z20 := const_result_extractor
    MOVQ            AX, ret_cap+80(FP)                              // Set ret_cap of the output slice
    MOVL            $const_ansWordL, AX                             // AX  := uint32{ansWordL}
    KXNORW          K6, K6, K6                                      // K6  := 0x0000ffff
    VMOVDQU16       CONST_GET_PTR(const_invert_zxwd, 0), Z21 // Z21 := const_invert_zxwd
    ADDQ            $64, SI                                         // SI  := uint64{cursorFwd}
    VPBROADCASTD    AX, Z19                                         // Z19 := uint32{ansWordL times 16}
    VMOVDQU32       CONST_GET_PTR(const_dword_compressor, 0), Z22   // Z22 := const_dword_compressor
    CMPQ            DX, DI
    JBE             handle_short_input                              // if dstLen <= ansSIMDLaneCount

    // The input contains more bytes than the number of lanes
    //
    // BX  := uint64{cursorRev}
    // DX  := loop terminator
    // SI  := uint64{cursorFwd}
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z16 := uint32{state[i]} for i in 15..0
    // Z17 := uint32{state[i]} for i in 31..16
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z19 := uint32{ansWordL times 16}
    // Z20 := const_result_extractor
    // Z21 := const_invert_zxwd
    // Z22 := const_dword_compressor
    // K5  := 0x55555555
    // K6  := 0x0000ffff

loop:
    VPANDD          Z18, Z16, Z4                                    // Z4  := uint32{slot[i]} for in 15..0
    KMOVW           K6, K1                                          // for gather
    VPGATHERDD      (R8)(Z4*4), K1, Z2                              // Z2  := uint32{tab[slot[i]]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z16, Z16               // Z16 := uint32{state[i] >> ansWordMBits} for in 15..0
    VPANDD          Z18, Z17, Z4                                    // Z4  := uint32{slot[i]} for in 31..16
    KMOVW           K6, K1                                          // for gather
    VPMOVZXWD       (SI), Z11                                       // Z11 := uint32{src_fwd[i]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z17, Z17               // Z17 := uint32{state[i] >> ansWordMBits} for in 31..16
    VMOVDQU16       -32(BX), Y12                                    // Z12 := uint16{0 times 16, src_rev[i]} for i in 15..0
    VPGATHERDD      (R8)(Z4*4), K1, Z3                              // Z3  := uint32{tab[slot[i]]} for i in 31..16
    VPERMW          Z12, Z21, Z12                                   // Z12 := uint32{src_rev[15-i]} for i in 15..0
    VPSRLD          $const_ansWordMBits, Z2, Z5                 // Z5  := uint32{bits 11:0: bias[i], garbage otherwise} for i in 15..0
    VPANDD          Z18, Z2, Z4                                     // Z4  := uint32{freq[i]} for i in 15..0
    VPSHUFB         Z20, Z2, Z2                                     // Z2  := uint8{garbage times 12, s[15..12], garbage times 12, s[11..8], garbage times 12, s[7..4], garbage times 12, s[3..0]}
    VPANDD          Z18, Z5, Z5                                     // Z5  := uint32{bias[i]} for i in 15..0
    VPMULLD         Z4, Z16, Z16                                    // Z16 := uint32{(state[i] >> ansWordMBits) * freq[i]} for i in 15..0
    VPANDD          Z18, Z3, Z4                                     // Z4  := uint32{freq[i]} for i in 31..16
    VPSRLD          $const_ansWordMBits, Z3, Z8                 // Z8  := uint32{bits 11:0: bias[i], garbage otherwise} for i in 31..16
    VPSHUFB         Z20, Z3, Z3                                     // Z3  := uint8{garbage times 12, s[31..28], garbage times 12, s[27..24], garbage times 12, s[23..20], garbage times 12, s[19..16]}
    VPANDD          Z18, Z8, Z8                                     // Z8  := uint32{bias[i]} for i in 31..16
    VPMULLD         Z4, Z17, Z17                                    // Z17 := uint32{(state[i] >> ansWordMBits) * freq[i]} for i in 31..16
    VPADDD          Z5, Z16, Z16                                    // Z16 := uint32{denormalized_state[i]} for i in 15..0
    VPERMT2D        Z3, Z22, Z2                                     // Z2  := uint8{garbage times 32, s[31..0]}
    VPCMPUD         $VPCMP_IMM_LT, Z19, Z16, K1                     // K1  := {denormalized_state[i] < ansWordL} for i in 15..0
    VPSLLD          $const_ansWordLBits, Z16, Z6                    // Z6  := uint32{denormalized_state[i] << ansWordLBits} for i in 15..0
    VPEXPANDD.Z     Z11, K1, Z4                                     // Z4  := uint32{src_fwd[k] for denormalized_state[i] < ansWordL; 0 otherwise} for i in 15..0
    KMOVW           K1, AX                                          // AX  := {denormalized_state[i] < ansWordL} for i in 15..0
    VPADDD          Z8, Z17, Z17                                    // Z17 := uint32{denormalized_state[i]} for i in 31..16
    POPCNTL         AX, AX                                          // AX  := uint64{#consumed src_fwd items}
    VPCMPUD         $VPCMP_IMM_LT, Z19, Z17, K2                     // K2  := {denormalized_state[i] < ansWordL} for i in 31..16
    VPSLLD          $const_ansWordLBits, Z17, Z7                    // Z7  := uint32{denormalized_state[i] << ansWordLBits} for i in 31..16
    VPORD           Z6, Z4, K1, Z16                                 // Z16 := uint32{normalized_state[i]} for i in 15..0
    LEAQ            (SI)(AX*2), SI                                  // Skip the consumed src_fwd items
    VPEXPANDD.Z     Z12, K2, Z4                                     // Z4  := uint32{src_rev[k] for denormalized_state[i] < ansWordL; 0 otherwise} for i in 31..16
    VMOVDQU8        Y2, (DI)                                        // Store the result bytes 31..0
    ADDQ            $32, DI                                         // Advance output_cursor
    KMOVW           K2, AX                                          // AX  := {denormalized_state[i] < ansWordL} for i in 31..16
    POPCNTL         AX, AX                                          // AX  := uint64{#consumed src_rev items}
    SUBQ            AX, BX                                          // Skip the consumed src_rev items, part #1
    VPORD           Z7, Z4, K2, Z17                                 // Z17 := uint32{normalized_state[i]} for i in 31..16
    SUBQ            AX, BX                                          // Skip the consumed src_rev items, part #2
    CMPQ            DX, DI
    JA              loop

process_last_chunk:

    // 1..32 bytes remains for processing
    //
    // DX  := loop terminator
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z16 := uint32{state[i]} for i in 15..0
    // Z17 := uint32{state[i]} for i in 31..16
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z20 := const_result_extractor
    // Z22 := const_dword_compressor
    // K2  := remaining_lanes_mask
    // K6  := 0x0000ffff

    KMOVW           K6, K1                                          // for gather
    VPANDD          Z18, Z16, Z4                                    // Z4  := uint32{slot[i]} for in 15..0
    SUBQ            DI, DX                                          // DX  := uint64{remaining-32}
    VPGATHERDD      (R8)(Z4*4), K1, Z2                              // Z2  := uint32{tab[slot[i]]} for i in 15..0
    MOVL            $-1, AX                                         // AX  := uint32{-1}
    ADDL            $32, DX                                         // DX  := uint64{remaining}
    VPANDD          Z18, Z17, Z4                                    // Z4  := uint32{slot[i]} for in 31..16
    SHLXQ           DX, AX, AX                                      // AX  := uint32{^lane_mask}
    VPSHUFB         Z20, Z2, Z2                                     // Z2  := uint8{garbage times 12, s[15..12], garbage times 12, s[11..8], garbage times 12, s[7..4], garbage times 12, s[3..0]}
    VPGATHERDD      (R8)(Z4*4), K6, Z3                              // Z3  := uint32{tab[slot[i]]} for i in 31..16
    NOTL            AX                                              // AX  := uint32{lane_mask}
    VPSHUFB         Z20, Z3, Z3                                     // Z3  := uint8{garbage times 12, s[31..28], garbage times 12, s[27..24], garbage times 12, s[23..20], garbage times 12, s[19..16]}
    VPERMT2D        Z3, Z22, Z2                                     // Z2  := uint8{s[i]} for i in (remainder_len-1)..0
    KMOVD           AX, K2                                          // K2  := {lane_mask[i]} for i in 31..0
    VMOVDQU8        Y2, K2, (DI)
    MOVL            $const_ecOK, ret1+88(FP)                        // Set the error code
    RET

handle_short_input:

    // Process 0..32 input bytes
    //
    // BX  := uint64{cursorRev}
    // CX  := uint64{dstLen}
    // DX  := loop terminator
    // SI  := uint64{cursorFwd}
    // DI  := uint64{output_cursor}
    // R8  := uint64{tab base}
    // Z18 := uint32{(ansWordM - 1) times 16}
    // Z19 := uint32{ansWordL times 16}
    // Z20 := const_result_extractor
    // K5  := 0x55555555
    // K6  := 0x0000ffff

    TESTQ           CX, CX                                          // EFLAGS.ZF==1 <=> the content is empty
    JNZ             process_last_chunk                              // there are some bytes for processing
    MOVL            $const_ecOK, ret1+88(FP)                        // Set the error code
    RET


// func ansDecodeTableAVX512Generic(tab *ANSDenseTable, src []byte) ([]byte, errorCode)
TEXT ·ansDecodeTableAVX512Generic(SB), NOSPLIT | NOFRAME, $0-32
    MOVQ            src_base+8(FP), SI                              // SI  := uint64{src.Data}
    MOVQ            src_len+16(FP), R12                             // R12 := uint64{src.Len}
    MOVL            $0x07070707, AX                                 // AX  := uintt32{0x07070707}
    VMOVDQU8        CONST_GET_PTR(consts_dt_composite_transformer, 0), Z16 // Z16 := consts_dt_composite_transformer
    SUBQ            $96, R12                                        // Skip the 96 control block bytes
    JB              error_wrong_source_size                         // Handle the error
    VMOVDQU8        (SI)(R12*1), Z5                                 // Z5  := uint3{packed_ctrl[191..0]}
    VMOVDQU8        64(SI)(R12*1), Y3                               // Z3  := uint3{packed_ctrl[255..192]}
    VPBROADCASTD    AX, Z20                                         // Z20 := uint8{0x07*}
    VPTERNLOGD      $0xff, Z1, Z1, Z1                               // Z1  := {-1*}
    VPXORD          Y0, Y0, Y0                                      // Z0  := {0*}
    VBROADCASTI32X4 CONST_GET_PTR(consts_dt_24to32_expander, 0), Z6 // Z6  := consts_dt_24to32_expander
    VPSRLD          $4, Z16, Z4                                     // Z4  := transformation 1
    VPERMD          Z5, Z16, Z2                                     // Z2  := uint3{packed_ctrl[127..0]}
    VPSRLD          $26, Z1, Z17                                    // Z17 := uint32{0x0000003f times 16}
    VPERMT2D        Z5, Z4, Z3                                      // Z3  := uint3{packed_ctrl[255..128]}
    VPSLLD          $16, Z17, Z18                                   // Z18 := uint32{0x003f0000 times 16}
    VPSHUFB         Z6, Z2, Z2                                      // Z2  := uint32{packed_ctrl[i*24+23..i*24]} for i in 15..0
    VPSLLD          $4, Z20, Z21                                    // Z21 := uint8{0x70*}
    VPSHUFB         Z6, Z3, Z3                                      // Z3  := uint32{packed_ctrl[i*24+23..i*24]} for i in 31..16
    VPSLLD          $24, Z17, Z19                                   // Z19 := uint32{0x3f000000 times 16}
    VPORD           Z21, Z20, Z21                                   // Z21 := uint8{0x77*}
    VPSLLD          $2, Z2, Z4                                      // Position bit 6 of ctrl[i*8+2] at the byte i*4+1 boundary for i in 31..0
    MOVL            $(16 << const_ansWordMBits), AX             // AX  := uint32{bias_increment}
    VPSLLD          $4, Z2, Z5                                      // Position bit 12 of ctrl[i*8+4] at the byte i*4+2 boundary for i in 31..0
    VPTERNLOGD      $0b1101_1000, Z17, Z2, Z4                       // Z4  := Z17 ? Z2 : Z4
    VPSLLD          $6, Z2, Z6                                      // Position bit 18 of ctrl[i*8+6] at the byte i*4+3 boundary for i in 31..0
    VPTERNLOGD      $0b1101_1000, Z18, Z5, Z4                       // Z4  := Z18 ? Z5 : Z4
    VPBROADCASTD    AX, Z24                                         // Z24 := uint32{bias_increment times 16}
    VPSLLD          $2, Z3, Z5                                      // Position bit 6 of ctrl[i*8+2] at the byte i*4+1 boundary for i in 63..32
    VPTERNLOGD      $0b1101_1000, Z19, Z6, Z4                       // Z4  := Z19 ? Z6 : Z4
    VPSLLD          $4, Z3, Z6                                      // Position bit 12 of ctrl[i*8+4] at the byte i*4+2 boundary for i in 63..32
    VPADDD          Z4, Z4, Z2                                      // Position bit 3 of ctrl[i*8+1] at the nibble i*8+1 boundary for i in 31..0
    VPSLLD          $6, Z3, Z7                                      // Position bit 18 of ctrl[i*8+6] at the byte i*4+3 boundary for i in 63..32
    VPTERNLOGD      $0b1101_1000, Z20, Z4, Z2                       // Z2  := Z20 ? Z4 : Z2
    VPTERNLOGD      $0b1101_1000, Z17, Z3, Z5                       // Z5  := Z17 ? Z3 : Z5
    VPANDD          Z21, Z2, Z2                                     // Z2  := uint4{ctrl[127..0]}
    VPTERNLOGD      $0b1101_1000, Z18, Z6, Z5                       // Z5  := Z18 ? Z6 : Z5
    VMOVDQU16       CONST_GET_PTR(consts_dt_offset_expander, 0), Y23// Y23 := consts_dt_offset_expander
    VPTERNLOGD      $0b1101_1000, Z19, Z7, Z5                       // Z5  := Z19 ? Z7 : Z5
    VPSLLD          $(20-const_ansWordMBits), Z24, Z25          // Z25 := uint32{sym_increment}
    VPADDD          Z5, Z5, Z3                                      // Position bit 3 of ctrl[i*8+1] at the nibble i*8+1 boundary for i in 63..32
    LEAQ            4(R12*8), R14                                   // R14 := uint64{bitCursor}
    VPTERNLOGD      $0b1101_1000, Z20, Z5, Z3                       // Z3  := Z20 ? Z5 : Z3
    VPANDD          Z21, Z3, Z3                                     // Z3  := uint4{ctrl[255..128]}
    VMOVDQU32       CONST_GET_PTR(consts_dt_slot_template, 0), Z17  // Z17 := uint32{consts_dt_slot_template}
    MOVQ            tab+0(FP), DI                                   // DI  := uint64{tableCursor}
    MOVQ            $-1, R15                                        // R15 := uint64{0xffff_ffff_ffff_ffff}

    // The entire control block has been decoded and is stored in the form of 256 nibbles in Z3:Z2.
    // The processing context is as follows:
    //
    // DI  := uint64{tableCursor}
    // SI  := uint64{src.Data}
    // R12 := uint64{byteCursor}
    // R14 := uint64{bitCursor}
    // R15 := uint64{0xffff_ffff_ffff_ffff}
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z2  := uint4{ctrl[127..0]}
    // Z3  := uint4{ctrl[255..128]}
    // Z17 := uint32{consts_dt_slot_template}
    // Z20 := uint8{0x07*}
    // Y23 := uint16{offset[0:7], expander[7:0]}
    // Z24 := uint32{bias_increment times 16}
    // Z25 := uint32{sym_increment}

decode_freqs:
    // Decode 16 freq values at a time. That consumes 16 nibbles (8 bytes) from the control block stream in Z3:Z2
    // and up to 49 nibbles from the data stream. That's because every control nibble can require up to 3 data nibbles
    // (16*3=48) and the stream reading can start at a position not aligned to a byte boundary, hence one more nibble
    // will need to be shifted in/out in the re-alignment process. Therefore, at least 33 bytes need to be fetched.
    // CAUTION: the nibbles are stored in reversed little endian order (i.e. little endian read right to left}!

    VPMOVZXBD       X2, Y4                                          // Y4  := uint32{(ctrl[i*2+1] << 4) | ctrl[i*2]} for i in 7..0
    LEAL            4(R14), CX                                      // CX  := uint32{bit 2: nibble_correction_amount, garbage otherwise}
    CMPQ            R12, $32                                        // Can a full 32-byte block be fethed safely?
    JLT             fetch_partial                                   // partial read before the buffer base address
    MOVBEQ          -8(SI)(R12*1), BX                               // BX  := uint4{data[15:0]}
    VALIGNQ         $1, Z2, Z3, Z2                                  // Skip 16 ctrl nibbles, part 1
    MOVBEQ          -16(SI)(R12*1), DX                              // DX  := uint4{data[31:16]}
    ANDB            $0x04, CX                                       // CX  := uint8{nibble_correction_amount}
    VALIGNQ         $1, Z3, Z1, Z3                                  // Skip 16 ctrl nibbles, part 2
    MOVBEQ          -24(SI)(R12*1), R8                              // R8  := uint4{data[47:32]}
    MOVBQZX         -25(SI)(R12*1), AX                              // AX  := uint4{data[49:48]}

data_fetched:
    VPSLLD          $12, Y4, Y5                                     // Y5  := uint32{(ctrl[i*2+1] << 16) | (ctrl[i*2] << 12)} for i in 7..0
    VPTERNLOGD      $0b1010_1000, Y20, Y5, Y4                       // Y4  := uint16{ctrl[15..0]}
    SHRQ            CX, BX:DX
    VPERMW          Y23, Y4, Y5                                     // Y5  := uint16{extractor[ctrl[15..0]]}
    VPXORD          Y1, Y4, Y4                                      // Y4  := uint16{^ctrl[15..0]}
    SHRQ            CX, DX:R8
    VPERMW          Y23, Y4, Y4                                     // Y4  := uint16{offset[^ctrl[15..0]]}
    SHRQ            CX, R8:AX

    // Y4 contains 16 offsets, Y5 16 extractors, R8:DX:BX 48 corrected data nibbles

    VPEXTRQ         $0, X5, CX                                      // CX  := uint64{extractors[3..0]}
    PDEPQ           CX, BX, AX                                      // AX  := uint64{extracted nibbles}
    VPEXTRQ         $1, X5, R9                                      // R9  := uint64{extractors[7..4]}
    POPCNTQ         CX, CX                                          // CX  := uint64{#extractor set bits}
    VEXTRACTI128    $1, Y5, X5                                      // X5  := uint64{{extractors[15..8]}
    SHRQ            CX, BX:DX                                       // Discard CX data bits part #1
    SUBQ            CX, R14                                         // Update bitCursor
    VPINSRQ         $0, AX, X0, X6                                  // X6  := uint16{0 times 4, base[3..0]}
    PDEPQ           R9, BX, AX                                      // AX  := uint64{extracted nibbles}
    SHRQ            CX, DX:R8                                       // Discard CX data bits part #2
    SHRQ            CX, R8                                          // Discard CX data bits part #3. Up to 48-12=36 nibbles remain in R8:DX:BX.
    POPCNTQ         R9, CX                                          // CX  := uint64{#extractor set bits}
    VPEXTRQ         $0, X5, R9                                      // R9  := uint64{extractors[11..8]}
    VPINSRQ         $1, AX, X6, X6                                  // X6  := uint16{base[7..0]}
    SHRQ            CX, BX:DX                                       // Discard CX data bits part #1
    SUBQ            CX, R14                                         // Update bitCursor
    PDEPQ           R9, BX, AX                                      // AX  := uint64{extracted nibbles}
    SHRQ            CX, DX:R8                                       // Discard CX data bits part #2. Up to 36-12=24 nibbles remain in DX:BX; R8 is no longer needed.
    POPCNTQ         R9, CX                                          // CX  := uint64{#extractor set bits}
    VPEXTRQ         $1, X5, R9                                      // R9  := uint64{extractors[15..12]}
    SUBQ            CX, R14                                         // Update bitCursor
    SHRQ            CX, BX:DX                                       // Discard CX data bits part. Up to 24-12=12 nibbles remain in BX; DX is no longer needed.
    VPINSRQ         $0, AX, X0, X5                                  // X5  := uint16{0 times 4, base[11..8]}
    PDEPQ           R9, BX, AX                                      // AX  := uint64{extracted nibbles}
    POPCNTQ         R9, CX                                          // CX  := uint64{#extractor set bits}
    SUBQ            CX, R14                                         // Update bitCursor
    VPINSRQ         $1, AX, X5, X5                                  // X5  := uint16{base[15..8]}
    MOVQ            R14, R12                                        // R12 := uint64{bitCursor}
    VINSERTI128     $1, X5, Y6, Y5                                  // Y5  := uint16{base[15..0]}
    SHRQ            $3, R12                                         // R12 := Transform bitCursor to byteCursor
    VPADDW          Y4, Y5, Y4                                      // Y4  := uint16{freq[15..0]}
    VPEXTRW         $0, X4, CX                                      // CX  := uint32{freq[0]}
    VPMOVZXWD       Y4, Z4                                          // Z4  := uint32{freq[15..0]}

    // Y4 contains the freq[i] values necessary for further processing.

process_sym_loop:
    VPBROADCASTD    CX, Z5                                          // Z5  := uint32{freq times 16}
    MOVL            CX, DX                                          // DX  := uint32{slotCounter}
    ANDL            $0x8000_000f, CX                                // EFLAGS.SF==1 <=> out_of_freqs
    JS              out_of_freqs                                    // Exhausted the freqs vector
    SHLXL           CX, R15, AX                                     // AX  := uint32{^write_mask}
    TESTL           DX, DX                                          // EFLAGS.ZF==1 <=> freq==0
    JZ              next_sym                                        // freq==0, skip the writes
    NOTL            AX                                              // AX  := uint32{write_mask}
    VPORD           Z17, Z5, Z5                                     // Z5  := uint32{slot_value[15..0]}
    KMOVW           AX, K1                                          // K1  := uint16{write_mask}
    SUBL            $16, DX                                         // Update slotCounter
    JAE             process_sym_loop_long                           // Fill at least 16 slots

process_sym_loop_short:
    VMOVDQU32       Z5, K1, (DI)                                    // Fill the remainder of 1..15 slots
    LEAQ            (DI)(CX*4), DI                                  // Update tableCursor

next_sym:
    VPEXTRD         $1, X4, CX                                      // CX  := uint32{freq[1]}
    VALIGND         $1, Z4, Z1, Z4                                  // Skip the currently consumed freq[0]
    VPADDD          Z25, Z17, Z17                                   // Update syms in the slot template
    JMP             process_sym_loop

process_sym_loop_long:
    VMOVDQU32       Z5, (DI)                                        // Unconditionally fill 16 tab slots
    VPADDD          Z24, Z5, Z5                                     // Update biases in the slot template
    ADDQ            $64, DI                                         // Update tableCursor
    SUBL            $16, DX                                         // Update slotCounter
    JAE             process_sym_loop_long                           // At least 16 slots still remain

    // Less than 16 slots remain
    TESTL           AX, AX                                          // Check the size of the remainder
    JNZ             process_sym_loop_short                          // There is a non-empty remainder
    JMP             next_sym                                        // No more slots to fill

out_of_freqs:
    VPTEST          X1, X2                                          // EFLAGS.CF==1 <=> X2=={-1*}
    JNC             decode_freqs                                    // Continue processing

    // Table building completed

    LEAQ            -4(R14), DX                                     // Prepare for rounding up
    MOVQ            SI, ret_base+32(FP)                             // Set ret_base of the output slice
    MOVQ            src_cap+24(FP), AX                              // AX  := uint64{src.Cap}
    SHRQ            $3, DX                                          // Round up to the whole number of bytes
    MOVQ            AX, ret_cap+48(FP)                              // Set ret_cap of the output slice
    MOVQ            DX, ret_len+40(FP)                              // Set ret_len of the output slice
    MOVL            $const_ecOK, ret1+56(FP)                        // Set the error code
    RET


fetch_partial:

    // The data access crosses the lower buffer boundary, so this case must be handled in a special way.
    // Note that it can only happen when working with an isolated serialized table. Usually the table is
    // appended to an ANS-compressed data block, which provide sufficient padding for generic handling.
    // Therefore, the performance of this handler is largely irrelevant.
    //
    // CX  := uint32{bit 2: nibble_correction_amount, garbage otherwise}
    // DI  := uint64{tableCursor}
    // SI  := uint64{src.Data}
    // R12 := uint64{byteCursor}
    // R14 := uint64{bitCursor}
    // R15 := uint64{0xffff_ffff_ffff_ffff}
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z2  := uint4{ctrl[127..0]}
    // Z3  := uint4{ctrl[255..128]}
    // Y4  := uint32{(ctrl[i*2+1] << 4) | ctrl[i*2]} for i in 7..0
    // Z20 := uint8{0x07*}
    // Y23 := uint16{offset[0:7], expander[7:0]}

    TESTQ           R12, R12                                        // EFLAGS.SF==1 <=> R12 < 0
    SHRXL           R12, R15, AX                                    // AX  := uint32{^fetch_mask}
    VALIGNQ         $1, Z2, Z3, Z2                                  // Skip 16 ctrl nibbles, part 1
    CMOVLMI         R15, AX                                         // For negative offsts fetch_mask must be 0
    VALIGNQ         $1, Z3, Z1, Z3                                  // Skip 16 ctrl nibbles, part 2
    NOTL            AX                                              // AX  := uint32{fetch_mask}
    ANDB            $0x04, CX                                       // CX  := uint8{nibble_correction_amount}
    KMOVD           AX, K1                                          // K1  := uint32{fetch_mask}
    VMOVDQU8.Z      -32(SI)(R12*1), K1, Y5                          // Y5  := uint8{data[31-i] if K[31-i] == 1; otherwise 0} for i in 31..0
    VEXTRACTI128    $1, Y5, X6                                      // X6  := uint64{data[15-i]} for i in 15..0
    VPEXTRQ         $0, X5, AX                                      // AX  := uint4{reversed data[63:48]}
    VPEXTRQ         $1, X5, R8                                      // R8  := uint4{reversed data[47:32]}
    VPEXTRQ         $0, X6, DX                                      // DX  := uint4{reversed data[31:16]}
    VPEXTRQ         $1, X6, BX                                      // BX  := uint4{reversed data[15:0]}
    BSWAPQ          AX                                              // AX  := uint4{data[63:48]}
    BSWAPQ          R8                                              // R8  := uint4{data[47:32]}
    BSWAPQ          DX                                              // DX  := uint4{data[31:16]}
    BSWAPQ          BX                                              // BX  := uint4{data[15:0]}
    JMP             data_fetched

error_wrong_source_size:
    MOVL            $const_ecWrongSourceSize, ret1+56(FP)           // Set the error code
    // fallthrough

error_done:
    XORL            AX, AX
    MOVQ            AX, ret_base+32(FP)                             // Set ret_base of the output slice
    MOVQ            AX, ret_len+40(FP)                              // Set ret_len of the output slice
    MOVQ            AX, ret_cap+48(FP)                              // Set ret_cap of the output slice
    RET


CONST_DATA_U32(const_result_extractor,  (0*4),  $0x0f0b0703)
CONST_DATA_U32(const_result_extractor,  (1*4),  $0x1f1b1713)
CONST_DATA_U32(const_result_extractor,  (2*4),  $0x2f2b2723)
CONST_DATA_U32(const_result_extractor,  (3*4),  $0x3f3b3733)
CONST_DATA_U32(const_result_extractor,  (4*4),  $0x4f4b4743)
CONST_DATA_U32(const_result_extractor,  (5*4),  $0x5f5b5753)
CONST_DATA_U32(const_result_extractor,  (6*4),  $0x6f6b6763)
CONST_DATA_U32(const_result_extractor,  (7*4),  $0x7f7b7773)
CONST_DATA_U32(const_result_extractor,  (8*4),  $0x0f0b0703)
CONST_DATA_U32(const_result_extractor,  (9*4),  $0x1f1b1713)
CONST_DATA_U32(const_result_extractor, (10*4),  $0x2f2b2723)
CONST_DATA_U32(const_result_extractor, (11*4),  $0x3f3b3733)
CONST_DATA_U32(const_result_extractor, (12*4),  $0x4f4b4743)
CONST_DATA_U32(const_result_extractor, (13*4),  $0x5f5b5753)
CONST_DATA_U32(const_result_extractor, (14*4),  $0x6f6b6763)
CONST_DATA_U32(const_result_extractor, (15*4),  $0x7f7b7773)
CONST_GLOBAL(const_result_extractor, $64)

CONST_DATA_U32(const_invert_zxwd,  (0*4),  $0x001f_000f)
CONST_DATA_U32(const_invert_zxwd,  (1*4),  $0x001f_000e)
CONST_DATA_U32(const_invert_zxwd,  (2*4),  $0x001f_000d)
CONST_DATA_U32(const_invert_zxwd,  (3*4),  $0x001f_000c)
CONST_DATA_U32(const_invert_zxwd,  (4*4),  $0x001f_000b)
CONST_DATA_U32(const_invert_zxwd,  (5*4),  $0x001f_000a)
CONST_DATA_U32(const_invert_zxwd,  (6*4),  $0x001f_0009)
CONST_DATA_U32(const_invert_zxwd,  (7*4),  $0x001f_0008)
CONST_DATA_U32(const_invert_zxwd,  (8*4),  $0x001f_0007)
CONST_DATA_U32(const_invert_zxwd,  (9*4),  $0x001f_0006)
CONST_DATA_U32(const_invert_zxwd, (10*4),  $0x001f_0005)
CONST_DATA_U32(const_invert_zxwd, (11*4),  $0x001f_0004)
CONST_DATA_U32(const_invert_zxwd, (12*4),  $0x001f_0003)
CONST_DATA_U32(const_invert_zxwd, (13*4),  $0x001f_0002)
CONST_DATA_U32(const_invert_zxwd, (14*4),  $0x001f_0001)
CONST_DATA_U32(const_invert_zxwd, (15*4),  $0x001f_0000)
CONST_GLOBAL(const_invert_zxwd, $64)

CONST_DATA_U32(const_dword_compressor,  (0*4),  $0x00)
CONST_DATA_U32(const_dword_compressor,  (1*4),  $0x04)
CONST_DATA_U32(const_dword_compressor,  (2*4),  $0x08)
CONST_DATA_U32(const_dword_compressor,  (3*4),  $0x0c)
CONST_DATA_U32(const_dword_compressor,  (4*4),  $0x10)
CONST_DATA_U32(const_dword_compressor,  (5*4),  $0x14)
CONST_DATA_U32(const_dword_compressor,  (6*4),  $0x18)
CONST_DATA_U32(const_dword_compressor,  (7*4),  $0x1c)
CONST_DATA_U32(const_dword_compressor,  (8*4),  $0x40)
CONST_DATA_U32(const_dword_compressor,  (9*4),  $0x24)
CONST_DATA_U32(const_dword_compressor, (10*4),  $0x28)
CONST_DATA_U32(const_dword_compressor, (11*4),  $0x2c)
CONST_DATA_U32(const_dword_compressor, (12*4),  $0x30)
CONST_DATA_U32(const_dword_compressor, (13*4),  $0x34)
CONST_DATA_U32(const_dword_compressor, (14*4),  $0x38)
CONST_DATA_U32(const_dword_compressor, (15*4),  $0x3c)
CONST_GLOBAL(const_dword_compressor, $64)

CONST_DATA_U32(consts_dt_composite_transformer,  (0*4),  $0b11100_0000)
CONST_DATA_U32(consts_dt_composite_transformer,  (1*4),  $0b11101_0001)
CONST_DATA_U32(consts_dt_composite_transformer,  (2*4),  $0b11110_0010)
CONST_DATA_U32(consts_dt_composite_transformer,  (3*4),  $0b00000_0000)
CONST_DATA_U32(consts_dt_composite_transformer,  (4*4),  $0b11111_0011)
CONST_DATA_U32(consts_dt_composite_transformer,  (5*4),  $0b00000_0100)
CONST_DATA_U32(consts_dt_composite_transformer,  (6*4),  $0b00001_0101)
CONST_DATA_U32(consts_dt_composite_transformer,  (7*4),  $0b00000_0000)
CONST_DATA_U32(consts_dt_composite_transformer,  (8*4),  $0b00010_0110)
CONST_DATA_U32(consts_dt_composite_transformer,  (9*4),  $0b00011_0111)
CONST_DATA_U32(consts_dt_composite_transformer, (10*4),  $0b00100_1000)
CONST_DATA_U32(consts_dt_composite_transformer, (11*4),  $0b00000_0000)
CONST_DATA_U32(consts_dt_composite_transformer, (12*4),  $0b00101_1001)
CONST_DATA_U32(consts_dt_composite_transformer, (13*4),  $0b00110_1010)
CONST_DATA_U32(consts_dt_composite_transformer, (14*4),  $0b00111_1011)
CONST_DATA_U32(consts_dt_composite_transformer, (15*4),  $0b00000_0000)
CONST_GLOBAL(consts_dt_composite_transformer, $64)

CONST_DATA_U32(consts_dt_slot_template,  (0*4),  $(0 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (1*4),  $(1 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (2*4),  $(2 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (3*4),  $(3 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (4*4),  $(4 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (5*4),  $(5 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (6*4),  $(6 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (7*4),  $(7 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (8*4),  $(8 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template,  (9*4),  $(9 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (10*4), $(10 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (11*4), $(11 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (12*4), $(12 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (13*4), $(13 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (14*4), $(14 << const_ansWordMBits))
CONST_DATA_U32(consts_dt_slot_template, (15*4), $(15 << const_ansWordMBits))
CONST_GLOBAL(consts_dt_slot_template, $64)

CONST_DATA_U16(consts_dt_offset_expander,  (0*2),  $0x0000)
CONST_DATA_U16(consts_dt_offset_expander,  (1*2),  $0x0000)
CONST_DATA_U16(consts_dt_offset_expander,  (2*2),  $0x0000)
CONST_DATA_U16(consts_dt_offset_expander,  (3*2),  $0x0000)
CONST_DATA_U16(consts_dt_offset_expander,  (4*2),  $0x0000)
CONST_DATA_U16(consts_dt_offset_expander,  (5*2),  $0x000f)
CONST_DATA_U16(consts_dt_offset_expander,  (6*2),  $0x00ff)
CONST_DATA_U16(consts_dt_offset_expander,  (7*2),  $0x0fff)
CONST_DATA_U16(consts_dt_offset_expander,  (8*2),  $0x0115)
CONST_DATA_U16(consts_dt_offset_expander,  (9*2),  $0x0015)
CONST_DATA_U16(consts_dt_offset_expander,  (10*2), $0x0005)
CONST_DATA_U16(consts_dt_offset_expander,  (11*2), $0x0004)
CONST_DATA_U16(consts_dt_offset_expander,  (12*2), $0x0003)
CONST_DATA_U16(consts_dt_offset_expander,  (13*2), $0x0002)
CONST_DATA_U16(consts_dt_offset_expander,  (14*2), $0x0001)
CONST_DATA_U16(consts_dt_offset_expander,  (15*2), $0x0000)
CONST_GLOBAL(consts_dt_offset_expander, $32)

CONST_DATA_U32(consts_dt_24to32_expander,  (0*4),  $0xff02_0100)
CONST_DATA_U32(consts_dt_24to32_expander,  (1*4),  $0xff05_0403)
CONST_DATA_U32(consts_dt_24to32_expander,  (2*4),  $0xff08_0706)
CONST_DATA_U32(consts_dt_24to32_expander,  (3*4),  $0xff0b_0a09)
CONST_GLOBAL(consts_dt_24to32_expander, $16)

CONST_DATA_U32(consts_enc_composite_transformer,   (0*4), $0b1111)
CONST_DATA_U32(consts_enc_composite_transformer,   (1*4), $0b1110)
CONST_DATA_U32(consts_enc_composite_transformer,   (2*4), $0b1101)
CONST_DATA_U32(consts_enc_composite_transformer,   (3*4), $0b1100)
CONST_DATA_U32(consts_enc_composite_transformer,   (4*4), $0b1011)
CONST_DATA_U32(consts_enc_composite_transformer,   (5*4), $0b1010)
CONST_DATA_U32(consts_enc_composite_transformer,   (6*4), $0b1001)
CONST_DATA_U32(consts_enc_composite_transformer,   (7*4), $0b1000)
CONST_DATA_U32(consts_enc_composite_transformer,   (8*4), $0b0111)
CONST_DATA_U32(consts_enc_composite_transformer,   (9*4), $0b0110)
CONST_DATA_U32(consts_enc_composite_transformer,  (10*4), $0b0101)
CONST_DATA_U32(consts_enc_composite_transformer,  (11*4), $0b0100)
CONST_DATA_U32(consts_enc_composite_transformer,  (12*4), $0b0011)
CONST_DATA_U32(consts_enc_composite_transformer,  (13*4), $0b0010)
CONST_DATA_U32(consts_enc_composite_transformer,  (14*4), $0b0001)
CONST_DATA_U32(consts_enc_composite_transformer,  (15*4), $0b0000)
CONST_GLOBAL(consts_enc_composite_transformer, $64)

CONST_DATA_U32(consts_enc_byte_in_word_inverter,   (0*4), $0x02030001)
CONST_DATA_U32(consts_enc_byte_in_word_inverter,   (1*4), $0x06070405)
CONST_DATA_U32(consts_enc_byte_in_word_inverter,   (2*4), $0x0a0b0809)
CONST_DATA_U32(consts_enc_byte_in_word_inverter,   (3*4), $0x0e0f0c0d)
CONST_GLOBAL(consts_enc_byte_in_word_inverter, $16)

CONST_DATA_U32(consts_enc_byte_in_dword_inverter,   (0*4), $0x00010203)
CONST_DATA_U32(consts_enc_byte_in_dword_inverter,   (1*4), $0x04050607)
CONST_DATA_U32(consts_enc_byte_in_dword_inverter,   (2*4), $0x08090a0b)
CONST_DATA_U32(consts_enc_byte_in_dword_inverter,   (3*4), $0x0c0d0e0f)
CONST_GLOBAL(consts_enc_byte_in_dword_inverter, $16)
