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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"
#include "../internal/asmutils/bc_imm_amd64.h"
#include "../internal/asmutils/ion_constants_amd64.h"


// func zionflattenAVX512BranchlessVarUint(shape []byte, buckets *zll.Buckets, fields [][]vmref, tape []ion.Symbol) int
TEXT ·zionflattenAVX512BranchlessVarUint(SB), NOSPLIT|NOFRAME, $0-0
    NO_LOCAL_POINTERS
    MOVQ            tape_len+64(FP), AX                         // AX := uint64{tape.Len}
    MOVQ            buckets+24(FP), BX                          // BX  := uint64{&buckets}
    MOVL            $0xaaaa, CX                                 // CX  := uint32{0xaaaa}
    MOVQ            tape_base+56(FP), R14                       // R14 := uint64{&tape[0]} (first symbol to match)
    VPXORQ          X0, X0, X0                                  // Z0 := {0*}
    KMOVW           CX, K5                                      // K5  := uint16{0xaaaa}
    MOVQ            $-1, DX
    MOVQ            fields+32(FP), DI                           // DI  := uint64{&fields[0]} (incremented later)
    BZHIQ           AX, DX, CX                                  // CX  := uint64{set tape.Len least significant bits}
    VMOVDQU32       const_zllBucketPos(BX), Z16                 // Z16 := int32{buckets.Pos[15..0]}
    SHLQ            $(const_zionStrideLog2 + 3), AX             // AX  := uint64{the number of bytes to pre-zero}
    VMOVDQU32       CONST_GET_PTR(consts_identity_d, 0), Z22    // Z22 := uint32{consts_identity_d}
    VPTERNLOGD      $0xff, Z1, Z1, Z1                           // Z1  := {-1*}
    MOVQ            const_zllBucketDecompressed(BX), R9         // R9  := uint64{&zll.Buckets.Decompressed[0]}
    KMOVW           CX, K7                                      // K7  := uint16{set tape.Len least significant bits}
    SHRL            $8, CX
    LEAQ            -64(DI)(AX*1), DX
    KMOVW           CX, K2
    VPSRLD          $27, Z1, Z21                                // Z21 := uint32{0x1f times 16}
    VMOVDQU8        Z0, (DX)                                    // n > 64, so the last (possibly unaligned) chunk can be filled unconditionally
    VPCMPD          $VPCMP_IMM_GT, Z1, Z16, K1                  // K1  := uint16{buckets.Pos[15..0] >= 0}
    VPADDD          Z22, Z22, Z2                                // Z2  := uint32{i * 2} for i in 15..0
    VMOVDQU64.Z     (R14), K7, Z15                              // Z15 := uint64{tape[i] if i < tape.Len; 0 otherwise} for i in 7..0
    LEAQ            -1(DX), CX
    TESTB           $0x3f, DX
    VMOVDQU64.Z     64(R14), K2, Z3                             // Z3 := uint64{tape[i] if i < tape.Len; 0 otherwise} for i in 15..8
    CMOVQEQ         CX, DX
    MOVQ            shape_base+0(FP), SI                        // SI  := uint64{shape_cursor}
    MOVQ            shape_len+8(FP), R8                         // R8  := uint64{shape.Len}
    ANDQ            $-64, DX
    VPBROADCASTMW2D K1, Z14                                     // Z14 := uint32{(buckets.Pos[15..0] >= 0) times 16}
    VMOVDQU8        CONST_GET_PTR(consts_nibble_shuffle, 0), Z23// Z23 := uint32{consts_nibble_shuffle}
    VPERMT2D        Z3, Z2, Z15                                 // Z15 := uint32{tape[i] if i < tape.Len; 0 otherwise} for i in 15..0
    VPSRLD          $24, Z1, Z24                                // Z24 := uint32{0x0000_00ff times 16}
    LEAQ            CONST_GET_PTR(consts_byte_ion_size_b, 0x00), R10 // R10 := uint64{&consts_byte_ion_size_b}
    VPSRLD          $28, Z1, Z20                                // Z20 := uint32{0x0f times 16}
    ADDQ            SI, R8                                      // R8  := uint64{shape_cursor_end}
    MOVQ            ·vmm+0(SB), R14                             // R14 := uint64{vmm_base}
    XORL            R12, R12                                    // R12 := uint64{processed_count}
    MOVL            $0x7f7f7f7f, R13                            // R13 := uint32{0x7f7f7f7f}
    KMOVW           K7, K6                                      // K6  := uint16{still active tape lanes}

    // BX  := uint64{&buckets}
    // DX  := uint64{fields_prezero_cursor}
    // SI  := uint64{shape_cursor}
    // DI  := uint64{&fields[0]} (incremented later)
    // R8  := uint64{shape_cursor_end}
    // R9  := uint64{&zll.Buckets.Decompressed[0]}
    // R10 := uint64{&consts_byte_ion_size_b}
    // R12 := uint64{processed_count*8}
    // R13 := uint32{0x7f7f7f7f}
    // R14 := uint64{vmm_base}
    // K5  := uint16{0xaaaa}
    // K6  := uint16{still active tape lanes}
    // K7  := uint16{set tape.Len least significant bits}
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z14 := uint32{(buckets.Pos[15..0] >= 0) times 16}
    // Z15 := uint32{tape[i] if i < tape.Len; 0 otherwise} for i in 15..0
    // Z16 := int32{buckets.Pos[15..0]}
    // Z20 := uint32{0x0f times 16}
    // Z21 := uint32{0x1f times 16}
    // Z22 := uint32{consts_identity_d}
    // Z23 := uint32{consts_nibble_shuffle}
    // Z24 := uint32{0x0000_00ff times 16}

prezero_aligned_loop:
    CMPQ            DX, DI
    JCS             prezero_loop_completed
    VMOVDQA64       Z0, (DX)
    SUBQ            $64, DX
    JMP             prezero_aligned_loop

prezero_loop_completed:
    VMOVDQU8        Z0, (DI)                                    // n > 64, so the finishing store cannot write out of the buffer

shape_loop:
    CMPQ            SI, R8
    JAE             processing_succeeded
    VPANDD.BCST     (SI), Z21, Z3                               // Z3  := uint32{fc times 16}
    VPBROADCASTQ    1(SI), Z2                                   // Z2  := uint64{shape[8..1] times 8}
    VMOVD           X3, R15                                     // R15 := uint32{fc}
    VPSHUFB         Z23, Z2, Z2                                 // Z2  := uint32{shape[i] times 2} for i in 8..1
    VPSUBD          Z3, Z22, Z3                                 // Z3  := int32{<0 when (i < fc); >=0 otherwise} for i in 15..0
    VPSRLD          $4, Z2, K5, Z2                              // Z2  := uint4{0 times 7, garbage, b[i]} for i in 15..0
    VPSRLD          $31, Z3, Z3                                 // Z3  := int32{1 when (i < fc); 0 otherwise} for i in 15..0
    VPANDD          Z20, Z2, Z2                                 // Z2  := uint32{b[i]} for i in 15..0
    VPSLLVD         Z2, Z3, Z3                                  // Z3  := uint32{1 << b[i]} for i in 15..0
    LEAL            3(R15), CX                                  // CX  := uint32{fc + 3}
    SUBL            $16, R15                                    // R15 := uint16{0 <=> fc == 16}
    JA              processing_failed                           // fc > 16
    SHRL            $1, CX                                      // CX  := uint64{skip := int((fc + 3) / 2)}
    VPTESTMD        Z14, Z3, K4                                 // K4  := uint16{active_lanes}
    ADDQ            CX, SI                                      // shape_cursor += skip
    KTESTW          K4, K4                                      // EFLAGS.ZF==0 <=> there are fields to extract
    JZ              field_loop_completed                        // no fields to extract

    // There are fields to extract. Assume there are no conflicts and start greedy parsing of streams, resolving conflicts later.

    VPCOMPRESSD.Z   Z2, K4, Z2                                  // Z2  := uint32{b[i]} for i in 15..0

field_loop:
    VPERMD          Z16, Z2, Z3                                 // Z3  := uint32{garbage times 15, buckets.Pos[b[0]]}
    KXNORW          K1, K1, K1                                  // K1  := uint16{0xffff}
    VMOVD           X2, AX                                      // AX  := uint64{b[0]}
    KADDW           K1, K4, K1                                  // K1  := uint16{K4 - 1}
    XORL            CX, CX                                      // CX  := uint32{0}
    VALIGND         $1, Z2, Z1, Z2                              // Drop the first queue item
    BTSL            AX, CX                                      // CX  := uint32{1 << b[0]}
    VMOVD           X3, R11                                     // R11 := uint64{buckets.Pos[b[0]]}
    KANDW           K4, K1, K4                                  // K4  := uint16{K4 & (K4 - 1)}

    // Decode the symbol varuint

    MOVL            (R9)(R11*1), DX                             // DX  := uint8{varuint[0..3]}
    KMOVW           CX, K3                                      // K3  := uint16{1 << b[0]}
    TESTB           $0x80, DX
    JZ              decode_multibyte_symbol
    ANDL            $0x7f, DX
    MOVBLZX         1(R9)(R11*1), CX                            // CX  := uint32{ION.Value}
    VPBROADCASTD    DX, Z3                                      // Z3  := uint32{symbol times 16}
    INCQ            R11

symbol_decoded:

    // Tape matching

    MOVBLZX         (R10)(CX*1), AX                             // AX  := uint64{consts_byte_ion_size_b[ION.Value]}
    VPCMPUD         $VPCMP_IMM_GT, Z15, Z3, K6, K1              // K1  := uint16{sym > tape[i]} for i in 15..0
    LEAQ            (R9)(R11*1), DX                             // DX  := uint64{&object}
    VPCMPUD         $VPCMP_IMM_EQ, Z15, Z3, K6, K2              // K2  := uint16{sym == tape[i]} for i in 15..0
    TESTB           $ION_SIZE_INVALID, AX                       // EFLAGS.ZF == 0 <=> the ION_SIZE_INVALID flag is set
    JNZ             processing_failed                           // The ION_SIZE_INVALID flag is set
    TESTB           $ION_SIZE_VARUINT, AX                       // EFLAGS.ZF == 0 <=> the ION_SIZE_VARUINT flag is set
    JNZ             decode_varuint_size
    INCL            AX                                          // AX  := uint32{fixed_content_size + 1}

size_decoded:
    KMOVW           K1, CX                                      // CX  := uint32{sym > tape[i]} for i in 15..0
    ADDQ            AX, R11                                     // bucketCursor += #object_bytes
    KANDNW          K6, K1, K6                                  // Mark the sym > tape[i] lanes as processed
    VPBROADCASTD    R11, K3, Z16                                // Update buckets.Pos[b[0]]
    POPCNTL         CX, CX                                      // CX  := uint32{the number of sym > tape[i] lanes} for i in 15..0
    KANDNW          K6, K2, K6                                  // Mark the sym == tape[i] lanes as processed
    SHLL            $(const_zionStrideLog2 + 3), CX             // CX  := CX * 8 * zionStride
    ADDQ            CX, DI
    KTESTW          K2, K2
    JZ              store_completed

    // Store the vmref

    MOVL            AX, 4(DI)(R12*1)                            // fields[struct].len = rax
    SUBQ            R14, DX                                     // DX  := uint32{&object - vmm_base}
    JS              trap                                        // DX should always be within vmm...
    MOVL            DX, 0(DI)(R12*1)                            // fields[struct].off = (&object - vmm)
    ADDQ            $(const_zionStride * 8), DI

store_completed:
    KTESTW          K4, K4
    JNZ             field_loop

field_loop_completed:
    TESTL           R15, R15
    JZ              shape_loop                                  // fc == 16

    ADDL            $8, R12
    KMOVW           K7, K6                                      // K6  := uint16{still active tape lanes}
    MOVQ            fields+32(FP), DI                           // DI  := uint64{&fields[0]} (incremented later)
    CMPL            R12, $(const_zionStride * 8)
    JNZ             shape_loop

    // Processing completed successfully

processing_succeeded:
    SUBQ            shape_base+0(FP), SI                        // SI  := #shape bytes consumed

epilogue:
    SHRL            $3, R12
    VMOVDQU32       Z16, const_zllBucketPos(BX)                 // Update buckets.Pos[15..0]
    MOVQ            R12, out+88(FP)                             // ret1 = structures out
    MOVQ            SI, in+80(FP)                               // ret0 = #shape bytes consumed
    RET

processing_failed: // Processing failed
    XORL            SI, SI
    JMP             epilogue

decode_multibyte_symbol:
    BSWAPL          DX
    ANDNL           DX, R13, AX                                 // AX  := uint8{continuation_marker[0..3]}
    JZ              processing_failed                           // No end markers present, corrupted input
    BSRL            AX, CX                                      // CX  := uint32{the bit position of the leftmost continuation_marker}
    SUBL            $0x07, CX
    SHLXL           CX, R13, AX                                 // AX  := uint32{content_selection_bitmap}
    PEXTL           AX, DX, DX                                  // DX  := uint32{symbol}
    ANDL            $0x01010101, AX                             // Leave only one bit per byte of content_selection_bitmap
    VPBROADCASTD    DX, Z3                                      // Z3  := uint32{symbol times 16}
    POPCNTL         AX, AX                                      // AX  := uint32{#varuint_bytes}
    ADDQ            AX, R11                                     // bucketCursor += #varuint_bytes
    MOVBLZX         (R9)(R11*1), CX                             // CX  := uint32{ION.Value}
    JMP             symbol_decoded


decode_varuint_size:
    MOVBEL          1(DX), AX                                   // AX  := uint8{varuint[0..3]}
    ANDNL           AX, R13, CX                                 // CX  := uint8{continuation_marker[0..3]}
    JZ              processing_failed                           // No end markers present, corrupted input
    BSRL            CX, CX                                      // CX  := uint32{the bit position of the leftmost continuation_marker}
    SUBL            $0x07, CX
    SHLXL           CX, R13, CX                                 // CX  := uint32{content_selection_bitmap}
    PEXTL           CX, AX, AX                                  // AX  := uint32{varuint}
    ANDL            $0x01010101, CX                             // Leave only one bit per byte of content_selection_bitmap
    POPCNTL         CX, CX                                      // CX  := uint32{#varuint_bytes}
    LEAL            1(CX)(AX*1), AX                             // AX  := uint32{variadic_content_size + 1}
    JMP             size_decoded


trap:
    BYTE $0xCC
    RET


// func zionflattenAVX512BranchingVarUint(shape []byte, buckets *zll.Buckets, fields [][]vmref, tape []ion.Symbol) int
TEXT ·zionflattenAVX512BranchingVarUint(SB), NOSPLIT|NOFRAME, $0-0
    NO_LOCAL_POINTERS
    MOVQ            tape_len+64(FP), AX                         // AX := uint64{tape.Len}
    MOVQ            buckets+24(FP), BX                          // BX  := uint64{&buckets}
    MOVL            $0xaaaa, CX                                 // CX  := uint32{0xaaaa}
    MOVQ            tape_base+56(FP), R14                       // R14 := uint64{&tape[0]} (first symbol to match)
    VPXORQ          X0, X0, X0                                  // Z0 := {0*}
    KMOVW           CX, K5                                      // K5  := uint16{0xaaaa}
    MOVQ            $-1, DX
    MOVQ            fields+32(FP), DI                           // DI  := uint64{&fields[0]} (incremented later)
    BZHIQ           AX, DX, CX                                  // CX  := uint64{set tape.Len least significant bits}
    VMOVDQU32       const_zllBucketPos(BX), Z16                 // Z16 := int32{buckets.Pos[15..0]}
    SHLQ            $(const_zionStrideLog2 + 3), AX             // AX  := uint64{the number of bytes to pre-zero}
    VMOVDQU32       CONST_GET_PTR(consts_identity_d, 0), Z22    // Z22 := uint32{consts_identity_d}
    VPTERNLOGD      $0xff, Z1, Z1, Z1                           // Z1  := {-1*}
    MOVQ            const_zllBucketDecompressed(BX), R9         // R9  := uint64{&zll.Buckets.Decompressed[0]}
    KMOVW           CX, K7                                      // K7  := uint16{set tape.Len least significant bits}
    SHRL            $8, CX
    LEAQ            -64(DI)(AX*1), DX
    KMOVW           CX, K2
    VPSRLD          $27, Z1, Z21                                // Z21 := uint32{0x1f times 16}
    VMOVDQU8        Z0, (DX)                                    // n > 64, so the last (possibly unaligned) chunk can be filled unconditionally
    VPCMPD          $VPCMP_IMM_GT, Z1, Z16, K1                  // K1  := uint16{buckets.Pos[15..0] >= 0}
    VPADDD          Z22, Z22, Z2                                // Z2  := uint32{i * 2} for i in 15..0
    VMOVDQU64.Z     (R14), K7, Z15                              // Z15 := uint64{tape[i] if i < tape.Len; 0 otherwise} for i in 7..0
    LEAQ            -1(DX), CX
    TESTB           $0x3f, DX
    VMOVDQU64.Z     64(R14), K2, Z3                             // Z3 := uint64{tape[i] if i < tape.Len; 0 otherwise} for i in 15..8
    CMOVQEQ         CX, DX
    MOVQ            shape_base+0(FP), SI                        // SI  := uint64{shape_cursor}
    MOVQ            shape_len+8(FP), R8                         // R8  := uint64{shape.Len}
    ANDQ            $-64, DX
    VPBROADCASTMW2D K1, Z14                                     // Z14 := uint32{(buckets.Pos[15..0] >= 0) times 16}
    VMOVDQU8        CONST_GET_PTR(consts_nibble_shuffle, 0), Z23// Z23 := uint32{consts_nibble_shuffle}
    VPERMT2D        Z3, Z2, Z15                                 // Z15 := uint32{tape[i] if i < tape.Len; 0 otherwise} for i in 15..0
    VPSRLD          $24, Z1, Z24                                // Z24 := uint32{0x0000_00ff times 16}
    LEAQ            CONST_GET_PTR(consts_byte_ion_size_b, 0x00), R10 // R10 := uint64{&consts_byte_ion_size_b}
    VPSRLD          $28, Z1, Z20                                // Z20 := uint32{0x0f times 16}
    ADDQ            SI, R8                                      // R8  := uint64{shape_cursor_end}
    MOVQ            ·vmm+0(SB), R14                             // R14 := uint64{vmm_base}
    XORL            R12, R12                                    // R12 := uint64{processed_count}
    MOVL            $0x7f7f7f7f, R13                            // R13 := uint32{0x7f7f7f7f}
    KMOVW           K7, K6                                      // K6  := uint16{still active tape lanes}

    // BX  := uint64{&buckets}
    // DX  := uint64{fields_prezero_cursor}
    // SI  := uint64{shape_cursor}
    // DI  := uint64{&fields[0]} (incremented later)
    // R8  := uint64{shape_cursor_end}
    // R9  := uint64{&zll.Buckets.Decompressed[0]}
    // R10 := uint64{&consts_byte_ion_size_b}
    // R12 := uint64{processed_count*8}
    // R13 := uint32{0x7f7f7f7f}
    // R14 := uint64{vmm_base}
    // K5  := uint16{0xaaaa}
    // K6  := uint16{still active tape lanes}
    // K7  := uint16{set tape.Len least significant bits}
    // Z0  := {0*}
    // Z1  := {-1*}
    // Z14 := uint32{(buckets.Pos[15..0] >= 0) times 16}
    // Z15 := uint32{tape[i] if i < tape.Len; 0 otherwise} for i in 15..0
    // Z16 := int32{buckets.Pos[15..0]}
    // Z20 := uint32{0x0f times 16}
    // Z21 := uint32{0x1f times 16}
    // Z22 := uint32{consts_identity_d}
    // Z23 := uint32{consts_nibble_shuffle}
    // Z24 := uint32{0x0000_00ff times 16}

//JMP     shape_loop

prezero_aligned_loop:
    CMPQ            DX, DI
    JCS             prezero_loop_completed
    VMOVDQA64       Z0, (DX)
    SUBQ            $64, DX
    JMP             prezero_aligned_loop

prezero_loop_completed:
    VMOVDQU8        Z0, (DI)                                    // n > 64, so the finishing store cannot write out of the buffer

shape_loop:
    CMPQ            SI, R8
    JAE             processing_succeeded
    VPANDD.BCST     (SI), Z21, Z3                               // Z3  := uint32{fc times 16}
    VPBROADCASTQ    1(SI), Z2                                   // Z2  := uint64{shape[8..1] times 8}
    VMOVD           X3, R15                                     // R15 := uint32{fc}
    VPSHUFB         Z23, Z2, Z2                                 // Z2  := uint32{shape[i] times 2} for i in 8..1
    VPSUBD          Z3, Z22, Z3                                 // Z3  := int32{<0 when (i < fc); >=0 otherwise} for i in 15..0
    VPSRLD          $4, Z2, K5, Z2                              // Z2  := uint4{0 times 7, garbage, b[i]} for i in 15..0
    VPSRLD          $31, Z3, Z3                                 // Z3  := int32{1 when (i < fc); 0 otherwise} for i in 15..0
    VPANDD          Z20, Z2, Z2                                 // Z2  := uint32{b[i]} for i in 15..0
    VPSLLVD         Z2, Z3, Z3                                  // Z3  := uint32{1 << b[i]} for i in 15..0
    LEAL            3(R15), CX                                  // CX  := uint32{fc + 3}
    SUBL            $16, R15                                    // R15 := uint16{0 <=> fc == 16}
    JA              processing_failed                           // fc > 16
    SHRL            $1, CX                                      // CX  := uint64{skip := int((fc + 3) / 2)}
    VPTESTMD        Z14, Z3, K4                                 // K4  := uint16{active_lanes}
    ADDQ            CX, SI                                      // shape_cursor += skip
    KTESTW          K4, K4                                      // EFLAGS.ZF==0 <=> there are fields to extract
    JZ              field_loop_completed                        // no fields to extract

    // There are fields to extract. Assume there are no conflicts and start greedy parsing of streams, resolving conflicts later.

    VPCOMPRESSD.Z   Z2, K4, Z2                                  // Z2  := uint32{b[i]} for i in 15..0

field_loop:
    VPERMD          Z16, Z2, Z3                                 // Z3  := uint32{garbage times 15, buckets.Pos[b[0]]}
    KXNORW          K1, K1, K1                                  // K1  := uint16{0xffff}
    VMOVD           X2, AX                                      // AX  := uint64{b[0]}
    KADDW           K1, K4, K1                                  // K1  := uint16{K4 - 1}
    XORL            CX, CX                                      // CX  := uint32{0}
    VALIGND         $1, Z2, Z1, Z2                              // Drop the first queue item
    BTSL            AX, CX                                      // CX  := uint32{1 << b[0]}
    VMOVD           X3, R11                                     // R11 := uint64{buckets.Pos[b[0]]}
    KANDW           K4, K1, K4                                  // K4  := uint16{K4 & (K4 - 1)}

    // Decode the symbol varuint

    MOVBEL          (R9)(R11*1), DX                             // DX  := uint8{varuint[0..3]}
    KMOVW           CX, K3                                      // K3  := uint16{1 << b[0]}
    ANDNL           DX, R13, AX                                 // AX  := uint8{continuation_marker[0..3]}
    JZ              processing_failed                           // No end markers present, corrupted input
    BSRL            AX, CX                                      // CX  := uint32{the bit position of the leftmost continuation_marker}
    SUBL            $0x07, CX
    SHLXL           CX, R13, AX                                 // AX  := uint32{content_selection_bitmap}
    PEXTL           AX, DX, DX                                  // DX  := uint32{symbol}
    ANDL            $0x01010101, AX                             // Leave only one bit per byte of content_selection_bitmap
    VPBROADCASTD    DX, Z3                                      // Z3  := uint32{symbol times 16}
    POPCNTL         AX, AX                                      // AX  := uint32{#varuint_bytes}
    ADDQ            AX, R11                                     // bucketCursor += #varuint_bytes

    // Tape matching

    VPCMPUD         $VPCMP_IMM_GT, Z15, Z3, K6, K1              // K1  := uint16{sym > tape[i]} for i in 15..0
    MOVL            (R9)(R11*1), DX                             // DX  := uint8{ION.Value, varuint[0..2]}
    VPCMPUD         $VPCMP_IMM_EQ, Z15, Z3, K6, K2              // K2  := uint16{sym == tape[i]} for i in 15..0
    MOVBLZX         DX, CX                                      // CX  := uint32{ION.Value}
    SHRL            $8, DX                                      // DX  := uint8{0, varuint[2..0]}
    MOVBLZX         (R10)(CX*1), CX                             // CX  := uint64{consts_byte_ion_size_b[ION.Value]}
    BSWAPL          DX                                          // DX  := uint8{varuint[0..2], 0}
    TESTB           $ION_SIZE_INVALID, CX                       // EFLAGS.ZF == 0 <=> the ION_SIZE_INVALID flag is set
    JNZ             processing_failed                           // The ION_SIZE_INVALID flag is set
    KANDNW          K6, K1, K6                                  // Mark the sym > tape[i] lanes as processed
    ANDNL           DX, R13, AX                                 // AX  := uint8{continuation_marker[3..0]}
    KANDNW          K6, K2, K6                                  // Mark the sym == tape[i] lanes as processed
    BSRL            AX, AX                                      // AX  := uint32{the bit position of the leftmost continuation_marker}
    SUBL            $0x07, AX
    SHLXL           AX, R13, AX                                 // AX  := uint32{content_selection_bitmap}
    PEXTL           AX, DX, DX                                  // DX  := uint32{decoded_varuint_value}
    ANDL            $0x01010101, AX                             // Leave only one bit per byte of content_selection_bitmap
    POPCNTL         AX, AX                                      // AX  := uint32{#varuint_bytes}
    LEAL            1(AX)(DX*1), AX                             // AX  := uint32{variadic_content_size + 1}
    LEAL            1(CX), DX                                   // DX  := uint32{fixed_content_size + 1}
    TESTB           $ION_SIZE_VARUINT, CX                       // EFLAGS.ZF == 0 <=> the ION_SIZE_VARUINT flag is set
    KMOVW           K1, CX                                      // CX  := uint32{sym > tape[i]} for i in 15..0
    CMOVLEQ         DX, AX                                      // AX  := uint32{variadic_content_size + 1 if the ION_SIZE_VARUINT flag is set; fixed_content_size + 1 otherwise}
    LEAQ            (R9)(R11*1), DX                             // DX  := uint64{&object}
    ADDQ            AX, R11                                     // bucketCursor += #object_bytes
    POPCNTL         CX, CX                                      // CX  := uint32{the number of sym > tape[i] lanes} for i in 15..0
    VPBROADCASTD    R11, K3, Z16                                // Update buckets.Pos[b[0]]
    SHLL            $(const_zionStrideLog2 + 3), CX             // CX  := CX * 8 * zionStride
    ADDQ            CX, DI
    KTESTW          K2, K2
    JZ              store_completed

    // Store the vmref

    SUBQ            R14, DX                                     // DX  := uint32{&object - vmm_base}
    JS              trap                                        // DX should always be within vmm...
    MOVL            AX, 4(DI)(R12*1)                            // fields[struct].len = rax
    MOVL            DX, 0(DI)(R12*1)                            // fields[struct].off = (&object - vmm)
    ADDQ            $(const_zionStride * 8), DI

store_completed:
    KTESTW          K4, K4
    JNZ             field_loop

field_loop_completed:
    TESTL           R15, R15
    JZ              shape_loop                                  // fc == 16
    ADDL            $8, R12
    KMOVW           K7, K6                                      // K6  := uint16{still active tape lanes}
    MOVQ            fields+32(FP), DI                           // DI  := uint64{&fields[0]} (incremented later)
    CMPL            R12, $(const_zionStride * 8)
    JNZ             shape_loop

    // Processing completed successfully

processing_succeeded:
    SUBQ            shape_base+0(FP), SI                        // SI  := #shape bytes consumed

epilogue:
    SHRL            $3, R12
    VMOVDQU32       Z16, const_zllBucketPos(BX)                 // Update buckets.Pos[15..0]
    MOVQ            R12, out+88(FP)                             // ret1 = structures out
    MOVQ            SI, in+80(FP)                               // ret0 = #shape bytes consumed
    RET

processing_failed: // Processing failed
    XORL            SI, SI
    JMP             epilogue

trap:
    BYTE $0xCC
    RET


// func zionflattenAVX512Legacy(shape []byte, buckets *zll.Buckets, fields [][]vmref, tape []ion.Symbol) int
TEXT ·zionflattenAVX512Legacy(SB), NOSPLIT|NOFRAME, $16
    NO_LOCAL_POINTERS
    MOVQ  shape+0(FP), SI     // SI = &shape[0]
    MOVQ  buckets+24(FP), R9  // R9 = *zll.Buckets
    VMOVDQU32 const_zllBucketPos(R9), Z0 // Z0 = buckets.Pos
    VPMOVD2M  Z0, K1
    KNOTW     K1, K1          // K1 = valid bucket bits
    KMOVW     K1, R8
    VPBROADCASTW R8, Y7       // Y7 = valid bucket bits x16
    MOVL      $1, R8
    VPBROADCASTW R8, Y3       // Y3 = $1 (words)
    MOVL      $0xf, R8
    VPBROADCASTB R8, X6       // X6 = 0x0f0f0f0f...
    XORL      CX, CX
    MOVL      CX, 8(SP)       // 8(SP) = structures processed = 0
    MOVQ      SI, 0(SP)       // 0(SP) = saved shape addr
begin_struct:
    MOVQ  fields+32(FP), DI      // DI = &fields[0] (incremented later)
    MOVQ  tape_base+56(FP), DX   // DX = &tape[0] (first symbol to match)
    XORL  R11, R11               // source bits
top:
    MOVQ    0(SP), SI
    MOVQ    shape_len+8(FP), AX
    ADDQ    shape_base+0(FP), AX // AX = &src[0] + len(src)
    SUBQ    SI, AX               // AX = remaining bytes
    JZ      ret_ok               // assert len(src) != 0
    MOVBLZX 0(SI), R10           // R10 = descriptor
    ANDL    $0x1f, R10
    CMPL    R10, $16
    JG      ret_err              // assert desc <= 0x10
    MOVL    R10, R11
    LEAL    1(R11), R13
    SHRL    $1, R13              // R13 = (desc[0]+1)/2 = # needed bytes
    DECQ    AX                   // consumed 1 byte
    INCQ    SI                   // advance by 1 byte
    CMPL    R13, AX
    JA      ret_err              // assert len(src)-1 >= # needed bytes
    MOVQ    0(SI), R12           // R12 = descriptor bits
    ADDQ    R13, SI              // adjust source base
    MOVQ    SI, 0(SP)            // save shape pointer
    TESTL   R11, R11
    JZ      done

    // test all of the descriptor nibbles
    // against the buckets we've decoded
    // and eliminate any that don't match
    // (doing this without branches removes
    // a dozen+ unpredictable branches!)
    MOVL          $1, R8
    SHLXL         R11, R8, R8
    DECL          R8              // R8 = mask of valid bits
    KMOVW         R8, K1          // K1 = lanes to evaluate
    VMOVQ         R12, X1
    RORQ          $4, R12
    VMOVQ         R12, X2
    VPUNPCKLBW    X2, X1, X1      // X1 = interleave bytes and rol(bytes, 4)
    VPANDD        X6, X1, X1      // X1 &= 0x0f0f... (ignore upper nibble)
    VPMOVZXBW     X1, Y4          // Y4 = unpacked nibbles into 16 words
    VPSLLVW       Y4, Y3, Y4      // Y4 = 1<<nibbles in 16 words
    VPTESTMW      Y7, Y4, K1, K1  // K1 = (1 << nibble) & bucket mask x 16
    KTESTW        K1, K1
    JZ            check_outer_loop // continue parsing shape if zero matching fields

    VPMOVZXBD     X1, Z1
    VPCOMPRESSD   Z1, K1, Z1       // Z1 = buckets to scan

    KMOVW         K1, R11
    POPCNTL       R11, R11        // R11 = actual buckets to process
unpack_loop:
    MOVQ    const_zllBucketDecompressed+0(R9), SI // SI = &zll.Buckets.Decompressed[0]
    VMOVD   X1, R13                               // extract next bucket from Z1
    MOVL    const_zllBucketPos(R9)(R13*4), R14    // R14 = bucket pos
    LEAQ    0(SI)(R14*1), SI                      // SI = actual ion field+value
    VALIGND $1, Z1, Z1, Z1                        // shift to next bucket

    // now we have SI pointing to an ion label + value;
    // we need to decode the # of bytes in this memory
    // and add it back to bucket.base

    // first, parse label varint:
    MOVL    0(SI), R14
    MOVL    R14, R15
    ANDL    $0x00808080, R15 // check for stop bit
    JZ      ret_err
    MOVL    $1, CX
    MOVL    R14, R8
    ANDL    $0x7f, R8
    JMP     test_stop_bit
more_label_bits:
    INCL    CX
    SHLL    $7, R8
    SHRL    $8, R14
    MOVL    R14, R15
    ANDL    $0x7f, R15
    ORL     R15, R8
test_stop_bit:
    TESTL   $0x80, R14
    JZ      more_label_bits
label_done:
    XORL    BX, BX
    MOVQ    tape_len+64(FP), AX
    SHLQ    $3, AX
    ADDQ    tape_base+56(FP), AX // AX = &tape[len(tape)] = end-of-tape
match_tape:
    CMPQ    DX, AX
    JGE     parse_value               // definitely not matching if tape exhausted
    CMPQ    0(DX), R8                 // *tape == symbol
    JEQ     exact_match
    JA      parse_value               // symbol <= tape
    MOVL    8(SP), R14                // R14 = current struct
    MOVQ    BX, 0(DI)(R14*8)          // fields[struct].{off, len} = 0
    ADDQ    $(const_zionStride*8), DI // fields += sizeof([]vmref)
    ADDQ    $8, DX                    // tape += sizeof(symbol)
    JMP     match_tape
exact_match:
    MOVL    $1, BX
    ADDQ    $8, DX
parse_value:
    // parse value
    MOVL    CX, R12          // save label size
    MOVL    0(SI)(CX*1), R14 // load first 4 bytes of value
    CMPB    R14, $0x11
    JE      just_one_byte
    MOVL    R14, R15
    ANDL    $0x0f, R15
    CMPL    R15, $0x0f
    JE      just_one_byte
    CMPL    R15, $0x0e
    JNE     end_varint       // will add R15 to CX
value_is_varint:
    INCL    CX
    SHRL    $8, R14
    TESTL   $0x00808080, R14  // if there isn't a stop bit, we have a problem
    JZ      ret_err
    MOVL    R14, R15
    ANDL    $0x7f, R15        // accum = desc[0]&0x7f
    TESTL   $0x80, R14
    JNZ     end_varint
varint_loop:
    INCL    CX
    SHLL    $7, R15
    SHRL    $8, R14
    MOVL    R14, R8
    ANDL    $0x7f, R8
    ORL     R8, R15
    TESTL   $0x80, R14
    JZ      varint_loop
end_varint:
    ADDL    R15, CX           // size += sizeof(object)
just_one_byte:
    INCL   CX                                // size += descriptor byte
    ADDL   CX, const_zllBucketPos(R9)(R13*4) // bucket base += size

    TESTL  BX, BX
    JZ     skip_bucket

    // adjust offset and size so we skip the label bits
    ADDQ   R12, SI
    SUBQ   R12, CX
    JS     trap              // size should always be positive

    // copy out the field into DI
    SUBQ   ·vmm+0(SB), SI
    JS     trap                      // SI should always be within vmm...
    MOVL   8(SP), R8
    MOVL   SI, 0(DI)(R8*8)           // fields[struct].off = (addr - vmm)
    MOVL   CX, 4(DI)(R8*8)           // fields[struct].len = rcx
    ADDQ   $(const_zionStride*8), DI // fields += stride
skip_bucket:
    DECL    R11               // elements--
    JNZ     unpack_loop       // continue while elements > 0
check_outer_loop:
    CMPQ    R10, $16          // loop again if shape[0] == 16
    JEQ     top
done:
    XORL    BX, BX
    MOVQ    tape_len+64(FP), AX
    SHLQ    $3, AX
    ADDQ    tape_base+56(FP), AX // AX = &tape[len(tape)] = end-of-tape
exhaust_tape:
    // write out MISSING for unmatched fields
    CMPQ    DX, AX
    JGE     check_structs_out         // definitely not matching if tape exhausted
    MOVL    8(SP), R14                // R14 = current struct
    MOVQ    BX, 0(DI)(R14*8)          // fields[struct].{off, len} = 0
    ADDQ    $(const_zionStride*8), DI // fields += sizeof([]vmref)
    ADDQ    $8, DX                    // tape += sizeof(symbol)
    JMP     exhaust_tape
check_structs_out:
    INCL    8(SP)             // struct++
    CMPL    8(SP), $const_zionStride
    JB      begin_struct      // continue while (struct < zionStride)
ret_ok:
    MOVQ    0(SP), AX
    SUBQ    shape_base+0(FP), AX
    MOVQ    AX, in+80(FP)     // ret0 = shape in = current shape position - start
    MOVL    8(SP), AX
    MOVQ    AX, out+88(FP)    // ret1 = structures out
    RET
ret_err:
    XORL    AX, AX
    MOVQ    AX, in+80(FP)    // set shape consumed = 0
    MOVL    8(SP), AX
    MOVQ    AX, out+88(FP)   // ret1 = structures out
    RET
trap:
    BYTE $0xCC
    RET


CONST_DATA_U32(consts_nibble_shuffle,   (0*4), $0xffff_ff00)
CONST_DATA_U32(consts_nibble_shuffle,   (1*4), $0xffff_ff00)
CONST_DATA_U32(consts_nibble_shuffle,   (2*4), $0xffff_ff01)
CONST_DATA_U32(consts_nibble_shuffle,   (3*4), $0xffff_ff01)
CONST_DATA_U32(consts_nibble_shuffle,   (4*4), $0xffff_ff02)
CONST_DATA_U32(consts_nibble_shuffle,   (5*4), $0xffff_ff02)
CONST_DATA_U32(consts_nibble_shuffle,   (6*4), $0xffff_ff03)
CONST_DATA_U32(consts_nibble_shuffle,   (7*4), $0xffff_ff03)
CONST_DATA_U32(consts_nibble_shuffle,   (8*4), $0xffff_ff04)
CONST_DATA_U32(consts_nibble_shuffle,   (9*4), $0xffff_ff04)
CONST_DATA_U32(consts_nibble_shuffle,  (10*4), $0xffff_ff05)
CONST_DATA_U32(consts_nibble_shuffle,  (11*4), $0xffff_ff05)
CONST_DATA_U32(consts_nibble_shuffle,  (12*4), $0xffff_ff06)
CONST_DATA_U32(consts_nibble_shuffle,  (13*4), $0xffff_ff06)
CONST_DATA_U32(consts_nibble_shuffle,  (14*4), $0xffff_ff07)
CONST_DATA_U32(consts_nibble_shuffle,  (15*4), $0xffff_ff07)
CONST_GLOBAL(consts_nibble_shuffle, $64)
