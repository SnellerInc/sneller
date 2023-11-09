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

#ifdef AVX512_VBMI_ENABLED

// NOTE: It's tempting to byte-swap with VPERMB to basically do that together
// with shuffling 8-byte quantities into 9-byte quantities, but it's not really
// improving anything as we would have to byte-swap the remaining FP64 anyway.
// So first byte-swapping and then shuffling is actually more readable and ends
// up having the same performance.
#define STORE_FP64_VECTORS(DST_PTR, VEC0, VEC1)                                \
  VPSHUFB Z20, VEC0, VEC0                  /* VEC0 <- bswap64(VEC0) (low)   */ \
  VPSHUFB Z20, VEC1, VEC1                  /* VEC1 <- bswap64(VEC1) (high)  */ \
                                                                               \
  /* Unconditionally write instead of extracting the remaining 8-byte value */ \
  VMOVDQU8 VEC0, 8(DST_PTR)                                                    \
  VPERMB.Z VEC0, Z18, K6, VEC0                                                 \
  VMOVDQU8 VEC1, 80(DST_PTR)                                                   \
  VPERMB.Z VEC1, Z18, K6, VEC1                                                 \
                                                                               \
  VPORD Z19, VEC0, VEC0                                                        \
  VPORD Z19, VEC1, VEC1                                                        \
                                                                               \
  VMOVDQU8 VEC0, 0(DST_PTR)                                                    \
  VMOVDQU8 VEC1, 72(DST_PTR)

#else

#define STORE_FP64_VECTORS(DST_PTR, VEC0, VEC1)                                \
  /* Move ION float64 headers into the destination so we can scatter bytes  */ \
  VMOVDQU8 Z19, 0(DST_PTR)                                                     \
  VMOVDQU8 Z19, 72(DST_PTR)                                                    \
                                                                               \
  KMOVW K0, K6                             /* K6 <- scatter predicate       */ \
  VPSHUFB Z20, VEC0, VEC0                  /* VEC0 <- bswap64(VEC0) (low)   */ \
  VPSCATTERQQ VEC0, K6, 1(DST_PTR)(Z18*1)                                      \
                                                                               \
  KMOVW K0, K6                             /* K6 <- scatter predicate       */ \
  VPSHUFB Z20, VEC1, VEC1                  /* VEC1 <- bswap64(VEC1) (high)  */ \
  VPSCATTERQQ VEC1, K6, 73(DST_PTR)(Z18*1)

#endif

TEXT FUNC_NAME(SB), NOSPLIT|NOFRAME, $16
  MOVQ dst+0(FP), DI                       // DI <- destination buffer
  MOVQ src+24(FP), SI                      // SI <- source ptr
  ADDQ dst_len+8(FP), DI                   // DI <- destination ptr

  MOVQ itemCount+48(FP), CX                // CX <- item count
  MOVQ DI, R8                              // R8 <- beginning of dst buffer (for bytesWritten)

  TESTQ CX, CX
  JZ done

#ifdef AVX512_VBMI_ENABLED
  MOVQ $0x7FBFDFEFF7FBFDFE, AX
  VMOVDQU8 DECODE_CONST(256), Z18          // Z18 <- VPERMB predicate to shuffle FP64 to 9-byte ION
  VMOVDQU8 DECODE_CONST(320), Z19          // Z19 <- ION headers in every 9th byte
  KMOVQ AX, K6                             // K6 <- Predicate for VPERMB shuffle (zeroed where false)
#else
  VMOVDQU8 DECODE_CONST(192), Z18          // Z18 <- 9-byte offset increments
  VPBROADCASTD DECODE_CONST(160 + 4), Z19  // Z19 <- dword(0x48484848)
  KXNORW K0, K0, K0                        // K0 <- all ones predicate for scatters
#endif

  VBROADCASTI64X2 DECODE_CONST(144), Z20   // Z20 <- bswap predicate
  VPBROADCASTD DECODE_CONST(160), Z21      // Z21 <- dword(4)
  VPBROADCASTD DECODE_CONST(160 + 8), Z22  // Z22 <- dword(0x20202020)
  VPBROADCASTD DECODE_CONST(160 + 12), Z23 // Z23 <- dword(0x00210021)

  VBROADCASTI64X2 DECODE_CONST(112), Z24   // Z24 <- CF12 expand predicate
  VBROADCASTI64X2 DECODE_CONST(128), Z25   // Z25 <- CF24 expand predicate

  VBROADCASTF64X2 DECODE_CONST(0), Z26     // Z26 <- CF12 exponents
  VBROADCASTF64X2 DECODE_CONST(16), Z27    // Z27 <- CF16 exponents
  VMOVUPD DECODE_CONST(32), Z28            // Z28 <- CF24 exponents
  VBROADCASTF64X2 DECODE_CONST(96), Z29    // Z29 <- CF32 exponents

  JMP cluster_loop


  // CLUSTER LOOP
  // ------------

cluster_next:
  TESTL CX, CX
  JZ done

cluster_loop:
  MOVBLZX 0(SI), AX                        // AX <- cluster header byte
  MOVL AX, BX                              // BX <- AX
  SHRL $5, AX                              // AX <- cluster encoding id
  ANDL $0x1F, BX                           // BX <- cluster length - 1
  INCQ SI                                  // SI <- SI + 1
  INCQ BX                                  // BX <- cluster length

  CMPL AX, $3
  JA cluster_dispatch_above_3

  CMPL AX, $1
  JA cluster_dispatch_above_1

  TESTL AX, AX
  JNZ cluster_dispatch_1


  // ENCODING 0 - INT0
  // -----------------

  SUBQ BX, CX

  // That's it - fill ION zeros, unconditionally...
  VMOVDQU8 Z22, 0(DI)

  // DI = DI + count * 1
  ADDQ BX, DI
  JMP cluster_next


  // ENCODING 1 - INTX
  // -----------------

cluster_dispatch_1:
  ORL $0x20, BX                            // BX <- ION header byte to store in destination
  MOVBEQQ 0(SI), AX                        // AX <- load a byte-swapped 64-bit integer from source
  MOVB BX, 0(DI)                           // [] <- store ION header byte

  ANDL $0xF, BX                            // BX <- length of the value (in bytes)
  MOVL $8, DX                              // DX <- 8
  SUBL BX, DX                              // DX <- 8 - length (in bytes)
  SHLL $3, DX                              // DX <- 64 - length (in bits)
  ADDQ BX, SI                              // SI <- SI + BX (advance source ptr)
  SHRXQ DX, AX, AX                         // AX <- AX << DX

  DECQ CX                                  // CX <- CX - 1 (this kind of encoding always encodes a single integer)
  MOVL AX, 1(DI)                           // [] <- store byte-swapped value as ION value payload
  LEAQ 1(DI)(BX*1), DI                     // DI <- DI + BX + 1 (advance destination ptr)

  JMP cluster_next


cluster_dispatch_above_1:
  CMPL AX, $3
  JE cluster_dispatch_3


  // ENCODING 2 - INT8
  // -----------------

  SUBQ BX, CX

  VPMOVSXBW 0(SI), Z0                      // Z0 <- 32 bytes extended to 16-bit ints
  VPSRLW $15, Z0, Z1                       // Z1 <- sign bit at LSB position
  VPSLLW $8, Z0, Z0                        // Z0 <- each odd byte is the ION value payload
  VPSLLW $4, Z1, Z1                        // Z1 <- sign bit at 4 bit (to combine with ION header)
  VPABSB Z0, Z0                            // Z0 <- absolute values in the ION value payload
  VPADDD Z23, Z1, Z1                       // Z1 <- ION header bytes (each even byte)
  VPORD Z1, Z0, Z0                         // Z0 <- ION header (1 byte) and payload (1 byte) combined
  VMOVDQU8 Z0, 0(DI)

  // DI = DI + count * 2
  // SI = SI + count * 1
  LEAQ 0(DI)(BX*2), DI                     // DI <- DI + BX * 2
  ADDQ BX, SI                              // SI <- SI + BX
  JMP cluster_next


  // ENCODING 3 - CF12
  // -----------------

cluster_dispatch_3:
  SHLL $1, BX                              // BX <- cluster length (CF12 and CF16 length represents element pairs)
  SUBQ BX, CX
  JMP cf12_loop

cf12_next:
  ADDQ $24, SI                             // SI <- SI + 24 (advance source pointer)
  ADDQ $144, DI                            // DI <- DI + 72*2 (advance destination pointer - 16 ION headers + encoded floats)
  SUBQ $16, BX                             // BX <- BX - 16 (decrease loop counter)

cf12_loop:
  VMOVDQU8 0(SI), X0                       // Z0 <- 12 bytes    [?? ?? ?? ?? hh Hg GG ff|Fe EE dd Dc CC bb Ba AA] (low)
  VMOVDQU8 12(SI), X2                      // Z2 <- 12 bytes    [?? ?? ?? ?? hh Hg GG ff|Fe EE dd Dc CC bb Ba AA] (high)

  VPSHUFB X24, X0, X0                      // Z0 <- 16 bytes    [hh Hg Hg GG ff Fe Fe EE|dd Dc Dc CC bb Ba Ba AA] (low)
  VPSHUFB X24, X2, X2                      // Z2 <- 16 bytes    [hh Hg Hg GG ff Fe Fe EE|dd Dc Dc CC bb Ba Ba AA] (high)
  VPSLLVW X21, X0, X0                      // Z0 <- normalized  [hh Hg gG G0 ff Fe eE E0|dd Dc cC C0 bb Ba aA A0] (low)
  VPSLLVW X21, X2, X2                      // Z2 <- normalized  [hh Hg gG G0 ff Fe eE E0|dd Dc cC C0 bb Ba aA A0] (high)

  VPSRLW $15, X0, X1                       // Z1 <- i16 indexes to exponent table (low)
  VPSLLW $1, X0, X0                        // Z0 <- i16 magnitudes (shifted to MSB) (low)
  VPMOVZXWQ X1, Z1                         // Z1 <- i64 indexes to exponent table (low)
  VPSRAW $5, X0, X0                        // Z0 <- i16 magnitudes (low)
  VPERMQ Z26, Z1, Z1                       // Z1 <- f64 exponents (low)
  VPMOVSXWD X0, Y0                         // Z0 <- i32 magnitudes (sign extended) (low)
  VCVTDQ2PD Y0, Z0                         // Z0 <- f64 magnitudes (low)
  VDIVPD Z1, Z0, Z0                        // Z0 <- f64 decoded values (low)

  VPSRLW $15, X2, X3                       // Z3 <- i16 indexes to exponent table (high)
  VPSLLW $1, X2, X2                        // Z2 <- i16 magnitudes (shifted to MSB) (high)
  VPMOVZXWQ X3, Z3                         // Z3 <- i64 indexes to exponent table (high)
  VPSRAW $5, X2, X2                        // Z2 <- i16 magnitudes (high)
  VPERMQ Z26, Z3, Z3                       // Z3 <- f64 exponents (high)
  VPMOVSXWD X2, Y2                         // Z2 <- i32 magnitudes (sign extended) (high)
  VCVTDQ2PD Y2, Z2                         // Z2 <- f64 magnitudes (high)
  VDIVPD Z3, Z2, Z2                        // Z2 <- f64 decoded values (high)

  STORE_FP64_VECTORS(DI, Z0, Z2)

  CMPQ BX, $16
  JA cf12_next

  // DI = DI + count * 9
  // SI = SI + count * 1.5
  LEAQ 0(BX)(BX*8), AX                     // AX <- BX * 9
  ADDQ BX, SI                              // SI <- SI + BX
  SHRQ $1, BX                              // BX <- BX / 2
  ADDQ AX, DI                              // DI <- DI + AX
  ADDQ BX, SI                              // SI <- SI + BX
  JMP cluster_next


cluster_dispatch_above_3:
  CMPL AX, $5
  JA cluster_dispatch_above_5

  NOP
  JE cluster_dispatch_5


  // ENCODING 4 - CF16
  // -----------------

  SHLL $1, BX                              // BX <- cluster length (CF12 and CF16 length represents element pairs)
  SUBQ BX, CX
  JMP cf16_loop

cf16_next:
  ADDQ $32, SI                             // SI <- SI + 32 (advance source pointer)
  ADDQ $144, DI                            // DI <- DI + 72*2 (advance destination pointer - 16 ION headers + encoded floats)
  SUBQ $16, BX                             // BX <- BX - 16 (decrease loop counter)

cf16_loop:
  VPMOVSXWQ 0(SI), Z0                      // Z0 <- 16 bytes (low)
  VPMOVSXWQ 16(SI), Z2                     // Z2 <- 16 bytes (high)

  VPSRLQ $63, Z0, Z1                       // Z1 <- i64 indexes to exponent table (low)
  VPSLLQ $49, Z0, Z0                       // Z0 <- i64 magnitudes (shifted to MSB) (low)
  VPERMQ Z27, Z1, Z1                       // Z1 <- f64 exponents (low)
  VPSRAQ $49, Z0, Z0                       // Z0 <- i64 magnitudes (low)
  VCVTQQ2PD Z0, Z0                         // Z0 <- f64 magnitudes (low)
  VDIVPD Z1, Z0, Z0                        // Z0 <- f64 decoded values (low)

  VPSRLQ $63, Z2, Z3                       // Z3 <- i64 indexes to exponent table (high)
  VPSLLQ $49, Z2, Z2                       // Z2 <- i64 magnitudes (shifted to MSB) (high)
  VPERMQ Z27, Z3, Z3                       // Z3 <- f64 exponents (high)
  VPSRAQ $49, Z2, Z2                       // Z2 <- i64 magnitudes (high)
  VCVTQQ2PD Z2, Z2                         // Z2 <- f64 magnitudes (high)
  VDIVPD Z3, Z2, Z2                        // Z2 <- f64 decoded values (high)

  STORE_FP64_VECTORS(DI, Z0, Z2)

  CMPQ BX, $16
  JA cf16_next

  // DI = DI + count * 9
  // SI = SI + count * 2
  LEAQ 0(BX)(BX*8), AX                     // AX <- BX * 9
  LEAQ 0(SI)(BX*2), SI                     // SI <- SI + BX * 2
  ADDQ AX, DI                              // DI <- DI + AX
  JMP cluster_next


  // ENCODING 5 - CF24
  // -----------------

cluster_dispatch_5:
  SUBQ BX, CX
  JMP cf24_loop

cf24_next:
  ADDQ $48, SI                             // SI <- SI + 24*2 (advance source pointer)
  ADDQ $144, DI                            // DI <- DI + 72*2 (advance destination pointer - 16 ION headers + encoded floats)
  SUBQ $16, BX                             // BX <- BX - 16 (decrease loop counter)

cf24_loop:
  VMOVDQU8 0(SI), X0                       // Z0 <- 12 bytes [?? ?? ?? ?? DD DD DD CC|CC CC BB BB BB AA AA AA] (low)
  VMOVDQU8 24(SI), X2                      // Z2 <- 12 bytes [?? ?? ?? ?? DD DD DD CC|CC CC BB BB BB AA AA AA] (high)
  VINSERTI32X4 $1, 12(SI), Y0, Y0          // Z0 <- 24 bytes [?? ?? ?? ?? DD DD DD CC|CC CC BB BB BB AA AA AA] (low)
  VINSERTI32X4 $1, 36(SI), Y2, Y2          // Z2 <- 24 bytes [?? ?? ?? ?? DD DD DD CC|CC CC BB BB BB AA AA AA] (high)

  VPSHUFB Y25, Y0, Y0                      // Z0 <- 32 bytes [00 DD DD DD 00 CC CC CC|00 BB BB BB 00 AA AA AA] (low)
  VPSHUFB Y25, Y2, Y2                      // Z2 <- 32 bytes [00 DD DD DD 00 CC CC CC|00 BB BB BB 00 AA AA AA] (high)

  VPSRLD $21, Y0, Y1                       // Z1 <- i32 indexes to exponent table (low)
  VPSLLD $11, Y0, Y0                       // Z0 <- i32 magnitudes (shifted to MSB) (low)
  VPMOVZXDQ Y1, Z1                         // Z1 <- i64 indexes to exponent table (low)
  VPSRAD $11, Y0, Y0                       // Z0 <- i32 magnitudes (low)
  VPERMQ Z28, Z1, Z1                       // Z1 <- f64 exponents (low)
  VCVTDQ2PD Y0, Z0                         // Z0 <- f64 magnitudes (low)
  VDIVPD Z1, Z0, Z0                        // Z0 <- f64 decoded values (low)

  VPSRLD $21, Y2, Y3                       // Z3 <- i32 indexes to exponent table (high)
  VPSLLD $11, Y2, Y2                       // Z2 <- i32 magnitudes (shifted to MSB) (high)
  VPMOVZXDQ Y3, Z3                         // Z3 <- i64 indexes to exponent table (high)
  VPSRAD $11, Y2, Y2                       // Z2 <- i32 magnitudes (high)
  VPERMQ Z28, Z3, Z3                       // Z3 <- f64 exponents (high)
  VCVTDQ2PD Y2, Z2                         // Z2 <- f64 magnitudes (high)
  VDIVPD Z3, Z2, Z2                        // Z2 <- f64 decoded values (high)

  STORE_FP64_VECTORS(DI, Z0, Z2)

  CMPQ BX, $16
  JA cf24_next

  // DI = DI + count * 9
  // SI = SI + count * 3
  LEAQ 0(BX)(BX*8), AX                     // AX <- BX * 9
  LEAQ 0(BX)(BX*2), BX                     // BX <- BX * 3
  ADDQ AX, DI                              // DI <- DI + AX
  ADDQ BX, SI                              // SI <- SI + BX
  JMP cluster_next


cluster_dispatch_above_5:
  CMPL AX, $7
  JE cluster_dispatch_7


  // ENCODING 6 - CF32
  // -----------------

  SUBQ BX, CX
  JMP cf32_loop

cf32_next:
  ADDQ $64, SI                             // SI <- SI + 64 (advance source pointer)
  ADDQ $144, DI                            // DI <- DI + 72*2 (advance destination pointer - 16 ION headers + encoded floats)
  SUBQ $16, BX                             // BX <- BX - 16 (decrease loop counter)

cf32_loop:
  VPMOVSXDQ 0(SI), Z0                      // Z0 <- 32 bytes (low)
  VPMOVSXDQ 32(SI), Z2                     // Z2 <- 32 bytes (high)

  VPSRLQ $63, Z0, Z1                       // Z1 <- i64 indexes to exponent table (low)
  VPSLLQ $33, Z0, Z0                       // Z0 <- i64 magnitudes (shifted to MSB) (low)
  VPERMQ Z29, Z1, Z1                       // Z1 <- f64 exponents (low)
  VPSRAQ $33, Z0, Z0                       // Z0 <- i64 magnitudes (low)
  VCVTQQ2PD Z0, Z0                         // Z0 <- f64 magnitudes (low)
  VDIVPD Z1, Z0, Z0                        // Z0 <- f64 decoded values (low)

  VPSRLQ $63, Z2, Z3                       // Z3 <- i64 indexes to exponent table (high)
  VPSLLQ $33, Z2, Z2                       // Z2 <- i64 magnitudes (shifted to MSB) (high)
  VPERMQ Z29, Z3, Z3                       // Z3 <- f64 exponents (high)
  VPSRAQ $33, Z2, Z2                       // Z2 <- i64 magnitudes (high)
  VCVTQQ2PD Z2, Z2                         // Z2 <- f64 magnitudes (high)
  VDIVPD Z3, Z2, Z2                        // Z2 <- f64 decoded values (high)

  STORE_FP64_VECTORS(DI, Z0, Z2)

  CMPQ BX, $16
  JA cf32_next

  // DI = DI + count * 9
  // SI = SI + count * 4
  LEAQ 0(BX)(BX*8), AX                     // AX <- BX * 9
  LEAQ 0(SI)(BX*4), SI                     // SI <- SI + BX * 4
  ADDQ AX, DI                              // DI <- DI + AX
  JMP cluster_next


  // ENCODING 7 - FP64
  // -----------------

cluster_dispatch_7:
  SUBQ BX, CX
  JMP fp64_loop

fp64_next:
  ADDQ $128, SI                            // SI <- SI + 128 (advance source pointer)
  ADDQ $144, DI                            // DI <- DI + 72*2 (advance destination pointer - 16 ION headers + encoded floats)
  SUBQ $16, BX                             // BX <- BX - 16 (decrease loop counter)

fp64_loop:
  VMOVUPD 0(SI), Z0                        // Z0 <- 64 bytes (low)
  VMOVUPD 64(SI), Z2                       // Z2 <- 64 bytes (high)

  STORE_FP64_VECTORS(DI, Z0, Z2)

  CMPQ BX, $16
  JA fp64_next

  // DI = DI + count * 9
  // SI = SI + count * 8
  LEAQ 0(BX)(BX*8), AX                     // AX <- BX * 9
  LEAQ 0(SI)(BX*8), SI                     // SI <- SI + BX * 8
  ADDQ AX, DI                              // DI <- DI + AX
  JMP cluster_next


  // Return
  // ------

done:
  // Return the number of bytes written to dst and the number of bytes read from src.
  SUBQ R8, DI
  SUBQ src+24(FP), SI
  MOVQ DI, ret+56(FP)
  MOVQ SI, ret1+64(FP)
  RET

#undef STORE_FP64_VECTORS
