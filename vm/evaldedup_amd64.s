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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"
#include "bc_amd64.h"

TEXT ·evaldedup(SB), NOSPLIT, $8
  NO_LOCAL_POINTERS
  XORQ R9, R9         // R9 = rows consumed
  MOVQ R9, ret+72(FP) // # rows output (set to zero for now)
  JMP  tail
loop:
  // load delims
  CMPQ         CX, $16
  JLT          genmask
  KXNORW       K0, K0, K1
doit:
  // unpack the next 16 (or fewer) delims
  // into Z0=indices, Z1=lengths
  MOVQ         delims+8(FP), DX
  VMOVDQU64.Z  0(DX)(R9*8), K1, Z2
  KSHIFTRW     $8, K1, K2
  VMOVDQU64.Z  64(DX)(R9*8), K2, Z3
  VPMOVQD      Z2, Y0
  VPMOVQD      Z3, Y1
  VINSERTI32X8 $1, Y1, Z0, Z0
  VPROLQ       $32, Z2, Z2
  VPROLQ       $32, Z3, Z3
  VPMOVQD      Z2, Y1
  VPMOVQD      Z3, Y2
  VINSERTI32X8 $1, Y2, Z1, Z1
  ADDQ         $16, R9

  // enter bytecode interpretation
  MOVQ bc+0(FP), DI
  MOVQ ·vmm+0(SB), SI       // real static base
  VMENTER()

  // load the low 64 bits of the sixteen hashes;
  // we should have Z15 = first 8 lo 64, Z16 = second 8 lo 64
  MOVQ        slot+64(FP), R8
  SHLQ        $8, R8 // TODO: BC REFACTOR, REMOVE...
  ADDQ        bytecode_hashmem(VIRT_BCPTR), R8
  VMOVDQU64   0(R8), Z15
  VMOVDQU64   64(R8), Z16
  VPUNPCKLQDQ Z16, Z15, Z15
  VMOVDQU64   128(R8), Z16
  VMOVDQU64   192(R8), Z17
  VPUNPCKLQDQ Z17, Z16, Z16
  VMOVDQU64   permute64+0(SB), Z18
  VPERMQ      Z15, Z18, Z15                      // Z15 = low 8 hashes (64-bit)
  VPERMQ      Z16, Z18, Z16                      // Z16 = hi 8 ''
  VMOVDQA64   Z15, Z17                           // Z17, Z18 = temporaries for rotated hashes
  VMOVDQA64   Z16, Z18

  // handle some easy deduplication within 8-lane chunks
  // by using CONFLICTQ to mask away identical hashes
  KSHIFTRW    $8, K1, K5
  KMOVB       K1, K4
  VPCONFLICTQ Z15, Z7
  VPTESTNMQ   Z7, Z7, K4, K4
  VPCONFLICTQ Z16, Z8
  VPTESTNMQ   Z8, Z8, K5, K5
  KUNPCKBW    K4, K5, K1
  KMOVW       K1, K2
  KMOVW       K1, K3

  // load some constants
  VPTERNLOGD    $0xff, Z10, Z10, Z10 // Z10 = all ones
  VPSRLD        $28, Z10, Z6         // Z6 = 0xf
  VPXORQ        Z14, Z14, Z14        // Z14 = constant 0
  VPXORQ        Z7, Z7, Z7           // Z7 = shift count

  // load table[0] into Z8 and copy to Z9
  MOVQ          tree+56(FP), R10
  MOVQ          radixTree64_index(R10), R15
  VMOVDQU32     0(R15), Z8         // Z8 = initial indices for (hash&mask)
  VMOVDQA32     Z8, Z9             // Z9 = same

  // extract low 32-bit words from hashes
  VPMOVQD       Z15, Y24
  VPMOVQD       Z16, Y25
  VINSERTI32X8  $1, Y25, Z24, Z11  // Z11 = lo32 x 16 words
  VPRORQ        $32, Z15, Z26      // rotate 32 bits to get hi 32
  VPRORQ        $32, Z16, Z27
  VPMOVQD       Z26, Y26
  VPMOVQD       Z27, Y27
  VINSERTI32X8  $1, Y27, Z26, Z12  // Z12 = hi32 x 16 words

  // compute the first table offset
  // as a permutation into the correct
  // initial slot (since we have a sixteen-wide splay)
  VPANDD        Z11, Z6, Z11
  VPANDD        Z12, Z6, Z12
  VPERMD        Z8, Z11, Z8
  VPERMD        Z9, Z12, Z9
  JMP           radix_loop_tail

radix_loop:
  // lo 32 bits x 16 -> Z24
  VPMOVQD       Z17, Y24
  VPMOVQD       Z18, Y25
  VINSERTI32X8  $1, Y25, Z24, Z24

  // hi 32 bits x 16 -> Z25
  VPSRLQ        $32, Z17, Z25
  VPSRLQ        $32, Z18, Z26
  VPMOVQD       Z25, Y25
  VPMOVQD       Z26, Y26
  VINSERTI32X8  $1, Y26, Z25, Z25

  VPANDD        Z24, Z6, Z24  // lo 8 &= mask
  VPANDD        Z25, Z6, Z25  // hi 8 &= mask
  VPSLLD        $4, Z8, Z11   // Z11 = index * 16 = ptr0
  VPSLLD        $4, Z9, Z12   // Z12 = index * 16 = ptr1
  VPADDD        Z11, Z24, Z11 // Z11 = (index * 16) + (hash & mask)
  VPADDD        Z12, Z25, Z12 // Z12 = (index * 16) + (hash & mask)
  KMOVW         K2, K4
  VPGATHERDD    0(R15)(Z11*4), K4, Z8 // Z8 = table[Z8][(hash&mask)]
  KMOVW         K3, K5
  VPGATHERDD    0(R15)(Z12*4), K5, Z9 // Z9 = table[Z9][(hash&mask)]
radix_loop_tail:
  VPRORQ        $4, Z17, Z17        // chomp 4 bits of hash
  VPRORQ        $4, Z18, Z18
  VPCMPD        $1, Z8, Z14, K2, K2 // select lanes with index > 0
  VPCMPD        $1, Z9, Z14, K3, K3
  KORTESTW      K2, K3
  JNZ           radix_loop          // loop while any indices are non-negative

  // determine if values[i] == hash in each lane
  VPTESTMD      Z8, Z8, K1, K2  // select index != 0
  VPTESTMD      Z9, Z9, K1, K3  //
  VPXORD        Z8, Z10, K2, Z8 // ^idx = value index
  VPXORD        Z9, Z10, K3, Z9

  MOVQ          radixTree64_values(R10), R15

  // load and test against hash0
  VEXTRACTI32X8 $1, Z8, Y24            // upper 8 indices
  KMOVB         K2, K5
  VPGATHERDQ    0(R15)(Y8*1), K5, Z26  // Z26 = first 8 hashes
  KSHIFTRW      $8, K2, K5
  VPGATHERDQ    0(R15)(Y24*1), K5, Z27 // Z27 = second 8 hashes
  VPCMPEQQ      Z15, Z26, K2, K5       // K5 = lo 8 match
  KSHIFTRW      $8, K2, K6
  VPCMPEQQ      Z16, Z27, K6, K6       // K6 = hi 8 match
  KUNPCKBW      K5, K6, K2             // (K5||K6) -> K2 = found lanes

  // load and test against hash1 (same as above)
  VEXTRACTI32X8 $1, Z9, Y25            // lower 8 indices
  VPROLQ        $32, Z15, Z28          // first 8 rol 32
  VPROLQ        $32, Z16, Z29          // second 8 rol 32
  KANDNQ        K3, K2, K3             // unset already found from K3
  KMOVB         K3, K5
  VPGATHERDQ    0(R15)(Y9*1), K5, Z26
  KSHIFTRW      $8, K3, K5
  VPGATHERDQ    0(R15)(Y25*1), K5, Z27
  VPCMPEQQ      Z28, Z26, K3, K4
  KSHIFTRW      $8, K3, K6
  VPCMPEQQ      Z29, Z27, K6, K6
  KUNPCKBW      K4, K6, K3
  KORW          K2, K3, K2             // K2 = found

  // select 'valid lanes != found' as K1,
  // the mask of lanes that are not duplicates
  //
  // if K1 is entirely unset, then we don't need
  // to do any additional work here.
  KXORW         K1, K2, K1
  KTESTW        K1, K1
  JZ            tail

  // compress output into delims and hashes
  MOVQ          delims+8(FP), DX
  MOVQ          hashes+32(FP), R11
  MOVQ          ret+72(FP), R10
  KSHIFTRW      $8, K1, K2
  KMOVB         K1, K1

  VPCOMPRESSQ   Z15, K1, 0(R11)(R10*8) // compress hashes
  KMOVD         K1, R8
  POPCNTL       R8, R8
  VPMOVZXDQ     Y1, Z2               // Z2 = first 8 lengths
  VPMOVZXDQ     Y0, Z3               // Z3 = first 8 offsets
  VEXTRACTI32X8 $1, Z0, Y0
  VEXTRACTI32X8 $1, Z1, Y1
  VPROLQ        $32, Z2, Z2
  VPORD         Z2, Z3, Z2           // Z2 = first 8 qword(length << 32 | offset)
  VPCOMPRESSQ   Z2, K1, 0(DX)(R10*8) // compress delims
  ADDQ          R8, R10
  // repeat above for next 8 lanes
  VPCOMPRESSQ   Z16, K2, 0(R11)(R10*8)
  KMOVW         K2, R8
  POPCNTL       R8, R8
  VPMOVZXDQ     Y1, Z2
  VPMOVZXDQ     Y0, Z3
  VPROLQ        $32, Z2, Z2
  VPORD         Z2, Z3, Z2
  VPCOMPRESSQ   Z2, K2, 0(DX)(R10*8)
  ADDQ          R8, R10
  MOVQ          R10, ret+72(FP)
tail:
  MOVQ delims_len+16(FP), CX
  SUBQ R9, CX
  JG   loop             // should be JLT, but operands are reversed
  RET
genmask:
  // K1 = (1 << CX)-1
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  KMOVW       R8, K1
  JMP         doit
trap:
  BYTE $0xCC

