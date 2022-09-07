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

// This file contains the implementation of APPROX_COUNT_DISTINCT opcode

// See evalaggregate_amd64.s
#define AggregateDataBuffer R10

TEXT bcaggapproxcount(SB), NOSPLIT|NOFRAME, $0
    MOVQ    R12, bytecode_spillArea(VIRT_BCPTR)

    // Note: The virtual hash registers are 128-bit ones, we use the higher 64 bits of each.
    MOVQ    0(VIRT_PCREG), R15
    ADDQ    AggregateDataBuffer, R15    // R15 -> [1 << aggApproxCountDistinctBucketBits]byte

    MOVWQZX 8(VIRT_PCREG), R8
    ADDQ    bytecode_hashmem(VIRT_BCPTR), R8

    MOVBQZX 10(VIRT_PCREG), R12         // R12  = bits per bucket
    MOVQ    $64, R13
    SUBQ    R12, R13                    // R13 = 64 - R12 - hash bits

    MOVQ    $16, R14                    // the number of hashes
scalar_loop:
    MOVQ    (R8), DX    // DX = higher 64-bit of the 128-bit hash
    SHLXQ   R12, DX, CX // CX - hash
    LZCNTQ  CX, CX
    INCQ    CX          // CX = lzcnt(hash) + 1
    SHRXQ   R13, DX, DX // DX - bucket id

    // update HLL register
    MOVBQZX (R15)(DX*1), BX
    CMPQ    BX, CX
    CMOVQLT CX, BX      // BX = max(DX, BX)
    MOVB    BL, (R15)(DX*1)

    ADDQ    $16, R8
    DECQ    R14
    JNZ     scalar_loop

next:
    MOVQ    bytecode_spillArea(VIRT_BCPTR), R12

    NEXT_ADVANCE(12)
