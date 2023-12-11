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
#include "bc_imm_amd64.h"
#include "../internal/asmutils/ion_constants_amd64.h"
#include "bc_constant_gen.h"

// func unpivotAtDistinctDeduplicate(rows []vmref, vmbase uintptr, bitvector *simdChunk)
TEXT ·unpivotAtDistinctDeduplicate(SB), NOSPLIT, $192-40
    NO_LOCAL_POINTERS
    VPBROADCASTD CONSTD_0xFF(), Z25     // Z25 := 0x000000ff * 16
    LEAQ        64(SP), BX
    VMOVDQU64   CONST_GET_PTR(consts_offsets_d_2, 0), Z24
    VPSRLD      $7, Z25, Z28            // Z28 := 0x00000001 * 16
    ANDQ        $-64, BX                // a 64-bytes aligned 128-byte local buffer
    MOVQ        rows_len+8(FP), CX
    VPSRLD      $1, Z25, Z26            // Z26 := 0x0000007f * 16
    MOVQ        rows_base+0(FP), SI
    VPSRLD      $3, Z25, Z27            // Z27 := 0x0000001f * 16
    MOVQ        vmbase+24(FP), R10
    VPSLLD      $7, Z28, Z29            // Z29 := 0x00000080 * 16
    MOVQ        bitvector+32(FP), DI

loop_rows:
    KXNORW      K0, K0, K1              // Assume eagerly there are at least 16 active lanes
    SUBQ        $16, CX
    JC          select_final_lanes      // Wrong, do the fixup

process_rows:
    // The mask of active lanes in K1 is correct here.
    // Fetch and deinterlace vmref pairs
    VMOVDQU64.Z (SI), K1, Z0            // the first vmrefs half
    KSHIFTRW    $8, K1, K2
    VMOVDQA64   Z24, Z16                // the permutation for offsets
    VPADDD      Z24, Z28, Z17           // the permutation for lengths
    VMOVDQU64.Z 64(SI), K2, Z1          // the second vmrefs half
    ADDQ        $128, SI

    // Deinterlace the vmrefs halves
    VPERMI2D    Z1, Z0, Z16             // Z16 := vmrefs offsets
    VPERMI2D    Z1, Z0, Z17             // Z17 := vmrefs lengths

    // Translate sizes into the end offsets
    VPADDD      Z16, Z17, Z17           // Z17 := vmrefs end offsets

loop_vmrefs:
    VPCMPUD     $VPCMP_IMM_LT, Z17, Z16, K1, K1 // K1 := the non-empty lanes
    KTESTW      K1, K1                  // EFLAGS.Z := all lanes are empty
    JZ          loop_rows               // Jump if all the vmrefs have been processed

    //  K1  := master active lanes
    //  Z16 := the read cursor

    KMOVW       K1, K2                  // K2 := preserved K1; K1 to be destroyed by GATHER
    VPGATHERDD  0(R10)(Z16*1), K1, Z0   // Z0 := the first four bytes of the ION "field name [VarUInt]" entity; K1 destroyed
    VPADDD.Z    Z28, Z16, K2, Z16       // update the read cursor by skipping the processed byte

    // Decode the VarUInt fields. The encoding uses 7-bit base, so 3 bytes
    // suffice to cover a 21-bit symbol space. The memory block address space
    // has 20 bits, so a valid symbol index will be reconstructed in at most
    // 3 steps. Unrolled. Process the first byte of the VarUInts (always present)

    VPTESTNMD   Z0, Z29, K2, K1         // K1 := the VarUInts requiring continuation
    VPANDD.Z    Z0, Z26, K2, Z1         // Z1 := the first 7 MSBs of the VarUInts
    VPADDD.Z    Z28, Z16, K2, Z16       // update the read cursor by skipping the processed byte
    VPSRLD.Z    $8, Z0, K2, Z0          // dispose the processed byte

    // Process the second byte of the VarUInts (if any)
    VPSLLD      $7, Z1, K1, Z1          // Make room for the VarUInt continuations
    VPTESTNMD   Z0, Z29, K1, K3         // K3 := the VarUInts requiring continuation
    VPANDD      Z0, Z26, K1, Z2         // Z2 := the next 7 MSBs of the VarUInts
    VPSRLD      $8, Z0, K1, Z0          // dispose the processed byte
    VPADDD      Z28, Z16, K1, Z16       // update the read cursor by skipping the processed byte
    VPORD       Z2, Z1, K1, Z1          // Z1 := the first 14 MSBs of the VarUInts
    KMOVW       K2, K1                  // Restore the master active lanes mask
    LEAQ        CONST_GET_PTR(consts_byte_ion_size_b, 0x00), AX // consts_byte_ion_size_b base address for the follow-up GATHER

    // Process the third byte of the VarUInts (if any)
    VPSLLD      $7, Z1, K3, Z1          // Make room for the VarUInt continuations
    VPANDD      Z0, Z26, K3, Z2         // Z2 := the next 7 MSBs of the VarUInts
    VPSRLD      $8, Z0, K3, Z0          // dispose the processed byte
    VPADDD      Z28, Z16, K3, Z16       // update the read cursor by skipping the processed byte
    VPORD       Z2, Z1, K3, Z1          // Z1 := the first 21 MSBs of the VarUInts

    // Finished the symbol ID decoding. Status:
    //
    //  K1  := master active lanes
    //  K2  := K1
    //  Z0  := the first byte of every DWORD contains the follow-up ION value prefix, rubbish elsewhere
    //  Z1  := symbol IDs
    //  Z16 := the updated read cursor
    //  AX  := &consts_byte_ion_size_b

    VPANDD.Z    Z0, Z25, K1, Z2         // Z2 := isolated ION value prefixes
    VPGATHERDD  0(AX)(Z2*1), K2, Z0     // Z0 := byte0: decoded ION object length; rubbish elsewhere; K2 destroyed
    VPANDD.Z    Z1, Z27, K1, Z2         // Z2 := the bit indices within a DWORD of the symbol IDs
    VPSRLD.Z    $5, Z1, K1, Z1          // Z1 := bitvector DWORD indices
    VPTESTMD    Z0, Z29, K1, K2         // K2 := the lanes requiring VarUInt decode
    VMOVDQA64   Z1, 0x00(BX)            // 0x00(BX) := bitvector DWORD indices
    VPSLLVD.Z   Z2, Z28, K1, Z2         // Z2 := 1 << (Z2 & 0x1f)
    KTESTW      K2, K2                  // EFLAGS.Z: are there any VarUInt lanes?
    VPANDD.Z    Z27, Z0, K1, Z0         // Z0 := valid fixed-size ION lengths or zeros for the VarUInt cases
    VMOVDQA64   Z2, 0x40(BX)            // 0x40(BX) := bitvector DWORD values

    //  K1  := master active lanes
    //  K2  := VarUInt lanes
    //  Z0  := valid fixed-size ION lengths or zeros for the VarUInt cases

    JZ skip_varuint_ion_values // if we are lucky

    // Decode the VarUInt fields. The encoding uses 7-bit base, so 3 bytes
    // suffice to cover a 21-bit address space. The memory block address space
    // has 20 bits, so a valid ION object size will be reconstructed in at most
    // 3 steps. Unrolled. Process the first byte of the VarUInts (always present)

    KMOVW       K2, K3
    VPGATHERDD  0(R10)(Z16*1), K2, Z1   // Z1 := the first four VarUInt bytes
    VPADDD      Z28, Z16, K3, Z16       // update the read cursor by skipping the processed byte
    VPTESTNMD   Z1, Z29, K3, K2         // K2 := the VarUInts requiring continuation
    VPANDD      Z1, Z26, K3, Z0         // the first 7 MSBs of the VarUInts
    VPSRLD      $8, Z1, K3, Z1          // dispose the processed byte

    // Process the second byte of the VarUInts (if any)

    VPSLLD      $7, Z0, K2, Z0          // Make room for the VarUInt continuations
    VPTESTNMD   Z1, Z29, K2, K3         // K3 := the VarUInts requiring continuation
    VPANDD      Z1, Z26, K2, Z2         // Z2 := the next 7 MSBs of the VarUInts
    VPSRLD      $8, Z1, K2, Z1          // dispose the processed byte
    VPORD       Z2, Z0, K2, Z0          // Z0 := the first 14 MSBs of the VarUInts
    VPADDD      Z28, Z16, K2, Z16       // update the read cursor by skipping the processed byte

    // Process the third byte of the VarUInts (if any)

    VPSLLD      $7, Z0, K3, Z0          // Make room for the VarUInt continuations
    VPANDD      Z1, Z26, K3, Z1         // Z1 := the next 7 MSBs of the VarUInts
    VPADDD      Z28, Z16, K3, Z16       // update the read cursor by skipping the processed byte
    VPORD       Z1, Z0, K3, Z0          // Z0 := the first 21 MSBs of the VarUInts

skip_varuint_ion_values:

    //  K1  := master active lanes
    //  Z0  := valid ION lengths

    VPADDD      Z16, Z0, K1, Z16        // update the read cursor by skipping the ION object

    // Unconditionally set the bitvector bits. For the inactive lanes the offset is zero, which always is a vaild value.
    // The value to be ORed with is either (1 << n) for active lanes or 0 for the inactive ones. This makes it a no-op
    // OR $0, (DI). Doing so unconditionally is cheaper than checking if it should be done and imposes a sequentially
    // consistent load-store semantics. An alternative solution based on conflict detection was dismissed due to the
    // unbearably high latency/throughtput figures of VPCONFLICTD. Manually repeated 16 times due to the deficiencies
    // of the assembler. BOHICA:

#define ALTER_BITVECTOR(LANE_ID)                            \
    MOVL    ((LANE_ID) * 4 + 0x00)(BX), DX    /* index */   \
    MOVL    ((LANE_ID) * 4 + 0x40)(BX), AX    /* value */   \
    ORL     AX, (DI)(DX*4)

    ALTER_BITVECTOR(0)
    ALTER_BITVECTOR(1)
    ALTER_BITVECTOR(2)
    ALTER_BITVECTOR(3)
    ALTER_BITVECTOR(4)
    ALTER_BITVECTOR(5)
    ALTER_BITVECTOR(6)
    ALTER_BITVECTOR(7)
    ALTER_BITVECTOR(8)
    ALTER_BITVECTOR(9)
    ALTER_BITVECTOR(10)
    ALTER_BITVECTOR(11)
    ALTER_BITVECTOR(12)
    ALTER_BITVECTOR(13)
    ALTER_BITVECTOR(14)
    ALTER_BITVECTOR(15)

#undef ALTER_BITVECTOR

    // Process the remaining vmrefs
    JMP         loop_vmrefs

select_final_lanes:
    NOTQ    CX
    MOVL    $0x7fff, AX
    SHRQ    CX, AX
    MOVL    $0, CX
    KMOVW   AX, K1
    JNZ     process_rows
    RET

// func fillVMrefs(p *[]vmref, v vmref, n int)
TEXT ·fillVMrefs(SB), NOSPLIT | NOFRAME, $0-0
    MOVQ    p+0(FP), BX
    MOVQ    n+16(FP), CX
    MOVQ    8(BX), DX       // p.len
    MOVQ    0(BX), DI       // p.data
    MOVQ    v+16(SP), AX    // Mute the "invalid MOVQ of v+8(FP); github.com/SnellerInc/sneller/vm.vmref is 8-byte value" go vet false positive.
    LEAQ    (DI)(DX*8), DI
    ADDQ    CX, DX
    REP;    STOSQ           // EFLAGS.DF=0 is assumed per the ABI
    MOVQ    DX, 8(BX)
    RET

// func copyVMrefs(p *[]vmref, q *vmref, n int)
TEXT ·copyVMrefs(SB), NOSPLIT | NOFRAME, $0-0
    MOVQ    p+0(FP), BX
    MOVQ    n+16(FP), CX
    MOVQ    8(BX), DX       // p.len
    MOVQ    0(BX), DI       // p.data
    MOVQ    q+8(FP), SI
    LEAQ    (DI)(DX*8), DI
    ADDQ    CX, DX
    REP;    MOVSQ           // EFLAGS.DF=0 is assumed per the ABI
    MOVQ    DX, 8(BX)
    RET
