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

// chacha8 random initialization vector
GLOBL chachaiv<>(SB), 8, $64
DATA  chachaiv<>+0(SB)/4, $0x9722F977  // XOR'd with length for real IV
DATA  chachaiv<>+4(SB)/4, $0x3320646e
DATA  chachaiv<>+8(SB)/4, $0x79622d32
DATA  chachaiv<>+12(SB)/4, $0x6b206574
DATA  chachaiv<>+16(SB)/4, $0x058A60F5
DATA  chachaiv<>+20(SB)/4, $0xB25F6FB1
DATA  chachaiv<>+24(SB)/4, $0x1FEFA3D9
DATA  chachaiv<>+28(SB)/4, $0xB9D8F520
DATA  chachaiv<>+32(SB)/4, $0xB415DBCC
DATA  chachaiv<>+36(SB)/4, $0x34B70366
DATA  chachaiv<>+40(SB)/4, $0x3F4DBB4D
DATA  chachaiv<>+44(SB)/4, $0xCBB67392
DATA  chachaiv<>+48(SB)/4, $0x61707865
DATA  chachaiv<>+52(SB)/4, $0x143BE9F6
DATA  chachaiv<>+56(SB)/4, $0xDA97A1A8
DATA  chachaiv<>+60(SB)/4, $0x6F0E9495

TEXT ·chacha8bulkseed(SB), 7, $16
  XORL AX, AX
  MOVQ buf+0(FP), R15
  MOVQ seed+48(FP), DX
  JMP  tail
aligned_loop:
  MOVQ      in+24(FP), DI
  VMOVDQU32 0(DI)(AX*8), Y0
  VPMOVQD   Y0, X10
  VPROLQ    $32, Y0, Y0
  VPMOVQD   Y0, X11
  VMOVDQU64 0(DX), Z9
  VBROADCASTI32X4 chachaiv<>+0(SB), Z21
  VPXORQ    Z9, Z21, Z9
  CALL      hashx4(SB)
  VMOVDQU64 Z9, 0(DX)
  ADDQ      $4, AX
  ADDQ      $64, DX
tail:
  MOVQ        in_len+32(FP), CX
  SUBQ        AX, CX
  CMPQ        CX, $4
  JGE         aligned_loop
  TESTL       CX, CX
  JZ          done
  SHLL        $1, CX
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  MOVB        R8, 8(SP)
  KMOVB       R8, K1
  MOVQ        in+24(FP), DI
  VMOVDQU32.Z 0(DI)(AX*8), K1, Y0
  VPMOVQD     Y0, X10
  VPROLQ      $32, Y0, Y0
  VPMOVQD     Y0, X11
  VMOVDQU64   0(DX), K1, Z9
  VBROADCASTI32X4 chachaiv<>+0(SB), Z21
  VPXORQ      Z9, Z21, Z9
  CALL        hashx4(SB)
  KMOVB       8(SP), K1
  VMOVDQU64   Z9, K1, 0(DX)
done:
  MOVQ in_len+32(FP), CX
  MOVQ CX, ret+72(FP)
  RET

TEXT ·chacha8bulk(SB), 7, $16
  XORL AX, AX
  MOVQ buf+0(FP), R15
  JMP  tail
aligned_loop:
  MOVQ      in+24(FP), DI
  VMOVDQU32 0(DI)(AX*8), Y0
  VPMOVQD   Y0, X10
  VPROLQ    $32, Y0, Y0
  VPMOVQD   Y0, X11
  VBROADCASTI32X4 chachaiv<>+00(SB), Z9
  CALL      hashx4(SB)
  MOVQ      AX, BX
  SHLL      $4, BX
  MOVQ      out+48(FP), DI
  VMOVDQU64 Z9, 0(DI)(BX*1)
  ADDQ      $4, AX
tail:
  MOVQ        in_len+32(FP), CX
  SUBQ        AX, CX
  CMPQ        CX, $4
  JGE         aligned_loop
  TESTL       CX, CX
  JZ          done
  SHLL        $1, CX
  MOVL        $1, R8
  SHLL        CX, R8
  SUBL        $1, R8
  MOVB        R8, 8(SP)
  KMOVB       R8, K1
  MOVQ        in+24(FP), DI
  VMOVDQU32.Z 0(DI)(AX*8), K1, Y0
  VPMOVQD     Y0, X10
  VPROLQ      $32, Y0, Y0
  VPMOVQD     Y0, X11
  VBROADCASTI32X4 chachaiv<>+00(SB), Z9
  CALL        hashx4(SB)
  MOVQ        AX, BX
  SHLL        $4, BX
  MOVQ        out+48(FP), DI
  KMOVB       8(SP), K1
  VMOVDQU64   Z9, K1, 0(DI)(BX*1)
done:
  MOVQ in_len+32(FP), CX
  MOVQ CX, ret+72(FP)
  RET

TEXT ·chacha8x4(SB), 7, $0
  MOVQ      base+0(FP), R15
  MOVQ      offsets+8(FP), DI
  MOVQ      lengths+16(FP), CX
  MOVOU     0(CX), X11
  MOVOU     0(DI), X10
  VBROADCASTI32X4 chachaiv<>+00(SB), Z9
  CALL      hashx4(SB)
  VMOVDQU32 Z9, ret+24(FP)
  RET
