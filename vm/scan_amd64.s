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

// © 2021, Sneller Inc. All rights reserved.

//+build !noasm !appengine

// func scan(buf []byte, dst [][2]uint32) (int, int)
TEXT ·scan(SB), 7, $8
	MOVQ   buf+0(FP), SI      // SI: &raw
	MOVQ   buf_len+8(FP), DX  // DX: len(raw)
  MOVL   start+24(FP), AX   // AX: start offset
  MOVQ   dst+32(FP), DI     // &dst
  MOVQ   dst_len+40(FP), R8 // R8 = len(dst)
	CALL   scanbody(SB)
  MOVQ   CX, ret+56(FP)     // count
	MOVL   AX, ret1+64(FP)    // next offset
	RET

//
// Input
//   SI: pointer to message
//   AX: offset to first value
//   DX: len(message)
//   DI: &dst
//   R8: len(dst)
//
// Output
//   AX: offset to next value
//   CX: number of items stored
//
TEXT scanbody(SB), 7, $0
  XORL CX, CX
  XORL R14, R14
  CMPL AX, DX
  JGE  done
  CMPL CX, R8
  JGE  done

restart:
  // prefetch the next record's header
  // The assumption is records have similar sizes, thus
  // we use the record size from the previous iteration.
  ADDQ SI, R14
  PREFETCHT0 0(R14)(AX*1)
  //MOVQ 0(R14)(AX*1), R13 // it's a bit slower and unsafe

  // work out the current record
  MOVQ 0(SI)(AX*1), R15
  INCQ AX
  MOVL R15, R14; ANDL $0xf0, R14
  CMPL R14, $0xd0
  JNZ  foundNoStruct
  MOVL R15, R14; ANDL $0x0f, R14
  CMPL R14, $0x0e
  JNZ  endloop
  XORL R14, R14
varint:
  SHLL $7, R14
  SHRQ $8, R15
  INCQ AX
  MOVL R15, R13
  ANDL $0x7f, R13
  ADDL R13, R14
  BTL  $7, R15
  JNC  varint
endloop:
  MOVL AX, 0(DI)(CX*8)
  MOVL R14, 4(DI)(CX*8)
  ADDL R14, AX
  INCL CX
  CMPL AX, DX
  JGE  done
  CMPL CX, R8
  JLT  restart
done:
 	RET

foundBVM:
	// We encountered the binary version marker (BVM)
	// Skip remaining 3 bytes (major version=0x01, minor version=0x00, end marker=0xea)
	//
	// TODO: We have to make sure that the subsequent Annot with the header info
	//       is identical between blocks that are padded
	//
	MOVQ    $3, R14
	JMP     foundNoStructDone

foundNoStruct:
  // We encountered a non-structure value
  //
  // First, check for starting byte of BVM marker
  //        (=0xe0; which is strictly speaking invalid as L should be >= 3) 
  MOVL    R15, R14;
  ANDL    $0xff, R14
  CMPL    R14, $0xe0
  JZ      foundBVM

  // Otherwise parse (and skip) length of this element
  MOVQ    R15, R14; ANDQ $0x0f, R14
  CMPQ    R14, $0x0e
  JNZ     foundNoStructDone
  XORL    R14, R14
varint2:
  SHLL    $7, R14
  SHRQ    $8, R15
  INCQ    AX
  MOVL    R15, R13
  ANDL    $0x7f, R13
  ADDL    R13, R14
  BTL     $7, R15
  JNC     varint2
foundNoStructDone:
  ADDQ    R14, AX  // Add length
  CMPQ    AX, DX   // Are we at the end of the message?
  JLT     restart
  JMP     done
