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

  // Takes a single uint16 parameter denoting opcode ID and returns the address of the associated handler
TEXT ·opcodeAddressUnsafe(SB), NOSPLIT|NOFRAME, $0-16
  MOVWQZX op+0(FP), AX   // 16-bit opcode ID
  LEAQ opaddrs+0(SB), CX // opaddrs table base address
  MOVQ (CX)(AX*8), AX
  MOVQ AX, ret+8(FP)     // return value
  RET

