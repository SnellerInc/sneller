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


TEXT ·bcobjectsize_test_uvint_length(SB), NOSPLIT, $56
    // prepare registers
    MOVWQZX     valid+0(FP), AX
    KMOVQ       AX, K1

    MOVQ        data+8(FP), SI
    MOVQ        offsets+32(FP), AX
    VMOVDQU64   (AX), Z30

    CALL objectsize_test_uvint_length(SB)

    MOVQ        mask+40(FP), DI
    KMOVW       K7, AX
    MOVW        AX, (DI)
    MOVQ        length+48(FP), DI
    VMOVDQU64   Z31, (DI)
    RET


TEXT ·bcobjectsize_test_object_header_size(SB), NOSPLIT, $56
    // prepare registers
    MOVWQZX     valid+0(FP), AX
    KMOVQ       AX, K1

    MOVQ        data+8(FP), SI
    MOVQ        offsets+32(FP), AX
    VMOVDQU64   (AX), Z30

    CALL objectsize_test_object_header_size(SB)

    MOVQ        mask+40(FP), DI
    KMOVW       K7, AX
    MOVW        AX, (DI)
    MOVQ        headerLength+48(FP), DI
    VMOVDQU64   Z1, (DI)
    MOVQ        objectLength+56(FP), DI
    VMOVDQU64   Z2, (DI)
    RET
