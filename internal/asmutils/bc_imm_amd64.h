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

// This file contains immediate values / predicates that can be used as operands in some instructions

// Immediates for VEXTRACT[...] that extracts low|high half of ZMM|YMM register
#define VEXTRACT_IMM_LO     0
#define VEXTRACT_IMM_HI     1

// Immediates for VROUND[PD|PS|SD|SS]
#define VROUND_IMM_NEAREST     0x00
#define VROUND_IMM_DOWN        0x01
#define VROUND_IMM_UP          0x02
#define VROUND_IMM_TRUNC       0x03
#define VROUND_IMM_CURRENT     0x04
#define VROUND_IMM_SUPPRESS    0x08
#define VROUND_IMM_NEAREST_SAE 0x08
#define VROUND_IMM_DOWN_SAE    0x09
#define VROUND_IMM_UP_SAE      0x0A
#define VROUND_IMM_TRUNC_SAE   0x0B
#define VROUND_IMM_CURRENT_SAE 0x0C

// Immediates for VPCMP[D|Q]
#define VPCMP_IMM_EQ        0x00   // ==
#define VPCMP_IMM_LT        0x01   // <
#define VPCMP_IMM_LE        0x02   // <=
#define VPCMP_IMM_FALSE     0x03   // False
#define VPCMP_IMM_NE        0x04   // !=
#define VPCMP_IMM_GE        0x05   // >=
#define VPCMP_IMM_GT        0x06   // >
#define VPCMP_IMM_TRUE      0x07   // True

// Immediates for VCMP[PD|PS|SD|SS]
#define VCMP_IMM_EQ_OQ      0x00   // Equal             (Quiet    , Ordered)
#define VCMP_IMM_LT_OS      0x01   // Less              (Signaling, Ordered)
#define VCMP_IMM_LE_OS      0x02   // Less/Equal        (Signaling, Ordered)
#define VCMP_IMM_UNORD_Q    0x03   // Unordered         (Quiet)
#define VCMP_IMM_NEQ_UQ     0x04   // Not Equal         (Quiet    , Unordered)
#define VCMP_IMM_NLT_US     0x05   // Not Less          (Signaling, Unordered)
#define VCMP_IMM_NLE_US     0x06   // Not Less/Equal    (Signaling, Unordered)
#define VCMP_IMM_ORD_Q      0x07   // Ordered           (Quiet)
#define VCMP_IMM_EQ_UQ      0x08   // Equal             (Quiet    , Unordered)
#define VCMP_IMM_NGE_US     0x09   // Not Greater/Equal (Signaling, Unordered)
#define VCMP_IMM_NGT_US     0x0A   // Not Greater       (Signaling, Unordered)
#define VCMP_IMM_FALSE_OQ   0x0B   // False             (Quiet    , Ordered)
#define VCMP_IMM_NEQ_OQ     0x0C   // Not Equal         (Quiet    , Ordered)
#define VCMP_IMM_GE_OS      0x0D   // Greater/Equal     (Signaling, Ordered)
#define VCMP_IMM_GT_OS      0x0E   // Greater           (Signaling, Ordered)
#define VCMP_IMM_TRUE_UQ    0x0F   // True              (Quiet    , Unordered)
#define VCMP_IMM_EQ_OS      0x10   // Equal             (Signaling, Ordered)
#define VCMP_IMM_LT_OQ      0x11   // Less              (Quiet    , Ordered)
#define VCMP_IMM_LE_OQ      0x12   // Less/Equal        (Quiet    , Ordered)
#define VCMP_IMM_UNORD_S    0x13   // Unordered         (Signaling)
#define VCMP_IMM_NEQ_US     0x14   // Not Equal         (Signaling, Unordered)
#define VCMP_IMM_NLT_UQ     0x15   // Not Less          (Quiet    , Unordered)
#define VCMP_IMM_NLE_UQ     0x16   // Not Less/Equal    (Quiet    , Unordered)
#define VCMP_IMM_ORD_S      0x17   // Ordered           (Signaling)
#define VCMP_IMM_EQ_US      0x18   // Equal             (Signaling, Unordered)
#define VCMP_IMM_NGE_UQ     0x19   // Not Greater/Equal (Quiet    , Unordered)
#define VCMP_IMM_NGT_UQ     0x1A   // Not Greater       (Quiet    , Unordered)
#define VCMP_IMM_FALSE_OS   0x1B   // False             (Signaling, Ordered)
#define VCMP_IMM_NEQ_OS     0x1C   // Not Equal         (Signaling, Ordered)
#define VCMP_IMM_GE_OQ      0x1D   // Greater/Equal     (Quiet    , Ordered)
#define VCMP_IMM_GT_OQ      0x1E   // Greater           (Quiet    , Ordered)
#define VCMP_IMM_TRUE_US    0x1F   // True              (Signaling, Unordered)

#define SHUFFLE_IMM_2x1b(HI, LO) ((HI) << 1 | LO)
#define SHUFFLE_IMM_4x2b(I3, I2, I1, I0) ((I3) << 6 | (I2 << 4) | (I1 << 2) | (I0))

#define TLOG_BLEND_AB 0xE4
#define TLOG_BLEND_BA 0xD8

#define TERNLOG_BLEND_AB 0xE4
#define TERNLOG_BLEND_BA 0xD8

// CMOVcc instructions that use INTEL syntax instead of Plan9
//
// Mapping of condition codes between Plan9 and Intel syntax:
//
// +----------+------------+
// | Plan9    | Intel      |
// +----------+------------+
// | OS       | o          |
// | OC       | no         |
// | CS, LO   | b, c, nae  |
// | CC, HS   | nb, nc, ae |
// | EQ       | e, z       |
// | NE       | ne, nz     |
// | LS       | be, na     |
// | HI       | nbe, a     |
// | MI       | s          |
// | PL       | ns         |
// | PS       | p, pe      |
// | PC       | np, po     |
// | LT       | l, nge     |
// | GE       | nl, ge     |
// | LE       | le, ng     |
// | GT       | nle, g     |
// +----------+------------+
#define CMOVL_AE  CMOVLCC
#define CMOVQ_AE  CMOVQCC

#define CMOVL_NB  CMOVLCC
#define CMOVQ_NB  CMOVQCC

#define CMOVL_NC  CMOVLCC
#define CMOVQ_NC  CMOVQCC

#define CMOVL_B   CMOVLCS
#define CMOVQ_B   CMOVQCS

#define CMOVL_C   CMOVLCS
#define CMOVQ_C   CMOVQCS

#define CMOVL_NAE CMOVLCS
#define CMOVQ_NAE CMOVQCS
