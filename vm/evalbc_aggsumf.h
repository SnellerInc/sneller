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

/*
Algorithm:

    t := sum + x

    if abs(sum) >= abs(x) {
        a = sum
        b = x
    } else {
        a = x
        b = sum
    }

    c += (a - t) + b
    sum = t

    // the correction `c` is applied at the very end of calculations
*/

// aggregate memory layout
#define COMP_OFFSET     (0*64)  // 16 * float64
#define SUM_OFFSET      (2*64)  // 16 * float64
#define COUNTER_OFFSET  (4*64)  // 16 * uint64

// Input:
// - x0, x1         - input float64 values
// - mask0, mask1   - two K registers to use
// - addr           - offset to [2 * 16]float64
// - the remaining arguments are temporaries
#define BC_NEUMAIER_SUM(x0, x1, mask0, mask1, addr, c0, c1, sum0, sum1, t0, t1, a0, a1, b0, b1, abs_sum0, abs_sum1, abs_x0, abs_x1, abs_mask)   \
  /* load compensation c */                            \
  VMOVDQU64     (0*64)(addr), c0                       \
  VMOVDQU64     (1*64)(addr), c1                       \
                                                       \
  /* load sum */                                       \
  VMOVDQU64     (2*64)(addr), sum0                     \
  VMOVDQU64     (3*64)(addr), sum1                     \
                                                       \
  /* t = sum + x */                                    \
  VADDPD        sum0, x0, t0                           \
  VADDPD        sum1, x1, t1                           \
                                                       \
  /* tmp0 = abs(sum) */                                \
  VPBROADCASTQ  CONSTF64_ABS_BITS(), abs_mask          \
  VPANDQ        abs_mask, sum0, abs_sum0               \
  VPANDQ        abs_mask, sum1, abs_sum1               \
                                                       \
  /* tmp1 = abs(x) */                                  \
  VPANDQ        abs_mask, x0, abs_x0                   \
  VPANDQ        abs_mask, x1, abs_x1                   \
                                                       \
  /* p = abs(sum) >= abs(x) */                         \
  VPCMPD        $VPCMP_IMM_GE, abs_sum0, abs_x0, mask0 \
  VPCMPD        $VPCMP_IMM_GE, abs_sum1, abs_x1, mask1 \
                                                       \
  /* a = p ? sum : x */                                \
  VBLENDMPD     sum0, x0, mask0, a0                    \
  VBLENDMPD     sum1, x1, mask1, a1                    \
                                                       \
  /* b = p ? x : sum */                                \
  VBLENDMPD     x0, sum0, mask0, b0                    \
  VBLENDMPD     x1, sum1, mask1, b1                    \
                                                       \
  /* sum = t; store sum */                             \
  VMOVDQU64     t0, (2*64)(addr)                       \
  VMOVDQU64     t1, (3*64)(addr)                       \
                                                       \
  /* c += (a - t) + b */                               \
  VSUBPD        t0, a0, a0  /* a := a - t */           \
  VSUBPD        t1, a1, a1                             \
  VADDPD        a0, b0, a0  /* a := a + b */           \
  VADDPD        a1, b1, a1                             \
  VADDPD        a0, c0, c0  /* c += a */               \
  VADDPD        a1, c1, c1                             \
                                                       \
  /* store c */                                        \
  VMOVDQU64     c0, (0*64)(addr)                       \
  VMOVDQU64     c1, (1*64)(addr)


/* Implementation of the algorithm for single ZMM (8 x float64) */
#define BC_NEUMAIER_SUM_LANE(x, c, sum, t, abs_mask, abs_sum, abs_x, pred)    \
  /* t = sum + x */                                                     \
  VADDPD        sum, x, t                                               \
                                                                        \
  /* tmp0 = abs(sum) */                                                 \
  VPANDQ        abs_mask, sum, abs_sum                                  \
                                                                        \
  /* tmp1 = abs(x) */                                                   \
  VPANDQ        abs_mask, x, abs_x                                      \
                                                                        \
  /* p = abs(sum) >= abs(x) */                                          \
  VPCMPD        $VPCMP_IMM_GE, abs_sum, abs_x, pred                     \
                                                                        \
  /* a = p ? sum : x */                                                 \
  /* b = p ? x : sum */                                                 \
  VBLENDMPD      sum, x, pred, a                                        \
  VBLENDMPD      x, sum, pred, b                                        \
                                                                        \
  /* c += (a - t) + b */                                                \
  VSUBPD        t, a, a  /* a := a - t */                               \
  VADDPD        a, b, a  /* a := a + b */                               \
  VADDPD        a, c, c  /* c += a */


TEXT bcaggslotsumf(SB), NOSPLIT|NOFRAME, $0
#define ones        Z2

#define x0          Z4
#define x1          Z5

#define sum0        Z16
#define c0          Z17
#define sum1        Z18
#define c1          Z19

#define t           Z20
#define abs_mask    Z21
#define abs_sum     Z22
#define abs_x       Z23
#define a           Z24
#define b           Z25

#define counter0    Z26
#define counter1    Z27

  BC_UNPACK_3xSLOT(BC_AGGSLOT_SIZE, OUT(DX), OUT(BX), OUT(CX))
  BC_LOAD_K1_K2_FROM_SLOT(OUT(K1), OUT(K6), IN(CX))
  BC_LOAD_F64_FROM_SLOT_MASKED(OUT(Z4), OUT(Z5), IN(BX), IN(K1), IN(K6))

  // Load the aggregation data pointer.
  BC_UNPACK_RU32(0, OUT(R15))
  ADDQ $const_aggregateTagSize, R15
  ADDQ radixTree64_values(R10), R15

  /* Load constants */
  BC_FILL_ONES(Z6)                              // = 0xffffff
  VPABSQ    Z6, ones                            // = uint64(1)
  VBROADCASTSD  CONSTF64_ABS_BITS(), abs_mask

  // load buckets
  VMOVDQU32     0(VIRT_VALUES)(DX*1), K1, Z6
  VPCONFLICTD.Z Z6, K1, Z11

  VPBROADCASTD  CONSTD_32(), Z31
  VPLZCNTD  Z11, Z30
  VPSUBD    Z30, Z31, Z30
  VPSLLD    $3, Z30, Z30
  VPADDD    Z30, Z6, Z6
  VEXTRACTI32X8 $1, Z6, Y7

  KMOVB K1, K2
  KMOVB K1, K3
  KMOVB K1, K4
  VPXORD sum0, sum0, sum0
  VPXORD c0, c0, c0
  VPXORD counter0, counter0, counter0
  VGATHERDPD (SUM_OFFSET)(R15)(Y6*1), K2, sum0
  VGATHERDPD (COMP_OFFSET)(R15)(Y6*1), K3, c0
  VGATHERDPD (COUNTER_OFFSET)(R15)(Y6*1), K4, counter0

  KMOVB K6, K2
  KMOVB K6, K3
  KMOVB K6, K4
  VPXORD sum1, sum1, sum1
  VPXORD c1, c1, c1
  VPXORD counter1, counter1, counter1
  VGATHERDPD (SUM_OFFSET)(R15)(Y7*1), K2, sum1
  VGATHERDPD (COMP_OFFSET)(R15)(Y7*1), K3, c1
  VGATHERDPD (COUNTER_OFFSET)(R15)(Y7*1), K4, counter1

  BC_NEUMAIER_SUM_LANE(x0, c0, sum0, t, abs_mask, abs_sum, abs_x, K3)
  VMOVDQA64 t, sum0

  BC_NEUMAIER_SUM_LANE(x1, c1, sum1, t, abs_mask, abs_sum, abs_x, K3)
  VMOVDQA64 t, sum1

  VPADDQ ones, counter0, counter0
  VPADDQ ones, counter1, counter1

  KMOVB K1, K2
  KMOVB K1, K3
  KMOVB K1, K4

  VSCATTERDPD sum0,     K2, (SUM_OFFSET)(R15)(Y6*1)
  VSCATTERDPD c0,       K3, (COMP_OFFSET)(R15)(Y6*1)
  VPSCATTERDQ counter0, K4, (COUNTER_OFFSET)(R15)(Y6*1)

  KMOVB K6, K4
  KMOVB K6, K5

  VSCATTERDPD sum1,     K4, (SUM_OFFSET)(R15)(Y7*1)
  VSCATTERDPD c1,       K5, (COMP_OFFSET)(R15)(Y7*1)
  VPSCATTERDQ counter1, K6, (COUNTER_OFFSET)(R15)(Y7*1)
next:
  NEXT_ADVANCE(BC_SLOT_SIZE*3 + BC_AGGSLOT_SIZE)


#undef counter0
#undef counter1
#undef x0
#undef x1
#undef sum0
#undef sum1
#undef c0
#undef c1
#undef t
#undef abs_mask
#undef abs_sum
#undef abs_x
#undef a
#undef b

#undef COMP_OFFSET
#undef SUM_OFFSET
#undef COUNTER_OFFSET
