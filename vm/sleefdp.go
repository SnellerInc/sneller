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

package vm

import "math"

// The content of this file was ported from SLEEF library <https://github.com/shibatch/sleef>,
// which is distributed under the following conditions:
//
//	Copyright Naoki Shibata and contributors 2010 - 2021.
//
// Distributed under the Boost Software License, Version 1.0.
//
//	(See accompanying file LICENSE.txt or copy at
//	      http://www.boost.org/LICENSE_1_0.txt)

const (
	sleefDblMin   = 2.2250738585072014e-308
	sleefRln2     = 1.44269504088896340735992468100189213742664595415298593413544940693
	sleefL2U      = 0.69314718055966295651160180568695068359375
	sleefL2L      = 0.28235290563031577122588448175013436025525412068e-12
	sleefL10U     = 0.30102999566383914498 // log 2 / log 10
	sleefL10L     = 1.4205023227266099418e-13
	sleefLog10of2 = 3.3219280948873623478703194294893901758648313930

	sleefPIA          = 3.1415926218032836914
	sleefPIB          = 3.1786509424591713469e-08
	sleefPIC          = 1.2246467864107188502e-16
	sleefPID          = 1.2736634327021899816e-24
	sleefTRIGRANGEMAX = 1e+14

	sleefPIA2          = 3.141592653589793116
	sleefPIB2          = 1.2246467991473532072e-16
	sleefTRIGRANGEMAX2 = 15.0

	sleefMPI   = 3.141592653589793238462643383279502884
	sleefM1PI  = 0.318309886183790671537767526745028724
	sleefM2PI  = 0.636619772367581343075535053490057448
	sleefM2PIH = 0.63661977236758138243
	sleefM2PIL = -3.9357353350364971764e-17
)

type sleefDouble2 struct {
	x float64
	y float64
}

type sleefDi struct {
	d float64
	i int32
}

type sleefDdi struct {
	dd sleefDouble2
	i  int32
}

func sleefDoubleToRawLongBits(d float64) int64 {
	return int64(math.Float64bits(d))
}

func sleefLongBitsToDouble(i int64) float64 {
	return math.Float64frombits(uint64(i))
}

func sleefILogbk(d float64) int64 {
	m := d < 4.9090934652977266e-91
	if m {
		d = 2.037035976334486e90 * d
	}
	q := (sleefDoubleToRawLongBits(d) >> 52) & 0x7ff
	if m {
		return q - (300 + 0x03ff)
	}
	return q - 0x03ff
}

func sleefTrunck(x float64) float64 {
	return float64(int64(x))
}

func sleefSign(d float64) float64 {
	return sleefMulsign(1.0, d)
}

func sleefIsInt(d float64) bool {
	x := d - float64(1<<31)*float64(int(d*(1.0/float64(1<<31))))
	return (x == float64(int(x))) || (sleefFabsk(d) >= float64(1<<53))
}

func sleefIsOdd(d float64) bool {
	x := d - float64(1<<31)*float64(int(d*(1.0/float64(1<<31))))
	return (int(x)&0x01 != 0) && sleefFabsk(d) < float64(1<<53)
}

func sleefFmink(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

func sleefFmaxk(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}

// sleefILogb2k is similar to ilogbk, but the argument has to be a normalized FP value.
func sleefILogb2k(d float64) int64 {
	return ((sleefDoubleToRawLongBits(d) >> 52) & 0x7ff) - 0x3ff
}

func sleefUpper(d float64) float64 {
	return sleefLongBitsToDouble(int64(uint64(sleefDoubleToRawLongBits(d)) & 0xfffffffff8000000))
}

func sleefPow2i(q int64) float64 {
	return sleefLongBitsToDouble((q + 0x3ff) << 52)
}

func sleefLdexpk(x float64, q int64) float64 {
	m := q >> 31
	m = (((m + q) >> 9) - m) << 7
	q = q - (m << 2)
	m += 0x3ff
	if m < 0 {
		m = 0
	}
	if m > 0x7ff {
		m = 0x7ff
	}
	u := sleefLongBitsToDouble(m << 52)
	x = x * u * u * u * u
	u = sleefLongBitsToDouble((q + 0x3ff) << 52)
	return x * u
}

func sleefLdexp2k(d float64, e int64) float64 { // faster than ldexpk, short reach
	return d * sleefPow2i(e>>1) * sleefPow2i(e-(e>>1))
}

func sleefLdexp3k(d float64, e int64) float64 { // very fast, no denormal
	return sleefLongBitsToDouble(sleefDoubleToRawLongBits(d) + (e << 52))
}

func sleefRintki(x float64) int64 {
	if x < 0.0 {
		return int64(x - 0.5)
	}
	return int64(x + 0.5)
}

func sleefRintk(x float64) float64 {
	return float64(sleefRintki(x))
}

func sleefIsNegZero(x float64) bool {
	return math.Signbit(x) && x == 0.0
}

func sleefFabsk(x float64) float64 {
	return math.Float64frombits(0x7fffffffffffffff & math.Float64bits(x))
}

func sleefMulsign(x, y float64) float64 {
	return math.Float64frombits(math.Float64bits(x) ^ (math.Float64bits(y) & (1 << 63)))
}

func sleefOrsign(x, y float64) float64 {
	return math.Float64frombits(math.Float64bits(x) | (math.Float64bits(y) & (1 << 63)))
}

func sleefDdadd2D2DD(x, y float64) sleefDouble2 {
	var r sleefDouble2
	r.x = x + y
	v := r.x - x
	r.y = (x - (r.x - v)) + (y - v)
	return r
}

func sleefDddivD2D2D2(n, d sleefDouble2) sleefDouble2 {
	t := 1.0 / d.x
	dh := sleefUpper(d.x)
	dl := d.x - dh
	th := sleefUpper(t)
	tl := t - th
	nhh := sleefUpper(n.x)
	nhl := n.x - nhh

	var q sleefDouble2
	q.x = n.x * t
	u := -q.x + nhh*th + nhh*tl + nhl*th + nhl*tl + q.x*(1-dh*th-dh*tl-dl*th-dl*tl)
	q.y = t*(n.y-q.x*d.y) + u
	return q
}

func sleefDdmulDD2D2(x, y sleefDouble2) float64 {
	xh := sleefUpper(x.x)
	xl := x.x - xh
	yh := sleefUpper(y.x)
	yl := y.x - yh
	return x.y*yh + xh*y.y + xl*yl + xh*yl + xl*yh + xh*yh
}

func sleefDdmulD2D2D(x sleefDouble2, y float64) sleefDouble2 {
	xh := sleefUpper(x.x)
	xl := x.x - xh
	yh := sleefUpper(y)
	yl := y - yh

	var r sleefDouble2
	r.x = x.x * y
	r.y = xh*yh - r.x + xl*yh + xh*yl + xl*yl + x.y*y
	return r
}

func sleefDdaddD2D2D2(x, y sleefDouble2) sleefDouble2 {
	// |x| >= |y|
	var r sleefDouble2
	r.x = x.x + y.x
	r.y = x.x - r.x + y.x + x.y + y.y
	return r
}

func sleefDdaddD2D2D(x sleefDouble2, y float64) sleefDouble2 {
	// |x| >= |y|
	var r sleefDouble2
	r.x = x.x + y
	r.y = x.x - r.x + y + x.y
	return r
}

func sleefDdaddD2DD2(x float64, y sleefDouble2) sleefDouble2 {
	// |x| >= |y|
	var r sleefDouble2
	r.x = x + y.x
	r.y = x - r.x + y.x + y.y
	return r
}

func sleefDdaddD2DD(x, y float64) sleefDouble2 {
	// |x| >= |y|
	var r sleefDouble2
	r.x = x + y
	r.y = x - r.x + y
	return r
}

func sleefDdadd2D2DD2(x float64, y sleefDouble2) sleefDouble2 {
	var r sleefDouble2
	r.x = x + y.x
	v := r.x - x
	r.y = (x - (r.x - v)) + (y.x - v) + y.y
	return r
}

func sleefDdadd2D2D2D2(x, y sleefDouble2) sleefDouble2 {
	var r sleefDouble2
	r.x = x.x + y.x
	v := r.x - x.x
	r.y = (x.x - (r.x - v)) + (y.x - v)
	r.y += x.y + y.y
	return r
}

func sleefDdadd2D2D2D(x sleefDouble2, y float64) sleefDouble2 {
	var r sleefDouble2
	r.x = x.x + y
	v := r.x - x.x
	r.y = (x.x - (r.x - v)) + (y - v)
	r.y += x.y
	return r
}

func sleefDdsubD2D2D2(x, y sleefDouble2) sleefDouble2 {
	// |x| >= |y|
	var r sleefDouble2
	r.x = x.x - y.x
	r.y = x.x - r.x - y.x + x.y - y.y
	return r
}

func sleefDdmulD2D2D2(x, y sleefDouble2) sleefDouble2 {
	xh := sleefUpper(x.x)
	xl := x.x - xh
	yh := sleefUpper(y.x)
	yl := y.x - yh

	var r sleefDouble2
	r.x = x.x * y.x
	r.y = xh*yh - r.x + xl*yh + xh*yl + xl*yl + x.x*y.y + x.y*y.x
	return r
}

func sleefDdmulD2DD(x, y float64) sleefDouble2 {
	xh := sleefUpper(x)
	xl := x - xh
	yh := sleefUpper(y)
	yl := y - yh

	var r sleefDouble2
	r.x = x * y
	r.y = xh*yh - r.x + xl*yh + xh*yl + xl*yl
	return r
}

func sleefDdnegD2D2(d sleefDouble2) sleefDouble2 {
	var r sleefDouble2
	r.x = -d.x
	r.y = -d.y
	return r
}

func sleefDdsqrtD2D2(d sleefDouble2) sleefDouble2 {
	t := math.Sqrt(d.x + d.y)
	return sleefDdscaleD2D2D(sleefDdmulD2D2D2(sleefDdadd2D2D2D2(d, sleefDdmulD2DD(t, t)), sleefDdrecD2D(t)), 0.5)
}

func sleefDdsqrtD2D(d float64) sleefDouble2 {
	t := math.Sqrt(d)
	return sleefDdscaleD2D2D(sleefDdmulD2D2D2(sleefDdadd2D2DD2(d, sleefDdmulD2DD(t, t)), sleefDdrecD2D(t)), 0.5)
}

func sleefDdrecD2D(d float64) sleefDouble2 {
	t := 1.0 / d
	dh := sleefUpper(d)
	dl := d - dh
	th := sleefUpper(t)
	tl := t - th

	var q sleefDouble2
	q.x = t
	q.y = t * (1 - dh*th - dh*tl - dl*th - dl*tl)
	return q
}

func sleefDdnormalizeD2D2(t sleefDouble2) sleefDouble2 {
	var s sleefDouble2
	s.x = t.x + t.y
	s.y = t.x - s.x + t.y
	return s
}

func sleefDdscaleD2D2D(d sleefDouble2, s float64) sleefDouble2 {
	var r sleefDouble2
	r.x = d.x * s
	r.y = d.y * s
	return r
}

func sleefDdsquD2D2(x sleefDouble2) sleefDouble2 {
	xh := sleefUpper(x.x)
	xl := x.x - xh
	var r sleefDouble2
	r.x = x.x * x.x
	r.y = xh*xh - r.x + (xh+xh)*xl + xl*xl + x.x*(x.y+x.y)
	return r
}

func sleefLogk(d float64) sleefDouble2 {
	o := d < sleefDblMin
	if o {
		d *= float64(int64(1<<32)) * float64(int64(1<<32))
	}

	e := sleefILogb2k(d * (1.0 / 0.75))
	m := sleefLdexp3k(d, -e)

	if o {
		e -= 64
	}

	x := sleefDddivD2D2D2(sleefDdadd2D2DD(-1.0, m), sleefDdadd2D2DD(1.0, m))
	x2 := sleefDdsquD2D2(x)
	x4 := x2.x * x2.x
	x8 := x4 * x4
	x16 := x8 * x8
	t := sleefPoly9(x2.x, x4, x8, x16,
		0.116255524079935043668677,
		0.103239680901072952701192,
		0.117754809412463995466069,
		0.13332981086846273921509,
		0.153846227114512262845736,
		0.181818180850050775676507,
		0.222222222230083560345903,
		0.285714285714249172087875,
		0.400000000000000077715612,
	)

	c := sleefDd(0.666666666666666629659233, 3.80554962542412056336616e-17)
	s := sleefDdmulD2D2D(sleefDd(0.693147180559945286226764, 2.319046813846299558417771e-17), float64(e))
	s = sleefDdaddD2D2D2(s, sleefDdscaleD2D2D(x, 2.0))
	x = sleefDdmulD2D2D2(x2, x)
	s = sleefDdaddD2D2D2(s, sleefDdmulD2D2D2(x, c))
	x = sleefDdmulD2D2D2(x2, x)
	s = sleefDdaddD2D2D2(s, sleefDdmulD2D2D(x, t))
	return s
}

func sleefExpk(d sleefDouble2) float64 {
	q := sleefRintki((d.x + d.y) * sleefRln2)

	s := sleefDdadd2D2D2D(d, float64(q)*-sleefL2U)
	s = sleefDdadd2D2D2D(s, float64(q)*-sleefL2L)
	s = sleefDdnormalizeD2D2(s)
	s2 := s.x * s.x
	s4 := s2 * s2
	s8 := s4 * s4

	u := sleefPoly10(s.x, s2, s4, s8,
		2.51069683420950419527139e-08,
		2.76286166770270649116855e-07,
		2.75572496725023574143864e-06,
		2.48014973989819794114153e-05,
		0.000198412698809069797676111,
		0.0013888888939977128960529,
		0.00833333333332371417601081,
		0.0416666666665409524128449,
		0.166666666666666740681535,
		0.500000000000000999200722,
	)

	t := sleefDdaddD2DD2(1.0, s)
	t = sleefDdaddD2D2D2(t, sleefDdmulD2D2D(sleefDdsquD2D2(s), u))

	u = sleefLdexpk(t.x+t.y, q)
	if d.x < -1000.0 {
		u = 0.0
	}
	return u
}

func sleefExpk2(d sleefDouble2) sleefDouble2 {
	q := sleefRintki((d.x + d.y) * sleefRln2)
	s := sleefDdadd2D2D2D(d, float64(q)*-sleefL2U)
	s = sleefDdadd2D2D2D(s, float64(q)*-sleefL2L)

	u := +0.1602472219709932072e-9
	u = sleefMLA(u, s.x, +0.2092255183563157007e-8)
	u = sleefMLA(u, s.x, +0.2505230023782644465e-7)
	u = sleefMLA(u, s.x, +0.2755724800902135303e-6)
	u = sleefMLA(u, s.x, +0.2755731892386044373e-5)
	u = sleefMLA(u, s.x, +0.2480158735605815065e-4)
	u = sleefMLA(u, s.x, +0.1984126984148071858e-3)
	u = sleefMLA(u, s.x, +0.1388888888886763255e-2)
	u = sleefMLA(u, s.x, +0.8333333333333347095e-2)
	u = sleefMLA(u, s.x, +0.4166666666666669905e-1)

	t := sleefDdadd2D2D2D(sleefDdmulD2D2D(s, u), +0.1666666666666666574e+0)
	t = sleefDdadd2D2D2D(sleefDdmulD2D2D2(s, t), 0.5)
	t = sleefDdadd2D2D2D2(s, sleefDdmulD2D2D2(sleefDdsquD2D2(s), t))
	t = sleefDdadd2D2DD2(1.0, t)

	t.x = sleefLdexp2k(t.x, q)
	t.y = sleefLdexp2k(t.y, q)

	if d.x < -1000 {
		return sleefDd(0, 0)
	}
	return t
}

func sleefRempisub(x float64) sleefDi {
	var ret sleefDi
	c := sleefMulsign(float64(1<<52), x)

	var rint4x float64
	if sleefFabsk(4.0*x) > float64(1<<52) {
		rint4x = 4 * x
	} else {
		rint4x = sleefOrsign(sleefMLA(4.0, x, c)-c, x)
	}
	var rintx float64
	if sleefFabsk(x) > float64(1<<52) {
		rintx = x
	} else {
		rintx = sleefOrsign(x+c-c, x)
	}
	ret.d = sleefMLA(-0.25, rint4x, x)
	ret.i = int32(sleefMLA(-4.0, rintx, rint4x))
	return ret
}

func sleefRempi(a float64) sleefDdi {
	var x, y sleefDouble2
	var di sleefDi
	ex := sleefILogb2k(a) - 55
	var q int32
	if ex > (700 - 55) {
		q = -64
	}
	a = sleefLdexp3k(a, int64(q))
	if ex < 0 {
		ex = 0
	}
	ex *= 4
	x = sleefDdmulD2DD(a, sleefRemPiTabDP[ex])
	di = sleefRempisub(x.x)
	q = di.i
	x.x = di.d

	x = sleefDdnormalizeD2D2(x)
	y = sleefDdmulD2DD(a, sleefRemPiTabDP[ex+1])
	x = sleefDdadd2D2D2D2(x, y)
	di = sleefRempisub(x.x)
	q += di.i

	x.x = di.d
	x = sleefDdnormalizeD2D2(x)
	y = sleefDdmulD2D2D(sleefDd(sleefRemPiTabDP[ex+2], sleefRemPiTabDP[ex+3]), a)
	x = sleefDdadd2D2D2D2(x, y)
	x = sleefDdnormalizeD2D2(x)
	x = sleefDdmulD2D2D2(x, sleefDd(3.141592653589793116*2, 1.2246467991473532072e-16*2))
	var ret sleefDdi
	ret.i = q
	if sleefFabsk(a) < 0.7 {
		ret.dd = sleefDd(a, 0)
	} else {
		ret.dd = x
	}
	return ret
}

func sleefDd(x, y float64) sleefDouble2 {
	return sleefDouble2{x, y}
}

func sleefMLA(x, y, z float64) float64 {
	return math.FMA(x, y, z)
	//return x*y + z
}

func sleefPoly2(x, c1, c0 float64) float64 {
	return sleefMLA(x, c1, c0)
}

func sleefPoly3(x, x2, c2, c1, c0 float64) float64 {
	return sleefMLA(x2, c2, sleefMLA(x, c1, c0))
}

func sleefPoly4(x, x2, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x2, sleefMLA(x, c3, c2), sleefMLA(x, c1, c0))
}

func sleefPoly6(x, x2, x4, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x4, sleefPoly2(x, c5, c4), sleefPoly4(x, x2, c3, c2, c1, c0))
}

func sleefPoly7(x, x2, x4, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x4, sleefPoly3(x, x2, c6, c5, c4), sleefPoly4(x, x2, c3, c2, c1, c0))
}

func sleefPoly8(x, x2, x4, c7, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x4, sleefPoly4(x, x2, c7, c6, c5, c4), sleefPoly4(x, x2, c3, c2, c1, c0))
}

func sleefPoly9(x, x2, x4, x8, c8, c7, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x8, c8, sleefPoly8(x, x2, x4, c7, c6, c5, c4, c3, c2, c1, c0))
}

func sleefPoly10(x, x2, x4, x8, c9, c8, c7, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x8, sleefPoly2(x, c9, c8), sleefPoly8(x, x2, x4, c7, c6, c5, c4, c3, c2, c1, c0))
}

func sleefPoly12(x, x2, x4, x8, cb, ca, c9, c8, c7, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x8, sleefPoly4(x, x2, cb, ca, c9, c8), sleefPoly8(x, x2, x4, c7, c6, c5, c4, c3, c2, c1, c0))
}

func sleefPoly16(x, x2, x4, x8, cf, ce, cd, cc, cb, ca, c9, c8, c7, c6, c5, c4, c3, c2, c1, c0 float64) float64 {
	return sleefMLA(x8, sleefPoly8(x, x2, x4, cf, ce, cd, cc, cb, ca, c9, c8), sleefPoly8(x, x2, x4, c7, c6, c5, c4, c3, c2, c1, c0))
}

func sleefLog10(d float64) float64 {
	o := d < sleefDblMin
	if o {
		d *= float64(int64(1<<32)) * float64(int64(1<<32))
	}

	e := sleefILogb2k(d * (1.0 / 0.75))
	m := sleefLdexp3k(d, -e)

	if o {
		e -= 64
	}

	x := sleefDddivD2D2D2(sleefDdadd2D2DD(-1.0, m), sleefDdadd2D2DD(1.0, m))
	x2 := x.x * x.x
	x4 := x2 * x2
	x8 := x4 * x4

	t := sleefPoly7(x2, x4, x8,
		+0.6653725819576758460e-1,
		+0.6625722782820833712e-1,
		+0.7898105214313944078e-1,
		+0.9650955035715275132e-1,
		+0.1240841409721444993e+0,
		+0.1737177927454605086e+0,
		+0.2895296546021972617e+0,
	)

	s := sleefDdmulD2D2D(sleefDd(0.30102999566398119802, -2.803728127785170339e-18), float64(e))
	s = sleefDdaddD2D2D2(s, sleefDdmulD2D2D2(x, sleefDd(0.86858896380650363334, 1.1430059694096389311e-17)))
	s = sleefDdaddD2D2D(s, x2*x.x*t)
	r := s.x + s.y

	if math.IsInf(d, 0) {
		r = math.Inf(1)
	}

	if d < 0.0 || math.IsNaN(d) {
		r = math.NaN()
	}

	if d == 0.0 {
		r = math.Inf(-1)
	}
	return r
}

func sleefAtan2k(y, x sleefDouble2) sleefDouble2 {
	var q int
	if x.x < 0 {
		x.x = -x.x
		x.y = -x.y
		q = -2
	}
	if y.x > x.x {
		t := x
		x = y
		y.x = -t.x
		y.y = -t.y
		q += 1
	}

	s := sleefDddivD2D2D2(y, x)
	t := sleefDdsquD2D2(s)
	t = sleefDdnormalizeD2D2(t)

	t2 := t.x * t.x
	t4 := t2 * t2
	t8 := t4 * t4
	u := sleefPoly16(t.x, t2, t4, t8,
		1.06298484191448746607415e-05,
		-0.000125620649967286867384336,
		0.00070557664296393412389774,
		-0.00251865614498713360352999,
		0.00646262899036991172313504,
		-0.0128281333663399031014274,
		0.0208024799924145797902497,
		-0.0289002344784740315686289,
		0.0359785005035104590853656,
		-0.041848579703592507506027,
		0.0470843011653283988193763,
		-0.0524914210588448421068719,
		0.0587946590969581003860434,
		-0.0666620884778795497194182,
		0.0769225330296203768654095,
		-0.0909090442773387574781907,
	)

	u = sleefMLA(u, t.x, 0.111111108376896236538123)
	u = sleefMLA(u, t.x, -0.142857142756268568062339)
	u = sleefMLA(u, t.x, 0.199999999997977351284817)
	u = sleefMLA(u, t.x, -0.333333333333317605173818)

	t = sleefDdaddD2D2D2(s, sleefDdmulD2D2D(sleefDdmulD2D2D2(s, t), u))
	if sleefFabsk(s.x) < 1e-200 {
		t = s
	}
	t = sleefDdadd2D2D2D2(sleefDdmulD2D2D(sleefDd(1.570796326794896557998982, 6.12323399573676603586882e-17), float64(q)), t)
	return t
}

func sleefCbrt(d float64) float64 {
	e := sleefILogbk(sleefFabsk(d)) + 1
	d = sleefLdexp2k(d, -e)
	r := (e + 6144) % 3

	q2 := sleefDd(1.0, 0.0)
	if r == 1 {
		q2 = sleefDd(1.2599210498948731907, -2.5899333753005069177e-17)
	} else if r == 2 {
		q2 = sleefDd(1.5874010519681995834, -1.0869008194197822986e-16)
	}
	q2.x = sleefMulsign(q2.x, d)
	q2.y = sleefMulsign(q2.y, d)
	d = sleefFabsk(d)

	x := -0.640245898480692909870982
	x = sleefMLA(x, d, 2.96155103020039511818595)
	x = sleefMLA(x, d, -5.73353060922947843636166)
	x = sleefMLA(x, d, 6.03990368989458747961407)
	x = sleefMLA(x, d, -3.85841935510444988821632)
	x = sleefMLA(x, d, 2.2307275302496609725722)

	y := x * x
	y = y * y
	x -= (d*y - x) * (1.0 / 3.0)
	z := x

	u := sleefDdmulD2DD(x, x)
	u = sleefDdmulD2D2D2(u, u)
	u = sleefDdmulD2D2D(u, d)
	u = sleefDdadd2D2D2D(u, -x)
	y = u.x + u.y

	y = -2.0 / 3.0 * y * z
	v := sleefDdadd2D2D2D(sleefDdmulD2DD(z, z), y)
	v = sleefDdmulD2D2D(v, d)
	v = sleefDdmulD2D2D2(v, q2)
	z = sleefLdexp2k(v.x+v.y, (e+6144)/3-2048)

	if math.IsInf(y, 0) {
		z = sleefMulsign(math.Inf(1), q2.x)
	}
	if d == 0 {
		z = sleefMulsign(0, q2.x)
	}
	return z
}

func sleefLn(d float64) float64 {
	o := d < sleefDblMin
	if o {
		d *= float64(int64(1<<32)) * float64(int64(1<<32))
	}

	e := sleefILogb2k(d * (1.0 / 0.75))
	m := sleefLdexp3k(d, -e)

	if o {
		e -= 64
	}

	x := sleefDddivD2D2D2(sleefDdadd2D2DD(-1.0, m), sleefDdadd2D2DD(1.0, m))
	x2 := x.x * x.x
	x4 := x2 * x2
	x8 := x4 * x4
	t := sleefPoly7(x2, x4, x8,
		0.1532076988502701353e+0,
		0.1525629051003428716e+0,
		0.1818605932937785996e+0,
		0.2222214519839380009e+0,
		0.2857142932794299317e+0,
		0.3999999999635251990e+0,
		0.6666666666667333541e+0,
	)

	s := sleefDdmulD2D2D(sleefDd(0.693147180559945286226764, 2.319046813846299558417771e-17), float64(e))
	s = sleefDdaddD2D2D2(s, sleefDdscaleD2D2D(x, 2.0))
	s = sleefDdaddD2D2D(s, x2*x.x*t)
	r := s.x + s.y

	if math.IsInf(d, 0) {
		r = math.Inf(1)
	}
	if d < 0.0 || math.IsNaN(d) {
		r = math.NaN()
	}
	if d == 0.0 {
		r = math.Inf(-1)
	}
	return r
}

func sleefLn1p(d float64) float64 {
	dp1 := d + 1.0
	o := dp1 < sleefDblMin
	if o {
		dp1 *= float64(int64(1<<32)) * float64(int64(1<<32))
	}

	e := sleefILogb2k(dp1 * (1.0 / 0.75))
	t := sleefLdexp3k(1.0, -e)
	m := sleefMLA(d, t, t-1.0)

	if o {
		e -= 64
	}

	x := sleefDddivD2D2D2(sleefDd(m, 0.0), sleefDdaddD2DD(2.0, m))
	x2 := x.x * x.x
	x4 := x2 * x2
	x8 := x4 * x4

	t = sleefPoly7(x2, x4, x8,
		0.1532076988502701353e+0,
		0.1525629051003428716e+0,
		0.1818605932937785996e+0,
		0.2222214519839380009e+0,
		0.2857142932794299317e+0,
		0.3999999999635251990e+0,
		0.6666666666667333541e+0,
	)

	s := sleefDdmulD2D2D(sleefDd(0.693147180559945286226764, 2.319046813846299558417771e-17), float64(e))
	s = sleefDdaddD2D2D2(s, sleefDdscaleD2D2D(x, 2.0))
	s = sleefDdaddD2D2D(s, x2*x.x*t)
	r := s.x + s.y

	if d > 1.0e+307 {
		r = math.Inf(1)
	}

	if d < -1.0 || math.IsNaN(d) {
		r = math.NaN()
	}

	if d == -1.0 {
		r = math.Inf(-1)
	}

	if sleefIsNegZero(d) {
		r = math.Copysign(0.0, -1)
	}
	return r
}

func sleefLog2(d float64) float64 {
	o := d < sleefDblMin
	if o {
		d *= float64(int64(1<<32)) * float64(int64(1<<32))
	}

	e := sleefILogb2k(d * (1.0 / 0.75))
	m := sleefLdexp3k(d, -e)

	if o {
		e -= 64
	}

	x := sleefDddivD2D2D2(sleefDdadd2D2DD(-1.0, m), sleefDdadd2D2DD(1.0, m))
	x2 := x.x * x.x
	x4 := x2 * x2
	x8 := x4 * x4

	t := sleefPoly7(x2, x4, x8,
		+0.2211941750456081490e+0,
		+0.2200768693152277689e+0,
		+0.2623708057488514656e+0,
		+0.3205977477944495502e+0,
		+0.4121985945485324709e+0,
		+0.5770780162997058982e+0,
		+0.96179669392608091449,
	)

	s := sleefDdadd2D2DD2(float64(e), sleefDdmulD2D2D2(x, sleefDd(2.885390081777926774, 6.0561604995516736434e-18)))
	s = sleefDdadd2D2D2D(s, x2*x.x*t)
	r := s.x + s.y

	if math.IsInf(d, 0) {
		r = math.Inf(1)
	}

	if d < 0.0 || math.IsNaN(d) {
		r = math.NaN()
	}

	if d == 0.0 {
		r = math.Inf(-1)
	}
	return r
}

func sleefExp10(d float64) float64 {
	q := sleefRintki(d * sleefLog10of2)
	s := sleefMLA(float64(q), -sleefL10U, d)
	s = sleefMLA(float64(q), -sleefL10L, s)

	u := +0.2411463498334267652e-3
	u = sleefMLA(u, s, +0.1157488415217187375e-2)
	u = sleefMLA(u, s, +0.5013975546789733659e-2)
	u = sleefMLA(u, s, +0.1959762320720533080e-1)
	u = sleefMLA(u, s, +0.6808936399446784138e-1)
	u = sleefMLA(u, s, +0.2069958494722676234e+0)
	u = sleefMLA(u, s, +0.5393829292058536229e+0)
	u = sleefMLA(u, s, +0.1171255148908541655e+1)
	u = sleefMLA(u, s, +0.2034678592293432953e+1)
	u = sleefMLA(u, s, +0.2650949055239205876e+1)
	u = sleefMLA(u, s, +0.2302585092994045901e+1)

	u = sleefDdnormalizeD2D2(sleefDdaddD2DD2(1.0, sleefDdmulD2DD(u, s))).x
	u = sleefLdexp2k(u, q)

	if d > 308.25471555991671 {
		u = math.Inf(1)
	}
	if d < -350.0 {
		u = 0
	}
	return u
}

func sleefPow(x, y float64) float64 {
	yisint := sleefIsInt(y)
	yisodd := yisint && sleefIsOdd(y)

	d := sleefDdmulD2D2D(sleefLogk(sleefFabsk(x)), y)
	result := sleefExpk(d)

	if d.x > 709.78271114955742909217217426 || math.IsNaN(result) {
		result = math.Inf(1)
	}

	// result *= (x > 0 ? 1 : (yisint ? (yisodd ? -1 : 1) : SLEEF_NAN));
	{
		var m float64
		if x > 0 {
			m = 1.0
		} else {
			if yisint {
				if yisodd {
					m = -1.0
				} else {
					m = 1.0
				}
			} else {
				m = math.NaN()
			}
		}
		result *= m
	}

	if math.IsInf(y, 0) {
		efx := sleefMulsign(sleefFabsk(x)-1.0, y)
		if efx < 0 {
			result = 0.0
		} else if efx == 0 {
			result = 1.0
		} else {
			result = math.Inf(1)
		}
	}

	if math.IsInf(x, 0) || x == 0.0 {
		m1 := 0.0
		if !(math.Signbit(y) != (x == 0.0)) {
			m1 = math.Inf(1)
		}
		m2 := x
		if !yisodd {
			m2 = 1.0
		}
		result = sleefMulsign(m1, m2)
	}

	if math.IsNaN(x) || math.IsNaN(y) {
		result = math.NaN()
	}
	if y == 0.0 || x == 1.0 {
		result = 1.0
	}
	return result
}

func sleefACos(d float64) float64 {
	o := sleefFabsk(d) < 0.5
	var x2 float64
	if o {
		x2 = d * d
	} else {
		x2 = (1 - sleefFabsk(d)) * 0.5
	}
	var x sleefDouble2
	if o {
		x = sleefDd(sleefFabsk(d), 0)
	} else {
		x = sleefDdsqrtD2D(x2)
	}
	if sleefFabsk(d) == 1.0 {
		x = sleefDd(0, 0)
	}
	x4 := x2 * x2
	x8 := x4 * x4
	x16 := x8 * x8
	u := sleefPoly12(x2, x4, x8, x16,
		+0.3161587650653934628e-1,
		-0.1581918243329996643e-1,
		+0.1929045477267910674e-1,
		+0.6606077476277170610e-2,
		+0.1215360525577377331e-1,
		+0.1388715184501609218e-1,
		+0.1735956991223614604e-1,
		+0.2237176181932048341e-1,
		+0.3038195928038132237e-1,
		+0.4464285681377102438e-1,
		+0.7500000000378581611e-1,
		+0.1666666666666497543e+0,
	)

	u *= x.x * x2
	y := sleefDdsubD2D2D2(sleefDd(3.141592653589793116/2, 1.2246467991473532072e-16/2),
		sleefDdaddD2DD(sleefMulsign(x.x, d), sleefMulsign(u, d)))
	x = sleefDdaddD2D2D(x, u)

	if !o {
		y = sleefDdscaleD2D2D(x, 2.0)
	}
	if !o && d < 0 {
		y = sleefDdsubD2D2D2(sleefDd(3.141592653589793116, 1.2246467991473532072e-16), y)
	}
	return y.x + y.y
}

func sleefHypot(x, y float64) float64 {
	x = sleefFabsk(x)
	y = sleefFabsk(y)
	minv := sleefFmink(x, y)
	n := minv
	maxv := sleefFmaxk(x, y)
	d := maxv

	if maxv < sleefDblMin {
		n *= float64(1 << 54)
		d *= float64(1 << 54)
	}

	t := sleefDddivD2D2D2(sleefDd(n, 0), sleefDd(d, 0))
	t = sleefDdmulD2D2D(sleefDdsqrtD2D2(sleefDdadd2D2D2D(sleefDdsquD2D2(t), 1.0)), maxv)
	ret := t.x + t.y
	if math.IsNaN(ret) {
		ret = math.Inf(1)
	}
	if minv == 0.0 {
		ret = maxv
	}
	if math.IsNaN(x) || math.IsNaN(y) {
		ret = math.NaN()
	}
	if math.IsInf(x, 1) || math.IsInf(y, 1) {
		ret = math.Inf(1)
	}
	return ret
}

func sleefExpm1(a float64) float64 {
	d := sleefDdadd2D2D2D(sleefExpk2(sleefDd(a, 0.0)), -1.0)
	x := d.x + d.y

	if a > 709.782712893383996732223 {
		x = math.Inf(1)
	}
	if a < -36.736800569677101399113302437 {
		x = -1.0 // log(1 - nexttoward(1, 0))
	}
	if sleefIsNegZero(a) {
		x = math.Copysign(0.0, -1)
	}
	return x
}

func sleefExp2(d float64) float64 {
	q := sleefRintki(d)
	s := d - float64(q)
	s2 := s * s
	s4 := s2 * s2
	s8 := s4 * s4

	u := sleefPoly10(s, s2, s4, s8,
		+0.4434359082926529454e-9,
		+0.7073164598085707425e-8,
		+0.1017819260921760451e-6,
		+0.1321543872511327615e-5,
		+0.1525273353517584730e-4,
		+0.1540353045101147808e-3,
		+0.1333355814670499073e-2,
		+0.9618129107597600536e-2,
		+0.5550410866482046596e-1,
		+0.2402265069591012214e+0,
	)
	u = sleefMLA(u, s, +0.6931471805599452862e+0)
	u = sleefDdnormalizeD2D2(sleefDdaddD2DD2(1.0, sleefDdmulD2DD(u, s))).x
	u = sleefLdexp2k(u, q)

	if d >= 1024.0 {
		u = math.Inf(1)
	}
	if d < -2000.0 {
		u = 0.0
	}
	return u
}

func sleefASin(d float64) float64 {
	o := sleefFabsk(d) < 0.5
	var x2 float64
	if o {
		x2 = d * d
	} else {
		x2 = (1 - sleefFabsk(d)) * 0.5
	}
	var x sleefDouble2
	if o {
		x = sleefDd(sleefFabsk(d), 0)
	} else {
		x = sleefDdsqrtD2D(x2)
	}
	if sleefFabsk(d) == 1.0 {
		x = sleefDd(0, 0)
	}
	x4 := x2 * x2
	x8 := x4 * x4
	x16 := x8 * x8
	u := sleefPoly12(x2, x4, x8, x16,
		+0.3161587650653934628e-1,
		-0.1581918243329996643e-1,
		+0.1929045477267910674e-1,
		+0.6606077476277170610e-2,
		+0.1215360525577377331e-1,
		+0.1388715184501609218e-1,
		+0.1735956991223614604e-1,
		+0.2237176181932048341e-1,
		+0.3038195928038132237e-1,
		+0.4464285681377102438e-1,
		+0.7500000000378581611e-1,
		+0.1666666666666497543e+0,
	)
	u *= x2 * x.x

	y := sleefDdaddD2D2D(sleefDdsubD2D2D2(sleefDd(3.141592653589793116/4, 1.2246467991473532072e-16/4), x), -u)
	var r float64
	if o {
		r = u + x.x
	} else {
		r = (y.x + y.y) * 2.0
	}
	r = sleefMulsign(r, d)
	return r
}

func sleefAtan2(y, x float64) float64 {
	if sleefFabsk(x) < 5.5626846462680083984e-309 {
		y *= float64(1 << 53)
		x *= float64(1 << 53)
	}

	d := sleefAtan2k(sleefDd(sleefFabsk(y), 0.0), sleefDd(x, 0.0))
	r := d.x + d.y
	r = sleefMulsign(r, x)

	if math.IsInf(x, 0) || x == 0.0 {
		var z float64
		if math.IsInf(x, 0) {
			z = sleefSign(x) * (sleefMPI / 2.0)
		}
		r = sleefMPI/2.0 - z
	}

	if math.IsInf(y, 0) {
		var z float64
		if math.IsInf(x, 0) {
			z = sleefSign(x) * (sleefMPI * 1.0 / 4.0)
		}
		r = sleefMPI/2.0 - z
	}

	if y == 0.0 {
		if sleefSign(x) == -1 {
			r = sleefMPI
		} else {
			r = 0.0
		}
	}

	if math.IsNaN(x) || math.IsNaN(y) {
		return math.NaN()
	}
	return sleefMulsign(r, y)
}

func sleefSin(d float64) float64 {
	var ql float64
	var u float64
	var s sleefDouble2

	if sleefFabsk(d) < sleefTRIGRANGEMAX2 {
		ql = sleefRintk(d * sleefM1PI)
		u = sleefMLA(ql, -sleefPIA2, d)
		s = sleefDdaddD2DD(u, ql*-sleefPIB2)
	} else if sleefFabsk(d) < sleefTRIGRANGEMAX {
		dqh := sleefTrunck(d*(sleefM1PI/(1<<24))) * float64(1<<24)
		ql = sleefRintk(sleefMLA(d, sleefM1PI, -dqh))
		u = sleefMLA(dqh, -sleefPIA, d)
		s = sleefDdaddD2DD(u, ql*-sleefPIA)
		s = sleefDdadd2D2D2D(s, dqh*-sleefPIB)
		s = sleefDdadd2D2D2D(s, ql*-sleefPIB)
		s = sleefDdadd2D2D2D(s, dqh*-sleefPIC)
		s = sleefDdadd2D2D2D(s, ql*-sleefPIC)
		s = sleefDdaddD2D2D(s, (dqh+ql)*-sleefPID)
	} else {
		ddi := sleefRempi(d)
		// ql = ((ddi.i&3)*2 + (ddi.dd.x > 0) + 1) >> 2
		z := (ddi.i&3)*2 + 1
		if ddi.dd.x > 0 {
			z++
		}
		z >>= 2
		ql = float64(z)
		if (ddi.i & 1) != 0 {
			ddi.dd = sleefDdadd2D2D2D2(ddi.dd, sleefDd(sleefMulsign(3.141592653589793116*-0.5, ddi.dd.x),
				sleefMulsign(1.2246467991473532072e-16*-0.5, ddi.dd.x)))
		}
		s = sleefDdnormalizeD2D2(ddi.dd)
		if math.IsInf(d, 0) || math.IsNaN(d) {
			s.x = math.NaN()
		}
	}

	t := s
	s = sleefDdsquD2D2(s)
	s2 := s.x * s.x
	s4 := s2 * s2
	u = sleefPoly6(s.x, s2, s4,
		2.72052416138529567917983e-15,
		-7.6429259411395447190023e-13,
		1.60589370117277896211623e-10,
		-2.5052106814843123359368e-08,
		2.75573192104428224777379e-06,
		-0.000198412698412046454654947,
	)
	u = sleefMLA(u, s.x, 0.00833333333333318056201922)
	x := sleefDdaddD2DD2(1, sleefDdmulD2D2D2(sleefDdaddD2DD(-0.166666666666666657414808, u*s.x), s))
	u = sleefDdmulDD2D2(t, x)

	if int64(ql)&1 != 0 {
		u = -u
	}
	if sleefIsNegZero(d) {
		u = d
	}
	return u
}

func sleefCos(d float64) float64 {
	d = sleefFabsk(d)
	var ql int
	var u float64
	var s, t, x sleefDouble2

	if d < sleefTRIGRANGEMAX2 {
		ql = int(sleefMLA(2.0, sleefRintk(d*sleefM1PI-0.5), 1.0))
		s = sleefDdadd2D2DD(d, float64(ql)*(-sleefPIA2*0.5))
		s = sleefDdaddD2D2D(s, float64(ql)*(-sleefPIB2*0.5))
	} else if d < sleefTRIGRANGEMAX {
		dqh := sleefTrunck(d*(sleefM1PI/float64(1<<23)) - 0.5*(sleefM1PI/float64(1<<23)))
		ql = int(2*sleefRintki(d*sleefM1PI-0.5-dqh*float64(1<<23)) + 1)
		dqh *= float64(1 << 24)
		u = sleefMLA(dqh, -sleefPIA*0.5, d)
		s = sleefDdadd2D2DD(u, float64(ql)*(-sleefPIA*0.5))
		s = sleefDdadd2D2D2D(s, dqh*(-sleefPIB*0.5))
		s = sleefDdadd2D2D2D(s, float64(ql)*(-sleefPIB*0.5))
		s = sleefDdadd2D2D2D(s, dqh*(-sleefPIC*0.5))
		s = sleefDdadd2D2D2D(s, float64(ql)*(-sleefPIC*0.5))
		s = sleefDdaddD2D2D(s, (dqh+float64(ql))*(-sleefPID*0.5))
	} else {
		ddi := sleefRempi(d)
		var ddiXgt int32
		if ddi.dd.x > 0 {
			ddiXgt = 1
		}
		ql = int(((ddi.i&3)*2 + ddiXgt + 7) >> 1)

		if (ddi.i & 1) == 0 {
			var t1 float64
			if ddi.dd.x > 0 {
				t1 = 1.0
			} else {
				t1 = -1.0
			}
			t2 := sleefMulsign(3.141592653589793116*-0.5, t1)
			t3 := sleefMulsign(1.2246467991473532072e-16*-0.5, t1)
			ddi.dd = sleefDdadd2D2D2D2(ddi.dd, sleefDd(t2, t3))
		}

		s = sleefDdnormalizeD2D2(ddi.dd)
		if math.IsInf(d, 0) || math.IsNaN(d) {
			s.x = math.NaN()
		}
	}
	t = s
	s = sleefDdsquD2D2(s)

	s2 := s.x * s.x
	s4 := s2 * s2
	u = sleefPoly6(s.x, s2, s4,
		2.72052416138529567917983e-15,
		-7.6429259411395447190023e-13,
		1.60589370117277896211623e-10,
		-2.5052106814843123359368e-08,
		2.75573192104428224777379e-06,
		-0.000198412698412046454654947,
	)
	u = sleefMLA(u, s.x, 0.00833333333333318056201922)
	x = sleefDdaddD2DD2(1, sleefDdmulD2D2D2(sleefDdaddD2DD(-0.166666666666666657414808, u*s.x), s))
	u = sleefDdmulDD2D2(t, x)

	if (ql & 2) == 0 {
		u = -u
	}
	return u
}

func sleefTan(d float64) float64 {
	var u float64
	var s, t, x, y sleefDouble2
	var ql int

	if sleefFabsk(d) < sleefTRIGRANGEMAX2 {
		ql = int(sleefRintki(d * (2.0 * sleefM1PI)))
		u = sleefMLA(float64(ql), -sleefPIA2*0.5, d)
		s = sleefDdaddD2DD(u, float64(ql)*(-sleefPIB2*0.5))
	} else if sleefFabsk(d) < sleefTRIGRANGEMAX {
		dqh := sleefTrunck(d*(sleefM2PI/float64(1<<24))) * float64(1<<24)
		var t1 float64
		if d < 0 {
			t1 = -0.5
		} else {
			t1 = 0.5
		}
		s = sleefDdadd2D2D2D(sleefDdmulD2D2D(sleefDd(sleefM2PIH, sleefM2PIL), d), t1-dqh)
		ql = int(s.x + s.y)
		u = sleefMLA(dqh, -sleefPIA*0.5, d)
		s = sleefDdaddD2DD(u, float64(ql)*(-sleefPIA*0.5))
		s = sleefDdadd2D2D2D(s, dqh*(-sleefPIB*0.5))
		s = sleefDdadd2D2D2D(s, float64(ql)*(-sleefPIB*0.5))
		s = sleefDdadd2D2D2D(s, dqh*(-sleefPIC*0.5))
		s = sleefDdadd2D2D2D(s, float64(ql)*(-sleefPIC*0.5))
		s = sleefDdaddD2D2D(s, (dqh+float64(ql))*(-sleefPID*0.5))
	} else {
		ddi := sleefRempi(d)
		ql = int(ddi.i)
		s = ddi.dd
		if math.IsInf(d, 0) || math.IsNaN(d) {
			s.x = math.NaN()
		}
	}

	t = sleefDdscaleD2D2D(s, 0.5)
	s = sleefDdsquD2D2(t)
	s2 := s.x * s.x
	s4 := s2 * s2
	u = sleefPoly8(s.x, s2, s4,
		+0.3245098826639276316e-3,
		+0.5619219738114323735e-3,
		+0.1460781502402784494e-2,
		+0.3591611540792499519e-2,
		+0.8863268409563113126e-2,
		+0.2186948728185535498e-1,
		+0.5396825399517272970e-1,
		+0.1333333333330500581e+0,
	)

	u = sleefMLA(u, s.x, +0.3333333333333343695e+0)
	x = sleefDdaddD2D2D2(t, sleefDdmulD2D2D(sleefDdmulD2D2D2(s, t), u))
	y = sleefDdaddD2DD2(-1.0, sleefDdsquD2D2(x))
	x = sleefDdscaleD2D2D(x, -2.0)

	if (ql & 1) != 0 {
		t = x
		x = y
		y = sleefDdnegD2D2(t)
	}

	x = sleefDddivD2D2D2(x, y)
	u = x.x + x.y

	if sleefIsNegZero(d) {
		u = d
	}
	return u
}

func sleefATan(d float64) float64 {
	d2 := sleefAtan2k(sleefDd(sleefFabsk(d), 0.0), sleefDd(1.0, 0.0))
	r := d2.x + d2.y
	if math.IsInf(d, 0) {
		r = 1.570796326794896557998982
	}
	return sleefMulsign(r, d)
}

func sleefExp(d float64) float64 {
	q := sleefRintki(d * sleefRln2)
	s := sleefMLA(float64(q), -sleefL2U, d)
	s = sleefMLA(float64(q), -sleefL2L, s)

	s2 := s * s
	s4 := s2 * s2
	s8 := s4 * s4
	u := sleefPoly10(s, s2, s4, s8,
		2.08860621107283687536341e-09,
		2.51112930892876518610661e-08,
		2.75573911234900471893338e-07,
		2.75572362911928827629423e-06,
		2.4801587159235472998791e-05,
		0.000198412698960509205564975,
		0.00138888888889774492207962,
		0.00833333333331652721664984,
		0.0416666666666665047591422,
		0.166666666666666851703837,
	)

	u = sleefMLA(u, s, +0.5)
	u = math.FMA(s, u, 1.0)
	u = math.FMA(u, s, 1.0)
	u = sleefLdexp2k(u, q)

	if d > 709.78271114955742909217217426 {
		u = math.Inf(1)
	}
	if d < -1000 {
		u = 0.0
	}
	return u
}
