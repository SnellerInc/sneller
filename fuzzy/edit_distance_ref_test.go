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

package fuzzy

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestEditDistanceRef
func TestEditDistanceRef(t *testing.T) {

	t.Parallel()
	type unitTest struct {
		needle         string
		data           string
		approxDistance int
		trueDistance   int
	}

	refImpl := editDistanceRef
	unitTests := []unitTest{
		{"ABC", "AXC", 1, 1},

		//equivalent
		{"a", "a", 0, 0},

		//substitution
		{"ab", "cb", 1, 1},
		{"abc", "dec", 2, 2},
		{"abcd", "efgd", 3, 3},

		//transposition
		{"ab", "ba", 1, 1},   //tra
		{"ab", "cba", 2, 2},  //del tra
		{"ab", "cdba", 3, 3}, //del, del, tra

		{"abc", "cb", 2, 2},   //del, tra
		{"abc", "dcb", 2, 2},  //sub, tra
		{"abc", "decb", 3, 3}, //sub, del, tra

		{"abcd", "dc", 3, 3},   //del, del, tra
		{"abcd", "edc", 3, 3},  //del, sub, tra
		{"abcd", "efdc", 3, 3}, //sub, sub, tra

		//deletion
		{"ab", "b", 1, 1},   //del
		{"abc", "c", 3, 2},  //del, del
		{"abcd", "d", 4, 3}, //del, del, del

		//insertion
		{"a", "ba", 1, 1},   //ins
		{"a", "bca", 3, 2},  //ins, ins
		{"a", "bcda", 4, 3}, //ins, ins, ins
	}

	run := func(ut unitTest) {
		if !utf8.ValidString(ut.needle) {
			t.Logf("needle is not valid UTF8; skipping this test")
			return
		}
		trueDistance := refImpl(ut.data, ut.needle)
		obsApproxDistance := EditDistance(ut.data, ut.needle, true, Approx2)

		if (trueDistance != ut.trueDistance) || (obsApproxDistance != ut.approxDistance) {
			t.Errorf("needle=%q; data %q; exp=%v; true=%v; approx=%v",
				ut.needle, ut.data, ut.approxDistance, trueDistance, obsApproxDistance)
		}
	}

	for _, ut := range unitTests {
		t.Run("TestEditDistanceRef", func(t *testing.T) {
			run(ut)
		})
	}
}

type unitTest struct {
	needle       string
	data         string
	expDistance  int
	trueDistance int
}

func unitTestsApprox2() []unitTest {
	return []unitTest{

		{"bb", "abb", 1, 1},

		//equivalent
		{"a", "a", 0, 0},
		{"ab", "ab", 0, 0},
		{"abc", "abc", 0, 0},

		//substitution 1x
		{"a", "b", 1, 1},
		{"ab", "cb", 1, 1},
		{"abc", "aXc", 1, 1},

		//transposition 1x
		{"ab", "ba", 1, 1},
		{"abc", "bac", 1, 1},
		{"abc", "acb", 1, 1},

		//deletion 1x
		{"a", "", 1, 1},
		{"ab", "a", 1, 1},
		{"abc", "ac", 1, 1},

		//insertion 1x
		{"", "a", 1, 1},
		{"a", "ab", 1, 1},
		{"ac", "abc", 1, 1},

		//transposition (ins+tra)
		{"abcd", "Xbacd", 2, 2},
	}
}

func TestEditDistanceApprox2(t *testing.T) {
	t.Parallel()

	unitTests := []unitTest{
		// choice refinements del
		{"ab", "bb", 2, 1},   // Approx3 OK; Approx2 NOK
		{"abb", "bbb", 2, 1}, // Approx3 NOK Approx2 NOK
		{"abc", "bbb", 3, 2}, // Approx3 OK; Approx2 NOK
		{"abb", "bbc", 2, 2}, // Approx3 OK; Approx2 OK

		// choice refinement ins
		{"aa", "ba", 2, 1},   // Approx3 OK; Approx2 NOK
		{"aaa", "baa", 2, 1}, // Approx3 NOK Approx2 NOK
		{"aac", "baa", 2, 2}, // Approx3 OK; Approx2 OK
		{"aaa", "bac", 3, 2}, // Approx3 OK; Approx2 NOK

		//two adjacent deletions
		{"a__bcdefgh", "abcdefgh", 9, 2},
		{"__a", "a", 3, 2},

		//two adjacent insertions
		{"abcdefgh", "a__bcdefgh", 9, 2},
		{"a", "__a", 3, 2},

		//deletion 2x
		{"abcd", "ad", 3, 2}, // known issue due to 2char horizon
		{"abcd", "bd", 2, 2},

		//insertion 2x
		{"abcd", "a__bcd", 5, 2}, // known issue due to 2char horizon
		{"abcd", "a_b_cd", 2, 2},

		//issues
		{"BA", "AA", 2, 1}, // known issue due to 2char horizon
		{"CAB", "CBAB", 2, 1},
	}

	run := func(ut unitTest) {
		obsDistance := editDistanceRef(ut.data, ut.needle)
		obsDistance2 := EditDistance(ut.data, ut.needle, true, Approx2)
		if (obsDistance != ut.trueDistance) || (obsDistance2 != ut.expDistance) {
			t.Errorf("needle=%q; data %q; exp=%v; true=%v; approx=%v",
				ut.needle, ut.data, ut.expDistance, obsDistance, obsDistance2)
		}
	}

	allTests := append(unitTests, unitTestsApprox2()...)
	for _, ut := range allTests {
		t.Run(fmt.Sprintf("needle=%v;data=%v", ut.needle, ut.data), func(t *testing.T) {
			run(ut)
		})
	}
}

func TestEditDistanceApprox3(t *testing.T) {
	t.Parallel()

	unitTests := []unitTest{

		// choice refinements del
		{"ab", "bb", 1, 1},   // Approx3 OK; Approx2 NOK // Choice: we choose del, sub is also possible.
		{"abb", "bbb", 2, 1}, // Approx3 NOK; Approx2 NOK
		{"abc", "bbb", 2, 2}, // Approx3 OK; Approx2 OK
		{"abb", "bbc", 2, 2}, // Approx3 OK; Approx2 OK

		// choice refinement ins
		{"aa", "ba", 1, 1},   // Approx3 OK; Approx2 NOK
		{"aaa", "baa", 2, 1}, // Approx3 NOK; Approx2 NOK
		{"aac", "baa", 2, 2}, // Approx3 OK; Approx2 OK
		{"aaa", "bac", 2, 2}, // Approx3 OK; Approx2 NOK

		//two adjacent deletions
		{"a__bcdefgh", "abcdefgh", 2, 2},
		{"__a", "a", 2, 2},

		//two adjacent insertions
		{"abcdefgh", "a__bcdefgh", 2, 2},
		{"a", "__a", 2, 2},

		//three adjacent deletions
		{"a___bcdefgh", "abcdefgh", 10, 3},
		{"___a", "a", 4, 3},

		//three adjacent insertions
		{"abcdefgh", "a___bcdefgh", 10, 3},
		{"a", "___a", 4, 3},

		//deletion 2x
		{"abcd", "ad", 2, 2},
		{"abcd", "bd", 3, 2}, //FIXME

		//insertion 2x
		{"abcd", "a__bcd", 2, 2},
		{"abcd", "a_b_cd", 3, 2}, //FIXME

		//issues
		{"BA", "AA", 1, 1},    // known issue due to 2char horizon
		{"CAB", "CBAB", 2, 1}, //FIXME
	}

	run := func(ut unitTest) {
		obsDistance := editDistanceRef(ut.data, ut.needle)
		obsDistance3 := EditDistance(ut.data, ut.needle, true, Approx3)

		if (obsDistance != ut.trueDistance) || (obsDistance3 != ut.expDistance) {
			t.Errorf("needle=%q; data %q; exp=%v; true=%v; approx=%v",
				ut.needle, ut.data, ut.expDistance, obsDistance, obsDistance3)
		}
	}

	allTests := append(unitTests, unitTestsApprox2()...)
	for _, ut := range allTests {
		t.Run(fmt.Sprintf("needle=%v;data=%v", ut.needle, ut.data), func(t *testing.T) {
			run(ut)
		})
	}
}

// TestGenerateTable code to generate the updated tables needed to config reference impl
func TestGenerateTable(t *testing.T) {
	t.Skip("Test code to generate assembly for approx3 kernel")
	type configType [4][4]byte

	createConfig := func(d1 Needle, d2 Data) (result configType) {
		for i := 0; i < 4; i++ {
			result[i] = [4]byte{'X', 'X', 'X', 'X'}
			n := d1[i]
			if n != '.' {
				for j := 0; j < 4; j++ {
					d := d2[j]
					if d != '.' {
						if n == d {
							result[i][j] = '1'
						} else {
							result[i][j] = '0'
						}
					}
				}
			}
		}
		return
	}

	mergeConfig := func(c1, c2 configType) (result configType) {
		for j1 := 0; j1 < 4; j1++ {
			for j2 := 0; j2 < 4; j2++ {
				if c1[j1][j2] == c2[j1][j2] {
					result[j1][j2] = c1[j1][j2]
				} else {
					result[j1][j2] = 'X'
				}
			}
		}
		return
	}

	toStringConfig := func(c configType) string {
		result := ""
		for i := 0; i < 4; i++ {
			result += string(c[i][:]) + " "
		}
		return result
	}

	toConfig := func(str string) (result configType) {

		result[0][0] = str[0]
		result[0][1] = str[1]
		result[0][2] = str[2]
		result[0][3] = str[3]

		result[1][0] = str[5]
		result[1][1] = str[6]
		result[1][2] = str[7]
		result[1][3] = str[8]

		result[2][0] = str[10]
		result[2][1] = str[11]
		result[2][2] = str[12]
		result[2][3] = str[13]

		result[3][0] = str[15]
		result[3][1] = str[16]
		result[3][2] = str[17]
		result[3][3] = str[18]
		return
	}

	toAsmA3 := func(info string, c configType, editDistance, advData, advNeedle int) string {

		_ = func(i, j int) (int, int) {
			if i == 0 {
				return j, j
			}
			if i == 1 {
				switch j {
				case 0:
					return 3, j
				case 1:
					return 0, j
				case 2:
					return 1, j
				case 3:
					return 2, j
				}
			}
			if i == 2 {
				switch j {
				case 0:
					return 2, j
				case 1:
					return 3, j
				case 2:
					return 0, j
				case 3:
					return 1, j
				}
			}
			if i == 3 {
				switch j {
				case 0:
					return 1, j
				case 1:
					return 2, j
				case 2:
					return 3, j
				case 3:
					return 0, j
				}
			}
			return -1, -1
		}

		remapA3 := func(i, j int) (int, int) {

			if i == 0 {
				switch j {
				case 0:
					return 0, 0
				case 1:
					return 1, 1
				case 2:
					return 2, 2
				}
			}
			if i == 1 {
				switch j {
				case 0:
					return 2, 0
				case 1:
					return 0, 1
				case 2:
					return 1, 2
				}
			}
			if i == 2 {
				switch j {
				case 0:
					return 1, 0
				case 1:
					return 2, 1
				case 2:
					return 0, 2
				}
			}
			return -1, -1
		}

		getMask1 := func(i int) string {
			result := ""
			for j := 0; j < 3; j++ {
				i2, j2 := remapA3(i, j)
				if c[i2][j2] == 'X' {
					result = "00" + result
				} else {
					result = "FF" + result
				}
			}
			return result
		}

		getMask2 := func(i int) string {
			result := ""
			for j := 0; j < 3; j++ {
				i2, j2 := remapA3(i, j)
				if c[i2][j2] == '1' {
					result = "FF" + result
				} else {
					result = "00" + result
				}
			}
			return result
		}

		toMask := func(m string) string {
			if strings.HasPrefix(m, "00FF") {
				m = m[2:]
			} else if strings.HasPrefix(m, "0000FF") {
				m = m[4:]
			} else if strings.HasPrefix(m, "000000FF") {
				m = m[6:]
			}
			return fmt.Sprintf("CONSTD_0x%v()", m)
		}

		_ = func() string {
			first := true
			result := ""
			for i := 0; i < 4; i++ {
				for j := 0; j < 4; j++ {
					i2, j2 := remapA3(i, j)
					mode := c[i2][j2]
					if mode != 'X' {
						if first {
							first = false
						} else {
							result += " && "
						}
						if mode == '1' {
							result += fmt.Sprintf("(N%v==D%v)", i2, j2)
						} else {
							result += fmt.Sprintf("(N%v!=D%v)", i2, j2)
						}
					}
				}
			}
			return result + "\n"

		}

		regs := [4]string{"Z9", "Z21", "Z22"}
		regScratch := "Z26"
		kScratch := "K3"
		regEditDistance := "Z28"
		regAdvData := "Z6"
		regAdvNeedle := "Z7"

		getConstantReg := func(i int) string {
			switch i {
			case 0:
				return "Z11"
			case 1:
				return "Z10"
			case 2:
				return "Z12"
			case 3:
				return "Z24"
			case 4:
				return "Z25"
			}
			return fmt.Sprintf("%v", i)
		}

		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("\n\n//; %v: %v (ED %v; advData %v; advNeedle %v)\n", info, toStringConfig(c), editDistance, advData, advNeedle))
		first := true

		for i := 0; i < 3; i++ {
			m1 := getMask1(i)
			m2 := getMask2(i)

			if m1 != "000000" {
				maskReg := "K3"
				if first {
					first = false
					maskReg = "K2"
				}
				if m1 == "FFFFFF" {
					if m2 == "000000" {
						sb.WriteString(fmt.Sprintf("VPCMPD $0, %v, %v, %v, K3\n", getConstantReg(0), regs[i], maskReg))
					} else {
						sb.WriteString(fmt.Sprintf("VPCMPD.BCST $0, %v), %v, %v, K3\n", toMask(m2), regs[i], maskReg))
					}
				} else {
					sb.WriteString(fmt.Sprintf("VPANDD.BCST %v, %v, %v\n", toMask(m1), regs[i], regScratch))
					if m2 == "000000" {
						sb.WriteString(fmt.Sprintf("VPCMPD $0, %v, %v, %v, K3\n", getConstantReg(0), regScratch, maskReg))
					} else {
						sb.WriteString(fmt.Sprintf("VPCMPD.BCST $0, %v, %v, %v, K3\n", toMask(m2), regScratch, maskReg))
					}
				}
			}
		}

		if editDistance != 1 {
			sb.WriteString(fmt.Sprintf("VMOVDQA32 %v, %v, %v //; ed := %v\n", getConstantReg(editDistance), kScratch, regEditDistance, editDistance))
		}
		if advData != 1 {
			sb.WriteString(fmt.Sprintf("VMOVDQA32 %v, %v, %v //; advData := %v\n", getConstantReg(advData), kScratch, regAdvData, advData))
		}
		if advNeedle != 1 {
			sb.WriteString(fmt.Sprintf("VMOVDQA32 %v, %v, %v //; advNeedle := %v\n", getConstantReg(advNeedle), kScratch, regAdvNeedle, advNeedle))
		}
		return sb.String()
	}

	type unitTest struct {
		n Needle
		d Data
	}

	analyse := func(info string, data []unitTest) {
		mergedConfig := createConfig(data[0].n, data[0].d)
		for _, del := range data {
			config := createConfig(del.n, del.d)
			mergedConfig = mergeConfig(mergedConfig, config)
			if false {
				t.Logf("needle=%q; data=%v; config=%v",
					del.n, del.d, toStringConfig(config))
			}
		}
		t.Logf("%v: %v", info, toStringConfig(mergedConfig))
	}

	sub1Approx2 := []unitTest{
		{
			"ab..",
			"xb..",
		},
		//{
		//	"aa..",
		//	"xa..", 1, 1, 1,
		//},
		{
			"ab..",
			"xc..",
		},
	}
	del1Approx2 := []unitTest{
		{
			"ab..",
			"bc..",
		},
		{ // Choice: we choose del, sub is also possible.
			"ab..",
			"bb..",
		},
	}
	ins1Approx2 := []unitTest{
		{
			"ab..",
			"xa..",
		},
		{ // Choice: we choose ins, sub is also possible.
			"aa..",
			"xa..",
		},
	}
	tra1Approx2 := []unitTest{
		{
			"ab..",
			"ba..",
		},
	}

	sub1Approx3 := []unitTest{
		{
			"abc.",
			"xbc.",
		},
		{
			"abb.",
			"xbb.",
		},
		{
			"abb.",
			"bbb.",
		},
		//{
		//	"aab.",
		//	"xab.",
		//},
		//{
		//	"aaa.",
		//	"xaa.",
		//},
		{
			"abc.",
			"bbc.",
		},
		{
			"abc.",
			"cbc.",
		},
		{
			"abc.",
			"dbd.",
		},
	}
	del1Approx3 := []unitTest{
		{
			"abc.",
			"bc..",
		},
		{ // Choice: we choose del, sub is also possible.
			"abb.",
			"bbb.",
		},
		{
			"abb.",
			"bbc.",
		},
		//{ choice refinement on approx2: choose sub
		//	"abc.",
		//	"bbb.", 1, 1, 0,
		//},
	}
	del2Approx3 := []unitTest{
		{
			"abc.",
			"cde.",
		},
		//	{ // this introduces more issues than that is solves...
		//		"abc.",
		//		"ccc.",
		//	},

		//	{  // this introduces more issues than that is solves...
		//		"abc.",
		//		"cdb.",
		//	},

		//{ this is ins1
		//	"abc.",
		//	"cad.",
		//},
		//{ this is sub1
		//	"abc.",
		//	"ccd.",
		//},
	}
	ins1Approx3 := []unitTest{
		{
			"ab..",
			"xab.",
		},
		{ // Choice: we choose ins, sub is also possible.
			"aaa.",
			"baa.",
		},
		{
			"aac.",
			"baa.",
		},
		//{ choice refinement on approx2: choose sub
		//	"aaa.",
		//	"bac.", 1, 0, 1,
		//},
	}
	ins2Approx3 := []unitTest{
		{
			"abc.",
			"xxa.",
		},

		//	{ // this introduces more issues than that is solves...
		//		"aba.",
		//		"xxa.",
		//	},

		//	{ // this introduces more issues than that is solves...
		//		"abb.",
		//		"xxa.",
		//	},
	}
	tra1Approx3 := []unitTest{
		{
			"abc.",
			"bac.",
		},
		{
			"abc.",
			"bad.",
		},
		//		{
		//			"aba.", // this is ins+eq, not tra+del
		//			"baa.", 1, 2, 2,
		//		},
		{
			"abb.",
			"bab.",
		},
	}

	del1Approx4 := []unitTest{
		{
			"abcd",
			"bcd.",
		},
		{ // Choice: we choose del, sub is also possible.
			"abbb",
			"bbbb",
		},
		{
			"abbb",
			"bbbc",
		},
		//{ choice refinement on approx3: choose sub
		//	"abbc",
		//	"bbbb", 1, 1, 0,
		//},
	}
	del2Approx4 := []unitTest{
		{
			"abcd",
			"cde.",
		},
		//{ choice refinement on approx3: choose sub
		//	"abbc",
		//	"bbbb", 1, 1, 0,
		//},
	}
	del3Approx4 := []unitTest{
		{
			"abcd",
			"d...",
		},
		{ // Choice: we choose del, sub is also possible.
			"abbb",
			"bbbb",
		},
		{
			"abbb",
			"bbbc",
		},
		//{ choice refinement on approx3: choose sub
		//	"abbc",
		//	"bbbb", 1, 1, 0,
		//},
	}
	ins1Approx4 := []unitTest{
		{
			"abc.",
			"xabc",
		},
		{ // Choice: we choose ins, sub is also possible.
			"aaa.",
			"baa.",
		},
		{
			"aac.",
			"baa.",
		},
		//{ choice refinement on approx2: choose sub
		//	"aaa.",
		//	"bac.", 1, 0, 1,
		//},
	}
	ins2Approx4 := []unitTest{
		{
			"abcd",
			"xxab",
		},

		//	{ // this introduces more issues than that is solves...
		//		"aba.",
		//		"xxa.",
		//	},

		//	{ // this introduces more issues than that is solves...
		//		"abb.",
		//		"xxa.",
		//	},
	}
	ins3Approx4 := []unitTest{
		{
			"abcd",
			"xxxa",
		},

		//	{ // this introduces more issues than that is solves...
		//		"aba.",
		//		"xxa.",
		//	},

		//	{ // this introduces more issues than that is solves...
		//		"abb.",
		//		"xxa.",
		//	},
	}

	analyse("Sub1 approx2", sub1Approx2)
	analyse("Sub1 approx3", sub1Approx3)
	t.Log("")
	analyse("Del1 approx2", del1Approx2)
	analyse("Del1 approx3", del1Approx3)
	analyse("Del1 approx4", del1Approx4)
	t.Log("")
	analyse("Ins1 approx2", ins1Approx2)
	analyse("Ins1 approx3", ins1Approx3)
	analyse("Ins1 approx4", ins1Approx4)
	t.Log("")
	analyse("Tra1 approx2", tra1Approx2)
	t.Log("")
	analyse("Del2 approx3", del2Approx3)
	analyse("Del2 approx4", del2Approx4)
	t.Log("")
	analyse("Ins2 approx3", ins2Approx3)
	analyse("Ins2 approx4", ins2Approx4)
	t.Log("")
	analyse("Tra1 approx3", tra1Approx3)
	t.Log("")
	analyse("Del3 approx4", del3Approx4)
	analyse("Ins3 approx4", ins3Approx4)

	t.Logf(toAsmA3("eq", toConfig("1XXX:XXXX:XXXX:XXXX"), 0, 1, 1))
	t.Logf(toAsmA3("tra", toConfig("01XX:1XXX:XXXX:XXXX"), 1, 2, 2))
	t.Logf(toAsmA3("del1", toConfig("00XX:1XXX:X1XX:XXXX"), 1, 0, 1))
	t.Logf(toAsmA3("del2", toConfig("000X:000X:100X:XXXX"), 1, 0, 1))
	t.Logf(toAsmA3("ins1", toConfig("01XX:0X1X:XXXX:XXXX"), 1, 1, 0))
	t.Logf(toAsmA3("ins2", toConfig("001X:000X:000X:XXXX"), 1, 1, 0))
	t.Logf(toAsmA3("tra+ins", toConfig("001X:100X:000X:XXXX"), 2, 3, 2))

}
