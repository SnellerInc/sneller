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

import (
	"github.com/SnellerInc/sneller/fastdate"
)

var dateDiffMQYDivTable = [3]int64{
	1,
	3,
	12,
}

func bcdateaddmonthgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+8).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).AddMonth(val2.values[i])
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 10
}

func bcdateaddmonthimmgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := int64(bcword64(bc, pc+6))

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+14).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).AddMonth(val2)
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 16
}

func bcdateaddquartergo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+8).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).AddQuarter(val2.values[i])
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 10
}

func bcdateaddyeargo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+8).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).AddYear(val2.values[i])
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 10
}

func bcdatebingo(bc *bytecode, pc int) int {
	stride := int64(bcword64(bc, pc+4))
	val1 := argptr[tsRegData](bc, pc+12)
	val2 := argptr[i64RegData](bc, pc+14)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+16).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).DateBin(fastdate.Timestamp(val2.values[i]), stride)
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 18
}

func bcdatediffmicrosecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+8).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).DateDiffMicrosecond(fastdate.Timestamp(val2.values[i]))
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 10
}

func bcdatediffparamgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)
	param := bcword64(bc, pc+8)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+16).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		result, ok := fastdate.Timestamp(val1.values[i]).DateDiffParam(fastdate.Timestamp(val2.values[i]), param)
		if ok {
			dst.values[i] = int64(result)
			retmask |= 1 << i
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 18
}

func bcdatediffmqygo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+4)
	val2 := argptr[i64RegData](bc, pc+6)

	predicate := int(bcword(bc, pc+8))
	divDiff := dateDiffMQYDivTable[predicate]

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+10).mask
	retmask := uint16(0)

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) == 0 {
			continue
		}

		diff := fastdate.Timestamp(val1.values[i]).DateDiffMonth(fastdate.Timestamp(val2.values[i]))
		diff /= divDiff

		dst.values[i] = int64(diff)
		retmask |= 1 << i
	}

	*argptr[i64RegData](bc, pc) = dst
	*argptr[kRegData](bc, pc+2) = kRegData{retmask}

	return pc + 12
}

func bcdateextractmicrosecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractMicrosecond())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractmillisecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractMillisecond())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractsecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractSecond())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractminutego(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractMinute())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextracthourgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractHour())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractdaygo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractDay())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractdowgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractDOW())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractdoygo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractDOY())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractmonthgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractMonth())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractquartergo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractQuarter())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdateextractyeargo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).ExtractYear())
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdatetounixepochgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = fastdate.Timestamp(val1.values[i]).ToUnixEpoch()
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdatetounixmicrogo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := i64RegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = val1.values[i]
		}
	}

	*argptr[i64RegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncmillisecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncMillisecond())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncsecondgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncSecond())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncminutego(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncMinute())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetrunchourgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncHour())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncdaygo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncDay())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncdowgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)
	dow := uint32(bcword(bc, pc+4))

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+6).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncDOW(dow))
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 8
}

func bcdatetruncmonthgo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncMonth())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncquartergo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncQuarter())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}

func bcdatetruncyeargo(bc *bytecode, pc int) int {
	val1 := argptr[tsRegData](bc, pc+2)

	dst := tsRegData{}
	msk := argptr[kRegData](bc, pc+4).mask

	for i := 0; i < bcLaneCount; i++ {
		if (msk & (1 << i)) != 0 {
			dst.values[i] = int64(fastdate.Timestamp(val1.values[i]).TruncYear())
		}
	}

	*argptr[tsRegData](bc, pc) = dst
	return pc + 6
}
