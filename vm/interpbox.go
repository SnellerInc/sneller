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
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func init() {
	opinfo[opboxf64].portable = bcboxf64go
	opinfo[opboxi64].portable = bcboxi64go
	opinfo[opboxts].portable = bcboxtsgo
	opinfo[opboxstr].portable = bcboxstrgo
	opinfo[opboxlist].portable = bcboxlistgo
	opinfo[opboxk].portable = bcboxkgo
	opinfo[opmakestruct].portable = bcmakestructgo
	opinfo[opmakelist].portable = bcmakelistgo
	opinfo[opalloc].portable = bcallocgo
	opinfo[opconcatstr].portable = bcconcatstrgo
}

func bcboxlistgo(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[sRegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask

	var out vRegData
	var buf ion.Buffer
	buf.Set(bc.scratch)
	p := len(bc.scratch)
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		lst := vmref{src.offsets[i], src.sizes[i]}.mem()
		buf.BeginList(-1)
		buf.UnsafeAppend(lst)
		buf.EndList()
		result := buf.Bytes()[p:]
		start, ok := vmdispl(result)
		if !ok {
			// had to realloc the buffer for space;
			// means the scratch buffer didn't have enough capacity:
			bc.err = bcerrMoreScratch
			return pc + 6
		}
		out.offsets[i] = start
		out.sizes[i] = uint32(len(result))
		out.typeL[i] = result[0]
		out.headerSize[i] = byte(ion.HeaderSizeOf(result))
		p = buf.Size()
	}
	bc.scratch = buf.Bytes()
	*dst = out
	return pc + 6
}

func bcboxstrgo(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[sRegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask

	var out vRegData
	var buf ion.Buffer
	buf.Set(bc.scratch)
	p := len(bc.scratch)
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		str := vmref{src.offsets[i], src.sizes[i]}.mem()
		buf.WriteStringBytes(str)
		result := buf.Bytes()[p:]
		start, ok := vmdispl(result)
		if !ok {
			// had to realloc the buffer for space;
			// means the scratch buffer didn't have enough capacity:
			bc.err = bcerrMoreScratch
			return pc + 6
		}
		out.offsets[i] = start
		out.sizes[i] = uint32(len(result))
		out.typeL[i] = result[0]
		out.headerSize[i] = byte(ion.HeaderSizeOf(result))
		p = buf.Size()
	}
	bc.scratch = buf.Bytes()
	*dst = out
	return pc + 6
}

func bcboxkgo(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[kRegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask
	var out vRegData

	p := len(bc.scratch)
	want := bcLaneCount
	if cap(bc.scratch)-p < want {
		bc.err = bcerrMoreScratch
		return pc + 6
	}
	bc.scratch = bc.scratch[:p+want]
	mem := bc.scratch[p:]
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		mem[i] = 0x10 // false
		if src.mask&(1<<i) != 0 {
			mem[i] |= 0x01 // true
		}
		start, ok := vmdispl(mem[i:])
		if !ok {
			panic("bad scratch buffer")
		}
		out.offsets[i] = start
		out.sizes[i] = 1
		out.typeL[i] = mem[0]
		out.headerSize[i] = 1
	}
	*dst = out
	return pc + 6
}

func bcboxtsgo(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[i64RegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask
	var out vRegData

	p := len(bc.scratch)
	want := 16 * bcLaneCount
	if cap(bc.scratch)-p < want {
		bc.err = bcerrMoreScratch
		return pc + 6
	}
	bc.scratch = bc.scratch[:p+want]
	mem := bc.scratch[p:]
	var buf ion.Buffer
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		buf.Set(mem[:0])
		buf.WriteTime(date.UnixMicro(src.values[i]))
		start, ok := vmdispl(buf.Bytes())
		if !ok {
			panic("bad scratch buffer")
		}
		out.offsets[i] = start
		out.sizes[i] = uint32(len(buf.Bytes()))
		out.typeL[i] = mem[0]
		out.headerSize[i] = 1 // ints and floats always have 1-byte headers
		mem = mem[16:]
	}
	*dst = out
	return pc + 6
}

func bcboxi64go(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[i64RegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask
	var out vRegData

	p := len(bc.scratch)
	want := 9 * bcLaneCount
	if cap(bc.scratch)-p < want {
		bc.err = bcerrMoreScratch
		return pc + 6
	}
	bc.scratch = bc.scratch[:p+want]
	mem := bc.scratch[p:]
	var buf ion.Buffer
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		buf.Set(mem[:0])
		buf.WriteInt(src.values[i])
		start, ok := vmdispl(buf.Bytes())
		if !ok {
			panic("bad scratch buffer")
		}
		out.offsets[i] = start
		out.sizes[i] = uint32(len(buf.Bytes()))
		out.typeL[i] = mem[0]
		out.headerSize[i] = 1 // ints and floats always have 1-byte headers
		mem = mem[9:]
	}
	*dst = out
	return pc + 6
}

func bcboxf64go(bc *bytecode, pc int) int {
	dst := argptr[vRegData](bc, pc)
	src := argptr[f64RegData](bc, pc+2)
	mask := argptr[kRegData](bc, pc+4).mask
	var out vRegData

	p := len(bc.scratch)
	want := 9 * bcLaneCount
	if cap(bc.scratch)-p < want {
		bc.err = bcerrMoreScratch
		return pc + 6
	}
	bc.scratch = bc.scratch[:p+want]
	mem := bc.scratch[p:]
	var buf ion.Buffer
	for i := 0; i < bcLaneCount; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		buf.Set(mem[:0])
		buf.WriteCanonicalFloat(src.values[i])
		start, ok := vmdispl(buf.Bytes())
		if !ok {
			panic("bad scratch buffer")
		}
		out.offsets[i] = start
		out.sizes[i] = uint32(len(buf.Bytes()))
		out.typeL[i] = mem[0]
		out.headerSize[i] = 1 // ints and floats always have 1-byte headers
		mem = mem[9:]
	}
	*dst = out
	return pc + 6
}

func bcmakestructgo(bc *bytecode, pc int) int {
	retv := argptr[vRegData](bc, pc)
	retk := argptr[kRegData](bc, pc+2)
	argk := argptr[kRegData](bc, pc+4)
	fields := int(bcword32(bc, pc+6))

	var buf ion.Buffer
	var out vRegData
	srcmask := argk.mask
	retmask := uint16(0)

	buf.Set(bc.scratch)
	p := len(bc.scratch)
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}

		// loop over 1 lane of each va-arg and compose the struct
		ipc := pc + 10
		buf.BeginStruct(-1)
	inner:
		for j := 0; j < fields; j++ {
			sym, _, _ := ion.ReadLabel(bc.compiled[ipc:])
			argv := argptr[vRegData](bc, ipc+4)
			fieldk := argptr[kRegData](bc, ipc+6)
			ipc += 8
			if fieldk.mask&(1<<i) == 0 || argv.sizes[i] == 0 {
				continue inner
			}
			val := vmref{argv.offsets[i], argv.sizes[i]}.mem()
			buf.BeginField(sym)
			buf.UnsafeAppend(val)
		}
		buf.EndStruct()

		mem := buf.Bytes()[p:]
		start, ok := vmdispl(mem)
		if !ok {
			bc.err = bcerrMoreScratch
			goto done
		}
		retmask |= (1 << i)
		out.offsets[i] = start
		out.sizes[i] = uint32(len(mem))
		out.typeL[i] = mem[0]
		out.headerSize[i] = byte(ion.HeaderSizeOf(mem))
		p = buf.Size()
	}

	bc.scratch = buf.Bytes()
	*retv = out
	retk.mask = retmask
done:
	return pc + 10 + (fields * 8)
}

func bcmakelistgo(bc *bytecode, pc int) int {
	retv := argptr[vRegData](bc, pc)
	retk := argptr[kRegData](bc, pc+2)
	argk := argptr[kRegData](bc, pc+4)
	fields := int(bcword32(bc, pc+6))

	var buf ion.Buffer
	var out vRegData
	srcmask := argk.mask
	retmask := uint16(0)

	buf.Set(bc.scratch)
	p := len(bc.scratch)
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}

		// loop over 1 lane of each va-arg and compose the struct
		ipc := pc + 10
		buf.BeginList(-1)
	inner:
		for j := 0; j < fields; j++ {
			argv := argptr[vRegData](bc, ipc)
			fieldk := argptr[kRegData](bc, ipc+2)
			ipc += 4
			if fieldk.mask&(1<<i) == 0 || argv.sizes[i] == 0 {
				continue inner
			}
			val := vmref{argv.offsets[i], argv.sizes[i]}.mem()
			buf.UnsafeAppend(val)
		}
		buf.EndList()

		mem := buf.Bytes()[p:]
		start, ok := vmdispl(mem)
		if !ok {
			bc.err = bcerrMoreScratch
			goto done
		}
		retmask |= (1 << i)
		out.offsets[i] = start
		out.sizes[i] = uint32(len(mem))
		out.typeL[i] = mem[0]
		out.headerSize[i] = byte(ion.HeaderSizeOf(mem))
		p = buf.Size()
	}

	bc.scratch = buf.Bytes()
	*retv = out
	retk.mask = retmask
done:
	return pc + 10 + (fields * 4)
}

func bcallocgo(bc *bytecode, pc int) int {
	retk := argptr[kRegData](bc, pc)
	rets := argptr[sRegData](bc, pc+2)
	srci := argptr[i64RegData](bc, pc+4)
	srck := argptr[kRegData](bc, pc+6)

	var ret sRegData
	srcmask := srck.mask
	retmask := srcmask
	for i := 0; i < bcLaneCount; i++ {
		if srcmask&(1<<i) == 0 {
			continue
		}
		size := srci.values[i]
		if cap(bc.scratch)-len(bc.scratch) < int(size) {
			bc.err = bcerrMoreScratch
			break
		}
		p := len(bc.scratch)
		bc.scratch = bc.scratch[:p+int(size)]
		pos, _ := vmdispl(bc.scratch[p:])
		ret.offsets[i] = pos
		ret.sizes[i] = uint32(size)
	}

	retk.mask = retmask
	*rets = ret
	return pc + 8
}

func bcconcatstrgo(bc *bytecode, pc int) int {
	rets := argptr[sRegData](bc, pc)
	retk := argptr[kRegData](bc, pc+2)
	nargs := int(bcword32(bc, pc+4))

	var out sRegData
	retmask := uint16(bc.vmState.validLanes.mask)

	// compute AND of all the input value masks
	ipc := pc + 8
	for j := 0; j < nargs; j++ {
		argk := argptr[kRegData](bc, ipc+2)
		retmask &= argk.mask
		ipc += 4
	}
	// compute the total size of the input in each lane
	// for only the lanes that are valid; then allocate
	// the sum of those in the scratch buffer
	totalsize := uint32(0)
	ipc = pc + 8
	for j := 0; j < nargs; j++ {
		args := argptr[sRegData](bc, ipc)
		for i := 0; i < bcLaneCount; i++ {
			if retmask&(1<<i) == 0 {
				continue
			}
			out.sizes[i] += args.sizes[i]
			totalsize += args.sizes[i]
		}
		ipc += 4
	}
	if cap(bc.scratch)-len(bc.scratch) < int(totalsize) {
		bc.err = bcerrMoreScratch
		goto done
	}

	// now actually build the strings
	for i := 0; i < bcLaneCount; i++ {
		if retmask&(1<<i) == 0 {
			continue
		}
		// extend bc.scratch and copy in the concatenated contents
		p := len(bc.scratch)
		bc.scratch = bc.scratch[:p+int(out.sizes[i])]
		pos, _ := vmdispl(bc.scratch[p : p+1])
		out.offsets[i] = pos
		ipc = pc + 8
		for j := 0; j < nargs; j++ {
			args := argptr[sRegData](bc, ipc)
			p += copy(bc.scratch[p:], vmref{args.offsets[i], args.sizes[i]}.mem())
			ipc += 4
		}
	}
	retk.mask = retmask
	*rets = out
done:
	return pc + 8 + (nargs * 4)
}
