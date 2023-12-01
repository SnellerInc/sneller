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
