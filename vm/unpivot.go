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

package vm

import (
	"encoding/binary"
	"io"
	"math/bits"

	"github.com/SnellerInc/sneller/ion"
)

const (
	outRowsCapacity = 1024 // The size of a buffer for the metadata describing individual rows
	atBufCapacity   = 16   // 1 byte for the ION prefix + 8 bytes for the symbol ID, aligned to the power of 2
)

type creatorFunc func(u *Unpivot, w io.WriteCloser, buf []byte, disp uint32) rowConsumer

type Unpivot struct {
	out      QuerySink
	as       *string
	at       *string
	fnCreate creatorFunc
}

func createUnpivoterAsAt(u *Unpivot, w io.WriteCloser, buf []byte, disp uint32) rowConsumer {
	return &unpivoterAsAt{
		unpivoterBase: unpivoterBase{
			parent:          u,
			out:             asRowConsumer(w),
			buf:             buf,
			bufDisplacement: disp,
		},
	}
}

func createUnpivoterAs(u *Unpivot, w io.WriteCloser, buf []byte, disp uint32) rowConsumer {
	return &unpivoterAs{
		unpivoterBase: unpivoterBase{
			parent:          u,
			out:             asRowConsumer(w),
			buf:             buf,
			bufDisplacement: disp,
		},
	}
}

func createUnpivoterAt(u *Unpivot, w io.WriteCloser, buf []byte, disp uint32) rowConsumer {
	return &unpivoterAt{
		unpivoterBase: unpivoterBase{
			parent:          u,
			out:             asRowConsumer(w),
			buf:             buf,
			bufDisplacement: disp,
		},
	}
}

// NewUnpivot creates a new Unpivot kernel that unpivots a tuple into a set of pairs, per PartiQL.pdf, $5.2
func NewUnpivot(as *string, at *string, dst QuerySink) (*Unpivot, error) {
	// Select the creator based on the provided labels
	var creator creatorFunc
	if as != nil {
		if at != nil {
			creator = createUnpivoterAsAt
		} else {
			creator = createUnpivoterAs
		}
	} else {
		if at != nil {
			creator = createUnpivoterAt
		} else {
			panic("'as' and 'at' cannot both be nil") // should have been validated before, double-checking here
		}
	}

	u := &Unpivot{
		out:      dst,
		as:       as,
		at:       at,
		fnCreate: creator,
	}
	return u, nil
}

func (u *Unpivot) Open() (io.WriteCloser, error) {
	w, err := u.out.Open()
	if err != nil {
		return nil, err
	}
	buf := Malloc()
	disp, _ := vmdispl(buf)
	c := u.fnCreate(u, w, buf, disp)
	return splitter(c), nil
}

func (u *Unpivot) Close() error {
	return u.out.Close()
}

type unpivoterBase struct {
	parent          *Unpivot
	out             rowConsumer // The downstream kernel
	buf             []byte      // The buffer for the underlying ION data
	bufDisplacement uint32
	outRows         [outRowsCapacity]vmref // The buffer for the metadata describing individual rows
}

func (u *unpivoterBase) Close() error {
	Free(u.buf)
	u.buf = nil
	return u.out.Close()
}

func (u *unpivoterBase) next() rowConsumer {
	return u.out
}

type varUInt []byte

// unpivoterAsAt handles the "UNPIVOT AS val AT key" case
type unpivoterAsAt struct {
	unpivoterBase
	encodedAs  varUInt
	encodedAt  varUInt
	asBeforeAt bool
}

// toVarUInt translates x into the ION VarUInt format
func toVarUInt(x uint) varUInt {
	const maxData = 10 // 64 bits/7 is 9.something bytes
	var data [maxData]byte
	data[maxData-1] = byte(x | 0x80)
	i := maxData - 2

	for x = x >> 7; x != 0; x = x >> 7 {
		data[i] = byte(x & 0x7f)
		i--
	}
	return data[i+1:]
}

// symbolToVarUInt translates an ION Symbol into the ION VarUInt format
func symbolToVarUInt(s ion.Symbol) varUInt {
	return toVarUInt(uint(s))
}

func skipVarUInt(data []byte) []byte {
	i := 0
	for data[i]&0x80 == 0 {
		i++
	}
	return data[i+1:]
}

func (u *unpivoterAsAt) symbolize(st *symtab, aux *auxbindings) error {
	asSym := st.Intern(*u.parent.as)
	u.encodedAs = symbolToVarUInt(asSym)
	atSym := st.Intern(*u.parent.at)
	u.encodedAt = symbolToVarUInt(atSym)
	u.asBeforeAt = asSym < atSym
	return u.out.symbolize(st, aux)
}

func (u *unpivoterAsAt) writeRows(rows []vmref, params *rowParams) error {
	outRowsCount := 0
	bufOffs := 0
	bufRemaining := len(u.buf)
	bufDisplacement := u.bufDisplacement

	asSymbolLen := len(u.encodedAs)
	atSymbolLen := len(u.encodedAt)
	var atBuffer [atBufCapacity]byte

	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			sym, rest, err := ion.ReadLabel(data)
			if err != nil {
				return err
			}
			asContentSize := ion.SizeOf(rest)
			data = rest[asContentSize:] // Seek to the next field of the input ION structure

			// Create the ION symbol working backwards to seamlessly handle the embedded variable-length uints
			// The symbol goes last in the Big Endian format
			binary.BigEndian.PutUint64(atBuffer[atBufCapacity-8:], uint64(sym))
			atContentSize := (bits.Len64(uint64(sym)) + 15) / 8
			atBufOffs := atBufCapacity - atContentSize
			atBuffer[atBufOffs] = byte(0x70 + atContentSize - 1) // The ION Symbol prefix

			// The total size of the struct representing the output pair
			structSize := asSymbolLen + asContentSize + atSymbolLen + atContentSize

			// Ensure there is enough room for the resulting serialized struct
			if (outRowsCount == outRowsCapacity) || (structSize > bufRemaining) {
				// Overflow, flush the buffers to the downstream kernel
				if err := u.out.writeRows(u.outRows[:outRowsCount], params); err != nil {
					return err
				}
				outRowsCount = 0
				bufOffs = 0
				bufRemaining = len(u.buf)
				bufDisplacement = u.bufDisplacement
			}

			u.outRows[outRowsCount][0] = bufDisplacement
			u.outRows[outRowsCount][1] = uint32(structSize)
			outRowsCount++
			bufDisplacement += uint32(structSize)
			bufRemaining -= structSize

			// Ensure the fields appear in the sorted order
			if u.asBeforeAt {
				bufOffs += copy(u.buf[bufOffs:], u.encodedAs)          // The AS field name [VarUInt]
				bufOffs += copy(u.buf[bufOffs:], rest[:asContentSize]) // The AS field value, taken verbatim from the input ION
				bufOffs += copy(u.buf[bufOffs:], u.encodedAt)          // The AT field name [VarUInt]
				bufOffs += copy(u.buf[bufOffs:], atBuffer[atBufOffs:]) // The AT field value, encoded as ION symbol
			} else {
				bufOffs += copy(u.buf[bufOffs:], u.encodedAt)          // The AT field name [VarUInt]
				bufOffs += copy(u.buf[bufOffs:], atBuffer[atBufOffs:]) // The AT field value, encoded as ION symbol
				bufOffs += copy(u.buf[bufOffs:], u.encodedAs)          // The AS field name [VarUInt]
				bufOffs += copy(u.buf[bufOffs:], rest[:asContentSize]) // The AS field value, taken verbatim from the input ION
			}
		}
	}
	if outRowsCount != 0 {
		// There is an unprocessed residue, flush it to the downstream kernel
		return u.out.writeRows(u.outRows[:outRowsCount], params)
	}
	return nil
}

// unpivoterAt handles the "UNPIVOT AS val" case
type unpivoterAs struct {
	unpivoterBase
	encodedAs varUInt
}

func (u *unpivoterAs) symbolize(st *symtab, aux *auxbindings) error {
	asSym := st.Intern(*u.parent.as)
	u.encodedAs = symbolToVarUInt(asSym)
	return u.out.symbolize(st, aux)
}

func (u *unpivoterAs) writeRows(rows []vmref, params *rowParams) error {
	outRowsCount := 0
	bufOffs := 0
	bufRemaining := len(u.buf)
	bufDisplacement := u.bufDisplacement
	asSymbolLen := len(u.encodedAs)

	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			// Skip the fileld ID
			rest := skipVarUInt(data)
			asContentSize := ion.SizeOf(rest)
			data = rest[asContentSize:] // Seek to the next field of the input ION structure

			// The total size of the struct representing the output pair
			structSize := asSymbolLen + asContentSize

			// Ensure there is enough room for the resulting serialized struct
			if (outRowsCount == outRowsCapacity) || (structSize > bufRemaining) {
				// Overflow, flush the buffers to the downstream kernel
				if err := u.out.writeRows(u.outRows[:outRowsCount], params); err != nil {
					return err
				}
				outRowsCount = 0
				bufOffs = 0
				bufRemaining = len(u.buf)
				bufDisplacement = u.bufDisplacement
			}

			u.outRows[outRowsCount][0] = bufDisplacement
			u.outRows[outRowsCount][1] = uint32(structSize)
			outRowsCount++
			bufDisplacement += uint32(structSize)
			bufRemaining -= structSize

			bufOffs += copy(u.buf[bufOffs:], u.encodedAs)          // The AS field name [VarUInt]
			bufOffs += copy(u.buf[bufOffs:], rest[:asContentSize]) // The AS field value, taken verbatim from the input ION
		}
	}
	if outRowsCount != 0 {
		// There is an unprocessed residue, flush it to the downstream kernel
		return u.out.writeRows(u.outRows[:outRowsCount], params)
	}
	return nil
}

// unpivoterAt handles the "UNPIVOT AT key" case
type unpivoterAt struct {
	unpivoterBase
	encodedAt varUInt
}

func (u *unpivoterAt) symbolize(st *symtab, aux *auxbindings) error {
	atSym := st.Intern(*u.parent.at)
	u.encodedAt = symbolToVarUInt(atSym)
	return u.out.symbolize(st, aux)
}

func (u *unpivoterAt) writeRows(rows []vmref, params *rowParams) error {
	outRowsCount := 0
	bufOffs := 0
	bufRemaining := len(u.buf)
	bufDisplacement := u.bufDisplacement

	atSymbolLen := len(u.encodedAt)
	var atBuffer [atBufCapacity]byte

	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			sym, rest, err := ion.ReadLabel(data)
			if err != nil {
				return err
			}
			asContentSize := ion.SizeOf(rest)
			data = rest[asContentSize:] // Seek to the next field of the input ION structure

			// Create the ION symbol working backwards to seamlessly handle the embedded variable-length uints
			// The symbol goes last in the Big Endian format
			binary.BigEndian.PutUint64(atBuffer[atBufCapacity-8:], uint64(sym))
			atContentSize := (bits.Len64(uint64(sym)) + 15) / 8
			atBufOffs := atBufCapacity - atContentSize
			atBuffer[atBufOffs] = byte(0x70 + atContentSize - 1) // The ION Symbol prefix

			// The total size of the struct representing the output pair
			structSize := atSymbolLen + atContentSize

			// Ensure there is enough room for the resulting serialized struct
			if (outRowsCount == outRowsCapacity) || (structSize > bufRemaining) {
				// Overflow, flush the buffers to the downstream kernel
				if err := u.out.writeRows(u.outRows[:outRowsCount], params); err != nil {
					return err
				}
				outRowsCount = 0
				bufOffs = 0
				bufRemaining = len(u.buf)
				bufDisplacement = u.bufDisplacement
			}

			u.outRows[outRowsCount][0] = bufDisplacement
			u.outRows[outRowsCount][1] = uint32(structSize)
			outRowsCount++
			bufDisplacement += uint32(structSize)
			bufRemaining -= structSize

			bufOffs += copy(u.buf[bufOffs:], u.encodedAt)          // The AT field name [VarUInt]
			bufOffs += copy(u.buf[bufOffs:], atBuffer[atBufOffs:]) // The AT field value, encoded as ION symbol
		}
	}
	if outRowsCount != 0 {
		// There is an unprocessed residue, flush it to the downstream kernel
		return u.out.writeRows(u.outRows[:outRowsCount], params)
	}
	return nil
}
