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
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
)

const (
	outRowsCapacity = 1024 // The size of a buffer for the metadata describing individual rows
)

type creatorFunc func(u *Unpivot, w io.WriteCloser) rowConsumer

type Unpivot struct {
	out      QuerySink
	as       *string
	at       *string
	fnCreate creatorFunc
}

func createUnpivoterAsAt(u *Unpivot, w io.WriteCloser) rowConsumer {
	return &unpivoterAsAt{
		unpivoterBase: unpivoterBase{
			parent: u,
			out:    asRowConsumer(w),
		},
	}
}

func createUnpivoterAs(u *Unpivot, w io.WriteCloser) rowConsumer {
	return &unpivoterAs{
		unpivoterBase: unpivoterBase{
			parent: u,
			out:    asRowConsumer(w),
		},
	}
}

func createUnpivoterAt(u *Unpivot, w io.WriteCloser) rowConsumer {
	return &unpivoterAt{
		unpivoterBase: unpivoterBase{
			parent: u,
			out:    asRowConsumer(w),
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
	c := u.fnCreate(u, w)
	return splitter(c), nil
}

func (u *Unpivot) Close() error {
	return u.out.Close()
}

type unpivoterBase struct {
	parent *Unpivot
	out    rowConsumer // The downstream kernel
	params rowParams
	dummy  []vmref // dummy rows
	syms   *symtab
}

func (u *unpivoterBase) Close() error {
	return u.out.Close()
}

func (u *unpivoterBase) next() rowConsumer {
	return u.out
}

type varUInt []byte

// unpivoterAsAt handles the "UNPIVOT AS val AT key" case
type unpivoterAsAt struct {
	unpivoterBase
}

func (u *unpivoterAsAt) symbolize(st *symtab, aux *auxbindings) error {
	if len(aux.bound) > 0 {
		return fmt.Errorf("UNPIVOT does not handle auxilliary bindings yet")
	}

	selfaux := auxbindings{}
	selfaux.push(*u.parent.at) // aux 1 = at
	selfaux.push(*u.parent.as) // aux 0 = as
	u.syms = st
	u.params.auxbound = shrink(u.params.auxbound, 2)
	u.params.auxbound[0] = slices.Grow(u.params.auxbound[0][:0], outRowsCapacity)
	u.params.auxbound[1] = slices.Grow(u.params.auxbound[1][:0], outRowsCapacity)
	u.dummy = slices.Grow(u.dummy[:0], outRowsCapacity)
	return u.out.symbolize(st, &selfaux)
}

func (u *unpivoterAsAt) writeRows(rows []vmref, params *rowParams) error {
	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			sym, rest, err := ion.ReadLabel(data)
			if err != nil {
				return err
			}
			// add a dummy record with 0 bytes of contents
			// for the "main" row; the rowParams contain
			// the only live bindings after this step
			v := rows[i]
			v[1] = 0
			u.dummy = append(u.dummy, v)
			restsize := ion.SizeOf(rest)
			u.params.auxbound[0] = append(u.params.auxbound[0], u.syms.symrefs[sym])
			restpos, _ := vmdispl(rest)
			u.params.auxbound[1] = append(u.params.auxbound[1], vmref{restpos, uint32(restsize)})
			data = rest[restsize:]

			if len(u.dummy) == cap(u.dummy) {
				// flush; note that the actual row content
				// will be ignored
				err := u.out.writeRows(u.dummy, &u.params)
				if err != nil {
					return err
				}
				u.dummy = u.dummy[:0]
				u.params.auxbound[0] = u.params.auxbound[0][:0]
				u.params.auxbound[1] = u.params.auxbound[1][:0]
			}
		}
	}
	if len(u.dummy) > 0 {
		// flush; note that the actual row content
		// will be ignored
		err := u.out.writeRows(u.dummy, &u.params)
		if err != nil {
			return err
		}
		u.dummy = u.dummy[:0]
		u.params.auxbound[0] = u.params.auxbound[0][:0]
		u.params.auxbound[1] = u.params.auxbound[1][:0]
	}
	return nil
}

// unpivoterAt handles the "UNPIVOT AS val" case
type unpivoterAs struct {
	unpivoterBase
	encodedAs varUInt
}

func (u *unpivoterAs) symbolize(st *symtab, aux *auxbindings) error {
	if len(aux.bound) > 0 {
		return fmt.Errorf("UNPIVOT doesn't handle auxilliary bindings yet")
	}
	selfaux := auxbindings{}
	selfaux.push(*u.parent.as) // aux[0] = as
	u.syms = st
	u.params.auxbound = shrink(u.params.auxbound, 1)
	u.params.auxbound[0] = slices.Grow(u.params.auxbound[0][:0], outRowsCapacity)
	u.dummy = slices.Grow(u.dummy[:0], outRowsCapacity)
	return u.out.symbolize(st, &selfaux)
}

func skipVarUInt(buf []byte) []byte {
	for len(buf) > 0 && buf[0]&0x80 == 0 {
		buf = buf[1:]
	}
	return buf[1:]
}

func (u *unpivoterAs) writeRows(rows []vmref, params *rowParams) error {
	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			// Skip the fileld ID
			rest := skipVarUInt(data)
			size := ion.SizeOf(rest)
			data = rest[size:] // Seek to the next field of the input ION structure

			v := rows[i]
			v[1] = 0
			u.dummy = append(u.dummy, v)
			vmoff, _ := vmdispl(rest)
			u.params.auxbound[0] = append(u.params.auxbound[0], vmref{vmoff, uint32(size)})
			if len(u.params.auxbound) == cap(u.params.auxbound) {
				err := u.out.writeRows(u.dummy, &u.params)
				if err != nil {
					return err
				}
				u.dummy = u.dummy[:0]
				u.params.auxbound[0] = u.params.auxbound[0][:0]
			}
		}
	}
	if len(u.dummy) > 0 {
		err := u.out.writeRows(u.dummy, &u.params)
		if err != nil {
			return err
		}
		u.dummy = u.dummy[:0]
		u.params.auxbound[0] = u.params.auxbound[0][:0]
	}
	return nil
}

// unpivoterAt handles the "UNPIVOT AT key" case
type unpivoterAt struct {
	unpivoterBase
	encodedAt varUInt
}

func (u *unpivoterAt) symbolize(st *symtab, aux *auxbindings) error {
	if len(aux.bound) > 0 {
		return fmt.Errorf("UNPIVOT doesn't handle auxilliary bindings yet")
	}
	selfaux := auxbindings{}
	selfaux.push(*u.parent.at) // aux[0] = as
	u.syms = st
	u.params.auxbound = shrink(u.params.auxbound, 1)
	u.params.auxbound[0] = slices.Grow(u.params.auxbound[0][:0], outRowsCapacity)
	u.dummy = slices.Grow(u.dummy[:0], outRowsCapacity)
	return u.out.symbolize(st, &selfaux)
}

func (u *unpivoterAt) writeRows(rows []vmref, params *rowParams) error {
	for i := range rows {
		data := rows[i].mem()
		// Iterate over all the struct fields
		for len(data) != 0 {
			sym, rest, err := ion.ReadLabel(data)
			if err != nil {
				return err
			}
			data = rest[ion.SizeOf(rest):] // Seek to the next field of the input ION structure

			v := rows[i]
			v[1] = 0
			u.dummy = append(u.dummy, v)
			u.params.auxbound[0] = append(u.params.auxbound[0], u.syms.symrefs[sym])

			if len(u.dummy) == cap(u.dummy) {
				err := u.out.writeRows(u.dummy, &u.params)
				if err != nil {
					return err
				}
				u.dummy = u.dummy[:0]
				u.params.auxbound[0] = u.params.auxbound[0][:0]
			}
		}
	}
	if len(u.dummy) > 0 {
		err := u.out.writeRows(u.dummy, &u.params)
		if err != nil {
			return err
		}
		u.dummy = u.dummy[:0]
		u.params.auxbound[0] = u.params.auxbound[0][:0]
	}
	return nil
}
