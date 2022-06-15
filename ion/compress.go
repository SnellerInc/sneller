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

package ion

import (
	"bytes"
	"hash/maphash"
)

func (c *Chunker) compress() {
	body := c.Buffer.Bytes()[c.lastst:]

	if c.lastoff > c.Align {
		println(c.lastoff, ">", c.Align)
		panic("lastoff > c.Align")
	}

	// scan body for repeated strings
	var stab strtab
	toscan := body
	for len(toscan) > 0 {
		toscan = scanstrs(&c.Symbols, toscan, &stab)
	}

	// add new symbols to symbol table,
	// or bail out if we don't add anything
	// and there were no pre-interned strings
	// we could still compress away
	if stab.choose(&c.Symbols, 3) == 0 && stab.prepop == 0 {
		return
	}

	// re-encode the data with the new
	// symbol table and compressed records
	// in tmpbuf, then swap buffers with c.Buffer
	c.tmpbuf.Reset()
	if c.flushID == 0 {
		c.Symbols.Marshal(&c.tmpbuf, true)
	} else {
		c.Symbols.MarshalPart(&c.tmpbuf, Symbol(c.flushID))
	}
	c.lastst = c.tmpbuf.Size()
	c.tmpID = c.Symbols.MaxID()
	for len(body) > 0 {
		// update c.lastoff to point to the start
		// of the last structure so that adjustSyms
		// can move around the last record correctly
		c.lastoff = c.tmpbuf.Size()
		body = compress(&c.Symbols, &c.tmpbuf, body)
	}
	if c.lastoff > c.Align {
		panic("lastoff > c.Align")
	}
	// swap tmp and main
	newbody := c.tmpbuf.Bytes()
	c.tmpbuf.Set(c.Buffer.Bytes()[:0])
	c.Buffer.Set(newbody)
}

const (
	strtabBits = 11
	strtabSize = 1 << strtabBits
	strtabMask = strtabSize - 1
)

type strentry struct {
	enc    []byte
	count  int
	chosen Symbol
}

// strtab is a lossy hash table of strings
type strtab struct {
	entries [strtabSize]strentry
	misses  int
	hits    int
	prepop  int
}

func (s *strtab) hash(str []byte) (*strentry, *strentry) {
	var h maphash.Hash
	h.Write(str)
	u := h.Sum64()
	e0 := &s.entries[u&strtabMask]
	e1 := &s.entries[(u>>strtabBits)&strtabMask]
	return e0, e1
}

// mark attempts to perform an insert of str;
// it will avoid inserting str if it is
// already part of the symbol table or if
// there is a colliding hash entry already present
func (s *strtab) mark(str []byte, st *Symtab) {
	if len(str) < 3 {
		return
	}
	e0, e1 := s.hash(str)
	if e0.enc == nil {
		if _, ok := st.getBytes(str); ok {
			s.prepop++
			return
		}
		e0.chosen = badSymbol
		e0.enc = str
		e0.count = 1
		s.hits++
		return
	}
	if bytes.Equal(str, e0.enc) {
		e0.count++
		return
	}
	if e1.enc == nil {
		if _, ok := st.getBytes(str); ok {
			s.prepop++
			return
		}
		e1.chosen = badSymbol
		e1.enc = str
		e1.count = 1
		s.hits++
		return
	}
	if bytes.Equal(str, e1.enc) {
		e1.count++
		return
	}

	// if we have an entry that isn't part
	// of the existing hash table and collides
	// with two candidate entries, clobber the
	// existing candidates if they don't save
	// as much space as interning this entry
	if _, ok := st.getBytes(str); ok {
		s.prepop++
		return
	}
	if e0.count == 1 && len(e0.enc) < len(str) {
		e0.chosen = badSymbol
		e0.enc = str
		return
	}
	if e1.count == 1 && len(e1.enc) < len(str) {
		e1.chosen = badSymbol
		e1.enc = str
		return
	}
	// track how many candidates we ignored
	// due to hash table collisions
	s.misses++
}

// allocate symbols for all entries
// that do not already have chosen symbols
func (s *strtab) choose(st *Symtab, threshold int) int {
	n := 0
	for i := range s.entries {
		if s.entries[i].enc == nil {
			continue
		}
		if s.entries[i].count < threshold {
			continue
		}
		if s.entries[i].chosen != badSymbol {
			continue
		}
		s.entries[i].chosen = st.InternBytes(s.entries[i].enc)
		n++
	}
	return n
}

func scanstrs(st *Symtab, body []byte, stab *strtab) []byte {
	switch TypeOf(body) {
	case ListType:
		body, rest := Contents(body)
		for len(body) > 0 {
			body = scanstrs(st, body, stab)
		}
		return rest
	case StructType:
		body, rest := Contents(body)
		for len(body) > 0 {
			_, body, _ = ReadLabel(body)
			body = scanstrs(st, body, stab)
		}
		return rest
	case StringType:
		body, rest, _ := ReadStringShared(body)
		stab.mark(body, st)
		return rest
	default:
		return body[SizeOf(body):]
	}
}

func compress(st *Symtab, out *Buffer, body []byte) []byte {
	switch TypeOf(body) {
	case StructType:
		body, rest := Contents(body)
		out.BeginStruct(-1)
		var lbl Symbol
		for len(body) > 0 {
			lbl, body, _ = ReadLabel(body)
			out.BeginField(lbl)
			body = compress(st, out, body)
		}
		out.EndStruct()
		return rest
	case ListType:
		body, rest := Contents(body)
		out.BeginList(-1)
		for len(body) > 0 {
			body = compress(st, out, body)
		}
		out.EndList()
		return rest
	case StringType:
		str, rest, _ := ReadStringShared(body)
		if sym, ok := st.getBytes(str); ok {
			out.WriteSymbol(sym)
		} else {
			out.BeginString(len(str))
			out.UnsafeAppend(str)
		}
		return rest
	default:
		size := SizeOf(body)
		out.UnsafeAppend(body[:size])
		return body[size:]
	}
}
