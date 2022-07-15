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

const (
	// allow hash table entries to be
	// evicted beyond some window (in bytes)
	evictWindow = 64 * 1024
)

func (c *Chunker) compress() {
	// we would like
	//   c.lastst <= c.lastcomp <= c.lastoff <= c.Align
	if c.lastoff > c.Align {
		panic("lastoff > c.Align")
	}
	if c.lastcomp < c.lastst {
		panic("lastcomp < c.lastst")
	}
	// scan body for repeated strings
	var stab strtab
	body := c.Buffer.Bytes()[c.lastcomp:]
	prefix := c.Buffer.Bytes()[c.lastst:c.lastcomp]
	toscan := body
	for len(toscan) > 0 {
		toscan = scanstrs(&c.Symbols, toscan, &stab)
	}
	if stab.prepop == 0 {
		// didn't turn any new strings to symbols
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
	newlastst := c.tmpbuf.Size()
	newtmpID := c.Symbols.MaxID()
	// directly copy input that has
	// already been compressed
	c.tmpbuf.UnsafeAppend(prefix)
	lastoff := c.tmpbuf.Size()
	for len(body) > 0 {
		// update c.lastoff to point to the start
		// of the last structure so that adjustSyms
		// can move around the last record correctly
		lastoff = c.tmpbuf.Size()
		body = compress(&c.Symbols, &c.tmpbuf, body)
	}
	if lastoff > c.Align {
		// rare situation: if the symbol table
		// has never been encoded, then prepending
		// it as we did above can cause lastoff to
		// grow rather than shrink, and in that case
		// we cannot compress because there isn't
		// space to grow the symbol table -- just bail out
		return
	}
	// commit the new compressed data
	c.lastoff = lastoff
	c.lastst = newlastst
	c.tmpID = newtmpID
	// swap tmp and main
	newbody := c.tmpbuf.Bytes()
	c.tmpbuf.Set(c.Buffer.Bytes()[:0])
	c.Buffer.Set(newbody)
	c.lastcomp = c.Buffer.Size()
}

const (
	strtabBits = 9
	strtabSize = 1 << strtabBits
	strtabMask = strtabSize - 1
)

type strentry struct {
	enc   []byte
	count int
}

func (s *strentry) init(str []byte) {
	s.enc = str
	s.count = 1
}

// since all string slices point into
// the same source buffer, we can compute
// the distance from one to the other just
// by looking at the remaining capacity
func distance(from, to []byte) int {
	return cap(from) - cap(to)
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
func (s *strtab) mark(str []byte, st *Symtab, threshold int) {
	if len(str) < 3 {
		return
	}
	if _, ok := st.getBytes(str); ok {
		s.prepop++
		return
	}
	e0, e1 := s.hash(str)
	if e0.enc == nil {
		e0.init(str)
		s.hits++
		return
	}
	if bytes.Equal(str, e0.enc) {
		e0.count++
		if e0.count >= threshold {
			st.InternBytes(e0.enc)
			*e0 = strentry{}
			s.prepop++
		}
		return
	}
	if e1.enc == nil {
		e1.init(str)
		s.hits++
		return
	}
	if bytes.Equal(str, e1.enc) {
		e1.count++
		if e1.count >= threshold {
			st.InternBytes(e1.enc)
			*e1 = strentry{}
			s.prepop++
		}
		return
	}

	lo := e0
	if e1.count < e0.count || (e0.count == e1.count && distance(e0.enc, e1.enc) < 0) {
		lo = e1
	}
	// if the oldest of (e0, e1) has count==1 and has
	// existed for longer than some window size, clobber it
	if lo.count == 1 && distance(lo.enc, str) >= evictWindow {
		lo.init(str)
		s.hits++
		return
	}

	// track how many candidates we ignored
	// due to hash table collisions
	s.misses++
}

// for each string in body (recursively),
// call stab.mark(str, st)
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
		stab.mark(body, st, 3)
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
