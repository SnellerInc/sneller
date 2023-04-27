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

func (c *Chunker) compress() bool {
	// we would like
	//   c.lastst <= c.lastcomp <= c.lastoff <= c.Align
	if c.lastoff > c.Align {
		panic("lastoff > c.Align")
	}
	if c.lastcomp < c.lastst {
		panic("lastcomp < c.lastst")
	}
	oldmax := c.Symbols.MaxID()
	// scan body for repeated strings
	stab := strtab{seed: maphash.MakeSeed()}
	body := c.Buffer.Bytes()[c.lastcomp:]
	prefix := c.Buffer.Bytes()[c.lastst:c.lastcomp]
	toscan := body
	for len(toscan) > 0 {
		toscan = scanstrs(&c.Symbols, toscan, &stab)
	}
	if stab.prepop == 0 {
		// didn't turn any new strings to symbols
		return false
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
		//
		// also, revert changes to the symbol table
		c.Symbols.Truncate(oldmax)
		return false
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
	return true
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
	seed    maphash.Seed
	entries [strtabSize]strentry
	misses  int
	hits    int
	prepop  int
}

func (s *strtab) hash(str []byte) (*strentry, *strentry) {
	u := maphash.Bytes(s.seed, str)
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
		stab.mark(body, st, 10)
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
			out.WriteStringBytes(str)
		}
		return rest
	default:
		size := SizeOf(body)
		out.UnsafeAppend(body[:size])
		return body[size:]
	}
}
