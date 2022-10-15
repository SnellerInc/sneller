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
	"github.com/SnellerInc/sneller/ion"
)

type syms interface {
	Get(ion.Symbol) string
	Intern(x string) ion.Symbol
	Symbolize(string) (ion.Symbol, bool)
}

func ionsyms(x syms) *ion.Symtab {
	if st, ok := x.(*ion.Symtab); ok {
		return st
	}
	return &x.(*symtab).Symtab
}

// symtab serves two purposes:
//
//  1. Wrap ion.Symtab; this is our source-of-truth
//     for the current symbol table
//  2. Store vm allocations for symbols and scratch buffers
//     so that they can be free'd from one place (and deterministically)
//     via the Reset method
type symtab struct {
	ion.Symtab

	// all allocated pages:
	curpage pageref
	opages  []pageref

	// symrefs[id] produces a boxed string
	// representing the symbol id
	symrefs []vmref

	// epoch keeps track of how many times
	// this symtab has been reset;
	// allocations are only valid across
	// an individual epoch
	epoch int
}

type pageref struct {
	mem   []byte // result from vm.Malloc
	off   int    // allocation offset
	maxid int    // max ID in this page
}

func (p *pageref) drop() {
	if p.mem != nil {
		Free(p.mem)
		p.mem = nil
	}
	p.off = 0
	p.maxid = 0
}

func (s *symtab) CloneInto(dst *symtab) {
	dst.free()
	s.Symtab.CloneInto(&dst.Symtab)
	dst.build()
}

func (s *symtab) Unmarshal(src []byte) ([]byte, error) {
	if ion.IsBVM(src) {
		s.resetNoFree()
	}
	ret, err := s.Symtab.Unmarshal(src)
	if err == nil {
		s.build()
	}
	return ret, err
}

func (s *symtab) Intern(x string) ion.Symbol {
	sym := s.Symtab.Intern(x)
	s.build()
	return sym
}

func (s *symtab) InternBytes(v []byte) ion.Symbol {
	sym := s.Symtab.InternBytes(v)
	s.build()
	return sym
}

func (s *symtab) Reset() {
	s.Symtab.Reset()
	s.free()
}

func (s *symtab) free() {
	s.curpage.drop()
	for i := range s.opages {
		s.opages[i].drop()
	}
	s.opages = s.opages[:0]
	s.symrefs = s.symrefs[:0]
	s.epoch++
}

func (s *symtab) resident() bool {
	return s.curpage.mem != nil
}

// drop auxilliary pages and reset
// the write offset into the current page
func (s *symtab) resetNoFree() {
	// keep s.curpage.mem if it is set
	s.curpage.off = 0
	s.curpage.maxid = 0
	for i := range s.opages {
		s.opages[i].drop()
	}
	s.opages = s.opages[:0]
	s.symrefs = s.symrefs[:0]
	s.epoch++
}

// tinymalloc allocates a small amount of memory
// directly from the symbol table page(s)
func (s *symtab) tinymalloc(n int) []byte {
	if s.curpage.mem == nil {
		s.curpage.mem = Malloc()
	}
	if n > len(s.curpage.mem) {
		panic("tinymalloc > page size")
	}
	// don't try to allocate from the current page;
	// just allocate a fresh one and return it:
	if n == PageSize {
		out := Malloc()
		s.opages = append(s.opages, pageref{
			mem: out,
			off: len(out), // entirely used
		})
		return out
	}
	if len(s.curpage.mem)-s.curpage.off < (n + 4) {
		s.opages = append(s.opages, s.curpage)
		s.curpage = pageref{}
		s.curpage.mem = Malloc()
	}
	out := s.curpage.mem[s.curpage.off:]
	out = out[:n:n]
	s.curpage.off += n
	return out
}

func (s *symtab) push(x string) {
	if s.curpage.mem == nil {
		s.curpage.mem = Malloc()
	}
	if len(x) > len(s.curpage.mem) {
		panic("len(str) > page size")
	}
	if len(s.curpage.mem)-s.curpage.off < (len(x) + 4) {
		s.opages = append(s.opages, s.curpage)
		s.curpage = pageref{}
		s.curpage.mem = Malloc()
	}
	pos, ok := vmdispl(s.curpage.mem[s.curpage.off:])
	if !ok {
		panic("symtab.curpage not in vmm")
	}
	n := ion.UnsafeWriteTag(s.curpage.mem[s.curpage.off:], ion.StringType, uint(len(x)))
	n += copy(s.curpage.mem[s.curpage.off+n:], x)
	s.curpage.off += n
	s.symrefs = append(s.symrefs, vmref{pos, uint32(n)})
	s.curpage.maxid = len(s.symrefs)
}

func (s *symtab) build() {
	for len(s.symrefs) < s.Symtab.MaxID() {
		s.push(s.Symtab.Get(ion.Symbol(len(s.symrefs))))
	}
}
