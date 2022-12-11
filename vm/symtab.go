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

	// memory source for symbol table + small allocs
	slab slab
	// symrefs[id] produces a boxed string
	// representing the symbol id
	symrefs []vmref

	// symrefs[:snapped] were in the previous snapshot call
	snapped int

	// epoch keeps track of how many times
	// this symtab has been reset;
	// allocations are only valid across
	// an individual epoch
	epoch int
}

func (s *symtab) snapshot() {
	x := &s.slab
	x.snapshot()
	s.snapped = len(s.symrefs)
}

func (s *symtab) rewind() {
	if s.snapped > 0 {
		s.Symtab.Truncate(s.snapped)
	} else if s.Symtab.MaxID() > 10 {
		s.Symtab.Reset()
	}
	s.slab.rewind()
	s.symrefs = s.symrefs[:s.snapped]
	s.snapped = 0
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
		s.buildFrom(src)
		if len(s.symrefs) != s.Symtab.MaxID() {
			panic("vm.symtab.Unmarshal: bad symbol bookkeeping")
		}
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
	s.slab.reset()
	s.symrefs = s.symrefs[:0]
	s.snapped = 0
	s.epoch++
}

func (s *symtab) resident() bool {
	return len(s.slab.pages) > 0
}

// drop auxilliary pages and reset
// the write offset into the current page
func (s *symtab) resetNoFree() {
	s.slab.resetNoFree()
	s.symrefs = s.symrefs[:0]
	s.epoch++
}

func (s *symtab) push(x string) {
	// compute needed size:
	need := len(x) + 1
	if len(x) >= 14 {
		need += ion.Uvsize(uint(len(x)))
	}

	mem := s.slab.malloc(need)
	pos, ok := vmdispl(mem)
	if !ok {
		panic("symtab.curpage not in vmm")
	}
	n := ion.UnsafeWriteTag(mem, ion.StringType, uint(len(x)))
	n += copy(mem[n:], x)
	if n != need {
		println("wrote", n, "wanted", need, "string-length", len(x))
		panic("bad symbol size bookkeeping")
	}
	s.symrefs = append(s.symrefs, vmref{pos, uint32(n)})
}

func (s *symtab) build() {
	for len(s.symrefs) < s.Symtab.MaxID() {
		s.push(s.Symtab.Get(ion.Symbol(len(s.symrefs))))
	}
}

// add a sequence of ion-encoded strings as symbols
// to s.vmrefs by way of copying the data to vm memory
// and then producing the appropriate descriptors
func (s *symtab) addsyms(raw []byte) {
	symbols := s.slab.malloc(len(raw))
	copy(symbols, raw)
	for len(symbols) > 0 {
		pos, ok := vmdispl(symbols)
		if !ok {
			panic("symbols not in vmm?")
		}
		size := ion.SizeOf(symbols)
		s.symrefs = append(s.symrefs, vmref{pos, uint32(size)})
		symbols = symbols[size:]
	}
}

// systemsyms is all 10 "system symbols"
// pre-encoded so that we can copy them
// into vm memory quickly
var systemsyms []byte

// encode systemsyms
func init() {
	var buf ion.Buffer
	var empty ion.Symtab
	for i := 0; i < 10; i++ {
		buf.WriteString(empty.Get(ion.Symbol(i)))
	}
	systemsyms = buf.Bytes()
}

// see ion.Symtab.Unmarshal
// this implementation assumes the caller
// has already successfully decoded src
// at least once, so it just panics on errors
func (s *symtab) buildFrom(src []byte) {
	if ion.IsBVM(src) {
		src = src[4:]
		s.addsyms(systemsyms)
	}
	var err error
	var sym ion.Symbol
	src, _ = ion.Contents(src) // unwrap annotation
	if len(src) == 0 {
		panic("vm.symtab.buildFrom: empty annotation")
	}
	_, src, err = ion.ReadLabel(src) // skip # fields
	if err != nil {
		panic(err)
	}
	sym, src, err = ion.ReadLabel(src)
	if err != nil {
		panic(err)
	}
	if sym != ion.SystemSymSymbolTable {
		panic("unexpected $ion_symbol_table symbol")
	}
	src, _ = ion.Contents(src) // unwrap symbol table structure
	for len(src) > 0 {
		sym, src, err = ion.ReadLabel(src)
		if err != nil {
			panic(err)
		}
		if sym != ion.SystemSymSymbols {
			src = src[ion.SizeOf(src):]
			continue
		}
		// unwrap symbols: [ ... ]
		// so that we're pointing to the
		// list of string values
		symlist, _ := ion.Contents(src)
		s.addsyms(symlist)
		return
	}
	panic("didn't find symbols: field")
}
