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
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
)

// Symtab is an ion symbol table
type Symtab struct {
	interned []string // symbol -> string lookup
	aliased  int      // read-only len of interned

	// toindex maps interned symbols to integers
	// but *only* if they are actually part of the symbol table;
	// it is allowed to contain erroneous string->int mappings,
	// so callers need to cross-check results against interned[]
	toindex map[string]int
	memsize int
}

func (s *Symtab) init() {
	s.toindex = maps.Clone(system2id)
}

// Reset resets a symbol table
// so that it no longer contains
// any symbols (except for the ion
// pre-defined symbols).
func (s *Symtab) Reset() {
	// NOTE: we could probably
	// get away with not deleting
	// s.toindex and instead deleting
	// its entries in order to avoid
	// it being re-allocated.
	// Not sure if deleting the entries
	// or re-allocating a new map is faster.
	s.clear()
}

// Truncate truncates the symbol table to the
// number of symbols indicated by max.
// Truncate can be used to restore a Symtab to
// its previous state as indicated by s.MaxID().
// Truncate panics if max is below the number
// of pre-interned "system" symbols (10).
func (s *Symtab) Truncate(max int) {
	max -= len(systemsyms)
	if max < 0 {
		panic("ion.Symtab.Truncate with max < 10")
	}
	tail := s.interned[max:]
	for _, k := range tail {
		s.memsize -= len(k)
	}
	s.interned = s.interned[:max]
}

// Get gets the string associated
// with the given interned symbol,
// or returns the empty string
// when there is no symbol with
// the given association.
func (s *Symtab) Get(x Symbol) string {
	lbl, _ := s.Lookup(x)
	return lbl
}

// Lookup gets the string associated
// with the given interned symbol.
// This returns ("", false) when the
// symbol is not present in the table.
func (s *Symtab) Lookup(x Symbol) (string, bool) {
	if int(x) < len(systemsyms) {
		return systemsyms[x], true
	}
	id := int(x) - len(systemsyms)
	if id < len(s.interned) {
		return s.interned[id], true
	}
	return "", false
}

// MaxID returns the total number of
// interned symbols. Note that ion
// defines ten symbols that are automatically
// interned, so an "empty" symbol table
// has MaxID() of 10.
func (s *Symtab) MaxID() int {
	return len(systemsyms) + len(s.interned)
}

func (s *Symtab) getBytes(buf []byte) (Symbol, bool) {
	if s.toindex == nil {
		i, ok := system2id[string(buf)]
		return Symbol(i), ok
	}
	return s.rawGetBytes(buf)
}

func (s *Symtab) rawGet(x string) (Symbol, bool) {
	i, ok := s.toindex[x]
	if ok && i >= len(systemsyms) {
		n := i - len(systemsyms)
		if n >= len(s.interned) {
			return 0, false
		}
		ok = s.interned[n] == x
	}
	return Symbol(i), ok
}

func (s *Symtab) rawGetBytes(buf []byte) (Symbol, bool) {
	i, ok := s.toindex[string(buf)]
	if ok && i >= len(systemsyms) {
		n := i - len(systemsyms)
		if n >= len(s.interned) {
			return 0, false
		}
		ok = s.interned[n] == string(buf)
	}
	return Symbol(i), ok
}

// InternBytes is identical to Intern,
// except that it accepts a []byte instead of
// a string as an argument.
func (s *Symtab) InternBytes(buf []byte) Symbol {
	if s.toindex == nil {
		s.init()
	}
	if sym, ok := s.rawGetBytes(buf); ok {
		return sym
	}
	id := len(s.interned) + len(systemsyms)
	s.toindex[string(buf)] = id
	s.append(string(buf))
	s.memsize += len(buf)
	return Symbol(id)
}

// Intern interns the given string
// if it is not already interned
// and returns the associated Symbol
func (s *Symtab) Intern(x string) Symbol {
	if s.toindex == nil {
		s.init()
	}
	if sym, ok := s.rawGet(x); ok {
		return sym
	}
	id := len(s.interned) + len(systemsyms)
	s.toindex[x] = id
	s.append(x)
	s.memsize += len(x)
	return Symbol(id)
}

// Symbolize returns the symbol associated
// with the string 'x' in the symbol table,
// or (0, false) if the string has not been
// interned.
func (s *Symtab) Symbolize(x string) (Symbol, bool) {
	if s.toindex == nil {
		i, ok := system2id[x]
		return Symbol(i), ok
	}
	return s.rawGet(x)
}

// SymbolizeBytes works identically to Symbolize,
// except that it accepts a []byte.
func (s *Symtab) SymbolizeBytes(x []byte) (Symbol, bool) {
	if s.toindex == nil {
		i, ok := system2id[string(x)]
		return Symbol(i), ok
	}
	return s.rawGetBytes(x)
}

// Equal checks if two symtabs are equal.
func (s *Symtab) Equal(o *Symtab) bool {
	return slices.Equal(s.interned, o.interned)
}

// CloneInto performs a deep copy
// of s into o. CloneInto takes care to
// use some of the existing storage in o
// in order to reduce the copying overhead.
func (s *Symtab) CloneInto(o *Symtab) {
	o.interned = s.alias()
	o.aliased = len(o.interned)
	if o.toindex == nil {
		o.toindex = make(map[string]int, len(o.interned)+len(systemsyms))
	}
	o.memsize = s.memsize
	if s.toindex != nil {
		for k, v := range s.toindex {
			if v < o.MaxID() {
				o.toindex[k] = v
			}
		}
	}
}

func (s *Symtab) append(v string) {
	if i := len(s.interned); i < cap(s.interned) {
		s.interned = s.interned[:i+1]
		s.set(i, v)
	} else {
		s.interned = append(s.interned, v)
		s.aliased = 0
	}
}

func (s *Symtab) set(i int, v string) {
	if s.interned[i] != v {
		if i < s.aliased {
			s.interned = slices.Clone(s.interned)
			s.aliased = 0
		}
		s.interned[i] = v
	}
}

// Merge adds new symbols from symtab `o` providing that
// the common symbols of the both symtabs are the same.
//
// Returns whether merge was OK. If it was, return if
// new symbols were added.
func (s *Symtab) Merge(o *Symtab) (modified bool, ok bool) {
	n1 := len(s.interned)
	n2 := len(o.interned)

	k := n1
	if n2 < n1 {
		k = n2
	}

	// check if prefixes are equal
	if !slices.Equal(s.interned[:k], o.interned[:k]) {
		return false, false
	}

	// copy new symbols
	for i := n1; i < n2; i++ {
		s.append(o.interned[i])
	}

	return (n1 < n2), true
}

func (s *Symtab) String() string {
	var b strings.Builder

	b.WriteString("{")
	for i, s := range s.interned {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString(s)
		b.WriteString(": ")
		b.WriteString(strconv.Itoa(i))
	}
	b.WriteString("}")

	return b.String()
}

// these symbols are predefined
var systemsyms = []string{
	"$0",
	"$ion",
	"$ion_1_0",
	"$ion_symbol_table",
	"name",
	"version",
	"imports",
	"symbols",
	"max_id",
	"$ion_shared_symbol_table",
}

const (
	// SystemSymImports is the pre-interned symbol for "imports"
	SystemSymImports Symbol = 6
	// SystemSymSymbols is the pre-interned symbol for "symbols"
	SystemSymSymbols Symbol = 7
	// SystemSymSymbolTable is the pre-interned symbol for "$ion_symbol_table"
	SystemSymSymbolTable Symbol = 3
)

var system2id map[string]int

func init() {
	system2id = make(map[string]int, len(systemsyms))
	for i := range systemsyms {
		system2id[systemsyms[i]] = i
	}
}

// MinimumID returns the lowest ID
// that a string could be symbolized as.
//
// System symbols have IDs less than 10;
// all other symbols have and ID of at least 10.
func MinimumID(str string) int {
	i, ok := system2id[str]
	if !ok {
		return len(systemsyms)
	}
	return i
}

// IsBVM returns whether or not
// the next 4 bytes of the message
// are a 4-byte ion BVM marker.
func IsBVM(buf []byte) bool {
	if len(buf) < 4 {
		return false
	}
	// BVM begins with 0xe0 and ends with 0xea
	word := binary.LittleEndian.Uint32(buf)
	return word&0xff0000ff == 0xea0000e0
}

const clearToItems = 1000

func (s *Symtab) clear() {
	// don't let the lookup cache remain enormous
	// if it is already quite large; remove some entries
	// so that the strings can be GC'd
	if s.toindex != nil && len(s.interned) > clearToItems {
		tail := s.interned[clearToItems:]
		for i := range tail {
			delete(s.toindex, tail[i])
		}
	}
	s.interned = s.interned[:0]
	s.memsize = 0
}

func start(x []byte) []byte {
	if len(x) > 8 {
		x = x[:8]
	}
	return x
}

// Unmarshal unmarshals a symbol
// table from 'src' into 's'.
// If 'src' begins with a BVM
// (see IsBVM), then any contents
// of the symbol table will be cleared
// before interning the new symbol values.
// Otherwise, the new symbols will be
// interned with IDs above the presently-interned
// symbols.
//
// BUGS: Support for ion "shared" symbol tables
// is not yet implemented.
func (s *Symtab) Unmarshal(src []byte) ([]byte, error) {
	if IsBVM(src) {
		s.clear()
		src = src[4:]
	}
	if len(src) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if t := TypeOf(src); t != AnnotationType {
		return nil, bad(t, AnnotationType, "Symtab.Unmarshal")
	}
	if len(src) < SizeOf(src) {
		return nil, fmt.Errorf("Symtab.Unmarshal: len(src)=%d, SizeOf(src)=%d", len(src), SizeOf(src))
	}
	body, rest := Contents(src)
	if body == nil {
		return nil, fmt.Errorf("Symtab.Unmarshal: Contents(%x)==nil", start(src))
	}
	// skip annotation_length field
	fields, body, err := ReadLabel(body)
	if err != nil {
		return nil, err
	}
	if fields != 1 {
		return nil, fmt.Errorf("%d annotations?", fields)
	}
	// read struct field
	sym, body, err := ReadLabel(body)
	if err != nil {
		return nil, err
	}
	if sym != SystemSymSymbolTable {
		// FIXME: add support for shared symbol tables
		return nil, fmt.Errorf("first annotation field not $ion_symbol_table")
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("reading $ion_symbol_table: %w", io.ErrUnexpectedEOF)
	}
	if t := TypeOf(body); t != StructType {
		return nil, bad(t, StructType, "Symtab.Unmarshal (in annotation)")
	}
	if s.toindex == nil {
		s.init()
	}
	body, _ = Contents(body)
	if body == nil {
		return nil, fmt.Errorf("Symtab.Unmarshal: Contents(structure(%x))==nil", start(body))
	}
	// walk through the body fields
	// and look for 'symbols: [...]'
	// from which we can intern strings
	for len(body) > 0 {
		sym, body, err = ReadLabel(body)
		if err != nil {
			return nil, fmt.Errorf("Symtab.Unmarshal (reading fields): %w", err)
		}
		switch sym {
		case SystemSymSymbols:
			var lst []byte
			lst, body = Contents(body)
			if lst == nil {
				return nil, fmt.Errorf("Symtab.Unmarshal: Contents(%x)==nil", start(body))
			}
			// an optimization: allocate the string memory *once*
			// and then produce the individual symbol strings
			// as sub-strings of the full string list
			fullstr := string(lst)
			anchor := cap(lst)
			for len(lst) > 0 {
				var strseg []byte
				strseg, lst, err = ReadStringShared(lst)
				if err != nil {
					return nil, fmt.Errorf("Symtab.Unmarshal (in 'symbols:') %w", err)
				}
				end := anchor - cap(lst)
				start := end - len(strseg)
				str := fullstr[start:end]
				// XXX what is the correct behavior here
				// when a string is interned more than
				// once?
				s.append(str)
				s.memsize += len(str)
				s.toindex[str] = len(s.interned) - 1 + len(systemsyms)
			}
		default:
			// skip unknown field
			s := SizeOf(body)
			if s < 0 || len(body) < s {
				return nil, fmt.Errorf("Symtab.Unmarshal: skipping field len=%d; len(body)=%d", s, len(body))
			}
			body = body[s:]
		}
	}

	return rest, nil
}

// MarshalPart writes a symbol table to dst
// with all the symbols starting at starting.
// If there are no symbols above starting, then
// MarshalPart does not write any data.
//
// Callers can use a previous result of
// s.MaxID plus MarshalPart to write incremental
// changes to symbol tables to an ion stream.
func (s *Symtab) MarshalPart(dst *Buffer, starting Symbol) {
	s.marshal(dst, starting, false)
}

// Marshal marshals the Symtab into 'dst'
// optionally with a BVM prefix.
//
// If withBVM is false and the symbol table
// is empty, then no data is written to dst.
func (s *Symtab) Marshal(dst *Buffer, withBVM bool) {
	s.marshal(dst, 0, withBVM)
}

func (s *Symtab) marshal(dst *Buffer, starting Symbol, withBVM bool) {
	if withBVM {
		dst.buf = append(dst.buf, 0xe0, 0x01, 0x00, 0xea)
	}
	count := 0
	if int(starting) > len(systemsyms) {
		count = int(starting) - len(systemsyms)
		if count > len(s.interned) {
			count = len(s.interned)
		}
	}
	if count == 0 && !withBVM {
		// no new data; append nothing
		return
	}
	interned := s.interned[count:]
	dst.BeginAnnotation(1)
	// $ion_symbol_table: { symbols: [ ... ] }
	dst.BeginField(SystemSymSymbolTable)
	dst.BeginStruct(-1)
	if !withBVM {
		dst.BeginField(SystemSymImports)
		dst.WriteSymbol(SystemSymSymbolTable)
	}
	dst.BeginField(SystemSymSymbols)
	dst.BeginList(-1)
	for i := range interned {
		dst.WriteString(interned[i])
	}
	dst.EndList()
	dst.EndStruct()
	dst.EndAnnotation()
}

// Contains returns true if s is a superset
// of the symbols within inner, and all of
// the symbols in inner have the same symbol
// ID in s.
//
// If x.Contains(y), then x is a semantically
// equivalent substitute for y.
func (s *Symtab) Contains(inner *Symtab) bool {
	return s.contains(inner.interned)
}

func (s *Symtab) contains(in []string) bool {
	return stcontains(s.interned, in)
}

// stcontains returns whether s is a superset of in.
func stcontains(s, in []string) bool {
	return len(in) == 0 || len(in) <= len(s) &&
		(&in[0] == &s[0] || slices.Equal(s[:len(in)], in))
}

// alias returns a reference to the current symbol
// table and marks the symbol table as aliased so it
// is not overwritten when resetting or cloning.
func (s *Symtab) alias() []string {
	n := len(s.interned)
	if n > s.aliased {
		s.aliased = n
	}
	return s.interned[:n:n]
}
