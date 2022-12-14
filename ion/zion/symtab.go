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

package zion

import (
	"fmt"

	"github.com/SnellerInc/sneller/ion"
)

type component struct {
	name   string
	symbol ion.Symbol
}

// this is a stripped-down version
// of ion.Symtab that just tracks
// the symbols we care about; it unmarshal
// data *significantly* faster than the
// standard symbol table because it does not
// allocate any strings while unmarshaling
type symtab struct {
	components []component
	selected   []ion.Symbol
	resolved   int
	nextID     int
}

func (s *symtab) reset() {
	for i := range s.components {
		s.components[i].symbol = ^ion.Symbol(0)
	}
	s.selected = s.selected[:0]
	s.nextID = 10
	s.resolved = 0
}

// implements zll.Symtab; isn't used in practice
// since we use zll.Buckets.SelectSymbols()
func (s *symtab) Symbolize(x string) (ion.Symbol, bool) {
	for i := range s.components {
		if s.components[i].name == x {
			return s.components[i].symbol, s.components[i].symbol != ^ion.Symbol(0)
		}
	}
	return 0, false
}

// this is an optimized version of ion.Symtab.Unmarshal
// that performs significantly fewer allocations
func (s *symtab) Unmarshal(x []byte) ([]byte, error) {
	if ion.IsBVM(x) {
		x = x[4:]
		s.reset()
	}
	sym, body, rest, err := ion.ReadAnnotation(x)
	if err != nil {
		return nil, err
	}
	if sym != 3 {
		return nil, fmt.Errorf("got symbol %d for $ion_symbol_table?", sym)
	}
	if ion.TypeOf(body) != ion.StructType {
		return nil, fmt.Errorf("type %s not appropriate for $ion_symbol_table", ion.TypeOf(body))
	}
	body, _ = ion.Contents(body)
	for len(body) > 0 {
		sym, body, err = ion.ReadLabel(body)
		if err != nil {
			return nil, fmt.Errorf("Symtab.Unmarshal (reading fields): %w", err)
		}
		switch sym {
		case 7: // "symbols:"
			var lst []byte
			lst, body = ion.Contents(body)
			if lst == nil {
				return nil, fmt.Errorf("zion.Decoder: Symtab.Unmarshal: Contents(%x)==nil", body)
			}
			for len(lst) > 0 {
				var str []byte
				str, lst, err = ion.ReadStringShared(lst)
				if err != nil {
					return nil, fmt.Errorf("Symtab.Unmarshal (in 'symbols:') %w", err)
				}
				if s.resolved < len(s.components) {
					for i := range s.components {
						if s.components[i].symbol != ^ion.Symbol(0) ||
							s.components[i].name != string(str) {
							continue
						}
						s.selected = append(s.selected, ion.Symbol(s.nextID))
						s.components[i].symbol = ion.Symbol(s.nextID)
						s.resolved++
						break
					}
				}
				s.nextID++
			}
		default:
			// skip unknown field
			s := ion.SizeOf(body)
			if s < 0 || len(body) < s {
				return nil, fmt.Errorf("Symtab.Unmarshal: skipping field len=%d; len(body)=%d", s, len(body))
			}
			body = body[s:]
		}
	}

	return rest, nil
}
