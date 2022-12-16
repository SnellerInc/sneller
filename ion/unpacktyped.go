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
	"errors"
	"fmt"
)

// UnpackTypedStruct walks a structure and expects the
// first field is "type". The value of field is passed
// to settype callback. Subsequent fields are passed
// to setitem callback.
func UnpackTypedStruct(st *Symtab, buf []byte, settype func(typename string) error, setfield func(name string, body []byte) error) ([]byte, error) {
	_, err := UnpackStruct(st, buf, func(name string, body []byte) error {
		if name != "type" {
			// Note: "type" would be usually the first field, but
			//       since fields are ordered by id, and it's
			//       not guaranteed.
			return nil
		}

		sym, _, err := ReadSymbol(body)
		if err != nil {
			return err
		}

		typename, ok := st.Lookup(sym)
		if !ok {
			return fmt.Errorf("symbol %d not found", sym)
		}

		err = settype(typename)
		if err != nil {
			return err
		}

		return errStop
	})

	if err != nil && err != errStop {
		return nil, err
	}

	if err != errStop {
		return nil, fmt.Errorf("field \"type\" not found")
	}

	return UnpackStruct(st, buf, func(name string, body []byte) error {
		if name != "type" {
			err := setfield(name, body)
			if err != nil {
				return fmt.Errorf("decoding field %q: %w", name, err)
			}
		}

		return nil
	})
}

var errStop = errors.New("stop iteration")

// StructParser is an Ion struct decoder.
type StructParser interface {
	// Init is called before parsing start
	Init(st *Symtab)

	// SetField is called for each structure field.
	SetField(name string, body []byte) error

	// Finalize is called after all fields were visited.
	// This method is meant to perform additional validation.
	Finalize() error
}

// TypeResolver is an object that uses the type name to
// select a proper parser for structure.
type TypeResolver interface {
	Resolve(typename string) (StructParser, error)
}

// UnpackTypedStructWithClasses performs exactly the same job as
// UnpackTypedStruct, but uses external classes to perform
// actual operations rather callbacks.
func UnpackTypedStructWithClasses(st *Symtab, buf []byte, resolver TypeResolver) ([]byte, error) {
	var parser StructParser
	var err error
	var rest []byte

	settype := func(typename string) error {
		parser, err = resolver.Resolve(typename)
		if err == nil {
			parser.Init(st)
		} else {
			parser = nil
		}
		return err
	}

	setitem := func(name string, body []byte) error {
		return parser.SetField(name, body)
	}

	rest, err = UnpackTypedStruct(st, buf, settype, setitem)
	var err2 error
	if parser != nil {
		err2 = parser.Finalize()
	}

	if err == nil {
		err = err2
	}

	return rest, err
}
