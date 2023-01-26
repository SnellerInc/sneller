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
	"fmt"
)

// UnpackTypedStruct walks a structure and expects the
// first field is "type". The value of field is passed
// to settype callback. Subsequent fields are passed
// to setitem callback.
//
// XXX: This method is deprecated. Use
// Struct.UnpackTyped.
func UnpackTypedStruct(st *Symtab, buf []byte, settype func(typename string) error, setfield func(name string, body []byte) error) ([]byte, error) {
	d, rest, err := ReadDatum(st, buf)
	if err != nil {
		return nil, err
	}
	s, err := d.Struct()
	if err != nil {
		return nil, err
	}
	return rest, s.UnpackTyped(settype, func(f Field) error {
		return setfield(f.Label, f.buf)
	})
}

func (s Struct) UnpackTyped(settype func(typ string) error, setfield func(Field) error) error {
	var fields []Field // fields seen before type
	found := false
	err := s.Each(func(f Field) error {
		if f.Label != "type" {
			if found {
				return setfield(f)
			}
			fields = append(fields, f)
			return nil
		}
		if found {
			// shouldn't be possible...
			return fmt.Errorf("duplicate type field")
		}
		typ, err := f.String()
		if err != nil {
			return err
		}
		err = settype(typ)
		if err != nil {
			return err
		}
		found = true
		for i := range fields {
			if err := setfield(fields[i]); err != nil {
				return err
			}
		}
		fields = nil
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("field \"type\" not found")
	}
	return nil
}

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
