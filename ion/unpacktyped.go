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

// UnpackTyped iterates the fields in a struct
// to find a string field named "type" which is
// passed to fn to resolve to a concrete type.
// The other fields are passed to SetField on
// the returned object.
//
// fn should return true to indicate that the
// type was resolved, and false to indicate that
// the type is not supported.
//
// If d is not a struct or the "type" field is
// not present, this returns an error.
func UnpackTyped[T FieldSetter](d Datum, fn func(typ string) (T, bool)) (T, error) {
	out, err := unpackTyped(d, func(typ string) (FieldSetter, bool) {
		return fn(typ)
	})
	if err != nil {
		var empty T
		return empty, err
	}
	return out.(T), nil
}

func unpackTyped(d Datum, fn func(typ string) (FieldSetter, bool)) (FieldSetter, error) {
	var out FieldSetter
	var fields []Field // fields seen before type
	found := false
	err := d.UnpackStruct(func(f Field) error {
		if f.Label != "type" {
			if found {
				return out.SetField(f)
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
		out, found = fn(typ)
		if !found {
			return fmt.Errorf("unrecognized type %q", typ)
		}
		for i := range fields {
			if err := out.SetField(fields[i]); err != nil {
				return err
			}
		}
		fields = nil
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("field \"type\" not found")
	}
	return out, nil
}

// FieldSetter is an object which can accept
// fields from an ion struct.
type FieldSetter interface {
	// SetField is called for each structure field.
	SetField(Field) error
}
