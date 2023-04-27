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
