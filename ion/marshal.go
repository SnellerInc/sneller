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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/date"
)

var structEncoders sync.Map

func init() {
	structEncoders.Store(reflect.TypeOf(time.Time{}), encodefn(func(st *Symtab, dst *Buffer, v reflect.Value) {
		dst.WriteTime(date.FromTime(v.Interface().(time.Time)))
	}))
	structEncoders.Store(reflect.TypeOf(date.Time{}), encodefn(func(st *Symtab, dst *Buffer, v reflect.Value) {
		dst.WriteTime(v.Interface().(date.Time))
	}))
	structEncoders.Store(reflect.TypeOf(Datum{}), encodefn(func(st *Symtab, dst *Buffer, v reflect.Value) {
		d := v.Interface().(Datum)
		if d.IsEmpty() {
			dst.WriteNull()
		} else {
			d.Encode(dst, st)
		}
	}))
}

type encodefn func(*Symtab, *Buffer, reflect.Value)

func compileEncoder(t reflect.Type) (encodefn, bool) {
	// in order to break dependency chains for (mutually-)recursive types,
	// force any concurrent lookups to delay compilation until eval time
	slow := func(st *Symtab, dst *Buffer, v reflect.Value) {
		fn, ok := encoderFunc(v.Type())
		if !ok {
			panic("ion.compileEncoder: failed to compile structure?")
		}
		fn(st, dst, v)
	}
	f, ok := structEncoders.LoadOrStore(t, encodefn(nil))
	if ok {
		fn := f.(encodefn)
		if fn != nil {
			return fn, true
		}
		return slow, true
	}
	type fieldEnc struct {
		index     int
		name      string
		fn        encodefn
		omitempty bool
	}

	var encs []fieldEnc
	fields := reflect.VisibleFields(t)
	for i := range fields {
		if fields[i].PkgPath != "" || len(fields[i].Index) != 1 {
			continue // unexported or promoted embedded struct field
		}
		name := fields[i].Name
		typ := fields[i].Type
		omitempty := false
		if val, ok := fields[i].Tag.Lookup("ion"); ok {
			var rest string
			name, rest, ok = strings.Cut(val, ",")
			if ok && rest == "omitempty" {
				omitempty = true
			}
		}
		if name == "-" {
			continue // explicitly ignored
		}
		efn, ok := encoderFunc(typ)
		if !ok {
			continue
		}
		encs = append(encs, fieldEnc{
			index:     fields[i].Index[0],
			name:      name,
			fn:        efn,
			omitempty: omitempty,
		})
	}
	self := func(st *Symtab, dst *Buffer, src reflect.Value) {
		dst.BeginStruct(-1)
		for i := range encs {
			val := src.Field(encs[i].index)
			if encs[i].omitempty && val.IsZero() {
				continue
			}
			dst.BeginField(st.Intern(encs[i].name))
			encs[i].fn(st, dst, val)
		}
		dst.EndStruct()
	}
	structEncoders.Store(t, encodefn(self))
	return self, true
}

func encodeList(st *Symtab, dst *Buffer, inner encodefn, src reflect.Value) {
	l := src.Len()
	dst.BeginList(-1)
	for i := 0; i < l; i++ {
		inner(st, dst, src.Index(i))
	}
	dst.EndList()
}

func encoderFunc(t reflect.Type) (encodefn, bool) {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.WriteInt(src.Int())
		}, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.WriteUint(src.Uint())
		}, true
	case reflect.Float32, reflect.Float64:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.WriteCanonicalFloat(src.Float())
		}, true
	case reflect.Slice:
		elem := t.Elem()
		if elem.Kind() == reflect.Uint8 {
			// encode []byte
			return func(st *Symtab, dst *Buffer, src reflect.Value) {
				dst.WriteBlob(src.Bytes())
			}, true
		}
		inner, ok := encoderFunc(elem)
		if !ok {
			return nil, false
		}
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			encodeList(st, dst, inner, src)
		}, true
	case reflect.String:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.WriteString(src.String())
		}, true
	case reflect.Map:
		kt := t.Key()
		if kt.Kind() != reflect.String {
			return nil, false
		}
		eval, ok := encoderFunc(t.Elem())
		if !ok {
			return nil, false
		}
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.BeginStruct(-1)
			iter := src.MapRange()
			for iter.Next() {
				dst.BeginField(st.Intern(iter.Key().String()))
				eval(st, dst, iter.Value())
			}
			dst.EndStruct()
		}, true
	case reflect.Struct:
		return compileEncoder(t)
	case reflect.Bool:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			dst.WriteBool(src.Bool())
		}, true
	case reflect.Pointer:
		body, ok := encoderFunc(t.Elem())
		if !ok {
			return nil, false
		}
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			if src.IsNil() {
				dst.WriteNull()
			} else {
				body(st, dst, src.Elem())
			}
		}, true
	case reflect.Interface:
		return func(st *Symtab, dst *Buffer, src reflect.Value) {
			if src.IsNil() {
				dst.WriteNull()
				return
			}
			val := src.Elem()
			fn, ok := encoderFunc(val.Type())
			if !ok {
				dst.WriteNull()
				return
			}
			fn(st, dst, val)
		}, true
	default:
		return nil, false
	}
}

// Marshal encodes src into dst, updating the symbol
// table as necessary for new symbols that are introduced
// as part of encoding.
func Marshal(st *Symtab, dst *Buffer, src any) error {
	v := reflect.ValueOf(src)
	t := v.Type()
	enc, ok := encoderFunc(t)
	if !ok {
		return fmt.Errorf("ion.Marshal: cannot marshal type %s", t)
	}
	enc(st, dst, v)
	return nil
}
