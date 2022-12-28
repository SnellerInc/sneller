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

package expr

import (
	"reflect"

	"github.com/SnellerInc/sneller/ion"
)

// Copy returns a deep copy of e.
func Copy(e Node) Node {
	v := reflect.New(reflect.TypeOf(e)).Elem()
	copyValue(v, reflect.ValueOf(e))
	return v.Interface().(Node)
}

func isValueType(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Chan, reflect.Slice, reflect.Map:
		return false
	case reflect.Struct:
		n := v.NumField()
		for i := 0; i < n; i++ {
			if !isValueType(v.Field(i)) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// call into.Set(v) with v cloned if it is not a value type
func copyValue(into, v reflect.Value) {
	// ion.Datum knows how to clone itself
	if d, ok := v.Interface().(ion.Datum); ok {
		into.Set(reflect.ValueOf(d.Clone()))
		return
	}
	// Binding has unexported fields, so we
	// need special handling for copying
	if b, ok := v.Interface().(Binding); ok {
		into.Set(reflect.ValueOf(Binding{Expr: Copy(b.Expr), as: b.as, explicit: b.explicit}))
		return
	}
	if isValueType(v) {
		into.Set(v)
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		elem := v.Elem()
		ret := reflect.New(elem.Type())
		copyValue(ret.Elem(), elem)
		into.Set(ret)
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		elem := v.Elem()
		ret := reflect.New(elem.Type())
		copyValue(ret.Elem(), elem)
		into.Set(ret.Elem())
	case reflect.Struct:
		n := v.NumField()
		for i := 0; i < n; i++ {
			copyValue(into.Field(i), v.Field(i))
		}
	case reflect.Slice:
		if v.IsNil() {
			return
		}
		l := v.Len()
		ret := reflect.MakeSlice(v.Type(), l, l)
		for i := 0; i < l; i++ {
			copyValue(ret.Index(i), v.Index(i))
		}
		into.Set(ret)
	default:
		// should have been handled by isValueType
		panic("unexpected Node field")
	}
}
