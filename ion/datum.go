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
	"bytes"
	"fmt"
	"io"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
)

// Datum represents an Ion datum
//
// A Datum should be one of
//   Float, Int, Uint, Struct, List, Bool,
//   BigInt, Timestamp, Annotation, ...
//
type Datum interface {
	Encode(dst *Buffer, st *Symtab)
	Type() Type

	// used for RelaxedEqual
	equal(Datum) bool
}

var (
	// all of these types must be datums
	_ Datum = Float(0)
	_ Datum = Int(0)
	_ Datum = Uint(0)
	_ Datum = &Struct{}
	_ Datum = &List{}
	_ Datum = Bool(true)
	_ Datum = Timestamp{}
	_ Datum = &Annotation{}
	_ Datum = Blob(nil)
	_ Datum = Interned("")
)

// Float is an ion float datum
type Float float64

func (f Float) Encode(dst *Buffer, st *Symtab) {
	dst.WriteFloat64(float64(f))
}

func (f Float) Type() Type { return FloatType }

func (f Float) equal(x Datum) bool {
	if f2, ok := x.(Float); ok {
		return f2 == f
	}
	if i, ok := x.(Int); ok {
		return float64(int64(f)) == float64(f) && int64(f) == int64(i)
	}
	if u, ok := x.(Uint); ok {
		return float64(uint64(f)) == float64(f) && uint64(f) == uint64(u)
	}
	return false
}

// UntypedNull is an ion "untyped null" datum
type UntypedNull struct{}

func (u UntypedNull) Type() Type                     { return NullType }
func (u UntypedNull) Encode(dst *Buffer, st *Symtab) { dst.WriteNull() }

func (u UntypedNull) equal(x Datum) bool {
	_, ok := x.(UntypedNull)
	return ok
}

// Int is an ion integer datum (signed or unsigned)
type Int int64

func (i Int) Type() Type {
	if i >= 0 {
		return UintType
	}
	return IntType
}

func (i Int) equal(x Datum) bool {
	switch x := x.(type) {
	case Int:
		return x == i
	case Uint:
		return i >= 0 && Uint(i) == x
	case Float:
		return float64(int64(x)) == float64(x) && int64(x) == int64(i)
	default:
		return false
	}
}

func (i Int) Encode(dst *Buffer, st *Symtab) {
	dst.WriteInt(int64(i))
}

// Uint is an ion integer datum (always unsigned)
type Uint uint64

func (u Uint) Type() Type                     { return UintType }
func (u Uint) Encode(dst *Buffer, st *Symtab) { dst.WriteUint(uint64(u)) }

func (u Uint) equal(x Datum) bool {
	switch x := x.(type) {
	case Uint:
		return u == x
	case Int:
		return x >= 0 && Uint(x) == u
	case Float:
		return float64(uint64(x)) == float64(x) && uint64(x) == uint64(u)
	default:
		return false
	}
}

// Field is a structure field in a Struct or Annotation datum
type Field struct {
	Label string
	Value Datum
	Sym   Symbol // symbol, if assigned
}

type field struct {
	sym Symbol
	val Datum
}

// Struct is an ion structure datum
type Struct struct {
	len int
	st  []string
	buf []byte
}

func NewStruct(st *Symtab, f []Field) *Struct {
	s := new(Struct)
	s.SetFields(st, f)
	return s
}

func (s *Struct) SetFields(st *Symtab, f []Field) {
	if st == nil {
		st = &Symtab{}
	}
	var fields []field
	for i := range f {
		fields = append(fields, field{
			sym: st.Intern(f[i].Label),
			val: f[i].Value,
		})
	}
	slices.SortFunc(fields, func(x, y field) bool {
		return x.sym < y.sym
	})
	var dst Buffer
	dst.BeginStruct(-1)
	for i := range fields {
		dst.BeginField(fields[i].sym)
		fields[i].val.Encode(&dst, st)
	}
	dst.EndStruct()
	s.len = len(f)
	s.st = st.alias()
	s.buf = dst.Bytes()
}

func (s *Struct) Type() Type { return StructType }

func (s *Struct) Encode(dst *Buffer, st *Symtab) {
	if len(s.buf) == 0 {
		dst.BeginStruct(-1)
		dst.EndStruct()
		return
	}
	// fast path: we can avoid resym
	if st.contains(s.st) {
		dst.UnsafeAppend(s.buf)
		return
	}
	var fields []field
	s.Each(func(f Field) bool {
		fields = append(fields, field{
			sym: st.Intern(f.Label),
			val: f.Value,
		})
		return true
	})
	slices.SortFunc(fields, func(x, y field) bool {
		return x.sym < y.sym
	})
	dst.BeginStruct(-1)
	for i := range fields {
		dst.BeginField(fields[i].sym)
		fields[i].val.Encode(dst, st)
	}
	dst.EndStruct()
}

func (s *Struct) equal(x Datum) bool {
	s2, ok := x.(*Struct)
	if !ok {
		return false
	}
	if s == s2 {
		return true
	}
	if bytes.Equal(s.buf, s2.buf) && stoverlap(s.st, s2.st) {
		return true
	}
	// TODO: optimize this
	f1 := s.Fields(nil)
	f2 := s2.Fields(nil)
	if len(f1) != len(f2) {
		return false
	}
	for i := range f1 {
		f1[i].Sym = 0
		f2[i].Sym = 0
	}
	slices.SortFunc(f1, func(x, y Field) bool {
		return x.Label < y.Label
	})
	slices.SortFunc(f2, func(x, y Field) bool {
		return x.Label < y.Label
	})
	for i := range f1 {
		if f1[i].Label != f2[i].Label {
			return false
		}
		if !Equal(f1[i].Value, f2[i].Value) {
			return false
		}
	}
	return true
}

func (s *Struct) Len() int { return s.len }

// Each calls fn for each field in the struct. If fn
// returns false, Each returns early. Each may return
// a non-nil error if the original struct encoding
// was malformed.
func (s *Struct) Each(fn func(Field) bool) error {
	if len(s.buf) == 0 {
		return nil
	}
	if TypeOf(s.buf) != StructType {
		return fmt.Errorf("expected a struct; found ion type %s", TypeOf(s.buf))
	}
	body, _ := Contents(s.buf)
	if body == nil {
		return errInvalidIon
	}
	st := s.symtab()
	for len(body) > 0 {
		var sym Symbol
		var err error
		sym, body, err = ReadLabel(body)
		if err != nil {
			return err
		}
		name, ok := st.Lookup(sym)
		if !ok {
			return fmt.Errorf("symbol %d not in symbol table", sym)
		}
		next := SizeOf(body)
		if next <= 0 || next > len(body) {
			return fmt.Errorf("next object size %d exceeds buffer size %d", next, len(body))
		}
		val, _, err := ReadDatum(&st, body[:next])
		if err != nil {
			return err
		}
		f := Field{
			Label: name,
			Value: val,
			Sym:   sym,
		}
		if !fn(f) {
			break
		}
		body = body[next:]
	}
	return nil
}

// Fields appends fields to the given slice and returns
// the appended slice.
func (s *Struct) Fields(fields []Field) []Field {
	fields = slices.Grow(fields, s.len)
	s.Each(func(f Field) bool {
		fields = append(fields, f)
		return true
	})
	return fields
}

func (s *Struct) Field(x Symbol) (Field, bool) {
	var field Field
	var ok bool
	s.Each(func(f Field) bool {
		if f.Sym == x {
			field, ok = f, true
			return false
		}
		return true
	})
	return field, ok
}

func (s *Struct) FieldByName(name string) (Field, bool) {
	var field Field
	var ok bool
	s.Each(func(f Field) bool {
		if f.Label == name {
			field, ok = f, true
			return false
		}
		return true
	})
	return field, ok
}

func (s *Struct) symtab() Symtab {
	return Symtab{interned: s.st}
}

// List is an ion list datum
type List struct {
	len int
	st  []string
	buf []byte
}

func NewList(st *Symtab, items []Datum) *List {
	lst := new(List)
	lst.SetItems(st, items)
	return lst
}

func (l *List) SetItems(st *Symtab, items []Datum) {
	if st == nil {
		st = &Symtab{}
	}
	var dst Buffer
	dst.BeginList(-1)
	for i := range items {
		items[i].Encode(&dst, st)
	}
	dst.EndList()
	l.len = len(items)
	l.st = st.alias()
	l.buf = dst.Bytes()
}

func (l *List) Type() Type { return ListType }

func (l *List) Encode(dst *Buffer, st *Symtab) {
	if len(l.buf) == 0 {
		dst.BeginList(-1)
		dst.EndList()
		return
	}
	// fast path: we can avoid resym
	if st.contains(l.st) {
		dst.UnsafeAppend(l.buf)
		return
	}
	dst.BeginList(-1)
	l.Each(func(d Datum) bool {
		d.Encode(dst, st)
		return true
	})
	dst.EndList()
}

func (l *List) Len() int { return l.len }

// Each iterates over each datum in the
// list and calls fn on each datum in order.
// Each returns when it encounters an internal error
// (due to malformed ion) or when fn returns false.
func (l *List) Each(fn func(Datum) bool) error {
	if len(l.buf) == 0 {
		return nil
	}
	if TypeOf(l.buf) != ListType {
		return fmt.Errorf("expected a list; found ion type %s", TypeOf(l.buf))
	}
	body, _ := Contents(l.buf)
	if body == nil {
		return errInvalidIon
	}
	st := l.symtab()
	for len(body) > 0 {
		next := SizeOf(body)
		if next <= 0 || next > len(body) {
			return fmt.Errorf("object size %d exceeds buffer size %d", next, len(body))
		}
		val, _, err := ReadDatum(&st, body[:next])
		if err != nil {
			return err
		}
		if !fn(val) {
			return nil
		}
		body = body[next:]
	}
	return nil
}

func (l *List) Items(items []Datum) []Datum {
	items = slices.Grow(items, l.len)
	l.Each(func(d Datum) bool {
		items = append(items, d)
		return true
	})
	return items
}

func (l *List) symtab() Symtab {
	return Symtab{interned: l.st}
}

func (l *List) equal(x Datum) bool {
	l2, ok := x.(*List)
	if !ok {
		return false
	}
	if l == l2 {
		return true
	}
	if bytes.Equal(l.buf, l2.buf) && stoverlap(l.st, l2.st) {
		return true
	}
	// TODO: optimize this
	i1 := l.Items(nil)
	i2 := l2.Items(nil)
	if len(i1) != len(i2) {
		return false
	}
	for i := range i1 {
		if !Equal(i1[i], i2[i]) {
			return false
		}
	}
	return true
}

// Bool is an ion bool datum
type Bool bool

func (b Bool) Type() Type                     { return BoolType }
func (b Bool) Encode(dst *Buffer, st *Symtab) { dst.WriteBool(bool(b)) }

func (b Bool) equal(x Datum) bool {
	b2, ok := x.(Bool)
	return ok && b2 == b
}

type String string

func (s String) Type() Type { return StringType }

func (s String) Encode(dst *Buffer, st *Symtab) { dst.WriteString(string(s)) }

func (s String) equal(x Datum) bool {
	s2, ok := x.(String)
	if ok {
		return s == s2
	}
	ir, ok := x.(Interned)
	return ok && s == String(ir)
}

type Blob []byte

func (b Blob) Type() Type { return BlobType }

func (b Blob) Encode(dst *Buffer, st *Symtab) { dst.WriteBlob([]byte(b)) }

func (b Blob) equal(x Datum) bool {
	b2, ok := x.(Blob)
	return ok && bytes.Equal(b, b2)
}

// Interned is a Datum that represents
// an interned string (a Symbol).
// Interned is always encoded as an ion symbol,
// but it is represented as a Go string for
// convenience.
type Interned string

func (i Interned) Type() Type { return SymbolType }

func (i Interned) Encode(dst *Buffer, st *Symtab) {
	dst.WriteSymbol(st.Intern(string(i)))
}

func (i Interned) equal(x Datum) bool {
	i2, ok := x.(Interned)
	if ok {
		return i == i2
	}
	str, ok := x.(String)
	return ok && Interned(str) == i
}

// Annotation objects represent
// ion annotation datums.
type Annotation struct {
	Fields []Field
}

func (a *Annotation) Type() Type { return AnnotationType }

func (a *Annotation) Encode(dst *Buffer, st *Symtab) {
	for i := range a.Fields {
		a.Fields[i].Sym = st.Intern(a.Fields[i].Label)
	}
	dst.BeginAnnotation(len(a.Fields))
	for i := range a.Fields {
		dst.BeginField(a.Fields[i].Sym)
		a.Fields[i].Value.Encode(dst, st)
	}
	dst.EndAnnotation()
}

func (a *Annotation) equal(x Datum) bool {
	return false
}

// Timestamp is an ion timestamp datum
type Timestamp date.Time

func (t Timestamp) Type() Type { return TimestampType }

func (t Timestamp) Encode(dst *Buffer, st *Symtab) {
	dst.WriteTime(date.Time(t))
}

func (t Timestamp) equal(x Datum) bool {
	t2, ok := x.(Timestamp)
	return ok && date.Time(t).Equal(date.Time(t2))
}

func decodeNullDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	s := SizeOf(b)
	if s <= 0 || s > len(b) {
		return nil, b, errInvalidIon
	}
	// note: we're skipping the whole datum here
	// so that a multi-byte nop pad is skipped appropriately
	return UntypedNull{}, b[s:], nil
}

func decodeBoolDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	q, rest, err := ReadBool(b)
	if err != nil {
		return nil, rest, err
	}
	return Bool(q), rest, nil
}

func decodeUintDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	if SizeOf(b) > 9 {
		return nil, b, fmt.Errorf("int size %d out of range", SizeOf(b))
	}
	u, rest, err := ReadUint(b)
	if err != nil {
		return nil, rest, err
	}
	return Uint(u), rest, nil
}

func decodeIntDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	if SizeOf(b) > 9 {
		return nil, b, fmt.Errorf("int size %d out of range", SizeOf(b))
	}
	i, rest, err := ReadInt(b)
	if err != nil {
		return nil, rest, err
	}
	return Int(i), rest, nil
}

func decodeFloatDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	f, rest, err := ReadFloat64(b)
	if err != nil {
		return nil, rest, err
	}
	return Float(f), rest, nil
}

func decodeDecimalDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	return nil, nil, fmt.Errorf("ion: decimal decoding unimplemented")
}

func decodeTimestampDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	t, rest, err := ReadTime(b)
	if err != nil {
		return nil, rest, err
	}
	return Timestamp(t), rest, nil
}

func decodeSymbolDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	t, rest, err := ReadSymbol(b)
	if err != nil {
		return nil, rest, err
	}
	return Interned(st.Get(t)), rest, nil
}

func decodeStringDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	s, rest, err := ReadString(b)
	if err != nil {
		return nil, rest, err
	}
	return String(s), rest, nil
}

func decodeBlobDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	buf, rest := Contents(b)
	if buf == nil {
		return nil, b, errInvalidIon
	}
	return Blob(buf), rest, nil
}

func decodeListDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	size := SizeOf(b)
	if size <= 0 || size > len(b) {
		return nil, nil, fmt.Errorf("size %d exceeds buffer size %d", size, len(b))
	}
	body, rest := Contents(b)
	if body == nil {
		return nil, nil, errInvalidIon
	}
	count := 0
	var err error
	for len(body) > 0 {
		body, err = validateDatum(st, body)
		if err != nil {
			return nil, nil, err
		}
		count++
	}
	b2 := make([]byte, size)
	copy(b2, b)
	return &List{
		len: count,
		st:  st.alias(),
		buf: b2,
	}, rest, nil
}

func decodeStructDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	size := SizeOf(b)
	if size <= 0 || size > len(b) {
		return nil, nil, fmt.Errorf("size %d exceeds buffer size %d", size, len(b))
	}
	fields, rest := Contents(b)
	if fields == nil {
		return nil, nil, errInvalidIon
	}
	count := 0
	for len(fields) > 0 {
		var sym Symbol
		var err error
		sym, fields, err = ReadLabel(fields)
		if err != nil {
			return nil, nil, err
		}
		if len(fields) == 0 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		_, ok := st.Lookup(sym)
		if !ok {
			return nil, nil, fmt.Errorf("symbol %d not in symbol table", sym)
		}
		fields, err = validateDatum(st, fields)
		if err != nil {
			return nil, nil, err
		}
		count++
	}
	b2 := make([]byte, size)
	copy(b2, b)
	return &Struct{
		len: count,
		st:  st.alias(),
		buf: b2,
	}, rest, nil
}

func decodeReserved(_ *Symtab, b []byte) (Datum, []byte, error) {
	return nil, b, fmt.Errorf("decoding error: tag %x is reserved", b[0])
}

func decodeAnnotationDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	body, rest := Contents(b)
	var nfields Symbol
	var f []Field
	var err error
	var sym Symbol
	var val Datum
	nfields, body, err = ReadLabel(body)
	if err != nil {
		return nil, body, err
	}
	for len(body) > 0 {
		sym, body, err = ReadLabel(body)
		if err != nil {
			return nil, body, err
		}
		if len(body) == 0 {
			return nil, body, io.ErrUnexpectedEOF
		}
		val, body, err = ReadDatum(st, body)
		if err != nil {
			return nil, body, err
		}
		f = append(f, Field{Sym: sym, Value: val, Label: st.Get(sym)})
	}
	if int(nfields) != len(f) {
		return nil, nil, fmt.Errorf("ion: annotation has %d fields but label says %d", len(f), nfields)
	}
	return &Annotation{Fields: f}, rest, nil
}

var _datumTable = [...](func(*Symtab, []byte) (Datum, []byte, error)){
	NullType:       decodeNullDatum,
	BoolType:       decodeBoolDatum,
	UintType:       decodeUintDatum,
	IntType:        decodeIntDatum,
	FloatType:      decodeFloatDatum,
	DecimalType:    decodeDecimalDatum,
	TimestampType:  decodeTimestampDatum,
	SymbolType:     decodeSymbolDatum,
	StringType:     decodeStringDatum,
	ClobType:       decodeBlobDatum, // fixme: treat clob differently than blob?
	BlobType:       decodeBlobDatum,
	ListType:       decodeListDatum,
	SexpType:       decodeListDatum, // fixme: treat sexp differently than list?
	StructType:     decodeStructDatum,
	AnnotationType: decodeAnnotationDatum,
	ReservedType:   decodeReserved,
}

var datumTable [16](func(*Symtab, []byte) (Datum, []byte, error))

func init() {
	copy(datumTable[:], _datumTable[:])
}

// ReadDatum reads the next datum from buf
// and returns it. ReadDatum does not return
// symbol tables directly; instead it unmarshals
// them into st and continues reading. It may
// return a nil datum if buf points to a symbol
// table followed by zero bytes of actual ion data.
//
// Any Symbol datums in buf are translated into
// Interned datums rather than Symbol datums,
// as this makes the returned Datum safe to
// re-encode with a new symbol table.
func ReadDatum(st *Symtab, buf []byte) (Datum, []byte, error) {
	var err error
	if IsBVM(buf) || TypeOf(buf) == AnnotationType {
		buf, err = st.Unmarshal(buf)
		if err != nil {
			return nil, nil, err
		}
		if len(buf) == 0 {
			return nil, buf, nil
		}
	}
	return datumTable[TypeOf(buf)](st, buf)
}

// validateDatum validates that the next datum in buf
// does not exceed the bounds of buf without actually
// interpretting it. This also handles symbol tables
// the same way that ReadDatum does.
func validateDatum(st *Symtab, buf []byte) (next []byte, err error) {
	if IsBVM(buf) || TypeOf(buf) == AnnotationType {
		buf, err = st.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		if len(buf) == 0 {
			return nil, nil
		}
	}
	size := SizeOf(buf)
	if size <= 0 || size > len(buf) {
		return nil, fmt.Errorf("size %d exceeds buffer size %d", size, len(buf))
	}
	return buf[size:], nil
}

// Equal returns whether a and b are
// semantically equivalent.
func Equal(a, b Datum) bool {
	return a.equal(b)
}

func stoverlap(st1, st2 []string) bool {
	return stcontains(st1, st2) || stcontains(st2, st1)
}
