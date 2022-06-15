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
	_ Datum = List{}
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
	if i2, ok := x.(Int); ok {
		return i == i2
	}
	if u, ok := x.(Uint); ok {
		return i >= 0 && Uint(i) == u
	}
	if f, ok := x.(Float); ok {
		return float64(int64(f)) == float64(f) && int64(f) == int64(i)
	}
	return false
}

func (i Int) Encode(dst *Buffer, st *Symtab) {
	dst.WriteInt(int64(i))
}

// Uint is an ion integer datum (always unsigned)
type Uint uint64

func (u Uint) Type() Type                     { return UintType }
func (u Uint) Encode(dst *Buffer, st *Symtab) { dst.WriteUint(uint64(u)) }

func (u Uint) equal(x Datum) bool {
	if u2, ok := x.(Uint); ok {
		return u == u2
	}
	if i, ok := x.(Int); ok {
		return i >= 0 && Uint(i) == u
	}
	if f, ok := x.(Float); ok {
		return float64(uint64(f)) == float64(f) && uint64(f) == uint64(u)
	}
	return false
}

// Field is a structure field in a Struct or Annotation datum
type Field struct {
	Label string
	Value Datum
	Sym   Symbol // symbol, if assigned
}

// Struct is an ion structure datum
type Struct struct {
	Fields []Field
}

func (s *Struct) Type() Type { return StructType }

func (s *Struct) Encode(dst *Buffer, st *Symtab) {
	for i := range s.Fields {
		s.Fields[i].Sym = st.Intern(s.Fields[i].Label)
	}
	slices.SortFunc(s.Fields, func(x, y Field) bool {
		return x.Sym < y.Sym
	})

	dst.BeginStruct(-1)
	for i := range s.Fields {
		dst.BeginField(s.Fields[i].Sym)
		s.Fields[i].Value.Encode(dst, st)
	}
	dst.EndStruct()
}

func (s *Struct) equal(x Datum) bool {
	s2, ok := x.(*Struct)
	if !ok {
		return false
	}
	if len(s.Fields) != len(s2.Fields) {
		return false
	}
	byname := func(x, y Field) bool {
		return x.Label < y.Label
	}
	slices.SortFunc(s.Fields, byname)
	slices.SortFunc(s2.Fields, byname)
	for i := range s.Fields {
		f2 := s2.Fields[i].Value
		if !f2.equal(s.Fields[i].Value) {
			return false
		}
	}
	return true
}

func (s *Struct) Field(x Symbol) *Field {
	for i := range s.Fields {
		if s.Fields[i].Sym == x {
			return &s.Fields[i]
		}
	}
	return nil
}

func (s *Struct) FieldByName(name string) *Field {
	for i := range s.Fields {
		if s.Fields[i].Label == name {
			return &s.Fields[i]
		}
	}
	return nil
}

// List is an ion list datum
type List []Datum

func (l List) Type() Type { return ListType }

func (l List) Encode(dst *Buffer, st *Symtab) {
	dst.BeginList(-1)
	for i := range l {
		l[i].Encode(dst, st)
	}
	dst.EndList()
}

func (l List) equal(x Datum) bool {
	l2, ok := x.(List)
	if !ok {
		return false
	}
	if len(l) != len(l2) {
		return false
	}
	for i := range l {
		if !l[i].equal(l2[i]) {
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
	// note: we're skipping the whole datum here
	// so that a multi-byte nop pad is skipped appropriately
	return UntypedNull{}, b[SizeOf(b):], nil
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
	return Blob(buf), rest, nil
}

func decodeListDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	body, rest := Contents(b)
	var out List
	var val Datum
	var err error
	for len(body) > 0 {
		val, body, err = ReadDatum(st, body)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, val)
	}
	return out, rest, nil
}

func decodeStructDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	fields, rest := Contents(b)
	var f []Field
	var sym Symbol
	var val Datum
	var err error
	for len(fields) > 0 {
		sym, fields, err = ReadLabel(fields)
		if err != nil {
			return nil, nil, err
		}
		val, fields, err = ReadDatum(st, fields)
		if err != nil {
			return nil, nil, err
		}
		f = append(f, Field{Sym: sym, Value: val, Label: st.Get(sym)})
	}
	return &Struct{Fields: f}, rest, nil
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

func RelaxedEqual(a, b Datum) bool {
	return a.equal(b)
}
