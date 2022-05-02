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
	"math/big"

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
}

var (
	// all of these types must be datums
	_ Datum = Float(0)
	_ Datum = Int(0)
	_ Datum = Uint(0)
	_ Datum = &Struct{}
	_ Datum = List{}
	_ Datum = Bool(true)
	_ Datum = new(BigInt)
	_ Datum = Timestamp{}
	_ Datum = &Annotation{}
	_ Datum = Blob(nil)
)

// Float is an ion float datum
type Float float64

func (f Float) Encode(dst *Buffer, st *Symtab) {
	dst.WriteFloat64(float64(f))
}

func (f Float) Type() Type { return FloatType }

// UntypedNull is an ion "untyped null" datum
type UntypedNull struct{}

func (u UntypedNull) Type() Type                     { return NullType }
func (u UntypedNull) Encode(dst *Buffer, st *Symtab) { dst.WriteNull() }

// Int is an ion integer datum (signed or unsigned)
type Int int64

func (i Int) Type() Type {
	if i >= 0 {
		return UintType
	}
	return IntType
}

func (i Int) Encode(dst *Buffer, st *Symtab) {
	dst.WriteInt(int64(i))
}

// Uint is an ion integer datum (always unsigned)
type Uint uint64

func (u Uint) Type() Type                     { return UintType }
func (u Uint) Encode(dst *Buffer, st *Symtab) { dst.WriteUint(uint64(u)) }

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

// Bool is an ion bool datum
type Bool bool

func (b Bool) Type() Type                     { return BoolType }
func (b Bool) Encode(dst *Buffer, st *Symtab) { dst.WriteBool(bool(b)) }

type String string

func (s String) Type() Type { return StringType }

func (s String) Encode(dst *Buffer, st *Symtab) { dst.WriteString(string(s)) }

type Blob []byte

func (b Blob) Type() Type { return BlobType }

func (b Blob) Encode(dst *Buffer, st *Symtab) { dst.WriteBlob([]byte(b)) }

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

// Timestamp is an ion timestamp datum
type Timestamp date.Time

func (t Timestamp) Type() Type { return TimestampType }

func (t Timestamp) Encode(dst *Buffer, st *Symtab) {
	dst.WriteTime(date.Time(t))
}

// BigNum is a datum that can hold
// arbitrary rational numbers
type BigNum big.Rat

func (b *BigNum) Encode(dst *Buffer, st *Symtab) {
	r := (*big.Rat)(b)
	if r.IsInt() {
		(*BigInt)(r.Num()).Encode(dst, st)
	}
	// FIXME: handle imprecise output here
	f, _ := r.Float64()
	Float(f).Encode(dst, st)
}

func (b *BigNum) Type() Type {
	r := (*big.Rat)(b)
	if r.IsInt() {
		if r.Sign() < 0 {
			return IntType
		}
		return UintType
	}
	// TODO: return decimal type
	return FloatType
}

// BigInt is an ion integer that
// can hold integers of arbitrary magnitude
type BigInt big.Int

func (b *BigInt) Type() Type {
	i := (*big.Int)(b)
	if i.Sign() < 0 {
		return IntType
	}
	return UintType
}

func (b *BigInt) Encode(dst *Buffer, st *Symtab) {
	i := (*big.Int)(b)
	if i.IsInt64() {
		dst.WriteInt(i.Int64())
		return
	}
	if i.IsUint64() {
		dst.WriteUint(i.Uint64())
		return
	}
	tag := (UintType << 4) | 0xe
	if i.Sign() < 0 {
		tag = (IntType << 4) | 0xe
	}
	buf := dst.grow(1 + (i.BitLen() * 8))
	buf[0] = byte(tag)
	i.FillBytes(buf[1:])
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
		body, rest := Contents(b)
		return (*BigInt)(new(big.Int).SetBytes(body)), rest, nil
	}
	u, rest, err := ReadUint(b)
	if err != nil {
		return nil, rest, err
	}
	return Uint(u), rest, nil
}

func decodeIntDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	if SizeOf(b) > 9 {
		body, rest := Contents(b)
		bi := new(big.Int).SetBytes(body)
		return (*BigInt)(bi.Neg(bi)), rest, nil
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

func decodeSymbolDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	t, rest, err := ReadSymbol(b)
	if err != nil {
		return nil, rest, err
	}
	return t, rest, nil
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
