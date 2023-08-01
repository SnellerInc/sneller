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
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
)

// Stop can be returned by the function passed
// to List.Each and Struct.Each to stop
// iterating and return early.
//
//lint:ignore ST1012 sentinel error
var Stop = errors.New("stop early")

// Datum represents any Ion datum.
//
// The Marshal and Unmarshal functions natively
// understand that Datum can be constructed and
// re-encoded from any ion value.
//
// A Datum should be a value returned by
//
//	Float, Int, Uint, Struct, List, Bool,
//	BigInt, Timestamp, Annotation, ..., or ReadDatum.
type Datum struct {
	st  []string
	buf []byte
}

func rawDatum(st *Symtab, b []byte) Datum {
	d := Datum{buf: b[:SizeOf(b)]}
	if st != nil {
		d.st = st.alias()
	}
	return d
}

// Empty is the zero value of a Datum.
var Empty = Datum{}

// CloneInto clobbers the underlying storage for dst
// with the contents of d.
func (d Datum) CloneInto(dst *Datum) {
	dst.st = d.st
	dst.buf = append(dst.buf[:0], d.buf...)
}

func (d Datum) Clone() Datum {
	return Datum{
		st:  d.st, // no need to clone
		buf: slices.Clone(d.buf),
	}
}

// Equal returns whether d and x are
// semantically equivalent.
func (d Datum) Equal(x Datum) bool {
	switch d.Type() {
	case NullType:
		return x.IsNull()
	case FloatType:
		if x.IsFloat() {
			d, _ := d.Float()
			x, _ := x.Float()
			return x == d || (math.IsNaN(d) && math.IsNaN(x))
		}
		if x.IsInt() {
			d, _ := d.Float()
			x, _ := x.Int()
			return float64(int64(d)) == float64(d) && int64(d) == int64(x)
		}
		if x.IsUint() {
			d, _ := d.Float()
			x, _ := x.Uint()
			return float64(uint64(d)) == float64(d) && uint64(d) == uint64(x)
		}
	case IntType:
		if x.IsInt() {
			d, _ := d.Int()
			x, _ := x.Int()
			return x == d
		}
		if x.IsUint() {
			d, _ := d.Int()
			x, _ := x.Uint()
			return d >= 0 && uint64(d) == x
		}
		if x.IsFloat() {
			d, _ := d.Int()
			x, _ := x.Float()
			return float64(int64(x)) == float64(x) && int64(x) == int64(d)
		}
	case UintType:
		if x.IsUint() {
			d, _ := d.Uint()
			x, _ := x.Uint()
			return d == x
		}
		if x.IsInt() {
			d, _ := d.Uint()
			x, _ := x.Int()
			return x >= 0 && uint64(x) == d
		}
		if x.IsFloat() {
			d, _ := d.Uint()
			x, _ := x.Float()
			return float64(uint64(x)) == float64(x) && uint64(x) == uint64(d)
		}
	case StructType:
		if x.IsStruct() {
			d, _ := d.Struct()
			x, _ := x.Struct()
			return d.Equal(x)
		}
		return false
	case ListType:
		if x.IsList() {
			d, _ := d.List()
			x, _ := x.List()
			return d.Equal(x)
		}
		return false
	case BoolType:
		if x.IsBool() {
			d, _ := d.Bool()
			x, _ := x.Bool()
			return d == x
		}
		return false
	case StringType:
		if x.IsString() {
			d, _ := d.String()
			x, _ := x.StringShared()
			return d == string(x)
		}
		if x.IsSymbol() {
			d, _ := d.String()
			x, _ := x.String()
			return d == x
		}
	case SymbolType:
		if x.IsString() {
			d, _ := d.String()
			x, _ := x.StringShared()
			return d == string(x)
		}
		if x.IsSymbol() {
			d, _ := d.String()
			x, _ := x.String()
			return d == x
		}
	case BlobType:
		if x.IsBlob() {
			d, _ := d.BlobShared()
			x, _ := x.BlobShared()
			return string(d) == string(x)
		}
	case TimestampType:
		if x.IsTimestamp() {
			d, _ := d.Timestamp()
			x, _ := x.Timestamp()
			return d.Equal(x)
		}
	}
	return false
}

// LessImprecise compares the raw Ion bytes.
//
// This method does not order correctly equal
// values having different binary representation.
// For instance a string can be saved literally,
// as a sequence of UTF-8 bytes, or be a symbol,
// that is a reference to the symbol table.
func (d Datum) LessImprecise(x Datum) bool {
	return bytes.Compare(d.buf, x.buf) < 0
}

func (d Datum) Type() Type {
	if len(d.buf) == 0 {
		return InvalidType
	}
	return TypeOf(d.buf)
}

type resymbolizer struct {
	// idmap is a cache of old->new symbol mappings
	idmap  []Symbol
	srctab *Symtab
	dsttab *Symtab
	expand bool
}

func (r *resymbolizer) reset() {
	for i := range r.idmap {
		r.idmap[i] = 0
	}
}

func (r *resymbolizer) get(sym Symbol) Symbol {
	if int(sym) < len(r.idmap) && r.idmap[sym] != 0 {
		return r.idmap[sym]
	}
	if int(sym) >= len(r.idmap) {
		if cap(r.idmap) > int(sym) {
			r.idmap = r.idmap[:int(sym)+1]
		} else {
			newmap := make([]Symbol, int(sym)+1)
			copy(newmap, r.idmap)
			r.idmap = newmap
		}
	}
	r.idmap[sym] = r.dsttab.Intern(r.srctab.Get(sym))
	return r.idmap[sym]
}

func (d Datum) JSON() string {
	var buf strings.Builder
	jw := NewJSONWriter(&buf, '\n')
	jw.st = d.symtab()
	jw.Write(d.buf)
	jw.Close()
	str := buf.String()
	return str[:len(str)-1] // remove '\n'
}

func (d Datum) Encode(dst *Buffer, st *Symtab) {
	if d.IsEmpty() {
		panic("ion: encoding empty datum")
	}
	// fast path: no need to resymbolize
	if len(d.st) == 0 || st.contains(d.st) {
		dst.UnsafeAppend(d.buf)
		return
	}
	srcsyms := d.symtab()
	rs := &resymbolizer{
		srctab: &srcsyms,
		dsttab: st,
	}
	rs.resym(dst, d.buf)
}

// performance-sensitive resymbolization path
func (r *resymbolizer) resym(dst *Buffer, buf []byte) []byte {
	switch TypeOf(buf) {
	case SymbolType:
		sym, rest, _ := ReadSymbol(buf)
		if r.expand {
			dst.WriteString(r.srctab.Get(sym))
		} else {
			dst.WriteSymbol(r.get(sym))
		}
		return rest
	case StructType:
		dst.BeginStruct(-1)
		body, rest := Contents(buf)
		var sym Symbol
		for len(body) > 0 {
			sym, body, _ = ReadLabel(body)
			dst.BeginField(r.get(sym))
			size := SizeOf(body)
			r.resym(dst, body[:size])
			body = body[size:]
		}
		dst.EndStruct()
		return rest
	case ListType:
		dst.BeginList(-1)
		body, rest := Contents(buf)
		for len(body) > 0 {
			size := SizeOf(body)
			r.resym(dst, body[:size])
			body = body[size:]
		}
		dst.EndList()
		return rest
	case AnnotationType:
		sym, body, rest, _ := ReadAnnotation(buf)
		dst.BeginAnnotation(1)
		dst.BeginField(r.get(sym))
		r.resym(dst, body)
		dst.EndAnnotation()
		return rest
	default:
		s := SizeOf(buf)
		dst.UnsafeAppend(buf[:s])
		return buf[s:]
	}
}

func (d Datum) IsEmpty() bool      { return len(d.buf) == 0 }
func (d Datum) IsNull() bool       { return d.Type() == NullType }
func (d Datum) IsFloat() bool      { return d.Type() == FloatType }
func (d Datum) IsInt() bool        { return d.Type() == IntType }
func (d Datum) IsUint() bool       { return d.Type() == UintType }
func (d Datum) IsStruct() bool     { return d.Type() == StructType }
func (d Datum) IsList() bool       { return d.Type() == ListType }
func (d Datum) IsAnnotation() bool { return d.Type() == AnnotationType }
func (d Datum) IsBool() bool       { return d.Type() == BoolType }
func (d Datum) IsSymbol() bool     { return d.Type() == SymbolType }
func (d Datum) IsString() bool     { return d.Type() == StringType }
func (d Datum) IsBlob() bool       { return d.Type() == BlobType }
func (d Datum) IsTimestamp() bool  { return d.Type() == TimestampType }

func (d Datum) Null() error                        { return d.null("") }
func (d Datum) Float() (float64, error)            { return d.float("") }
func (d Datum) Int() (int64, error)                { return d.int("") }
func (d Datum) Uint() (uint64, error)              { return d.uint("") }
func (d Datum) Struct() (Struct, error)            { return d.struc("") }
func (d Datum) List() (List, error)                { return d.list("") }
func (d Datum) Annotation() (string, Datum, error) { return d.annotation("") }
func (d Datum) Bool() (bool, error)                { return d.bool("") }
func (d Datum) Symbol() (Symbol, error)            { return d.symbol("") }
func (d Datum) String() (string, error)            { return d.string("") }
func (d Datum) Blob() ([]byte, error)              { return d.blob("") }
func (d Datum) Timestamp() (date.Time, error)      { return d.timestamp("") }

func (d Datum) CoerceFloat() (float64, error) {
	i, err := d.Int()
	if err == nil {
		return float64(i), nil
	}
	return d.Float()
}

// StringShared returns a []byte aliasing the
// contents of this Datum and should be copied
// as necessary to avoid issues that may arise
// from retaining aliased bytes.
//
// Unlike String, this method will not work with
// a symbol datum.
func (d Datum) StringShared() ([]byte, error) { return d.stringShared("") }

// BlobShared returns a []byte aliasing the
// contents of this Datum and should be copied
// as necessary to avoid issues that may arise
// from retaining aliased bytes.
func (d Datum) BlobShared() ([]byte, error) { return d.blobShared("") }

// UnpackStruct calls d.Struct and calls
// UnpackStruct on the result.
func (d Datum) UnpackStruct(fn func(Field) error) error { return d.unpackStruct("", fn) }

// UnpackList calls d.List and calls UnpackList
// on the result.
func (d Datum) UnpackList(fn func(Datum) error) error { return d.unpackList("", fn) }

// Iterator calls d.List and calls Iterator on
// the result.
func (d Datum) Iterator() (Iterator, error) { return d.iterator("") }

func (d Datum) null(field string) error {
	if !d.IsNull() {
		return d.bad(field, NullType)
	}
	return nil
}

func (d Datum) float(field string) (float64, error) {
	if !d.IsFloat() {
		return 0, d.bad(field, FloatType)
	}
	f, _, err := ReadFloat64(d.buf)
	if err != nil {
		panic(err)
	}
	return f, nil
}

func (d Datum) int(field string) (int64, error) {
	if !d.IsInt() && !d.IsUint() {
		return 0, d.bad(field, IntType)
	}
	i, _, err := ReadInt(d.buf)
	if err != nil {
		panic(err)
	}
	return i, nil
}

func (d Datum) uint(field string) (uint64, error) {
	if !d.IsUint() {
		return 0, d.bad(field, UintType)
	}
	u, _, err := ReadUint(d.buf)
	if err != nil {
		panic(err)
	}
	return u, nil
}

func (d Datum) struc(field string) (Struct, error) {
	if !d.IsStruct() {
		return Struct{}, d.bad(field, StructType)
	}
	return Struct{st: d.st, buf: d.buf}, nil
}

// Field returns the value associated with the
// field with the given name if d is a struct.
// If d is not a struct or the field is not
// present, this returns Empty.
func (d Datum) Field(name string) Datum {
	if !d.IsStruct() {
		return Empty
	}
	s, _ := d.Struct()
	f, ok := s.FieldByName(name)
	if !ok {
		return Empty
	}
	return f.Datum
}

func (d Datum) list(field string) (List, error) {
	if !d.IsList() {
		return List{}, d.bad(field, ListType)
	}
	return List{st: d.st, buf: d.buf}, nil
}

func (d Datum) annotation(field string) (string, Datum, error) {
	if !d.IsAnnotation() {
		return "", Empty, d.bad(field, AnnotationType)
	}
	sym, body, _, err := ReadAnnotation(d.buf)
	if err != nil {
		panic(err)
	}
	st := d.symtab()
	s, ok := st.Lookup(sym)
	if !ok {
		panic("ion.Datum.Annotation: missing symbol")
	}
	return s, Datum{st: d.st, buf: body}, nil
}

func (d Datum) bool(field string) (bool, error) {
	if !d.IsBool() {
		return false, d.bad(field, BoolType)
	}
	b, _, err := ReadBool(d.buf)
	if err != nil {
		panic(err)
	}
	return b, nil
}

func (d Datum) symbol(field string) (Symbol, error) {
	if !d.IsSymbol() {
		return 0, d.bad(field, SymbolType)
	}
	sym, _, err := ReadSymbol(d.buf)
	if err != nil {
		panic(err)
	}
	return sym, nil
}

func (d Datum) string(field string) (string, error) {
	if d.IsSymbol() {
		sym, _ := d.Symbol()
		st := d.symtab()
		s, ok := st.Lookup(sym)
		if !ok {
			panic("ion.Datum.String: missing symbol")
		}
		return s, nil
	}
	s, err := d.stringShared(field)
	return string(s), err
}

func (d Datum) stringShared(field string) ([]byte, error) {
	if !d.IsString() {
		return nil, d.bad(field, StringType)
	}
	s, _, err := ReadStringShared(d.buf)
	if err != nil {
		panic(err)
	}
	return s, nil
}

func (d Datum) blob(field string) ([]byte, error) {
	b, err := d.blobShared(field)
	return slices.Clone(b), err
}

func (d Datum) blobShared(field string) ([]byte, error) {
	if !d.IsBlob() {
		return nil, d.bad(field, BlobType)
	}
	b, _ := Contents(d.buf)
	if b == nil {
		panic("ion.Datum.Blob: invalid ion")
	}
	return b, nil
}

func (d Datum) timestamp(field string) (date.Time, error) {
	if !d.IsTimestamp() {
		return date.Time{}, d.bad(field, TimestampType)
	}
	t, _, err := ReadTime(d.buf)
	if err != nil {
		panic(err)
	}
	return t, nil
}

func (d Datum) unpackStruct(field string, fn func(Field) error) error {
	s, err := d.struc(field)
	if err != nil {
		return err
	}
	return s.Each(fn)
}

func (d Datum) unpackList(field string, fn func(Datum) error) error {
	l, err := d.list(field)
	if err != nil {
		return err
	}
	return l.Each(fn)
}

func (d Datum) iterator(field string) (Iterator, error) {
	l, err := d.list(field)
	if err != nil {
		return Iterator{}, err
	}
	return l.Iterator()
}

// DecodeRelated attempts to decode a datum that
// was encoded using the same symbol table as
// this datum, for example when a struct
// contains a blob holding the compressed
// encoded form of another composite object.
//
// This method only works with lists and
// structs. Calling this method on any other
// data type will result in an error.
func (d Datum) DecodeRelated(b []byte) (Datum, error) {
	if !d.IsList() && !d.IsStruct() {
		return Empty, fmt.Errorf("ion.Datum.DecodeRelated: receiver must be List or Struct")
	}
	st := d.symtab()
	d2, _, err := ReadDatum(&st, b)
	return d2, err
}

func (d *Datum) bad(field string, want Type) error {
	return &TypeError{Wanted: want, Found: d.Type(), Field: field}
}

func (d *Datum) symtab() Symtab {
	return Symtab{interned: d.st}
}

// Raw returns the raw byte slice underlying d.
// The returned slice aliases the contents of d,
// so care should be taken when retaining or
// modifying the returned slice.
func (d Datum) Raw() []byte {
	return d.buf
}

func Float(f float64) Datum {
	var buf Buffer
	buf.WriteFloat64(f)
	return Datum{buf: buf.Bytes()}
}

// Null is the untyped null datum.
var Null = Datum{buf: []byte{0x0f}}

func Int(i int64) Datum {
	var buf Buffer
	buf.WriteInt(i)
	return Datum{buf: buf.Bytes()}
}

func Uint(u uint64) Datum {
	var buf Buffer
	buf.WriteUint(u)
	return Datum{buf: buf.Bytes()}
}

// Field is a structure field in a Struct or Annotation datum
type Field struct {
	Label string
	Datum
	Sym Symbol // symbol, if assigned
}

func ReadField(st *Symtab, body []byte) (Field, []byte, error) {
	sym, body, err := ReadLabel(body)
	if err != nil {
		return Field{}, nil, err
	}
	name, ok := st.Lookup(sym)
	if !ok {
		return Field{}, nil, fmt.Errorf("symbol %d not in symbol table", sym)
	}
	val, rest, err := ReadDatum(st, body)
	if err != nil {
		return Field{}, nil, err
	}
	return Field{Label: name, Datum: val, Sym: sym}, rest, nil
}

func (f *Field) Encode(dst *Buffer, st *Symtab) {
	dst.BeginField(st.Intern(f.Label))
	f.Datum.Encode(dst, st)
}

func (f *Field) Equal(f2 *Field) bool {
	return f.Label == f2.Label && f.Sym == f2.Sym && f.Datum.Equal(f2.Datum)
}

func (f Field) Clone() Field {
	return Field{
		Label: f.Label,
		Datum: f.Datum.Clone(),
		Sym:   f.Sym,
	}
}

func (f Field) Null() error                        { return f.null(f.Label) }
func (f Field) Float() (float64, error)            { return f.float(f.Label) }
func (f Field) Int() (int64, error)                { return f.int(f.Label) }
func (f Field) Uint() (uint64, error)              { return f.uint(f.Label) }
func (f Field) Struct() (Struct, error)            { return f.struc(f.Label) }
func (f Field) List() (List, error)                { return f.list(f.Label) }
func (f Field) Annotation() (string, Datum, error) { return f.annotation(f.Label) }
func (f Field) Bool() (bool, error)                { return f.bool(f.Label) }
func (f Field) Symbol() (Symbol, error)            { return f.symbol(f.Label) }
func (f Field) String() (string, error)            { return f.string(f.Label) }
func (f Field) Blob() ([]byte, error)              { return f.blob(f.Label) }
func (f Field) Timestamp() (date.Time, error)      { return f.timestamp(f.Label) }

// StringShared returns a []byte aliasing the
// contents of this Field and should be copied
// as necessary to avoid issues that may arise
// from retaining aliased bytes.
//
// Unlike String, this method will not work with
// a symbol datum.
func (f Field) StringShared() ([]byte, error) { return f.stringShared(f.Label) }

// BlobShared returns a []byte aliasing the
// contents of this Field and should be copied
// as necessary to avoid issues that may arise
// from retaining aliased bytes.
func (f Field) BlobShared() ([]byte, error) { return f.blobShared(f.Label) }

// UnpackStruct calls f.Struct and calls
// UnpackStruct on the result.
func (f Field) UnpackStruct(fn func(Field) error) error { return f.unpackStruct(f.Label, fn) }

// UnpackList calls f.List and calls UnpackList
// on the result.
func (f Field) UnpackList(fn func(Datum) error) error { return f.unpackList(f.Label, fn) }

// Iterator calls f.List and calls Iterator on
// the result.
func (f Field) Iterator() (Iterator, error) { return f.iterator(f.Label) }

// NewString constructs a new ion Datum from a string.
func NewString(s string) Datum {
	var buf Buffer
	buf.WriteString(s)
	return Datum{buf: buf.Bytes()}
}

var emptyStruct = []byte{0xd0}

// Struct is an ion structure datum
type Struct struct {
	st    []string
	buf   []byte
	struc struct{} //lint:ignore U1000 disallow conversion
}

func NewStruct(st *Symtab, f []Field) Struct {
	if len(f) == 0 {
		return Struct{}
	}
	var dst Buffer
	if st == nil {
		st = &Symtab{}
	}
	dst.WriteStruct(st, f)
	return Struct{st: st.alias(), buf: dst.Bytes()}
}

func (b *Buffer) WriteStruct(st *Symtab, f []Field) {
	if len(f) == 0 {
		b.UnsafeAppend(emptyStruct)
		return
	}
	b.BeginStruct(-1)
	for i := range f {
		f[i].Encode(b, st)
	}
	b.EndStruct()
}

func (s Struct) Datum() Datum {
	if len(s.buf) == 0 {
		return Datum{buf: emptyStruct}
	}
	return Datum{st: s.st, buf: s.buf}
}

func (s Struct) Encode(dst *Buffer, st *Symtab) {
	// fast path: we can avoid resym
	if s.IsEmpty() || st.contains(s.st) {
		dst.UnsafeAppend(s.bytes())
		return
	}
	dst.BeginStruct(-1)
	s.Each(func(f Field) error {
		f.Encode(dst, st)
		return nil
	})
	dst.EndStruct()
}

func (s Struct) Equal(s2 Struct) bool {
	if s.IsEmpty() {
		return s2.IsEmpty()
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
	slices.SortFunc(f1, func(x, y Field) int {
		return strings.Compare(x.Label, y.Label)
	})
	slices.SortFunc(f2, func(x, y Field) int {
		return strings.Compare(x.Label, y.Label)
	})
	for i := range f1 {
		if f1[i].Label != f2[i].Label {
			return false
		}
		if !Equal(f1[i].Datum, f2[i].Datum) {
			return false
		}
	}
	return true
}

func (s Struct) Len() int {
	if s.IsEmpty() {
		return 0
	}
	n := 0
	s.Each(func(Field) error {
		n++
		return nil
	})
	return n
}

func (s *Struct) IsEmpty() bool {
	if len(s.buf) == 0 {
		return true
	}
	body, _ := Contents(s.buf)
	return len(body) == 0
}

func (s *Struct) bytes() []byte {
	if len(s.buf) == 0 {
		return emptyStruct
	}
	return s.buf
}

// Each calls fn for each field in the struct.
// If fn returns Stop, Each stops and returns nil.
// If fn returns any other non-nil error, Each
// stops and returns that error. If Each
// encounters a malformed field while unpacking
// the struct, Each returns a non-nil error.
func (s Struct) Each(fn func(Field) error) error {
	if s.IsEmpty() {
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
		f, rest, err := ReadField(&st, body)
		if err != nil {
			return err
		}
		err = fn(f)
		if err == Stop {
			break
		} else if err != nil {
			return err
		}
		body = rest
	}
	return nil
}

// Fields appends fields to the given slice and returns
// the appended slice.
func (s Struct) Fields(fields []Field) []Field {
	fields = slices.Grow(fields, s.Len())
	s.Each(func(f Field) error {
		fields = append(fields, f)
		return nil
	})
	return fields
}

// WithField adds or overwrites a field in s
// and returns a new Struct with the updated field.
// If a field with f.Label is already present in the
// structure, it is overwritten with f. Otherwise,
// f is added to the existing fields.
func (s Struct) WithField(f Field) Struct {
	fields := s.Fields(nil)
	found := false
	for i := range fields {
		if fields[i].Label == f.Label {
			fields[i] = f
			found = true
			break
		}
	}
	if !found {
		fields = append(fields, f)
	}
	return NewStruct(nil, fields)
}

func (s Struct) Field(x Symbol) (Field, bool) {
	var field Field
	var ok bool
	s.Each(func(f Field) error {
		if f.Sym == x {
			field, ok = f, true
			return Stop
		}
		return nil
	})
	return field, ok
}

func (s Struct) FieldByName(name string) (Field, bool) {
	var field Field
	var ok bool
	s.Each(func(f Field) error {
		if f.Label == name {
			field, ok = f, true
			return Stop
		}
		return nil
	})
	return field, ok
}

// mergeFields merges the given fields with the
// fields of this struct into a new struct,
// overwriting any previous fields with
// conflicting names.
//
// This should only be used for testing in this
// package.
func (s Struct) mergeFields(st *Symtab, fields []Field) Struct {
	into := make([]Field, 0, s.Len()+len(fields))
	add := func(f Field) {
		for i := range into {
			if into[i].Label == f.Label {
				into[i] = f
				return
			}
		}
		into = append(into, f)
	}
	s.Each(func(f Field) error {
		add(f)
		return nil
	})
	for i := range fields {
		add(fields[i])
	}
	return NewStruct(st, into)
}

func (s *Struct) symtab() Symtab {
	return Symtab{interned: s.st}
}

var emptyList = []byte{0xb0}

// List is an ion list datum
type List struct {
	st   []string
	buf  []byte
	list struct{} //lint:ignore U1000 disallow conversion
}

func NewList(st *Symtab, items []Datum) List {
	if len(items) == 0 {
		return List{}
	}
	var dst Buffer
	if st == nil {
		st = &Symtab{}
	}
	dst.WriteList(st, items)
	return List{
		st:  st.alias(),
		buf: dst.Bytes(),
	}
}

func (b *Buffer) WriteList(st *Symtab, items []Datum) {
	if len(items) == 0 {
		b.UnsafeAppend(emptyList)
		return
	}
	b.BeginList(-1)
	for i := range items {
		items[i].Encode(b, st)
	}
	b.EndList()
}

func (l List) Datum() Datum {
	if len(l.buf) == 0 {
		return Datum{buf: emptyList}
	}
	return Datum{st: l.st, buf: l.buf}
}

func (l List) Encode(dst *Buffer, st *Symtab) {
	// fast path: we can avoid resym
	if l.IsEmpty() || st.contains(l.st) {
		dst.UnsafeAppend(l.bytes())
		return
	}
	dst.BeginList(-1)
	l.Each(func(d Datum) error {
		d.Encode(dst, st)
		return nil
	})
	dst.EndList()
}

// Len returns the number of items in the list.
// If the list is malformed, the results are
// undefined.
func (l List) Len() int {
	if l.IsEmpty() {
		return 0
	}
	body, _ := Contents(l.buf)
	n := 0
	for len(body) > 0 {
		next := SizeOf(body)
		if next <= 0 || next > len(body) {
			// malformed ion...
			return n
		}
		n++
		body = body[next:]
	}
	return n
}

func (l List) IsEmpty() bool {
	if len(l.buf) == 0 {
		return true
	}
	body, _ := Contents(l.buf)
	return len(body) == 0
}

func (l *List) bytes() []byte {
	if l.IsEmpty() {
		return emptyList
	}
	return l.buf
}

// Iterator returns an iterator which can be
// used to iterator over the items in the list.
func (l List) Iterator() (Iterator, error) {
	if l.IsEmpty() {
		return Iterator{}, nil
	}
	if TypeOf(l.buf) != ListType {
		return Iterator{}, fmt.Errorf("expected a list; found ion type %s", TypeOf(l.buf))
	}
	body, _ := Contents(l.buf)
	if body == nil {
		return Iterator{}, errInvalidIon
	}
	return Iterator{
		st:  l.st,
		buf: body,
	}, nil
}

// Each iterates over each datum in the
// list and calls fn on each datum in order.
// Each returns when it encounters an internal error
// (due to malformed ion) or when fn returns a
// non-nil error.
func (l List) Each(fn func(Datum) error) error {
	i, err := l.Iterator()
	if err != nil {
		return err
	}
	for !i.Done() {
		v, err := i.Next()
		if err != nil {
			return err
		}
		err = fn(v)
		if err == Stop {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (l List) Items(items []Datum) []Datum {
	items = slices.Grow(items, l.Len())
	l.Each(func(d Datum) error {
		items = append(items, d)
		return nil
	})
	return items
}

func (l List) Equal(l2 List) bool {
	if l.IsEmpty() {
		return l2.IsEmpty()
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

// ErrUnexpectedEnd is returned if Iterator.Next
// is called after the list has been exhausted.
var ErrUnexpectedEnd = errors.New("unexpected end of list")

// An Iterator can be used to iterate over items
// in a list.
type Iterator struct {
	st   []string
	buf  []byte
	iter struct{} //lint:ignore U1000 disallow conversion
}

// Next returns the next item in the list. The
// caller should check Done first to ensure that
// there are still items in the list.
//
// If Next is called after reaching the the end
// of the list, this returns ErrUnexpectedEnd.
// If there is an error decoding the next item
// in the list, that error is returned.
func (i *Iterator) Next() (Datum, error) {
	if len(i.buf) == 0 {
		return Empty, ErrUnexpectedEnd
	}
	st := i.symtab()
	v, rest, err := ReadDatum(&st, i.buf)
	if err != nil {
		return Empty, err
	}
	i.buf = rest
	return v, nil
}

// Done returns whether the iterator has reached
// the end of the list. If Done returns false,
// the next call to Next will not return
func (i *Iterator) Done() bool {
	return len(i.buf) == 0
}

func (i *Iterator) Float() (float64, error)       { return iternext(i, Datum.Float) }
func (i *Iterator) Int() (int64, error)           { return iternext(i, Datum.Int) }
func (i *Iterator) Uint() (uint64, error)         { return iternext(i, Datum.Uint) }
func (i *Iterator) Struct() (Struct, error)       { return iternext(i, Datum.Struct) }
func (i *Iterator) List() (List, error)           { return iternext(i, Datum.List) }
func (i *Iterator) Bool() (bool, error)           { return iternext(i, Datum.Bool) }
func (i *Iterator) Symbol() (Symbol, error)       { return iternext(i, Datum.Symbol) }
func (i *Iterator) String() (string, error)       { return iternext(i, Datum.String) }
func (i *Iterator) Blob() ([]byte, error)         { return iternext(i, Datum.Blob) }
func (i *Iterator) Timestamp() (date.Time, error) { return iternext(i, Datum.Timestamp) }

func iternext[V any](i *Iterator, fn func(Datum) (V, error)) (V, error) {
	d, err := i.Next()
	if err != nil {
		var empty V
		return empty, err
	}
	return fn(d)
}

func (i *Iterator) symtab() Symtab {
	return Symtab{interned: i.st}
}

var (
	False = Datum{buf: []byte{0x10}}
	True  = Datum{buf: []byte{0x11}}
)

func Bool(b bool) Datum {
	if b {
		return True
	}
	return False
}

func String(s string) Datum {
	var buf Buffer
	buf.WriteString(s)
	return Datum{buf: buf.Bytes()}
}

func Blob(b []byte) Datum {
	var buf Buffer
	buf.WriteBlob(b)
	return Datum{buf: buf.Bytes()}
}

// Interned returns a Datum that represents
// an interned string (a Symbol).
// Interned is always encoded as an ion symbol.
func Interned(st *Symtab, s string) Datum {
	if st == nil {
		st = new(Symtab)
	}
	var buf Buffer
	sym := st.Intern(s)
	buf.WriteSymbol(sym)
	return Datum{st: st.alias(), buf: buf.Bytes()}
}

// Annotation objects represent
// ion annotation datums.
func Annotation(st *Symtab, label string, val Datum) Datum {
	var dst Buffer
	if st == nil {
		st = &Symtab{}
	}
	dst.BeginAnnotation(1)
	dst.BeginField(st.Intern(label))
	if val.IsEmpty() {
		dst.WriteNull()
	} else {
		val.Encode(&dst, st)
	}
	dst.EndAnnotation()
	return Datum{
		st:  st.alias(),
		buf: dst.Bytes(),
	}
}

func Timestamp(t date.Time) Datum {
	var buf Buffer
	buf.WriteTime(t)
	return Datum{buf: buf.Bytes()}
}

func decodeNullDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	s := SizeOf(b)
	if s <= 0 || s > len(b) {
		return Empty, b, errInvalidIon
	}
	// note: we're skipping the whole datum here
	// so that a multi-byte nop pad is skipped appropriately
	return Null, b[s:], nil
}

func decodeBoolDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	_, rest, err := ReadBool(b)
	if err != nil {
		return Empty, rest, err
	}
	return rawDatum(nil, b), rest, nil
}

func decodeUintDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	if SizeOf(b) > 9 {
		return Empty, b, fmt.Errorf("int size %d out of range", SizeOf(b))
	}
	_, rest, err := ReadUint(b)
	if err != nil {
		return Empty, rest, err
	}
	return rawDatum(nil, b), rest, nil
}

func decodeIntDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	if SizeOf(b) > 9 {
		return Empty, b, fmt.Errorf("int size %d out of range", SizeOf(b))
	}
	_, rest, err := ReadInt(b)
	if err != nil {
		return Empty, rest, err
	}
	return rawDatum(nil, b), rest, nil
}

func decodeFloatDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	_, rest, err := ReadFloat64(b)
	if err != nil {
		return Empty, rest, err
	}
	return rawDatum(nil, b), rest, nil
}

func decodeDecimalDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	return Empty, nil, fmt.Errorf("ion: decimal decoding unimplemented")
}

func decodeTimestampDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	_, rest, err := ReadTime(b)
	if err != nil {
		return Empty, rest, err
	}
	return rawDatum(nil, b), rest, nil
}

func decodeSymbolDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	sym, rest, err := ReadSymbol(b)
	if err != nil {
		return Empty, rest, err
	}
	if _, ok := st.Lookup(sym); !ok {
		return Empty, rest, fmt.Errorf("symbol %d not in symbol table", sym)
	}
	return rawDatum(st, b), rest, nil
}

func decodeBytesDatum(_ *Symtab, b []byte) (Datum, []byte, error) {
	buf, rest := Contents(b)
	if buf == nil {
		return Empty, b, errInvalidIon
	}
	return rawDatum(nil, b), rest, nil
}

func decodeListDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	size := SizeOf(b)
	if size <= 0 || size > len(b) {
		return Empty, nil, fmt.Errorf("size %d exceeds buffer size %d", size, len(b))
	}
	body, rest := Contents(b)
	if body == nil {
		return Empty, nil, errInvalidIon
	}
	return rawDatum(st, b), rest, nil
}

func decodeStructDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	size := SizeOf(b)
	if size <= 0 || size > len(b) {
		return Empty, nil, fmt.Errorf("size %d exceeds buffer size %d", size, len(b))
	}
	fields, rest := Contents(b)
	if fields == nil {
		return Empty, nil, errInvalidIon
	}
	return rawDatum(st, b), rest, nil
}

func decodeReserved(_ *Symtab, b []byte) (Datum, []byte, error) {
	return Empty, b, fmt.Errorf("decoding error: tag %x is reserved", b[0])
}

func decodeAnnotationDatum(st *Symtab, b []byte) (Datum, []byte, error) {
	sym, body, rest, err := ReadAnnotation(b)
	if err != nil {
		return Empty, rest, err
	}
	if _, ok := st.Lookup(sym); !ok {
		return Empty, rest, fmt.Errorf("symbol %d not in symbol table", sym)
	}
	_, err = validateDatum(st, body)
	if err != nil {
		return Empty, rest, err
	}
	return Datum{
		st:  st.alias(),
		buf: b[:SizeOf(b)],
	}, rest, nil
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
//
// The returned datum will share memory with buf and so
// the caller must guarantee that the contents of buf
// will not be modified until it is no longer needed.
func ReadDatum(st *Symtab, buf []byte) (Datum, []byte, error) {
	var err error
	if IsBVM(buf) || TypeOf(buf) == AnnotationType {
		buf, err = st.Unmarshal(buf)
		if err != nil {
			return Empty, nil, err
		}
		if len(buf) == 0 {
			return Empty, buf, nil
		}
	}
	switch t := TypeOf(buf); t {
	case NullType:
		return decodeNullDatum(st, buf)
	case BoolType:
		return decodeBoolDatum(st, buf)
	case UintType:
		return decodeUintDatum(st, buf)
	case IntType:
		return decodeIntDatum(st, buf)
	case FloatType:
		return decodeFloatDatum(st, buf)
	case DecimalType:
		return decodeDecimalDatum(st, buf)
	case TimestampType:
		return decodeTimestampDatum(st, buf)
	case SymbolType:
		return decodeSymbolDatum(st, buf)
	case StringType:
		return decodeBytesDatum(st, buf)
	case ClobType:
		return decodeBytesDatum(st, buf)
	case BlobType:
		return decodeBytesDatum(st, buf)
	case ListType:
		return decodeListDatum(st, buf)
	case SexpType:
		return decodeListDatum(st, buf)
	case StructType:
		return decodeStructDatum(st, buf)
	case AnnotationType:
		return decodeAnnotationDatum(st, buf)
	case ReservedType:
		return decodeReserved(st, buf)
	default:
		return Empty, nil, fmt.Errorf("unsupported type: %x", t)
	}
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
	return a.Equal(b)
}

func stoverlap(st1, st2 []string) bool {
	return stcontains(st1, st2) || stcontains(st2, st1)
}
