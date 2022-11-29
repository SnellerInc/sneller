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
	"math"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
)

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
		return x.Null()
	case FloatType:
		f, _ := d.Float()
		switch x.Type() {
		case FloatType:
			f2, _ := x.Float()
			return f2 == f || (math.IsNaN(f) && math.IsNaN(f2))
		case IntType:
			i, _ := x.Int()
			return float64(int64(f)) == float64(f) && int64(f) == int64(i)
		case UintType:
			u, _ := x.Uint()
			return float64(uint64(f)) == float64(f) && uint64(f) == uint64(u)
		}
		return false
	case IntType:
		i, _ := d.Int()
		switch x.Type() {
		case IntType:
			x, _ := x.Int()
			return x == i
		case UintType:
			x, _ := x.Uint()
			return i >= 0 && uint64(i) == x
		case FloatType:
			x, _ := x.Float()
			return float64(int64(x)) == float64(x) && int64(x) == int64(i)
		}
		return false
	case UintType:
		u, _ := d.Uint()
		switch x.Type() {
		case UintType:
			x, _ := x.Uint()
			return u == x
		case IntType:
			x, _ := x.Int()
			return x >= 0 && uint64(x) == u
		case FloatType:
			x, _ := x.Float()
			return float64(uint64(x)) == float64(x) && uint64(x) == uint64(u)
		}
		return false
	case StructType:
		s, _ := d.Struct()
		s2, ok := x.Struct()
		return ok && s.Equal(s2)
	case ListType:
		l, _ := d.List()
		l2, ok := x.List()
		return ok && l.Equal(l2)
	case BoolType:
		b, _ := d.Bool()
		b2, ok := x.Bool()
		return ok && b == b2
	case StringType, SymbolType:
		s, _ := d.String()
		s2, ok := x.String()
		return ok && s == s2
	case BlobType:
		b, _ := d.Blob()
		b2, ok := x.Blob()
		return ok && string(b) == string(b2)
	case TimestampType:
		t, _ := d.Timestamp()
		t2, ok := x.Timestamp()
		return ok && t.Equal(t2)
	}
	return false
}

func (d Datum) Type() Type {
	if len(d.buf) == 0 {
		return InvalidType
	}
	return TypeOf(d.buf)
}

func (d Datum) Encode(dst *Buffer, st *Symtab) {
	// fast path: no need to resymbolize
	if len(d.st) == 0 || st.contains(d.st) {
		dst.UnsafeAppend(d.buf)
		return
	}
	switch typ := d.Type(); typ {
	case SymbolType:
		s, _ := d.String()
		dst.WriteSymbol(st.Intern(s))
	case StructType:
		s, _ := d.Struct()
		s.Encode(dst, st)
	case ListType:
		l, _ := d.List()
		l.Encode(dst, st)
	case AnnotationType:
		lbl, val, _ := d.Annotation()
		dst.BeginAnnotation(1)
		dst.BeginField(st.Intern(lbl))
		val.Encode(dst, st)
		dst.EndAnnotation()
	default:
		panic("resymbolizing non-symbolized type: " + typ.String())
	}
}

func (d Datum) Empty() bool {
	return len(d.buf) == 0
}

func (d Datum) Null() bool {
	return d.Type() == NullType
}

func (d Datum) Float() (float64, bool) {
	if d.Type() == FloatType {
		f, _, err := ReadFloat64(d.buf)
		if err != nil {
			panic(err)
		}
		return f, true
	}
	return 0, false
}

func (d Datum) Int() (int64, bool) {
	if d.Type() == IntType {
		i, _, err := ReadInt(d.buf)
		if err != nil {
			panic(err)
		}
		return i, true
	}
	return 0, false
}

func (d Datum) Uint() (uint64, bool) {
	if d.Type() == UintType {
		u, _, err := ReadUint(d.buf)
		if err != nil {
			panic(err)
		}
		return u, true
	}
	return 0, false
}

func (d Datum) Struct() (Struct, bool) {
	if d.Type() == StructType {
		return Struct{st: d.st, buf: d.buf}, true
	}
	return Struct{}, false
}

// Field returns the value associated with the
// field with the given name if d is a struct.
// If d is not a struct or the field is not
// present, this returns Empty.
func (d Datum) Field(name string) Datum {
	s, ok := d.Struct()
	if !ok {
		return Empty
	}
	f, ok := s.FieldByName(name)
	if !ok {
		return Empty
	}
	return f.Value
}

func (d Datum) List() (List, bool) {
	if d.Type() == ListType {
		return List{st: d.st, buf: d.buf}, true
	}
	return List{}, false
}

func (d Datum) Annotation() (string, Datum, bool) {
	if d.Type() == AnnotationType {
		sym, body, _, err := ReadAnnotation(d.buf)
		if err != nil {
			panic(err)
		}
		st := d.symtab()
		s, ok := st.Lookup(sym)
		if !ok {
			panic("ion.Datum.Annotation: missing symbol")
		}
		return s, Datum{st: d.st, buf: body}, true
	}
	return "", Empty, false
}

func (d Datum) Bool() (v, ok bool) {
	if d.Type() == BoolType {
		b, _, err := ReadBool(d.buf)
		if err != nil {
			panic(err)
		}
		return b, true
	}
	return false, false
}

func (d Datum) Symbol() (Symbol, bool) {
	if d.Type() == SymbolType {
		sym, _, err := ReadSymbol(d.buf)
		if err != nil {
			panic(err)
		}
		return sym, true
	}
	return 0, false
}

func (d Datum) String() (string, bool) {
	switch d.Type() {
	case StringType:
		s, _, err := ReadString(d.buf)
		if err != nil {
			panic(err)
		}
		return s, true
	case SymbolType:
		sym, _ := d.Symbol()
		st := d.symtab()
		s, ok := st.Lookup(sym)
		if !ok {
			panic("ion.Datum.String: missing symbol")
		}
		return s, true
	}
	return "", false
}

func (d Datum) Blob() ([]byte, bool) {
	if d.Type() == BlobType {
		b, _ := Contents(d.buf)
		if b == nil {
			panic("ion.Datum.Blob: invalid ion")
		}
		return b, true
	}
	return nil, false
}

func (d Datum) Timestamp() (date.Time, bool) {
	if d.Type() == TimestampType {
		t, _, err := ReadTime(d.buf)
		if err != nil {
			panic(err)
		}
		return t, true
	}
	return date.Time{}, false
}

func (d *Datum) symtab() Symtab {
	return Symtab{interned: d.st}
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
	Value Datum
	Sym   Symbol // symbol, if assigned
}

func (f *Field) Equal(f2 *Field) bool {
	return f.Label == f2.Label && f.Sym == f2.Sym && f.Value.Equal(f2.Value)
}

type composite struct {
	st  []string
	buf []byte
	_   struct{} // disallow conversion to Datum
}

var emptyStruct = []byte{0xd0}

// Struct is an ion structure datum
type Struct composite

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
		b.BeginField(st.Intern(f[i].Label))
		f[i].Value.Encode(b, st)
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
	if s.Empty() || st.contains(s.st) {
		dst.UnsafeAppend(s.bytes())
		return
	}
	dst.BeginStruct(-1)
	s.Each(func(f Field) bool {
		dst.BeginField(st.Intern(f.Label))
		f.Value.Encode(dst, st)
		return true
	})
	dst.EndStruct()
}

func (s Struct) Equal(s2 Struct) bool {
	if s.Empty() {
		return s2.Empty()
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

func (s Struct) Len() int {
	if s.Empty() {
		return 0
	}
	n := 0
	s.Each(func(Field) bool {
		n++
		return true
	})
	return n
}

func (s *Struct) Empty() bool {
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

// Each calls fn for each field in the struct. If fn
// returns false, Each returns early. Each may return
// a non-nil error if the original struct encoding
// was malformed.
func (s Struct) Each(fn func(Field) bool) error {
	if s.Empty() {
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
func (s Struct) Fields(fields []Field) []Field {
	fields = slices.Grow(fields, s.Len())
	s.Each(func(f Field) bool {
		fields = append(fields, f)
		return true
	})
	return fields
}

func (s Struct) Field(x Symbol) (Field, bool) {
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

func (s Struct) FieldByName(name string) (Field, bool) {
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

// mergeFields merges the given fields with the
// fields of this structinto a new struct,
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
	s.Each(func(f Field) bool {
		add(f)
		return true
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
type List composite

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
	if l.empty() || st.contains(l.st) {
		dst.UnsafeAppend(l.bytes())
		return
	}
	dst.BeginList(-1)
	l.Each(func(d Datum) bool {
		d.Encode(dst, st)
		return true
	})
	dst.EndList()
}

func (l List) Len() int {
	if l.empty() {
		return 0
	}
	n := 0
	l.Each(func(Datum) bool {
		n++
		return true
	})
	return n
}

func (l *List) empty() bool {
	if len(l.buf) == 0 {
		return true
	}
	body, _ := Contents(l.buf)
	return len(body) == 0
}

func (l *List) bytes() []byte {
	if l.empty() {
		return emptyList
	}
	return l.buf
}

// Each iterates over each datum in the
// list and calls fn on each datum in order.
// Each returns when it encounters an internal error
// (due to malformed ion) or when fn returns false.
func (l List) Each(fn func(Datum) bool) error {
	if l.empty() {
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

func (l List) Items(items []Datum) []Datum {
	items = slices.Grow(items, l.Len())
	l.Each(func(d Datum) bool {
		items = append(items, d)
		return true
	})
	return items
}

func (l *List) symtab() Symtab {
	return Symtab{interned: l.st}
}

func (l List) Equal(l2 List) bool {
	if l.empty() {
		return l2.empty()
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
	if val.Empty() {
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
	for len(body) > 0 {
		var err error
		body, err = validateDatum(st, body)
		if err != nil {
			return Empty, nil, err
		}
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
	for len(fields) > 0 {
		var sym Symbol
		var err error
		sym, fields, err = ReadLabel(fields)
		if err != nil {
			return Empty, nil, err
		}
		if len(fields) == 0 {
			return Empty, nil, io.ErrUnexpectedEOF
		}
		_, ok := st.Lookup(sym)
		if !ok {
			return Empty, nil, fmt.Errorf("symbol %d not in symbol table", sym)
		}
		fields, err = validateDatum(st, fields)
		if err != nil {
			return Empty, nil, err
		}
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

var _datumTable = [...](func(*Symtab, []byte) (Datum, []byte, error)){
	NullType:       decodeNullDatum,
	BoolType:       decodeBoolDatum,
	UintType:       decodeUintDatum,
	IntType:        decodeIntDatum,
	FloatType:      decodeFloatDatum,
	DecimalType:    decodeDecimalDatum,
	TimestampType:  decodeTimestampDatum,
	SymbolType:     decodeSymbolDatum,
	StringType:     decodeBytesDatum,
	ClobType:       decodeBytesDatum, // fixme: treat clob differently than blob?
	BlobType:       decodeBytesDatum,
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
	return a.Equal(b)
}

func stoverlap(st1, st2 []string) bool {
	return stcontains(st1, st2) || stcontains(st2, st1)
}
