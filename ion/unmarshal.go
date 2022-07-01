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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/SnellerInc/sneller/date"
)

// Type is one of the ion datatypes
type Type byte

const (
	NullType Type = iota
	BoolType
	UintType // unsigned integer
	IntType  // signed integer; always negative
	FloatType
	DecimalType
	TimestampType
	SymbolType
	StringType
	ClobType
	BlobType
	ListType
	SexpType
	StructType
	AnnotationType
	ReservedType
)

func (t Type) String() string {
	switch t {
	case NullType:
		return "null"
	case BoolType:
		return "bool"
	case UintType:
		return "uint"
	case IntType:
		return "int"
	case FloatType:
		return "float"
	case DecimalType:
		return "decimal"
	case TimestampType:
		return "timestamp"
	case SymbolType:
		return "symbol"
	case StringType:
		return "string"
	case ClobType:
		return "clob"
	case BlobType:
		return "blob"
	case ListType:
		return "list"
	case SexpType:
		return "sexp"
	case StructType:
		return "struct"
	case AnnotationType:
		return "annotation"
	case ReservedType:
		return "reserved"
	default:
		return "invalid"
	}
}

// TypeOf returns the type of the
// next object in the buffer
func TypeOf(msg []byte) Type {
	return Type(msg[0] >> 4)
}

// DecodeTLV explodes TLV byte into: type (t), raw length (l)
func DecodeTLV(b byte) (t Type, l byte) {
	t = Type(b >> 4)
	l = b & 0x0f
	return
}

// SizeOf returns the size of the next
// ion object, including the beginning
// TLV descriptor bytes.
//
// The return value of SizeOf is unspecified
// when msg is not a valid ion object.
func SizeOf(msg []byte) int {
	if len(msg) == 0 {
		return -1
	}
	if msg[0] == 0x11 {
		return 1
	}
	lo := msg[0] & 0x0f
	switch lo {
	case 0x0f:
		return 1
	case 0x0e:
		out := 0
		i := 0
		rest := msg[1:]
		if len(rest) > 8 {
			// guard against overflow
			rest = rest[:8]
		}
		for i = range rest {
			out <<= 7
			out += int(rest[i] & 0x7f)
			if rest[i]&0x80 != 0 {
				return out + i + 2
			}
		}
		return -1 // unterminated rest
	default:
		return int(lo) + 1
	}
}

// Contents parses the TLV descriptor
// at the beginning of 'msg' and returns
// the bytes that correspond to the
// non-descriptor bytes of the object,
// plus the remaining bytes in the buffer
// as the second return value.
// The returned []byte will be nil if
// the encoded object size does not
// fit into 'msg'. (Note that a returned
// slice that is zero-length but non-nil
// means something different than a nil slice.)
func Contents(msg []byte) ([]byte, []byte) {
	if len(msg) == 0 {
		return nil, msg
	}
	if msg[0] == 0x11 {
		return msg[:0], msg[1:]
	}
	lo := msg[0] & 0x0f
	if lo == 0x0f {
		return msg[:0], msg[1:]
	}
	if lo < 0x0e {
		if len(msg) < int(lo)+1 {
			return nil, msg
		}
		return msg[1 : 1+lo], msg[1+lo:]
	}

	// lo must be equal to 0x0e
	rest := msg[1:]
	out := 0
	i := 0
	for i = range rest {
		out <<= 7
		out += int(rest[i] & 0x7f)
		if rest[i]&0x80 != 0 {
			if len(rest) < i+out+1 || out < 0 {
				return nil, msg
			}
			return rest[i+1 : i+out+1], rest[i+out+1:]
		}
	}
	return nil, msg
}

// Composite returns whether or not
// the type is an object containing
// other objects.
func (t Type) Composite() bool {
	switch t {
	case ListType, SexpType, StructType:
		return true
	default:
		return false
	}
}

// Integer returns whether or not
// the type is an integer type
// (either IntType or UintType).
func (t Type) Integer() bool {
	switch t {
	case IntType, UintType:
		return true
	default:
		return false
	}
}

// TypeError is the type of the error
// returned from read operations that
// try to evaluate a function that
// is typed incorrectly for the encoded data.
type TypeError struct {
	Wanted, Found Type
	Func          string
}

func (t *TypeError) Error() string {
	return fmt.Sprintf("ion.%s: found type %s, wanted type %s", t.Func, t.Found, t.Wanted)
}

func bad(got, want Type, fn string) error {
	return &TypeError{Wanted: want, Found: got, Func: fn}
}

func toosmall(got, want int, fn string) error {
	return fmt.Errorf("ion.%s: want at least %d bytes but have %d", fn, want, got)
}

var errInvalidIon = fmt.Errorf("invalid TLV encoding bytes")

func expectedinttype(got Type, fn string) error {
	return fmt.Errorf("ion.%s: found type %s, wanted an integer type", fn, got)
}

// ReadString reads a string from 'msg'
// and returns the string and the subsequent
// message bytes.
func ReadString(msg []byte) (string, []byte, error) {
	if t := TypeOf(msg); t != StringType {
		return "", nil, bad(t, StringType, "ReadString")
	}
	body, rest := Contents(msg)
	if body == nil {
		return "", nil, errInvalidIon
	}
	return string(body), rest, nil
}

// ReadBytesShared read a []byte (as an ion 'blob')
// and returns the blob and the subsequent
// message bytes. Note that the returned []byte
// aliases the input message, so the caller
// must copy those bytes into a new buffer if
// the original buffer is expected to be clobbered.
func ReadBytesShared(msg []byte) ([]byte, []byte, error) {
	if t := TypeOf(msg); t != BlobType {
		return nil, nil, bad(t, BlobType, "ReadBytesShared")
	}
	body, rest := Contents(msg)
	if body == nil {
		return nil, nil, errInvalidIon
	}
	return body, rest, nil
}

// ReadStringShared reads a string from 'msg'
// and returns the string and the subsequent
// message bytes. The returned slice containing
// the string contents aliases the input slice.
func ReadStringShared(msg []byte) ([]byte, []byte, error) {
	if t := TypeOf(msg); t != StringType {
		return nil, nil, bad(t, StringType, "ReadString")
	}
	body, rest := Contents(msg)
	if body == nil {
		return nil, nil, errInvalidIon
	}
	return body, rest, nil
}

// ReadBytes reads an ion blob from message.
// The returned slice does not alias msg.
// See also: ReadBytesShared.
func ReadBytes(msg []byte) ([]byte, []byte, error) {
	orig, rest, err := ReadBytesShared(msg)
	if err != nil {
		return nil, rest, err
	}
	out := make([]byte, len(orig))
	copy(out, orig)
	return out, rest, err
}

// ReadFloat64 reads an ion float as a float64
// and returns the value and the subsequent
// message bytes.
func ReadFloat64(msg []byte) (float64, []byte, error) {
	switch msg[0] {
	case 0x40:
		return 0.0, msg[1:], nil
	case 0x44:
		if len(msg) < 5 {
			return 0, nil, toosmall(len(msg), 5, "ReadFloat64")
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(msg[1:]))), msg[5:], nil
	case 0x48:
		if len(msg) < 9 {
			return 0, nil, toosmall(len(msg), 9, "ReadFloat64")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(msg[1:])), msg[9:], nil
	}
	if t := TypeOf(msg); t != FloatType {
		return 0, nil, bad(t, FloatType, "ReadFloat64")
	}
	return 0, nil, fmt.Errorf("ReadFloat64: cannot parse descriptor %x", msg[0])
}

// ReadFloat32 reads an ion flaot as a float32
// and returns the value and the subsequent
// message bytes.
func ReadFloat32(msg []byte) (float32, []byte, error) {
	switch msg[0] {
	case 0x40:
		return 0.0, msg[1:], nil
	case 0x44:
		if len(msg) < 5 {
			return 0, nil, toosmall(len(msg), 5, "ReadFloat32")
		}
		return math.Float32frombits(binary.BigEndian.Uint32(msg[1:])), msg[5:], nil
	}
	if t := TypeOf(msg); t != FloatType {
		return 0, nil, bad(t, FloatType, "ReadFloat32")
	}
	return 0, nil, fmt.Errorf("ReadFloat32: cannot parse descriptor %x", msg[0])
}

func readmag(msg []byte) uint64 {
	u := uint64(0)
	for i := range msg {
		u <<= 8
		u |= uint64(msg[i])
	}
	return u
}

// ReadInt reads an ion integer as an int64
// and returns the subsequent message bytes
func ReadInt(msg []byte) (int64, []byte, error) {
	t := TypeOf(msg)
	if t < UintType || t > IntType {
		return 0, nil, bad(t, IntType, "ReadInt")
	}
	body, rest := Contents(msg)
	if body == nil {
		return 0, nil, errInvalidIon
	}
	if len(body) > 8 {
		return 0, nil, fmt.Errorf("integer of %d bytes out of range", len(body))
	}
	mag := readmag(body)
	max := uint64(math.MaxInt64)
	if t == IntType {
		max++
	}
	if mag > max {
		return 0, nil, fmt.Errorf("ion.ReadInt: magnitude %d out of range for int64", mag)
	}
	v := int64(mag)
	if t == IntType {
		v = -v
	}
	return v, rest, nil
}

// ReadUint reads an ion integer as a uint64
// and returns the subsequent message bytes
func ReadUint(msg []byte) (uint64, []byte, error) {
	if t := TypeOf(msg); t != UintType {
		return 0, nil, bad(t, UintType, "ReadUint")
	}
	body, rest := Contents(msg)
	if body == nil {
		return 0, nil, errInvalidIon
	}
	if len(body) > 8 {
		return 0, nil, fmt.Errorf("ion.ReadUint: integer of %d bytes out of range", len(body))
	}
	return readmag(body), rest, nil
}

// ReadSymbol reads an ion symbol
// from msg and returns the subsequent message bytes,
// or an error if one is encountered.
func ReadSymbol(msg []byte) (Symbol, []byte, error) {
	if t := TypeOf(msg); t != SymbolType {
		return 0, nil, bad(t, SymbolType, "ReadSymbol")
	}
	body, rest := Contents(msg)
	if body == nil {
		return 0, nil, errInvalidIon
	}
	if len(body) > 4 {
		return 0, nil, fmt.Errorf("ion.ReadSymbol: integer of %d bytes out of range", len(body))
	}
	return Symbol(readmag(body)), rest, nil
}

// ReadIntMagnitude reads magnitude of an integer (either signed or unsigned)
// and returns the subsequent message bytes
func ReadIntMagnitude(msg []byte) (uint64, []byte, error) {
	t, L := DecodeTLV(msg[0])
	if t != IntType && t != UintType {
		return 0, nil, expectedinttype(t, "ReadIntMagnitude")
	}
	if L <= 8 && len(msg) >= int(L)+1 {
		switch L {
		case 0:
			return 0, msg[1:], nil
		case 1:
			return uint64(msg[1]), msg[2:], nil
		case 2:
			_ = msg[2]
			val := (uint64(msg[1]) << 8) | uint64(msg[2])
			return val, msg[3:], nil
		case 3:
			_ = msg[3]
			val := (uint64(msg[1]) << 16) | (uint64(msg[2]) << 8) | uint64(msg[3])
			return val, msg[4:], nil
		case 4:
			_ = msg[4]
			val := (uint64(msg[1]) << 24) | (uint64(msg[2]) << 16) | (uint64(msg[3]) << 8) | uint64(msg[4])
			return val, msg[5:], nil
		case 5:
			_ = msg[5]
			val := (uint64(msg[1]) << 32) | (uint64(msg[2]) << 24) | (uint64(msg[3]) << 16) | (uint64(msg[4]) << 8) | uint64(msg[5])
			return val, msg[6:], nil
		case 6:
			_ = msg[6]
			val := (uint64(msg[1]) << 40) | (uint64(msg[2]) << 32) | (uint64(msg[3]) << 24) | (uint64(msg[4]) << 16) | (uint64(msg[5]) << 8) | uint64(msg[6])
			return val, msg[7:], nil
		case 7:
			_ = msg[7]
			val := (uint64(msg[1]) << 48) | (uint64(msg[2]) << 40) | (uint64(msg[3]) << 32) | (uint64(msg[4]) << 24) | (uint64(msg[5]) << 16) | (uint64(msg[6]) << 8) | uint64(msg[7])
			return val, msg[8:], nil
		case 8:
			_ = msg[8]
			val := (uint64(msg[1]) << 56) | (uint64(msg[2]) << 48) | (uint64(msg[3]) << 40) | (uint64(msg[4]) << 32) | (uint64(msg[5]) << 24) | (uint64(msg[6]) << 16) | (uint64(msg[7]) << 8) | uint64(msg[8])
			return val, msg[9:], nil
		}
	}

	body, rest := Contents(msg)
	if body == nil {
		return 0, nil, errInvalidIon
	}
	if len(body) > 8 {
		return 0, nil, fmt.Errorf("integer of %d bytes out of range", len(body))
	}
	return readmag(body), rest, nil
}

// ReadBool reads a boolean value
// and returns it along with the
// subsequent message bytes
func ReadBool(msg []byte) (bool, []byte, error) {
	switch msg[0] {
	case 0x10:
		return false, msg[1:], nil
	case 0x11:
		return true, msg[1:], nil
	default:
		return false, nil, bad(TypeOf(msg), BoolType, "ReadBool")
	}
}

// ReadLabel reads a symbol preceding a structure field
// and returns the subsequent message bytes.
func ReadLabel(msg []byte) (Symbol, []byte, error) {
	uv, rest, ok := readuv(msg)
	if !ok {
		return 0, nil, errInvalidIon
	}
	return Symbol(uv), rest, nil
}

// read unsigned varint
func readuv(msg []byte) (uint, []byte, bool) {
	out := uint(0)
	i := 0
	prefix := msg
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	for i = range prefix {
		out <<= 7
		out += uint(prefix[i] & 0x7f)
		if prefix[i]&0x80 != 0 {
			return out, msg[i+1:], true
		}
	}
	return 0, nil, false
}

// read a 1-byte unsigned varint
func readuv1(msg []byte) (uint, []byte, bool) {
	out := uint(msg[0] & 0x7f)
	done := msg[0]&0x80 != 0
	return out, msg[1:], done
}

// read a 1- or 2-byte unsigned varint
func readuv2(msg []byte) (uint, []byte, bool) {
	out := uint(msg[0] & 0x7f)
	if msg[0]&0x80 != 0 {
		return out, msg[1:], true
	}
	if len(msg) < 2 {
		return 0, nil, false
	}
	out = (out << 7) + uint(msg[1]&0x7f)
	done := msg[1]&0x80 != 0
	return out, msg[2:], done
}

// read signed varint
func readiv(msg []byte) (int, []byte, bool) {
	out := int(msg[0] & 0x3f)

	// fast-path: 1-byte varint
	if msg[0]&0x80 != 0 {
		if msg[0]&0x40 != 0 {
			out = -out
		}
		return out, msg[1:], true
	}

	sign := msg[0] & 0x40
	i := 0
	msg = msg[1:]
	done := false
	if len(msg) > 0 {
		for i = range msg {
			out <<= 7
			out |= int(msg[i] & 0x7f)
			if msg[i]&0x80 != 0 {
				done = true
				break
			}
		}
		msg = msg[i+1:]
	} else {
		done = true
	}
	if sign != 0 {
		out = -out
	}
	return out, msg, done
}

// read a 1-byte signed varint
func readiv1(msg []byte) (int, []byte, bool) {
	out := int(msg[0] & 0x3f)
	if msg[0]&0x40 != 0 {
		out = -out
	}
	done := msg[0]&0x80 != 0
	return out, msg[1:], done
}

// read a fixed-width integer with a sign bit
func readint(msg []byte) int64 {
	out := int64(msg[0] & 0x7f)
	sign := msg[0] >> 7
	rest := msg[1:]
	for i := range rest {
		out <<= 8
		out += int64(rest[i])
	}
	if sign != 0 {
		out = -out
	}
	return out
}

// ReadTime reads a timestamp object
// and returns the subsequent message bytes.
func ReadTime(msg []byte) (date.Time, []byte, error) {
	if t := TypeOf(msg); t != TimestampType {
		return date.Time{}, nil, bad(t, TimestampType, "ReadTime")
	}

	var out date.Time
	body, rest := Contents(msg)
	var year, month, day, hour, minute, second uint
	month, day = 1, 1
	var offset, fracexp, nsec int
	var frac int64
	var ok bool
	if len(body) == 0 {
		return out, nil, errInvalidIon
	}
	offset, body, ok = readiv(body)
	if !ok || len(body) == 0 {
		return out, nil, errInvalidIon
	}
	year, body, ok = readuv2(body)
	if ok && len(body) > 0 {
		month, body, ok = readuv1(body)
	}
	if ok && len(body) > 0 {
		day, body, ok = readuv1(body)
	}
	if ok && len(body) > 0 {
		hour, body, ok = readuv1(body)
	}
	if ok && len(body) > 0 {
		minute, body, ok = readuv1(body)
	}
	if ok && len(body) > 0 {
		second, body, ok = readuv1(body)
	}
	if ok && len(body) > 0 {
		fracexp, body, ok = readiv1(body)
	}
	if ok && len(body) > 0 {
		frac = readint(body)
	}
	if !ok {
		return out, rest, errInvalidIon
	}
	// TODO: use offset + fractional time components
	_ = offset
	switch fracexp {
	case -6:
		nsec = int(frac) * 1000 // fractional component is exactly microseconds
	case -9:
		nsec = int(frac) // fractional component is exactly nanoseconds
	default:
		// unhandled!
	}
	out = date.Date(int(year), int(month), int(day), int(hour), int(minute), int(second), nsec)
	return out, rest, nil
}

// ReadAnnotation reads an annotation
// and returns the associated label,
// the contents of the annotation, and
// the remaining bytes in buf (in that order).
func ReadAnnotation(buf []byte) (Symbol, []byte, []byte, error) {
	if t := TypeOf(buf); t != AnnotationType {
		return 0, nil, nil, fmt.Errorf("ion.ReadAnnotation: got type %s", t)
	}
	size := SizeOf(buf)
	if size <= 0 || size > len(buf) {
		return 0, nil, nil, fmt.Errorf("ion.ReadAnnotation: size %d > %d", size, len(buf))
	}
	var labels, first uint
	var ok bool
	contents, rest := Contents(buf)
	labels, contents, ok = readuv(contents)
	if !ok {
		return 0, nil, rest, fmt.Errorf("ion.ReadAnnotation: could not read #labels")
	}
	if labels == 0 {
		return 0, nil, rest, fmt.Errorf("ion.ReadAnnotation: 0 labels disallowed")
	}
	first, contents, ok = readuv(contents)
	if !ok {
		return 0, nil, rest, fmt.Errorf("ion.ReadAnnotation: could not read 1st label")
	}
	labels--
	for labels > 0 {
		// strip other labels
		_, contents, ok = readuv(contents)
		if !ok {
			return 0, nil, rest, fmt.Errorf("ion.ReadAnnotation: could not read auxilliary labels")
		}
	}
	return Symbol(first), contents, rest, nil
}

// UnpackList calls fn for each item in a list,
// returning the remaining bytes.
func UnpackList(body []byte, fn func([]byte) error) (rest []byte, err error) {
	if TypeOf(body) != ListType {
		return body, fmt.Errorf("expected a list; found ion type %s", TypeOf(body))
	}
	body, rest = Contents(body)
	if body == nil {
		return rest, fmt.Errorf("invalid list encoding")
	}
	for len(body) > 0 {
		next := SizeOf(body)
		if next <= 0 || next > len(body) {
			return rest, fmt.Errorf("object size %d exceeds buffer size %d", next, len(body))
		}
		err := fn(body[:next])
		if err != nil {
			return rest, err
		}
		body = body[next:]
	}
	return rest, nil
}

// UnpackStruct calls fn for each field in a struct,
// returning the remaining bytes.
func UnpackStruct(st *Symtab, body []byte, fn func(string, []byte) error) (rest []byte, err error) {
	if TypeOf(body) != StructType {
		return body, fmt.Errorf("expected a struct; found ion type %s", TypeOf(body))
	}
	body, rest = Contents(body)
	if body == nil {
		return rest, fmt.Errorf("invalid struct encoding")
	}
	var sym Symbol
	for len(body) > 0 {
		sym, body, err = ReadLabel(body)
		if err != nil {
			return rest, err
		}
		name, ok := st.Lookup(sym)
		if !ok {
			return rest, fmt.Errorf("symbol %d not in symbol table", sym)
		}
		next := SizeOf(body)
		if next <= 0 || next > len(body) {
			return rest, fmt.Errorf("next object size %d exceeds buffer size %d", next, len(body))
		}
		err = fn(name, body[:next])
		if err != nil {
			return rest, err
		}
		body = body[next:]
	}
	return rest, nil
}
