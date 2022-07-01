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
	"io"
	"math"
	"math/bits"

	"github.com/SnellerInc/sneller/date"
)

type segkind int

const (
	segstruct segkind = iota
	seglist
	segannotation
)

type segment struct {
	off, width int
	kind       segkind
}

// Buffer buffers ion objects.
//
// The contents of Buffer can be
// inspected directly with Buffer.Bytes()
// or written to an io.Writer with
// Buffer.WriteTo.
type Buffer struct {
	buf  []byte
	segs []segment
	//
	// TODO: cache the most recent size
	// of segments at each depth and use
	// that to determine the initial guess
	// for the size of the TLV bytes?
}

// Save takes a snapshot of the current state of the
// buffer.
func (b *Buffer) Save(snap *Snapshot) {
	snap.buf = b.buf
	snap.segs = append(snap.segs[:0], b.segs...)
}

// Load resets the buffer to the state at the time the
// snapshot was saved.
func (b *Buffer) Load(snap *Snapshot) {
	b.buf = snap.buf
	b.segs = append(b.segs[:0], snap.segs...)
}

// Set sets the buffer used by 'b'
// and resets the state of the buffer.
// Subsequent calls to Write* functions
// on 'b' will append to the given buffer.
func (b *Buffer) Set(p []byte) {
	b.Reset()
	b.buf = p
}

// Symbol represents an ion Symbol
type Symbol uint

func (u Symbol) Type() Type                     { return SymbolType }
func (u Symbol) Encode(dst *Buffer, st *Symtab) { dst.WriteSymbol(u) }

// BeginStruct begins a structure.
// Fields of the structure should
// be written with paired calls
// to BeginField and one of the Write* methods,
// followed by Buffer.EndStruct.
func (b *Buffer) BeginStruct(hint int) {
	// TODO: use hint
	b.segs = append(b.segs, segment{
		off:   len(b.buf),
		width: 2,
		kind:  segstruct,
	})
	b.buf = append(b.buf, 0xde, 0)
}

// uvsize returns the encoded size
// of value as a uvarint
func uvsize(value uint) int {
	// a bit of a hack: oring in 1
	// does not change the result except
	// for the number 0, because we need
	// bits.Len to return 1 in that case
	return (bits.Len(value|1) + 6) / 7
}

func (b *Buffer) term(seg *segment) {
	size := len(b.buf) - (seg.off + seg.width)
	if size < 14 {
		// we over-allocated...
		if seg.width > 1 {
			copy(b.buf[seg.off+1:], b.buf[seg.off+seg.width:])
			b.buf = b.buf[:seg.off+1+size]
		}
		b.buf[seg.off] = byte(b.buf[seg.off]&0xf0) | byte(size)
		return
	}
	// need one byte for descriptor
	// plus space for the uvarint
	needwidth := uvsize(uint(size)) + 1
	if seg.width != needwidth {
		// if we didn't allocate enough space,
		// make sure there is enough space at
		// the end of the buffer to allow the
		// data to be shifted right
		for s := seg.width; s < needwidth; s++ {
			b.buf = append(b.buf, 0)
		}
		// copy (forwards or backwards)
		// in order to produce the correct number
		// of leading bytes...
		n := copy(b.buf[seg.off+needwidth:], b.buf[seg.off+seg.width:])
		seg.width = needwidth
		b.buf = b.buf[:seg.off+seg.width+n]
	}
	// put uvarint for segment size
	b.buf[seg.off] = byte(b.buf[seg.off]&0xf0) | 0xe
	for i := seg.width - 1; i > 0; i-- {
		b.buf[seg.off+i] = byte(size & 0x7f)
		size >>= 7
	}
	b.buf[seg.off+seg.width-1] |= 0x80
}

// EndStruct ends a structure.
//
// If EndStruct is not paired with a
// corresponding BeginStruct call, it
// will panic.
func (b *Buffer) EndStruct() {
	s := &b.segs[len(b.segs)-1]
	if s.kind != segstruct {
		panic("EndStruct() called when current segment is not a struct")
	}
	b.segs = b.segs[:len(b.segs)-1]
	b.term(s)
}

// BeginList begins a list object.
// Subsequent calls to the Buffer.Write*
// methods will write list elements until
// Buffer.EndList is called.
func (b *Buffer) BeginList(hint int) {
	// TODO: use hint
	b.segs = append(b.segs, segment{
		off:   len(b.buf),
		width: 1, // assume list is short
		kind:  seglist,
	})
	b.buf = append(b.buf, 0xb0)
}

// EndList ends a list object.
//
// If EndList is not paried with a
// corresponding BeginList call,
// it will panic.
func (b *Buffer) EndList() {
	s := &b.segs[len(b.segs)-1]
	if s.kind != seglist {
		panic("EndList() called when current segment is not a list")
	}
	b.segs = b.segs[:len(b.segs)-1]
	b.term(s)
}

// BeginAnnotation begins an annotation object.
// 'labels' should indicate the number annotation
// fields before the wrapped object, and must
// be greater than zero.
func (b *Buffer) BeginAnnotation(labels int) {
	b.segs = append(b.segs, segment{
		off:   len(b.buf),
		width: 2,
		kind:  segannotation,
	})
	b.buf = append(b.buf, 0xe0, 0)
	// put the number of annotations
	b.putuv(uint(labels))
}

// EndAnnotation ends an annotation object.
func (b *Buffer) EndAnnotation() {
	// FIXME: verify that the number
	// of objects supplied to BeginAnnotation()
	// were actually written.
	s := &b.segs[len(b.segs)-1]
	if s.kind != segannotation {
		panic("EndAnnotation() called when current segment is not an annotation")
	}
	b.segs = b.segs[:len(b.segs)-1]
	b.term(s)
}

// get the next 'n' bytes at the end of the buffer
func (b *Buffer) grow(n int) []byte {
	off := len(b.buf)
	if cap(b.buf)-off >= n {
		b.buf = b.buf[:off+n]
	} else {
		nb := make([]byte, off+n, n+(2*off))
		copy(nb, b.buf)
		b.buf = nb
	}
	return b.buf[off:]
}

// write an integer as a uvarint
func (b *Buffer) putuv(s uint) {
	dst := b.grow(uvsize(s))
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = byte(s & 0x7f)
		s >>= 7
	}
	dst[len(dst)-1] |= 0x80
}

// BeginField begins a field of a structure
// or a label of an annotation.
// BeginField will panic if the buffer is not
// in an appropriate structure field context
func (b *Buffer) BeginField(sym Symbol) {
	b.putuv(uint(sym))
}

// WriteBool writes a bool into the buffer
func (b *Buffer) WriteBool(n bool) {
	bt := byte(0x10)
	if n {
		bt++
	}
	b.buf = append(b.buf, bt)
}

// WriteNull writes an ion NULL value into the buffer
func (b *Buffer) WriteNull() {
	b.buf = append(b.buf, 0x0f)
}

// BeginString begins a string object with
// the given size.
// Typically this call is followed by
// calls to UnsafeAppend() to add
// the rest of the string contents.
func (b *Buffer) BeginString(size int) {
	if size < 14 {
		b.buf = append(b.buf, 0x80|byte(size))
	} else {
		b.buf = append(b.buf, 0x8e)
		b.putuv(uint(size))
	}
}

// WriteString writes a string as an ion string
// into a Buffer
func (b *Buffer) WriteString(s string) {
	b.BeginString(len(s))
	copy(b.grow(len(s)), s)
}

// WriteInt writes an integer to the buffer.
func (b *Buffer) WriteInt(i int64) {
	mag := uint64(i)
	pre := byte(0x20)
	if i < 0 {
		mag = uint64(-i)
		pre = 0x30
	}
	b.writeint(mag, pre)
}

func (b *Buffer) writeint(mag uint64, pre byte) {
	// size of integer in bytes
	size := (bits.Len64(mag) + 7) >> 3
	b.buf = append(b.buf, pre|byte(size))
	mag = bits.ReverseBytes64(mag)
	mag >>= (8 - size) * 8
	for size != 0 {
		b.buf = append(b.buf, byte(mag))
		mag >>= 8
		size--
	}
}

func (b *Buffer) WriteSymbol(s Symbol) {
	b.writeint(uint64(s), 0x70)
}

// WriteUint writes an unsigned integer to the buffer.
func (b *Buffer) WriteUint(u uint64) {
	b.writeint(u, 0x20)
}

// WriteFloat64 writes an ion float64 to the buffer
func (b *Buffer) WriteFloat64(f float64) {
	if f == 0.0 {
		b.buf = append(b.buf, 0x40)
		return
	}
	dst := b.grow(9)
	dst[0] = 0x48
	binary.BigEndian.PutUint64(dst[1:], math.Float64bits(f))
}

// WriteFloat32 writes an ion float32 to the buffer
func (b *Buffer) WriteFloat32(f float32) {
	if f == 0.0 {
		b.buf = append(b.buf, 0x40)
		return
	}
	dst := b.grow(5)
	dst[0] = 0x44
	binary.BigEndian.PutUint32(dst[1:], math.Float32bits(f))
}

func (b *Buffer) WriteCanonicalFloat(f float64) {
	if f >= 0 {
		if u := uint64(f); float64(u) == f {
			b.WriteUint(u)
			return
		}
	}
	if i := int64(f); float64(i) == f {
		b.WriteInt(i)
		return
	}
	b.WriteFloat64(f)
}

// WriteBlob writes a []byte as an ion 'blob' to the buffer.
func (b *Buffer) WriteBlob(p []byte) {
	if len(p) < 14 {
		b.buf = append(b.buf, 0xa0|byte(len(p)))
	} else {
		b.buf = append(b.buf, 0xae)
		b.putuv(uint(len(p)))
	}
	copy(b.grow(len(p)), p)
}

// WriteTo implements io.WriterTo
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	i, err := w.Write(b.buf)
	return int64(i), err
}

// WriteTime writes a date.Date as an ion timestamp object.
//
// WriteTime only supports microsecond-precision timestamps.
func (b *Buffer) WriteTime(t date.Time) {
	year := t.Year()
	nano := t.Nanosecond()
	micro := nano / 1000
	length := 8

	if micro != 0 {
		length += 4
	}

	desc := byte(TimestampType<<4) | byte(length)
	b.buf = append(b.buf,
		desc, // descriptor byte
		0x80, // offset = 0
		byte(year>>7),
		byte(year&0x7f)|0x80,
		byte(t.Month()|0x80),
		byte(t.Day()|0x80),
		byte(t.Hour()|0x80),
		byte(t.Minute()|0x80),
		byte(t.Second()|0x80),
	)

	if micro != 0 {
		b.buf = append(b.buf,
			0xC0|0x06, // fraction_exponent = -6,
			byte(micro>>16),
			byte(micro>>8),
			byte(micro),
		)
	}
}

type TimeTrunc int

const (
	TruncToYear TimeTrunc = iota
	TruncToMonth
	TruncToDay
	TruncToHour
	TruncToMinute
	TruncToSecond
)

// truncate returns d with truncation applied.
func (t TimeTrunc) truncate(d date.Time) date.Time {
	switch t {
	case TruncToYear:
		return date.Date(d.Year(), 1, 1, 0, 0, 0, 0)
	case TruncToMonth:
		return date.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0)
	case TruncToDay:
		return date.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0)
	case TruncToHour:
		return date.Date(d.Year(), d.Month(), d.Day(), d.Hour(), 0, 0, 0)
	case TruncToMinute:
		return date.Date(d.Year(), d.Month(), d.Day(), d.Hour(), d.Minute(), 0, 0)
	case TruncToSecond:
		fallthrough
	default:
		return date.Date(d.Year(), d.Month(), d.Day(), d.Hour(), d.Minute(), d.Second(), 0)
	}
}

// WriteTruncatedTime writes a date.Date as an ion timestamp object and
// lets the caller decide how precise the output is.
func (b *Buffer) WriteTruncatedTime(t date.Time, trunc TimeTrunc) {
	year := t.Year()

	var size byte
	switch trunc {
	case TruncToYear:
		size = 3
	case TruncToMonth:
		size = 4
	case TruncToDay:
		size = 5
	case TruncToHour:
		size = 6
	case TruncToMinute:
		size = 7
	case TruncToSecond:
		size = 8
	}

	buf := b.grow(int(size) + 1)

	buf[0] = byte(TimestampType<<4) | size
	buf[1] = 0x80 // offset = 0
	buf[2] = byte(year >> 7)
	buf[3] = byte(year&0x7f) | 0x80
	if trunc == TruncToYear {
		return
	}

	buf[4] = byte(t.Month() | 0x80)
	if trunc == TruncToMonth {
		return
	}

	buf[5] = byte(t.Day() | 0x80)
	if trunc == TruncToDay {
		return
	}

	buf[6] = byte(t.Hour() | 0x80)
	if trunc == TruncToHour {
		return
	}

	buf[7] = byte(t.Minute() | 0x80)
	if trunc == TruncToMinute {
		return
	}

	buf[8] = byte(t.Second() | 0x80)
	// case: trunc == TruncToSecond
}

// Bytes returns the current contents of the buffer.
func (b *Buffer) Bytes() []byte { return b.buf }

// Reset resets a buffer to its initial state.
func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
	b.segs = b.segs[:0]
}

// Ok returns false if there are any
// open calls to BeginStruct or BeginList
// that have not been paired with
// EndStruct or EndList, respectively.
func (b *Buffer) Ok() bool {
	return len(b.segs) == 0
}

// StartChunk writes BVM marker and symtab.
func (b *Buffer) StartChunk(symtab *Symtab) {
	bvm := true
	symtab.Marshal(b, bvm)
}

// UnsafeAppend appends arbitrary data
// to the buffer.
func (b *Buffer) UnsafeAppend(buf []byte) {
	copy(b.grow(len(buf)), buf)
}

// Size returns the number of bytes in the buffer.
func (b *Buffer) Size() int {
	return len(b.buf)
}
