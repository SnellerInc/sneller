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
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"

	"github.com/SnellerInc/sneller/date"
)

// Peek peeks at the type and size of
// the next object in 'r'.
//
// The returned size can be used to
// read only the next object from the buffer.
// For example:
//
//  t, s, _ := Peek(r)
//  buf := make([]byte, s)
//  io.ReadFull(r, buf)
//
func Peek(r *bufio.Reader) (Type, int, error) {
	p, err := r.Peek(10)
	if len(p) == 0 {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, err
	}
	// drop BVM
	prefix := 0
	if IsBVM(p) {
		p = p[4:]
		prefix = 4
	}
	return TypeOf(p), SizeOf(p) + prefix, nil
}

type jswriter interface {
	io.Writer
	io.ByteWriter
	WriteString(s string) (int, error)
}

func toJSON(st *Symtab, w jswriter, buf []byte, s *scratch) (int, []byte, error) {
	switch TypeOf(buf) {
	case NullType:
		if buf[0]&0x0f != 0x0f {
			// the outer loop handles pads that
			// are not 1 byte, so this pad *must*
			// be a one-byte pad if it is not the null object
			return 0, buf[1:], nil
		}
		n, err := w.WriteString("null")
		return n, buf[1:], err
	case BoolType:
		b, rest, err := ReadBool(buf)
		if err != nil {
			return 0, rest, fmt.Errorf("ToJSON: %w", err)
		}
		var n int
		if b {
			n, err = w.WriteString("true")
		} else {
			n, err = w.WriteString("false")
		}
		return n, rest, err
	case UintType:
		u, rest, err := ReadUint(buf)
		if err != nil {
			return 0, rest, fmt.Errorf("ToJSON: %w", err)
		}
		n, err := w.Write(s.uint(u))
		return n, rest, err
	case IntType:
		i, rest, err := ReadInt(buf)
		if err != nil {
			return 0, rest, fmt.Errorf("ToJSON: %w", err)
		}
		n, err := w.Write(s.int(i))
		return n, rest, err
	case FloatType:
		// in order to preserve precision,
		// handle float32 differently than float64
		if buf[0] == 0x44 {
			f, rest, err := ReadFloat32(buf)
			if err != nil {
				return 0, rest, fmt.Errorf("ToJSON: %w", err)
			}
			n, err := w.Write(s.f32(f))
			return n, rest, err
		}
		f, rest, err := ReadFloat64(buf)
		if err != nil {
			return 0, buf, fmt.Errorf("ToJSON: %w", err)
		}
		n, err := w.Write(s.f64(f))
		return n, rest, err
	case DecimalType:
		return 0, buf, fmt.Errorf("ToJSON: decimal not implemented")
	case TimestampType:
		t, rest, err := ReadTime(buf)
		if err != nil {
			return 0, rest, fmt.Errorf("ToJSON: %w", err)
		}
		n, err := w.Write(s.time(t))
		return n, rest, err
	case SymbolType:
		sym, rest, err := ReadSymbol(buf)
		if err != nil {
			return 0, rest, fmt.Errorf("ToJSON: %w", err)
		}
		n, err := w.Write(s.string(st.Get(sym)))
		return n, rest, err
	case StringType:
		body, rest := Contents(buf)
		if body == nil {
			return 0, buf, fmt.Errorf("ToJSON: bad string")
		}
		n, err := w.Write(s.quoted(body))
		return n, rest, err
	case ClobType, BlobType:
		body, rest := Contents(buf)
		if body == nil {
			return 0, buf, fmt.Errorf("ToJSON: bad blob")
		}
		// FIXME: don't allocate a new buffer here
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(body)))
		base64.StdEncoding.Encode(dst, body)
		err := w.WriteByte('"')
		if err != nil {
			return 0, rest, err
		}
		n, err := w.Write(dst)
		if err != nil {
			return n + 1, rest, err
		}
		err = w.WriteByte('"')
		return n + 2, rest, err
	case ListType, SexpType:
		body, rest := Contents(buf)
		if body == nil {
			return 0, buf, fmt.Errorf("ToJSON: bad list")
		}
		err := w.WriteByte('[')
		if err != nil {
			return 0, rest, err
		}
		nn := 1
		first := true
		for len(body) > 0 {
			var n int
			if !first {
				n, err = w.WriteString(", ")
				nn += n
				if err != nil {
					return nn, rest, err
				}
			}
			n, body, err = toJSON(st, w, body, s)
			nn += n
			if err != nil {
				return nn, rest, err
			}
			first = false
		}
		err = w.WriteByte(']')
		nn++
		return nn, rest, err
	case StructType:
		body, rest := Contents(buf)
		if body == nil {
			return 0, buf, fmt.Errorf("ToJSON: bad structure")
		}
		err := w.WriteByte('{')
		if err != nil {
			return 0, rest, err
		}
		nn := 1
		n := 0
		var sym Symbol
		first := true
		for len(body) > 0 {
			if !first {
				n, err = w.WriteString(", ")
				nn += n
				if err != nil {
					return 0, rest, err
				}
			}
			sym, body, err = ReadLabel(body)
			if err != nil {
				return nn, rest, err
			}
			name := st.Get(sym)
			if name == "" {
				name = "$" + strconv.Itoa(int(sym))
			}
			n, err = w.Write(s.string(name))
			nn += n
			if err != nil {
				return nn, rest, err
			}
			n, err = w.WriteString(": ")
			nn += n
			if err != nil {
				return nn, rest, err
			}
			n, body, err = toJSON(st, w, body, s)
			nn += n
			if err != nil {
				return nn, rest, err
			}
			first = false
		}
		err = w.WriteByte('}')
		nn++
		return nn, rest, err
	case AnnotationType:
		// if this isn't a symbol table
		// we will simply ignore it...
		rest, err := st.Unmarshal(buf)
		if err != nil {
			return 0, buf[SizeOf(buf):], nil
		}
		return 0, rest, nil
	case ReservedType:
		return 0, buf, fmt.Errorf("object tag 0xf is invalid")
	default:
		panic("impossible TypeOf(msg)")
	}
}

// helper for formatting json objects
type scratch struct {
	buf []byte
}

func (s *scratch) f32(f float32) []byte {
	s.buf = strconv.AppendFloat(s.buf[:0], float64(f), 'g', -1, 32)
	return s.buf
}

func (s *scratch) f64(f float64) []byte {
	s.buf = strconv.AppendFloat(s.buf[:0], f, 'g', -1, 64)
	return s.buf
}

func (s *scratch) int(i int64) []byte {
	s.buf = strconv.AppendInt(s.buf[:0], i, 10)
	return s.buf
}

func (s *scratch) uint(u uint64) []byte {
	s.buf = strconv.AppendUint(s.buf[:0], u, 10)
	return s.buf
}

func (s *scratch) time(t date.Time) []byte {
	s.buf = append(s.buf[:0], '"')
	s.buf = t.AppendRFC3339(s.buf)
	s.buf = append(s.buf, '"')
	return s.buf
}

// ToJSON reads a stream of ion objects from 'r'
// and writes them to 'w'.
// Each top-level object in the stream of objects
// is written on its own line.
// (See also: jsonlines.org, ndjson.org)
//
// Ion structures are written as json objects,
// lists and sexps are written as arrays,
// symbols, strings and timestamps are written as strings,
// blobs and clobs are written as base64-encoded strings,
// numbers are written as numbers
// (using as many characters as are necessary to preserve
// precision upon decoding the numbers),
// and null objects are written as JSON nulls.
//
// Symbol tables present in the stream of objects
// are decoded and used to print symbols and structure fields.
// Per the ion binary specification, a BVM marker flushes
// the current symbol table. (Annotation objects and padding
// objects do not produce any JSON output.)
//
// ToJSON returns the number of bytes written to w
// and the first error encountered (if any).
func ToJSON(w io.Writer, r *bufio.Reader) (int, error) {
	nn := 0
	var n int
	var err error
	var s scratch
	var buf []byte
	var st Symtab
	var typ Type

	var b *bufio.Writer
	js, ok := w.(jswriter)
	if !ok {
		b = bufio.NewWriter(w)
		js = b
	}

	for {
		var size int
		typ, size, err = Peek(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}
		if typ == NullType && size > 1 {
			r.Discard(size)
			continue
		}
		var this []byte
		peeked := false
		if size < r.Size() {
			peeked = true
			this, err = r.Peek(size)
			if err != nil {
				return n, err
			}
		} else {
			if cap(buf) >= size {
				this = buf[:size]
			} else {
				buf = make([]byte, size)
				this = buf
			}
			_, err = io.ReadFull(r, this)
			if err != nil {
				return n, err
			}
		}
		n, _, err = toJSON(&st, js, this, &s)
		nn += n
		if peeked {
			r.Discard(size)
		}
		if err != nil {
			if b != nil {
				b.Flush()
			}
			chunk := this
			if len(chunk) > 32 {
				chunk = chunk[:32]
			}
			return nn, fmt.Errorf("translating %x: %w", chunk, err)
		}
		if n == 0 {
			// don't emit a newline
			// for objects with no
			// representation
			continue
		}
		err = js.WriteByte('\n')
		nn++
		if err != nil {
			return nn, err
		}
	}
	if b != nil {
		err := b.Flush()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// JSONWriter is an io.WriteCloser
// that performs inline translation
// of chunks of ion data into JSON objects.
// See NewJSONWriter.
type JSONWriter struct {
	// W is the output io.Writer into which
	// the JSON data is written.
	W io.Writer

	s  scratch
	b  *bufio.Writer
	js jswriter
	st Symtab

	anyout bool // any output has been written
	nd     bool // is ndjson
}

// NewJSONWriter creates a new JSON writer
// which writes either NDJSON or a JSON array
// depending on the value of sep:
//
//  If sep is '\n', then the returned JSONWriter
//  writes NDJSON lines from each input object,
//  and the Close method is a no-op.
//
//  If sep is ',', then the return JSONWriter
//  writes a JSON array containing all the ion
//  values passed to Write. The call to Close
//  writes the final ']' byte.
//
// NewJSONWriter will panic if sep is not one
// of the recognized bytes.
func NewJSONWriter(w io.Writer, sep byte) *JSONWriter {
	var b *bufio.Writer
	js, ok := w.(jswriter)
	if !ok {
		b = bufio.NewWriter(w)
		js = b
	}
	switch sep {
	case '\n', ',':
	default:
		panic("invalid sep passed to NewJSONWriter")
	}
	return &JSONWriter{W: w, b: b, js: js, nd: sep == '\n'}
}

func (w *JSONWriter) Close() error {
	if w.nd {
		return nil
	}
	if !w.anyout {
		_, err := io.WriteString(w.W, "[]")
		return err
	}
	w.js.WriteByte(']')
	return w.flush()
}

// Write implements io.Writer
//
// The buffer passed to Write must contain complete ion objects.
func (w *JSONWriter) Write(src []byte) (int, error) {
	p := len(src)
	var size int
	for len(src) > 0 {
		comma := w.anyout && !w.nd
		annot := false
		if IsBVM(src) {
			size = 4 + SizeOf(src[4:])
			comma = false
			annot = true
		} else {
			size = SizeOf(src)
			if TypeOf(src) == NullType && size > 1 {
				// skip nop pad
				src = src[size:]
				continue
			}
			annot = TypeOf(src) == AnnotationType
			comma = comma && !annot
		}
		if comma {
			w.js.WriteByte(',')
		} else if !w.anyout && !w.nd && !annot {
			w.js.WriteByte('[')
		}
		n, _, err := toJSON(&w.st, w.js, src[:size], &w.s)
		if err != nil {
			w.flush()
			return 0, err
		}
		if n > 0 {
			w.anyout = true
			if w.nd {
				w.js.WriteByte('\n')
			}
		}
		src = src[size:]
	}
	return p, w.flush()
}

func (w *JSONWriter) flush() error {
	if w.b != nil {
		return w.b.Flush()
	}
	return nil
}
