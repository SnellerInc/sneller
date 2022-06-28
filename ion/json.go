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
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func jsonStruct(st *Symtab, d *json.Decoder) (Datum, error) {
	var out []Field
	for {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		if tok == json.Delim('}') {
			break
		}
		name, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected a string struct field; found %v", tok)
		}
		body, err := d.Token()
		if err != nil {
			return nil, err
		}
		dat, err := fromJSON(st, body, d)
		if err != nil {
			return nil, err
		}
		out = append(out, Field{
			Label: name,
			Value: dat,
		})
	}
	return NewStruct(st, out), nil
}

func jsonArray(st *Symtab, d *json.Decoder) (Datum, error) {
	var out []Datum
	for {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		if tok == json.Delim(']') {
			break
		}
		dat, err := fromJSON(st, tok, d)
		if err != nil {
			return nil, err
		}
		out = append(out, dat)
	}
	return NewList(st, out), nil
}

func fromJSON(st *Symtab, tok json.Token, d *json.Decoder) (Datum, error) {
	itod := func(i int64) Datum {
		if i >= 0 {
			return Uint(i)
		}
		return Int(i)
	}
	switch t := tok.(type) {
	case json.Delim:
		if t == json.Delim('{') {
			return jsonStruct(st, d)
		}
		if t == json.Delim('[') {
			return jsonArray(st, d)
		}
		return nil, fmt.Errorf("fromJSON: unexpected delim %v", t)
	case float64:
		// normalize integers:
		if t > 0 {
			if u := uint64(t); float64(u) == t {
				return Uint(u), nil
			}
		} else if i := int64(t); float64(i) == t {
			return Int(i), nil
		}
		return Float(t), nil
	case int:
		return itod(int64(t)), nil
	case int64:
		return itod(t), nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return itod(i), nil
		}
		f, err := t.Float64()
		if err == nil {
			if i := int64(f); float64(i) == f {
				return itod(i), nil
			}
			return Float(f), nil
		}
		return nil, fmt.Errorf("number %q out of range", t.String())
	case string:
		// N.B. -gcflags=-m says this conversion
		// does not escape to the heap:
		if t, ok := date.Parse([]byte(t)); ok {
			return Timestamp(t), nil
		}
		return String(t), nil
	case bool:
		return Bool(t), nil
	case time.Time:
		// probably not possible?
		return Timestamp(date.FromTime(t)), nil
	case nil:
		return UntypedNull{}, nil
	default:
		return nil, fmt.Errorf("fromJSON: unexpected token %v", t)
	}
}

// FromJSON decodes one JSON datum from 'd'
// and returns it as an ion Datum.
func FromJSON(st *Symtab, d *json.Decoder) (Datum, error) {
	d.UseNumber()
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	dat, err := fromJSON(st, tok, d)
	if err == io.EOF {
		// decoding a single datum should
		// succeed without hitting EOF
		err = io.ErrUnexpectedEOF
	}
	return dat, err
}
