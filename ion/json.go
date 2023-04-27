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
			return Empty, err
		}
		if tok == json.Delim('}') {
			break
		}
		name, ok := tok.(string)
		if !ok {
			return Empty, fmt.Errorf("expected a string struct field; found %v", tok)
		}
		body, err := d.Token()
		if err != nil {
			return Empty, err
		}
		dat, err := fromJSON(st, body, d)
		if err != nil {
			return Empty, err
		}
		out = append(out, Field{
			Label: name,
			Datum: dat,
		})
	}
	return NewStruct(st, out).Datum(), nil
}

func jsonArray(st *Symtab, d *json.Decoder) (Datum, error) {
	var out []Datum
	for {
		tok, err := d.Token()
		if err != nil {
			return Empty, err
		}
		if tok == json.Delim(']') {
			break
		}
		dat, err := fromJSON(st, tok, d)
		if err != nil {
			return Empty, err
		}
		out = append(out, dat)
	}
	return NewList(st, out).Datum(), nil
}

func fromJSON(st *Symtab, tok json.Token, d *json.Decoder) (Datum, error) {
	itod := func(i int64) Datum {
		if i >= 0 {
			return Uint(uint64(i))
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
		return Empty, fmt.Errorf("fromJSON: unexpected delim %v", t)
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
		return Empty, fmt.Errorf("number %q out of range", t.String())
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
		return Null, nil
	default:
		return Empty, fmt.Errorf("fromJSON: unexpected token %v", t)
	}
}

// FromJSON decodes one JSON datum from 'd'
// and returns it as an ion Datum.
func FromJSON(st *Symtab, d *json.Decoder) (Datum, error) {
	d.UseNumber()
	tok, err := d.Token()
	if err != nil {
		return Empty, err
	}
	dat, err := fromJSON(st, tok, d)
	if err == io.EOF {
		// decoding a single datum should
		// succeed without hitting EOF
		err = io.ErrUnexpectedEOF
	}
	return dat, err
}
