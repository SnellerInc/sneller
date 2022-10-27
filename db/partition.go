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

package db

import (
	"fmt"
	"strconv"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func makePartitions(parts []Partition) ([]blockfmt.Template, error) {
	cons := make([]blockfmt.Template, len(parts))
	for i := range parts {
		field := parts[i].Field
		if field == "" {
			return nil, fmt.Errorf("empty partition name")
		}
		// ensure there are no duplicates
		for j := i + 1; j < len(parts); j++ {
			if field == parts[j].Field {
				return nil, fmt.Errorf("duplicate partition name %q", field)
			}
		}
		// generate template from field if blank
		template := parts[i].Value
		if template == "" {
			_, rest := splitident(field)
			if rest != "" {
				return nil, fmt.Errorf("cannot use field name %q as value template", field)
			}
			template = "$" + field
		}
		eval, err := compileEval(parts[i].Type, template)
		if err != nil {
			return nil, err
		}
		cons[i] = blockfmt.Template{
			Field: field,
			Eval:  eval,
		}
	}
	return cons, nil
}

type evalfn func(*blockfmt.Input) (ion.Datum, error)

func compileEval(typ, tmpl string) (evalfn, error) {
	var fn func(string) (ion.Datum, error)
	switch typ {
	case "string", "":
		fn = func(s string) (ion.Datum, error) {
			return ion.String(s), nil
		}
	case "int":
		fn = func(s string) (ion.Datum, error) {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return ion.Empty, err
			}
			return ion.Int(i), nil
		}
	case "date":
		fn = func(s string) (ion.Datum, error) {
			t, ok := date.Parse([]byte(s + "T00:00:00Z"))
			if !ok {
				return ion.Empty, fmt.Errorf("invalid date %q", s)
			}
			return ion.Timestamp(t), nil
		}
	case "datetime", "timestamp":
		fn = func(s string) (ion.Datum, error) {
			t, ok := date.Parse([]byte(s))
			if !ok {
				return ion.Empty, fmt.Errorf("invalid datetime %q", s)
			}
			return ion.Timestamp(t), nil
		}
	default:
		return nil, fmt.Errorf("invalid type %q", typ)
	}
	return func(in *blockfmt.Input) (ion.Datum, error) {
		var mr matcher
		err := mr.match(in.Glob, in.Path, tmpl)
		if err != nil {
			return ion.Empty, err
		}
		if !mr.found {
			// this shouldn't happen in practice
			// because the glob pattern in the input
			// object should always match file path
			return ion.Empty, fmt.Errorf("path does not match pattern")
		}
		return fn(string(mr.result))
	}, nil
}
