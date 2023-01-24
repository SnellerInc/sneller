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

package plan

import (
	"fmt"
	"io"
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan/pir"

	"golang.org/x/exp/slices"
)

type replacement struct {
	lock sync.Mutex

	rows []ion.Struct
}

func first(s *ion.Struct) (ion.Field, bool) {
	var field ion.Field
	var ok bool
	s.Each(func(f ion.Field) error {
		field, ok = f, true
		return ion.Stop
	})
	return field, ok
}

func (r *replacement) toScalar() (expr.Constant, bool) {
	if len(r.rows) == 0 {
		return expr.Null{}, true
	}
	s := &r.rows[0]
	f, ok := first(s)
	if !ok {
		return expr.Null{}, true
	}
	return expr.AsConstant(f.Datum)
}

func (r *replacement) toScalarList() (ion.Bag, bool) {
	var ret ion.Bag
	for i := range r.rows {
		f, ok := first(&r.rows[i])
		if !ok {
			continue
		}
		ret.AddDatum(f.Datum)
	}
	return ret, true
}

func (r *replacement) toList() (expr.Constant, bool) {
	lst := new(expr.List)
	for i := range r.rows {
		val, ok := expr.AsConstant(r.rows[i].Datum())
		if !ok {
			return nil, false
		}
		lst.Values = append(lst.Values, val)
	}
	return lst, true
}

func (r *replacement) toStruct() (expr.Constant, bool) {
	if len(r.rows) == 0 {
		return &expr.Struct{}, true
	}
	return expr.AsConstant(r.rows[0].Datum())
}

func (r *replacement) toHashLookup(kind, label string, x, elseval expr.Node) (expr.Node, bool) {
	if len(r.rows) == 0 {
		return expr.Missing{}, true
	}
	var conv rowConverter
	switch kind {
	case "scalar":
		conv = &scalarConverter{label: label}
	case "struct":
		conv = &structConverter{label: label}
	case "list":
		conv = &listConverter{label: label}
	default:
		return nil, false
	}
	for i := range r.rows {
		if !conv.add(&r.rows[i]) {
			return nil, false
		}
	}
	return conv.result(x, elseval), true
}

type rowConverter interface {
	add(row *ion.Struct) (ok bool)
	result(key, elseval expr.Node) *expr.Lookup
}

type scalarConverter struct {
	label        string
	keys, values ion.Bag
}

func (c *scalarConverter) result(key, elseval expr.Node) *expr.Lookup {
	return &expr.Lookup{
		Expr:   key,
		Else:   elseval,
		Keys:   c.keys,
		Values: c.values,
	}
}

func (c *scalarConverter) add(row *ion.Struct) bool {
	if row.Len() != 2 {
		return false
	}
	f := row.Fields(make([]ion.Field, 0, 2))
	if f[0].Label != c.label {
		f[0], f[1] = f[1], f[0]
		if f[0].Label != c.label {
			return false
		}
	}
	c.keys.AddDatum(f[0].Datum)
	c.values.AddDatum(f[1].Datum)
	return true
}

type structConverter struct {
	label        string
	keys, values ion.Bag
}

func (c *structConverter) result(key, elseval expr.Node) *expr.Lookup {
	return &expr.Lookup{
		Expr:   key,
		Else:   elseval,
		Keys:   c.keys,
		Values: c.values,
	}
}

func (c *structConverter) add(row *ion.Struct) bool {
	if row.Len() == 0 {
		return false
	}
	var key ion.Datum
	fields := make([]ion.Field, 0, row.Len()-1)
	row.Each(func(f ion.Field) error {
		if key.IsEmpty() && f.Label == c.label {
			key = f.Datum
			return nil
		}
		fields = append(fields, f)
		return nil
	})
	if key.IsEmpty() {
		return false
	}
	c.keys.AddDatum(key)
	c.values.AddDatum(ion.NewStruct(nil, fields).Datum())
	return true
}

type listConverter struct {
	label string
	m     map[expr.Constant]*expr.List
}

func (c *listConverter) result(key, elseval expr.Node) *expr.Lookup {
	l := &expr.Lookup{Expr: key, Else: elseval}
	for k, v := range c.m {
		l.Keys.AddDatum(k.Datum())
		l.Values.AddDatum(v.Datum())
	}
	return l
}

func (c *listConverter) add(row *ion.Struct) bool {
	if row.Len() == 0 {
		return false
	}
	var key expr.Constant
	var ok bool
	fields := make([]expr.Field, 0, row.Len()-1)
	row.Each(func(f ion.Field) error {
		var val expr.Constant
		val, ok = expr.AsConstant(f.Datum)
		if !ok {
			return ion.Stop
		}
		if f.Label == c.label {
			key = val
			return nil
		}
		fields = append(fields, expr.Field{
			Label: f.Label,
			Value: val,
		})
		return nil
	})
	if !ok || key == nil {
		return false
	}
	lst := c.m[key]
	if lst == nil {
		lst = &expr.List{}
		if c.m == nil {
			c.m = make(map[expr.Constant]*expr.List)
		}
		c.m[key] = lst
	}
	lst.Values = append(lst.Values, &expr.Struct{Fields: fields})
	return true
}

type subreplacement struct {
	parent *replacement
	curst  ion.Symtab
	tmp    []ion.Struct
}

func (s *subreplacement) Write(buf []byte) (int, error) {
	buf = slices.Clone(buf)
	orig := len(buf)
	s.tmp = s.tmp[:0]
	var err error
	var d ion.Datum
	for len(buf) > 0 {
		d, buf, err = ion.ReadDatum(&s.curst, buf)
		if err != nil {
			return orig - len(buf), err
		}
		if d.IsEmpty() || d.IsNull() {
			continue // symbol table or nop pad
		}
		st, _ := d.Struct()
		s.tmp = append(s.tmp, st)
	}
	s.parent.lock.Lock()
	defer s.parent.lock.Unlock()
	s.parent.rows = append(s.parent.rows, s.tmp...)
	s.tmp = s.tmp[:0]
	if len(s.parent.rows) > pir.LargeSize {
		return orig, fmt.Errorf("%d items in subreplacement exceeds limit", len(s.parent.rows))
	}
	return orig, nil
}

func (s *subreplacement) Close() error {
	return nil
}

func (r *replacement) Open() (io.WriteCloser, error) {
	return &subreplacement{
		parent: r,
	}, nil
}

func (r *replacement) Close() error {
	return nil
}

// replace substitutes replacement tokens
// like IN_REPLACEMENT(expr, id)
// and SCALAR_REPLACMENT(id)
// with the appropriate constant from
// the replacement list
type replacer struct {
	err     error
	inputs  []replacement
	simpl   expr.Rewriter
	rewrote bool
}

// we perform simplification after substitution
// so that any constprop opportunities that appear
// after replacement get taken care of
func (r *replacer) simplify(e expr.Node) expr.Node {
	if !r.rewrote {
		return e
	}
	if r.simpl == nil {
		r.simpl = expr.Simplifier(expr.NoHint)
	}
	return r.simpl.Rewrite(e)
}

func (r *replacer) Walk(e expr.Node) expr.Rewriter {
	if r.err != nil {
		return nil
	}
	return r
}

func (r *replacer) Rewrite(e expr.Node) expr.Node {
	b, ok := e.(*expr.Builtin)
	if !ok {
		return r.simplify(e)
	}
	switch b.Func {
	default:
		return r.simplify(e)
	case expr.ListReplacement:
		r.rewrote = true
		id := int(b.Args[0].(expr.Integer))
		value, ok := r.inputs[id].toList()
		if !ok {
			r.err = fmt.Errorf("cannot interpolate value %v as constant", r.inputs[id].rows)
		}
		return value
	case expr.InReplacement:
		r.rewrote = true
		id := int(b.Args[1].(expr.Integer))
		lst, ok := r.inputs[id].toScalarList()
		if !ok {
			r.err = fmt.Errorf("cannot interpolate %v as const list", r.inputs[id].rows)
			return e
		}
		return &expr.Member{
			Arg: b.Args[0],
			Set: lst,
		}
	case expr.HashReplacement:
		r.rewrote = true
		id := int(b.Args[0].(expr.Integer))
		kind := string(b.Args[1].(expr.String))
		label := string(b.Args[2].(expr.String))
		var elseval expr.Node
		if len(b.Args) == 5 {
			elseval = b.Args[4]
		}
		fn, ok := r.inputs[id].toHashLookup(kind, label, b.Args[3], elseval)
		if !ok {
			r.err = fmt.Errorf("cannot interpolate %v as case", r.inputs[id].rows)
			return e
		}
		return fn
	case expr.StructReplacement:
		r.rewrote = true
		id := int(b.Args[0].(expr.Integer))
		value, ok := r.inputs[id].toStruct()
		if !ok {
			r.err = fmt.Errorf("cannot interpolate %v as a structure", r.inputs[id].rows)
			return e
		}
		return value
	case expr.ScalarReplacement:
		r.rewrote = true
		id := int(b.Args[0].(expr.Integer))
		value, ok := r.inputs[id].toScalar()
		if !ok {
			r.err = fmt.Errorf("cannot interpolate value %v as a constant", r.inputs[id].rows)
			return e
		}
		return value
	}
}
