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
)

type replacement struct {
	lock sync.Mutex

	rows []ion.Struct
}

func first(s *ion.Struct) (ion.Field, bool) {
	var field ion.Field
	var ok bool
	s.Each(func(f ion.Field) bool {
		field, ok = f, true
		return false
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
	return expr.AsConstant(f.Value)
}

func (r *replacement) toScalarList() ([]expr.Constant, bool) {
	out := make([]expr.Constant, 0, len(r.rows))
	for i := range r.rows {
		f, ok := first(&r.rows[i])
		if !ok {
			continue
		}
		v, ok := expr.AsConstant(f.Value)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

func (r *replacement) toList() (expr.Constant, bool) {
	lst := new(expr.List)
	for i := range r.rows {
		val, ok := expr.AsConstant(&r.rows[i])
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
	return expr.AsConstant(&r.rows[0])
}

func (r *replacement) toHashLookup(kind, label string, x expr.Node) (expr.Node, bool) {
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
	return expr.Call("HASH_LOOKUP", conv.args(x)...), true
}

type rowConverter interface {
	add(row *ion.Struct) (ok bool)
	args(...expr.Node) []expr.Node
}

type scalarConverter struct {
	label string
	argv  []expr.Node
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
	key, ok := expr.AsConstant(f[0].Value)
	if !ok {
		return false
	}
	val, ok := expr.AsConstant(f[1].Value)
	if !ok {
		return false
	}
	c.argv = append(c.argv, key, val)
	return true
}

func (c *scalarConverter) args(n ...expr.Node) []expr.Node {
	return append(n, c.argv...)
}

type structConverter struct {
	label string
	argv  []expr.Node
}

func (c *structConverter) add(row *ion.Struct) bool {
	if row.Len() == 0 {
		return false
	}
	var key expr.Constant
	var ok bool
	fields := make([]expr.Field, 0, row.Len()-1)
	row.Each(func(f ion.Field) bool {
		var val expr.Constant
		val, ok = expr.AsConstant(f.Value)
		if !ok {
			return false
		}
		if f.Label == c.label {
			key = val
			return true
		}
		fields = append(fields, expr.Field{
			Label: f.Label,
			Value: val,
		})
		return true
	})
	if !ok || key == nil {
		return false
	}
	c.argv = append(c.argv, key, &expr.Struct{Fields: fields})
	return true
}

func (c *structConverter) args(n ...expr.Node) []expr.Node {
	return append(n, c.argv...)
}

type listConverter struct {
	label string
	m     map[expr.Constant]*expr.List
}

func (c *listConverter) add(row *ion.Struct) bool {
	if row.Len() == 0 {
		return false
	}
	var key expr.Constant
	var ok bool
	fields := make([]expr.Field, 0, row.Len()-1)
	row.Each(func(f ion.Field) bool {
		var val expr.Constant
		val, ok = expr.AsConstant(f.Value)
		if !ok {
			return false
		}
		if f.Label == c.label {
			key = val
			return true
		}
		fields = append(fields, expr.Field{
			Label: f.Label,
			Value: val,
		})
		return true
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

func (c *listConverter) args(n ...expr.Node) []expr.Node {
	if len(n)+len(c.m) > cap(n) {
		n2 := make([]expr.Node, len(n), len(n)+len(c.m))
		copy(n2, n)
		n = n2
	}
	for k, v := range c.m {
		n = append(n, k, v)
	}
	return n
}

type subreplacement struct {
	parent *replacement
	curst  ion.Symtab
	tmp    []ion.Struct
}

func (s *subreplacement) Write(buf []byte) (int, error) {
	orig := len(buf)
	s.tmp = s.tmp[:0]
	var err error
	var d ion.Datum
	for len(buf) > 0 {
		d, buf, err = ion.ReadDatum(&s.curst, buf)
		if err != nil {
			return orig - len(buf), err
		}
		if d == nil {
			continue // processed a symbol table
		}
		if _, ok := d.(ion.UntypedNull); ok {
			continue // skip nop pad
		}
		s.tmp = append(s.tmp, *(d.(*ion.Struct)))
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
		r.simpl = expr.Simplifier(expr.HintFn(expr.NoHint))
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
			Arg:    b.Args[0],
			Values: lst,
		}
	case expr.HashReplacement:
		r.rewrote = true
		id := int(b.Args[0].(expr.Integer))
		kind := string(b.Args[1].(expr.String))
		label := string(b.Args[2].(expr.String))
		fn, ok := r.inputs[id].toHashLookup(kind, label, b.Args[3])
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
