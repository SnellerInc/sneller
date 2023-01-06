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

package pir

import (
	"fmt"

	"github.com/SnellerInc/sneller/expr"
)

func joinhash(b *Trace, eq *EquiJoin) expr.Node {
	id := len(b.Replacements)
	b.Replacements = append(b.Replacements, nil) // will be assigned to later
	return expr.Call(expr.HashReplacement, expr.Integer(id), expr.String("list"), expr.String("$__key"), eq.value)
}

type joinResult struct {
	eq   *EquiJoin
	into expr.Node
	used []string
	err  error
}

type joinVisitor struct {
	trace   *Trace
	parent  Step
	results []joinResult
}

func (j *joinVisitor) get(eq *EquiJoin) *joinResult {
	for i := range j.results {
		if j.results[i].eq == eq {
			return &j.results[i]
		}
	}
	j.results = append(j.results, joinResult{eq: eq})
	return &j.results[len(j.results)-1]
}

func (j *joinVisitor) addError(eq *EquiJoin, err error) {
	res := j.get(eq)
	if res.err == nil {
		res.err = err
	}
}

func (j *joinVisitor) markUsed(eq *EquiJoin, used string) {
	res := j.get(eq)
	if res.into == nil {
		res.into = joinhash(j.trace, eq)
	}
	res.used = append(res.used, used)
}

func (j *joinVisitor) Visit(e expr.Node) expr.Visitor {
	switch e := e.(type) {
	case expr.Ident:
		step, _ := j.parent.get(string(e))
		if eq, ok := step.(*EquiJoin); ok {
			j.addError(eq, fmt.Errorf("raw reference to binding %s unsupported", string(e)))
		}
	case *expr.Dot:
		id, ok := e.Inner.(expr.Ident)
		if !ok {
			return j
		}
		step, _ := j.parent.get(string(id))
		if eq, ok := step.(*EquiJoin); ok {
			j.markUsed(eq, e.Field)
		}
		// do not continue traversing
		return nil
	}
	return j
}

func joinelim(b *Trace) error {
	any := false
	for s := b.top; s != nil; s = s.parent() {
		_, ok := s.(*EquiJoin)
		if ok {
			any = true
			break
		}
	}
	// it's often the case there aren't any joins
	if !any {
		return nil
	}

	jw := joinVisitor{
		trace: b,
	}
	start := len(b.Replacements)
	for s := b.top; s != nil; s = s.parent() {
		jw.parent = s.parent()
		if jw.parent == nil {
			break
		}
		s.walk(&jw)
	}
	for i := range jw.results {
		jr := &jw.results[i]
		if jr.err != nil {
			return jr.err
		}
		eq := jr.eq
		// insert identity bindings for projected columns
		for j := range jr.used {
			eq.built.Columns = append(eq.built.Columns, expr.Identity(jr.used[j]))
		}
		t, err := build(b, eq.built, eq.env)
		if err != nil {
			return err
		}
		b.Replacements[start+i] = t
	}

	// now remove all the equijoin steps;
	// they should no longer be referenced
	var prev Step
	for s := b.top; s != nil; s = s.parent() {
		eq, ok := s.(*EquiJoin)
		if !ok {
			prev = s
			continue
		}
		var res *joinResult
		for i := range jw.results {
			if jw.results[i].eq == eq {
				res = &jw.results[i]
				break
			}
		}
		// convert this EquiJoin step
		// into an IterValue step
		//
		// NOTE: if we could tell that the join column
		// is distinct on the build side, we could instead
		// just substitute the HASH_REPLACEMENT() into all
		// the table references and be done with it rather
		// than introducing an (expensive) unnesting step here
		nv := &IterValue{
			Value:  res.into,
			Result: eq.built.From.(*expr.Table).Result(),
		}
		nv.setparent(eq.parent())
		if prev == nil {
			b.top = nv
		} else {
			prev.setparent(nv)
		}
		prev = nv
	}
	return nil
}
