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

package pir

import (
	"fmt"

	"github.com/SnellerInc/sneller/expr"
)

func joinhash(b *Trace, eq *EquiJoin) expr.Node {
	id := len(b.Replacements)
	b.Replacements = append(b.Replacements, nil) // will be assigned to later
	return expr.Call(expr.HashReplacement, expr.Integer(id), expr.String("joinlist"), expr.String("$__key"), eq.value)
}

type joinResult struct {
	eq   *EquiJoin
	into expr.Node
	used []string
	err  error
}

type joinRewriter struct {
	trace   *Trace
	parent  Step
	results []joinResult
}

func (j *joinRewriter) get(eq *EquiJoin) *joinResult {
	for i := range j.results {
		if j.results[i].eq == eq {
			return &j.results[i]
		}
	}
	j.results = append(j.results, joinResult{eq: eq})
	return &j.results[len(j.results)-1]
}

func (j *joinRewriter) addError(eq *EquiJoin, err error) {
	res := j.get(eq)
	if res.err == nil {
		res.err = err
	}
}

func (j *joinRewriter) markUsed(eq *EquiJoin, used string) int {
	res := j.get(eq)
	for i, str := range res.used {
		if str == used {
			return i
		}
	}
	n := len(res.used)
	res.used = append(res.used, used)
	return n
}

func (j *joinRewriter) Walk(e expr.Node) expr.Rewriter {
	if d, ok := e.(*expr.Dot); ok {
		// don't walk foo.bar -> foo
		if _, ok := d.Inner.(expr.Ident); ok {
			return nil
		}
	}
	return j
}

func (j *joinRewriter) Rewrite(e expr.Node) expr.Node {
	switch e := e.(type) {
	case expr.Ident:
		step, _ := j.parent.get(string(e))
		if eq, ok := step.(*EquiJoin); ok {
			j.addError(eq, fmt.Errorf("raw reference to binding %s unsupported", string(e)))
		}
	case *expr.Dot:
		id, ok := e.Inner.(expr.Ident)
		if !ok {
			return e
		}
		step, _ := j.parent.get(string(id))
		if eq, ok := step.(*EquiJoin); ok {
			// turn v.foo into v[index] where index
			// is the associated field position
			return &expr.Index{Inner: id, Offset: j.markUsed(eq, e.Field)}
		}
		// do not continue traversing
		return e
	}
	return e
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

	jw := joinRewriter{
		trace: b,
	}
	fn := func(e expr.Node, _ bool) expr.Node {
		return expr.Rewrite(&jw, e)
	}
	start := len(b.Replacements)
	for s := b.top; s != nil; s = s.parent() {
		jw.parent = s.parent()
		if jw.parent == nil {
			break
		}
		s.rewrite(fn)
	}
	for i := range jw.results {
		jr := &jw.results[i]
		if jr.err != nil {
			return jr.err
		}
		jr.into = joinhash(b, jr.eq)
		eq := jr.eq
		lstitems := make([]expr.Node, len(jr.used))
		for j := range jr.used {
			lstitems[j] = expr.Ident(jr.used[j])
		}
		collist := expr.Call(expr.MakeList, lstitems...)
		eq.built.Columns = append(eq.built.Columns,
			expr.Bind(collist, "$__val"))
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
		if res == nil {
			return fmt.Errorf("unable to eliminate join on %s = %s",
				expr.ToString(eq.key), expr.ToString(eq.value))
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
