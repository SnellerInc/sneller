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
	"reflect"
)

type fpoStatus uint

const (
	fpoIntact  fpoStatus = iota // the node remains unmodified
	fpoUpdate                   // the node fields have been modified, but the node instance remains valid
	fpoReplace                  // the current node is to be replaced with a new one
)

// fpoContext is an extra information to pass to optimization function
type fpoContext struct {
	trace *Trace
}

// fixedPointOptimizer applies rewriting rules to PIR traces until
// fixed-point is reached, i.e. "nothing changes".
type fixedPointOptimizer struct {
	rules map[string][]any
}

// newFixedPointOptimizer creates a new instance of fixedPointOptimizer.
// Rules can be readily provided at the creation time or added later with add()
func newFixedPointOptimizer(rules ...any) fixedPointOptimizer {
	fpo := fixedPointOptimizer{rules: make(map[string][]any)}
	for i := range rules {
		fpo.add(rules[i])
	}
	return fpo
}

// add appends a rewriting rule to the set of already known rules.
// A rule is a function (*T) -> (Step, fpoStatus), where T is the type
// of the PIR node of interest.
func (fpo *fixedPointOptimizer) add(rule any) *fixedPointOptimizer {
	t := reflect.TypeOf(rule)
	if err := fpo.validateRuleType(t); err != nil {
		panic(err)
	}
	tin := t.In(0)
	name := tin.String()
	fpo.rules[name] = append(fpo.rules[name], rule)
	return fpo
}

// validateRuleType checks if the rule type describes a function (*T) -> (Step, fpoStatus)
func (fpo *fixedPointOptimizer) validateRuleType(t reflect.Type) error {
	if t.Kind() != reflect.Func {
		return fmt.Errorf("expected a fuction, but %s is provided", t.String())
	}
	if n := t.NumIn(); n != 2 {
		return fmt.Errorf("a rule function must take exactly one input parameter, but %s takes %d", t.String(), n)
	}
	if t.In(1).String() != "*pir.fpoContext" {
		return fmt.Errorf("the second argument to the function has to be *fpoContext, but is %s", t.In(1).String())
	}
	if t.NumOut() != 2 || t.Out(0).String() != "pir.Step" || t.Out(1).String() != "pir.fpoStatus" {
		return fmt.Errorf("the result type of %s should have been (Step, fpoStatus)", t.String())
	}
	return nil
}

// apply tries to find a rule matching the dynamic type T of s.
// Every rule with the signature (*T) -> (Step, fpoStatus) is given a chance in an unspecified order.
// The first rewriting terminates further matching in the current iteration phase.
func (fpo *fixedPointOptimizer) apply(s Step, context *fpoContext) (Step, fpoStatus) {
	// the rule must already have been validated, so invoke the function without further checks
	tin := reflect.TypeOf(s)
	name := tin.String()
	rules := fpo.rules[name]
	in := [2]reflect.Value{reflect.ValueOf(s), reflect.ValueOf(context)}
	for i := range rules {
		out := reflect.ValueOf(rules[i]).Call(in[:])
		status := fpoStatus(out[1].Uint())
		if status != fpoIntact {
			// some form of rewriting has occured
			return out[0].Interface().(Step), status
		}
	}
	return nil, fpoIntact
}

// optimize tries to rewrite PIR nodes by appying all the known rules.
// Rewriting attempts continue till a fixed point is achieved.
func (fpo *fixedPointOptimizer) optimize(b *Trace, context *fpoContext) {
	// while something can still be done...
	for somethingChanged := true; somethingChanged; {
		somethingChanged = false
		// apply all the matching rules to the trace
		var prev Step
		for s := b.top; s != nil; {
			n, status := fpo.apply(s, context)
			switch status {
			case fpoUpdate:
				somethingChanged = true
				// restart rule application at the current node to boost the cascading effect
			case fpoReplace:
				somethingChanged = true
				if prev != nil {
					// mid-list replacement
					prev.setparent(n)
				} else {
					// top-level replacement
					b.top = n
				}
				// restart rule application at the newly introduced node to boost the cascading effect
				s = n
			case fpoIntact:
				// no rule has matched, move on
				prev = s
				s = s.parent()
			}
		}
	}
}
