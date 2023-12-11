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
	"github.com/SnellerInc/sneller/expr"
)

var rules = []func(t *Trace) error{
	checkSortSize,
}

func checkAggregateWorkInProgress(e expr.Node) error {
	var err error
	v := expr.WalkFunc(func(e expr.Node) bool {
		if err != nil {
			return false
		}
		_, ok := e.(*expr.Select)
		if ok {
			// do not traverse sub-queries
			return false
		}
		agg, ok := e.(*expr.Aggregate)
		if ok {
			if !agg.Op.WindowOnly() && agg.Over != nil {
				err = errorf(agg, "window function in unexpected position")
				return false
			}
		}
		return true
	})
	expr.Walk(v, e)
	return err
}

func checkNoAggregateInCondition(e expr.Node, context string) error {
	var err error
	v := expr.WalkFunc(func(e expr.Node) bool {
		if err != nil {
			return false
		}
		_, ok := e.(*expr.Select)
		if ok {
			// do not visit sub-selects
			return false
		}
		_, ok = e.(*expr.Aggregate)
		if ok {
			err = errorf(e, "aggregate functions are not allowed in %s", context)
			return false
		}
		return true
	})
	expr.Walk(v, e)
	return err
}

func checkSortSize(t *Trace) error {
	final := t.Final()
	if b, ok := final.(*Bind); ok {
		final = b.parent()
	}
	f, ok := final.(*Order)
	if ok {
		if c := t.Class(); !c.Small() {
			return errorf(f.Columns[0].Column,
				"this ORDER BY requires a LIMIT statement (up to %d)",
				LargeSize)
		}
		return nil
	}
	l, ok := final.(*Limit)
	if !ok {
		return nil
	}
	p, ok := Input(l).(*Order)
	if !ok {
		return nil
	}
	pos := l.Count + l.Offset
	if pos > LargeSize {
		return errorf(p.Columns[0].Column,
			"this ORDER BY has a LIMIT+OFFSET %d greater than the maximum allowed value %d", pos, LargeSize)
	}
	return nil
}

func postcheck(t *Trace) error {
	for _, r := range rules {
		if err := r(t); err != nil {
			return err
		}
	}
	return nil
}
