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
	"github.com/SnellerInc/sneller/expr"
)

var rules = []func(t *Trace) error{
	checkSortSize,
}

func checkAggregateWorkInProgress(e expr.Node) error {
	var err error
	v := visitfn(func(e expr.Node) bool {
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
			if agg.Over != nil {
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
	v := visitfn(func(e expr.Node) bool {
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
	f, ok := t.Final().(*Order)
	if ok {
		if c := t.Class(); !c.Small() {
			return errorf(f.Columns[0].Column,
				"this ORDER BY requires a LIMIT statement (up to %d)",
				LargeSize)
		}
		return nil
	}
	l, ok := t.Final().(*Limit)
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
