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

type stepHint struct {
	parent Step
}

func (s *stepHint) TypeOf(e expr.Node) expr.TypeSet {
	if s.parent == nil {
		return expr.NoHint.TypeOf(e)
	}
	p, ok := e.(expr.Ident)
	if !ok {
		return expr.NoHint.TypeOf(e)
	}
	origin, node := s.parent.get(string(p))
	if origin == nil {
		return expr.NoHint.TypeOf(e)
	}
	if orig, ok := origin.(*IterTable); ok {
		schema := orig.Schema
		if schema == nil {
			schema = expr.NoHint
		}
		return expr.TypeOf(e, schema)
	}
	next := origin.parent()
	if node == nil || next == nil {
		return expr.NoHint.TypeOf(e)
	}
	hint := &stepHint{parent: next}
	return expr.TypeOf(node, hint)
}

func (s *stepHint) Values(expr.Node) *expr.FiniteSet {
	return nil
}
