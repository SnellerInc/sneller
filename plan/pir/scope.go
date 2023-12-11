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
