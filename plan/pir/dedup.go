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

func mergereplacements(b *Trace) {
	// repl[x] is the replacement for input x
	var repl []int
	// compare each input pair and deduplicate
	for i := 0; i < len(b.Replacements)-1; i++ {
		ri := b.Replacements[i]
		if ri == nil {
			continue
		}
		for j := i + 1; j < len(b.Replacements); j++ {
			rj := b.Replacements[j]
			if rj == nil || !ri.Equals(rj) {
				continue
			}
			if repl == nil {
				repl = make([]int, len(b.Replacements))
				for i := range repl {
					repl[i] = i
				}
			}
			repl[j] = i
			b.Replacements[j] = nil
		}
	}
	// replace references to deduplicated inputs
	for from, to := range repl {
		if from == to {
			continue
		}
		b.Rewrite(replrw(func(bi *expr.Builtin) expr.Node {
			switch bi.Func {
			case expr.ListReplacement, expr.HashReplacement,
				expr.StructReplacement, expr.ScalarReplacement:
				id, ok := bi.Args[0].(expr.Integer)
				if ok && int(id) == from {
					bi.Args[0] = expr.Integer(to)
				}
			case expr.InReplacement:
				id, ok := bi.Args[1].(expr.Integer)
				if ok && int(id) == from {
					bi.Args[1] = expr.Integer(to)
				}
			}
			return bi
		}))
		b.Replacements[from] = nil
	}
	// remove nil replacements
	inputs := b.Replacements[:0]
	for i := range b.Replacements {
		if b.Replacements[i] != nil {
			inputs = append(inputs, b.Replacements[i])
		}
	}
	b.Replacements = inputs
}

type replrw func(*expr.Builtin) expr.Node

func (r replrw) Rewrite(e expr.Node) expr.Node {
	if bi, ok := e.(*expr.Builtin); ok {
		return r(bi)
	}
	return e
}

func (r replrw) Walk(e expr.Node) expr.Rewriter {
	return r
}
