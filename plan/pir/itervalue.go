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
	"sort"

	"github.com/SnellerInc/sneller/expr"
)

func gensym(node, path int) string {
	return fmt.Sprintf("$_%d_%d", node, path)
}

func (i *IterValue) OuterBind() []expr.Binding {
	return i.liveacross
}

func (i *IterValue) InnerBind() []expr.Binding {
	return i.liveat
}

// populate an IterValue instruction with
// the scope information related to which
// path variables are live past the cross join
//
// FIXME: this ought to run in linear time,
// not in the super-linear time that it runs now...
func itervalueinfo(b *Trace) {
	// we have to perform this in execution order,
	// which means we need to find all the IterValue nodes
	// and then walk them in reverse
	var iv []Step
	for cur := b.top; cur != nil; cur = cur.parent() {
		ivn, ok := cur.(*IterValue)
		if !ok || ivn.Wildcard() {
			continue
		}
		iv = append(iv, cur)
	}
	if len(iv) == 0 {
		return
	}
	for nodenum := len(iv) - 1; nodenum >= 0; nodenum-- {
		cur := iv[nodenum]
		iter := cur.(*IterValue)
		var after []Step
		for p := iter.parent(); p != nil; p = p.parent() {
			after = append(after, p)
		}

		var (
			liveacross = make(map[*expr.Path]expr.Binding) // live across the cross-join
			liveat     = make(map[*expr.Path]expr.Binding) // variables produced by the cross-join
			rewrote    = make(map[*expr.Path]*expr.Path)
		)
		pathnum := 0
		rewriteupto(b, cur, rewritefn(func(e expr.Node) expr.Node {
			p, ok := e.(*expr.Path)
			if !ok {
				return e
			}
			if rw := rewrote[p]; rw != nil {
				return rw
			}
			origin := b.origin(p)
			if origin == nil {
				return e
			}
			if origin == cur {
				gen := gensym(nodenum, pathnum)
				pathnum++
				liveat[p] = expr.Bind(p, gen)
				out := &expr.Path{First: gen}
				rewrote[p] = out
				b.scope[out] = scopeinfo{origin: cur, node: p}
				return out
			}
			for i := range after {
				if after[i] == origin {
					gen := gensym(nodenum, pathnum)
					pathnum++
					liveacross[p] = expr.Bind(p, gen)
					out := &expr.Path{First: gen}
					rewrote[p] = out
					b.scope[out] = scopeinfo{origin: cur, node: p}
					return out
				}
			}
			return e
		}))

		// To make the final results reproducible,
		// we sort the set of output paths
		for _, bind := range liveacross {
			iter.liveacross = append(iter.liveacross, bind)
		}
		sort.Slice(iter.liveacross, func(i, j int) bool {
			ip := iter.liveacross[i].Expr.(*expr.Path)
			jp := iter.liveacross[j].Expr.(*expr.Path)
			return ip.Less(jp)
		})
		for _, bind := range liveat {
			iter.liveat = append(iter.liveat, bind)
		}
		sort.Slice(iter.liveat, func(i, j int) bool {
			ip := iter.liveat[i].Expr.(*expr.Path)
			jp := iter.liveat[j].Expr.(*expr.Path)
			return ip.Less(jp)
		})

		// track the live values in the filter expression as well;
		// these need to be converted to LOAD(num) expressions
		if iter.Filter != nil {
			iter.Filter = expr.Rewrite(rewritefn(func(e expr.Node) expr.Node {
				p, ok := e.(*expr.Path)
				if !ok {
					return e
				}
				_, ok = liveacross[p]
				if !ok {
					if _, ok := liveat[p]; ok || b.origin(p) == iter {
						return e
					}
				}
				for i := range iter.liveacross {
					if iter.liveacross[i].Expr.(*expr.Path) == p {
						return expr.Call("LOAD", expr.Integer(i))
					}

				}
				// bind := expr.Bind(p, gensym(nodenum, pathnum))
				// pathnum++
				// iter.liveacross = append(iter.liveacross, bind)
				// liveacross[p] = bind
				panic("rewrite error")
			}), iter.Filter)
		}
	}
}
