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

// if we have par -> self,
// replace par -> x -> ... -> self
// as deep as we can
func pushlimit(par Step, self *Limit, b *Trace) {
	var skip Step
search:
	for next := self.parent(); next != nil; next = next.parent() {
		switch n := next.(type) {
		case *Bind: // Bind is the only op that preserves the number of rows
			skip = n
		case *Aggregate:
			if self.Count == 1 && len(n.GroupBy) == 0 {
				// AGGREGATE ... LIMIT 1 is redundant
				if b.top == self {
					b.top = n
				} else {
					// splice us out of the chain
					par.setparent(n)
				}
				return
			}
		default:
			break search
		}
	}
	if skip == nil {
		return
	}
	next := self.parent()
	self.setparent(skip.parent())
	skip.setparent(self)
	if b.top == self {
		b.top = next
	} else {
		par.setparent(next)
	}
}

func limitpushdown(b *Trace) {
	var parent Step
	for s := b.top; s != nil; s = s.parent() {
		lim, ok := s.(*Limit)
		if ok {
			pushlimit(parent, lim, b)
		}
		parent = s
	}
}
