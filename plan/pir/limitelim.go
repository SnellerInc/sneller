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
			if self.Count >= 1 && len(n.GroupBy) == 0 {
				// In AGGREGATE ... LIMIT N for N >= 1 the limit is redundant,
				// as the aggregate yields exactly one row.
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
