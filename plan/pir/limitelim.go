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

func limitpushdown(b *Trace) {
	for s := b.top; s != nil; s = s.parent() {
		lim, ok := s.(*Limit)
		if !ok {
			continue
		}
		next := s.parent()
		switch n := next.(type) {
		case *Bind:
			// bindings don't change the #
			// of output rows, so we can push down
			// past them
			lim.setparent(n.parent())
			n.setparent(lim)
			if s == b.top {
				b.top = n
			}
		default:
		}
	}
}
