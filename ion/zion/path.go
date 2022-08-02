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

package zion

import (
	"github.com/SnellerInc/sneller/ion"
)

type pathset struct {
	bits     []uint64
	buckets  uint32
	selector uint8
}

func (p *pathset) empty() bool {
	return len(p.bits) == 0
}

func (p *pathset) set(x ion.Symbol) {
	v := uint(x)
	word := int(v >> 6)
	for len(p.bits) <= word {
		p.bits = append(p.bits, 0)
	}
	p.bits[word] |= 1 << (v & 63)
	p.buckets |= 1 << sym2bucket(0, p.selector, x)
}

func (p *pathset) useBucket(i int) bool {
	return p.buckets&(1<<i) != 0
}

func (p *pathset) contains(x ion.Symbol) bool {
	v := uint(x)
	word := int(v >> 6)
	return word < len(p.bits) && p.bits[word]&(1<<(v&63)) != 0
}

func (p *pathset) clear() {
	p.bits = p.bits[:0]
	p.buckets = 0
}
