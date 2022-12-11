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

package vm

// exists to tickle the "go vet" copylocks check
type noCopy struct{}

func (n noCopy) Lock()   {}
func (n noCopy) Unlock() {}

type pageref struct {
	mem []byte // result from vm.Malloc
	off int    // allocation offset
}

func (p *pageref) drop() {
	if p.mem != nil {
		Free(p.mem)
		p.mem = nil
	}
	p.off = 0
}

type slab struct {
	_        noCopy
	pages    []pageref
	oldpages []pageref // recorded in snapshot()
}

// reset rewinds the slab state
func (s *slab) reset() {
	for i := range s.pages {
		s.pages[i].drop()
	}
	s.pages = s.pages[:0]
	s.oldpages = s.oldpages[:0]
}

// resetNoFree is like reset but it
// keeps one page available for allocation
func (s *slab) resetNoFree() {
	switch len(s.pages) {
	default:
		tail := s.pages[1:]
		for i := range tail {
			tail[i].drop()
		}
		s.pages = s.pages[:1]
		fallthrough
	case 1:
		s.pages[0].off = 0
	case 0: // nothing to do
	}
	s.oldpages = s.oldpages[:0] // invalidate snapshot
}

// snapshot captures the state of the slab
// so that it can be reverted with rewind()
//
// only one snapshot can be stored at once
func (s *slab) snapshot() {
	s.oldpages = append(s.oldpages[:0], s.pages...)
}

// rewind rewinds the state to a snapshot captured with snapshot()
//
// if no snapshot was captured, rewind has the same effect as reset()
func (s *slab) rewind() {
	if len(s.oldpages) > len(s.pages) {
		panic("bad slab state: len(oldpages) > len(pages)")
	}
	tail := s.pages[len(s.oldpages):]
	for i := range tail {
		tail[i].drop()
	}
	s.pages = append(s.pages[:0], s.oldpages...) // restore old state
	s.oldpages = s.oldpages[:0]                  // invalidate snapshot
}

func (s *slab) malloc(n int) []byte {
	need := n
	if need > PageSize {
		panic("malloc > page size")
	}
	// typically we only have more than 1 page
	// allocated when we've reserved a whole page
	// for scratch space, so the previous page probably
	// has a large hole in which smaller allocations
	// will still fit; the total number of pages allocated
	// should still be low enough (much less than 10)
	// so just doing the brute force thing here should be fine
	for i := range s.pages {
		p := &s.pages[i]
		mem := p.mem[p.off:]
		if len(mem) >= need {
			p.off += need
			return mem[:n:need]
		}
	}
	mem := Malloc()
	s.pages = append(s.pages, pageref{mem: mem, off: need})
	return mem[:n:need]
}
