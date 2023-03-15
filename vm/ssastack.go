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

import (
	"fmt"
	"math"

	"golang.org/x/exp/slices"
)

// stackslot is a relative offset into a virtual stack buffer
type stackslot uint16

const invalidstackslot = stackslot(0xFFFF)
const permanentStackSlot = stackslot(0x0001)

const bcStackAlignment = 64

func (r regclass) align() int {
	switch r {
	case regK:
		return 2 // don't bother cache-line-aligning these tiny spills
	case regV:
		return 32 // regV is 1.5 cachelines, so let it be 50% mis-aligned
	default:
		return bcStackAlignment
	}
}

// size returns the register size of the register class
func (r regclass) size() int {
	switch r {
	case regK:
		return kRegSize
	case regS:
		return sRegSize
	case regV:
		return vRegSize
	case regB:
		return bRegSize
	case regH:
		return hRegSize
	case regL:
		return lRegSize
	default:
		panic("undefined reclass.size()")
	}
}

// Calculates a stack slot from the given index.
//
// Can be used to calculate stack slots that will be reserved by a
// program from an increasing set of indexes that start from zero.
func stackSlotFromIndex(rc regclass, index int) stackslot {
	slot := uint(index) * uint(rc.size())

	// The last slot is invalidstackslot, we have to refuse it
	// too as it's not a valid slot.
	if slot >= uint(invalidstackslot) {
		panic(fmt.Sprintf("Overflow in stackSlotFromIndex(): the specified index %d is too big to be a stack slot", index))
	}

	return stackslot(slot)
}

// stackmap manages a virtual stack. It allows to allocate stack regions either permanently or temporarily.
// Temporary allocations can be reused by multiple virtual registers that don't live at the same time.
type stackmap struct {
	allocator spanalloc                 // low-level stack allocator
	idToSlot  [_maxregclass][]stackslot // a map that holds allocated value IDs and their stack slot
}

func (s *stackmap) reserveSlot(rc regclass, slot stackslot) {
	s.allocator.reserve(int(slot), rc.size())
}

// Allocates a slot of the specified register class
//
// NOTE: This does not associate the allocated slot with a value, use allocValue() for that.
func (s *stackmap) allocSlot(rc regclass) stackslot {
	pos := s.allocator.get(rc.size(), rc.align())
	return stackslot(pos)
}

// frees an already allocated slot so it can be reused later.
func (s *stackmap) freeSlot(rc regclass, slot stackslot) {
	s.allocator.drop(int(slot))
}

func (s *stackmap) assignFreeableSlot(rc regclass, valueID int, slot stackslot) {
	idToSlot := s.idToSlot[rc]
	for i := len(idToSlot); i <= valueID; i++ {
		idToSlot = append(idToSlot, invalidstackslot)
	}
	idToSlot[valueID] = slot
	s.idToSlot[rc] = idToSlot
}

func (s *stackmap) assignPermanentSlot(rc regclass, valueID int, slot stackslot) {
	idToSlot := s.idToSlot[rc]
	for i := len(idToSlot); i <= valueID; i++ {
		idToSlot = append(idToSlot, invalidstackslot)
	}
	idToSlot[valueID] = slot | permanentStackSlot
	s.idToSlot[rc] = idToSlot
}

func (s *stackmap) allocValue(rc regclass, valueID int) stackslot {
	slot := s.allocSlot(rc)
	if valueID >= 0 {
		s.assignFreeableSlot(rc, valueID, slot)
	}
	return slot
}

func (s *stackmap) freeValue(rc regclass, valueID int) {
	slot := s.idToSlot[rc][valueID]
	if slot == invalidstackslot {
		panic("invalid stack slot used in stackmap.freeValue(): the value is not allocated")
	}

	// Don't free a permanent slot - if we do that, it would enter
	// in a freeSlot array and the allocator would pick it next.
	if (slot & permanentStackSlot) != 0 {
		return
	}

	s.freeSlot(rc, slot)
	s.idToSlot[rc][valueID] = invalidstackslot
}

func (s *stackmap) slotOf(rc regclass, valueID int) stackslot {
	idToSlot := s.idToSlot[rc]

	if len(idToSlot) <= valueID {
		return invalidstackslot
	}

	slot := idToSlot[valueID]
	if slot == invalidstackslot {
		return slot
	}

	return slot &^ permanentStackSlot
}

func (s *stackmap) hasFreeableSlot(rc regclass, valueID int) bool {
	slot := s.slotOf(rc, valueID)
	return (slot & permanentStackSlot) == 0
}

func (s *stackmap) stackSize() int {
	return s.stackSizeInUInt64Units() << 3
}

func (s *stackmap) stackSizeInUInt64Units() int {
	offset := s.allocator.max
	return int((offset + 7) >> 3)
}

type span struct {
	pos, size int
}

// used for slices.BinarySearchFunc to search for position
func spancmp(sp span, i int) int {
	return sp.pos - i
}

func (s *span) end() int { return s.pos + s.size }

type spanalloc struct {
	// free and used are ordered by pos
	free, used []span
	max        int // max pos+size
}

func (s *spanalloc) reserve(pos, size int) {
	if len(s.free) != 0 {
		panic("spanalloc.reserve not called early enough")
	}
	if pos < s.lastused() {
		panic("spanalloc.reserve called on overlapping slots")
	}
	// for now, just reserve space in between
	// the reserved slots as well:
	if len(s.used) > 0 && s.used[len(s.used)-1].end() < pos {
		s.used[len(s.used)-1].size = pos - s.used[len(s.used)-1].pos
	}
	s.used = append(s.used, span{pos: pos, size: size})
	slices.SortFunc(s.used, func(a, b span) bool {
		return a.pos-b.pos < 0
	})
}

func (s *spanalloc) lastused() int {
	l := len(s.used)
	if l == 0 {
		return 0
	}
	return s.used[l-1].end()
}

// get allocates a span of size n with the given alignment
// and returns its position
//
// align must be a power of 2
func (s *spanalloc) get(n, align int) int {
	best := -1         // current best fit candidate
	fit := math.MaxInt // gap bytes for current candidate

	// traverse freelist and pick the free span
	// with the size closest to the desired size
	for i := range s.free {
		size := s.free[i].size
		pos := s.free[i].pos
		end := s.free[i].end()
		alignedpos := (pos + align - 1) &^ (align - 1)
		if alignedpos+n > end {
			continue // doesn't fit
		}
		gap := size - n
		if gap < fit {
			best = i
			fit = gap
			if fit == 0 {
				break // found a perfect fit
			}
		}
	}
	if best == -1 {
		// allocate after the last chunk
		pos := s.lastused()
		alignedpos := (pos + align - 1) &^ (align - 1)
		if alignedpos-pos > 0 {
			// create a free span for padding:
			s.free = append(s.free, span{pos: pos, size: alignedpos - pos})
		}
		s.used = append(s.used, span{pos: alignedpos, size: n})
		if end := alignedpos + n; end > s.max {
			s.max = end
		}
		return alignedpos
	}
	pos := s.free[best].pos
	alignedpos := (pos + align - 1) &^ (align - 1)
	if pos < alignedpos {
		// free leading padding
		s.free = slices.Insert(s.free, best, span{pos: pos, size: alignedpos - pos})
		best++
	}
	end := s.free[best].end()
	if end > alignedpos+n {
		// free trailing padding
		s.free[best].pos = alignedpos + n
		s.free[best].size = end - (alignedpos + n) // tail gap
	} else {
		s.free = slices.Delete(s.free, best, best+1)
	}

	// create the new used span
	i, ok := slices.BinarySearchFunc(s.used, alignedpos, spancmp)
	if ok {
		panic("duplicate position in used span list")
	}
	s.used = slices.Insert(s.used, i, span{pos: alignedpos, size: n})
	return alignedpos
}

func (s *spanalloc) drop(pos int) {
	n, ok := slices.BinarySearchFunc(s.used, pos, spancmp)
	if !ok {
		panic(fmt.Sprintf("free of position %v; used slots: %#v", pos, s.used))
	}
	width := s.used[n].size
	s.used = slices.Delete(s.used, n, n+1)

	n, ok = slices.BinarySearchFunc(s.free, pos, spancmp)
	if ok {
		panic(fmt.Sprintf("used and free lists both contain pos %v", pos))
	}
	l := len(s.free)
	if n > 0 && s.free[n-1].end() == pos {
		// extend preceding span forwards instead of inserting a new span:
		s.free[n-1].size += width
		// ... and extend through the subsequent span
		// if it begins immediately following the extended span:
		if n < l && s.free[n-1].end() == s.free[n].pos {
			s.free[n-1].size += s.free[n].size
			s.free = slices.Delete(s.free, n, n+1)
		}
	} else if n < l && s.free[n].pos == pos+width {
		// extend subsequent span backwards instead of inserting a new span:
		s.free[n].pos = pos
		s.free[n].size += width
	} else {
		// slow path: create a new span
		s.free = slices.Insert(s.free, n, span{pos: pos, size: width})
	}
	// don't bother keeping around free spans
	// that point past the last-used span
	if s.free[len(s.free)-1].pos == s.lastused() {
		s.free = s.free[:len(s.free)-1]
	}
}
