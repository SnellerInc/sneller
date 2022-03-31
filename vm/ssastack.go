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
)

// stackslot is a relative offset into a virtual stack buffer
type stackslot uint16

const invalidstackslot = stackslot(0xFFFF)

// The type of the stack used by bytecode programs.
//
// At the moment the stack is separated to virtual stack and hash stack.
// Virtual stack is used to spill all registers except hash registers.
type stacktype uint8

const (
	stackTypeV stacktype = iota
	stackTypeH
	stackTypeCount
)

// Groups registers that have the same size so the allocator can reuse freed
// registers more efficiently (sharing stack sizes instead of register types)
type regsizegroup uint8

const (
	regSizeGroup1xK regsizegroup = iota
	regSizeGroup2xVec
	regSizeGroup4xVec
	regSizeGroupCount
)

var stackTypeByRegClass = [...]stacktype{
	regK: stackTypeV,
	regS: stackTypeV,
	regV: stackTypeV,
	regB: stackTypeV,
	regH: stackTypeH,
	regL: stackTypeV,
}

var regSizeGroupByRegClass = [...]regsizegroup{
	regK: regSizeGroup1xK,
	regS: regSizeGroup2xVec,
	regV: regSizeGroup2xVec,
	regB: regSizeGroup2xVec,
	regH: regSizeGroup4xVec,
	regL: regSizeGroup2xVec,
}

var regSizeByRegClass = [...]uint32{
	regK: kRegSize,
	regS: sRegSize,
	regV: vRegSize,
	regB: bRegSize,
	regH: hRegSize,
	regL: lRegSize,
}

// Calculates a stack slot from the given index.
//
// Can be used to calculate stack slots that will be reserved by a
// program from an increasing set of indexes that start from zero.
func stackSlotFromIndex(rc regclass, index int) stackslot {
	slot := uint(index) * uint(regSizeByRegClass[rc])

	// The last slot is invalidstackslot, we have to refuse it
	// too as it's not a valid slot.
	if slot >= uint(invalidstackslot) {
		panic(fmt.Sprintf("Overflow in stackSlotFromIndex(): the specified index %d is too big to be a stack slot", index))
	}

	return stackslot(slot)
}

type reservedslot struct {
	offset uint32
	size   uint32
}

// Used by stackmap to track the allocation of separate stack (low level interface).
type stackalloc struct {
	offset        uint32         // offset in a virtual stack where next stack will be allocated
	reservedIndex int            // index of the first reserved slot in reservedSlots array
	reservedSlots []reservedslot // sorted array that holds all reserved slots of this stack
}

// Reserves a slot of the specified size on the stack.
//
// This is a special purpose function that was designed to reserve certain areas in the stack that hold
// data from a previous execution of a different program and then serve as inputs to a next program. It's
// an error if the reserved slot overlaps another slot, which was either reserved or already allocated.
func (s *stackalloc) reserveSlot(slot stackslot, size int) {
	offset := int(slot)

	if offset < int(s.offset) {
		panic(fmt.Sprintf(
			"invalid state in stackmap.reserveSlot(): cannot reserve slot %d of size %d, which is before the current offset %d",
			offset, size, s.offset))
	}

	for i, reservedSlot := range s.reservedSlots {
		// Skips slots before `offset`.
		if int(reservedSlot.offset)+int(reservedSlot.size) <= offset {
			continue
		}

		if int(reservedSlot.offset) >= offset+size {
			// Inserts the required slot into reservedSlots array, keeping it sorted.
			s.reservedSlots = append(s.reservedSlots[:i+1], s.reservedSlots[i:]...)
			s.reservedSlots[i] = reservedslot{offset: uint32(offset), size: uint32(size)}
			return
		}

		panic(fmt.Sprintf(
			"invalid state in stackmap.reserveSlot(): cannot reserve slot %d of size %d, which overlaps with slot %d of size %d",
			offset, size, reservedSlot.offset, reservedSlot.size))
	}

	// Append to the end if we haven't inserted the slot anywhere in the middle.
	s.reservedSlots = append(s.reservedSlots, reservedslot{offset: uint32(offset), size: uint32(size)})
}

// Allocates the requested size on the stack
func (s *stackalloc) allocSlot(size int) stackslot {
	// Check whether we have to consider reserved slots. If true, then we have to check whether
	// the current allocation would not overlap with regions, which were explicitly reserved.
	slot := invalidstackslot

	if s.reservedIndex < len(s.reservedSlots) {
		for s.reservedIndex < len(s.reservedSlots) {
			reservedSlot := s.reservedSlots[s.reservedIndex]
			if s.offset > reservedSlot.offset {
				panic(fmt.Sprintf("invalid state in stackalloc.allocSlot(): current offset %d is greater than the closest reserved offset %d", s.offset, reservedSlot.offset))
			}

			slot = stackslot(s.offset)
			remainingSpace := int(reservedSlot.offset - s.offset)
			if remainingSpace > size {
				// Cannot advance reservedIndex as there will still be space left after
				// allocating the current slot.
				s.offset += uint32(size)
				return slot
			}

			s.reservedIndex++
			s.offset = reservedSlot.offset + reservedSlot.size
			if remainingSpace == size {
				return slot
			}

			// Reset, we cannot use it.
			slot = invalidstackslot
		}
	}

	if slot == invalidstackslot {
		slot = stackslot(s.offset)
		s.offset += uint32(size)
	}

	return slot
}

// stackmap manages a virtual stack. It allows to allocate stack regions either permanently or temporarily.
// Temporary allocations can be reused by multiple virtual registers that don't live at the same time.
type stackmap struct {
	allocator [stackTypeCount]stackalloc     // low-level allocator for each virtual stack
	freeSlots [regSizeGroupCount][]stackslot // a map of unuset slots for each register size group
	idToSlot  [_maxregclass][]stackslot      // a map that holds allocated value IDs and their stack slots
}

// Initializes the stackmap.
func (s *stackmap) init() {}

func (s *stackmap) reserveSlot(rc regclass, slot stackslot) {
	stackType := stackTypeByRegClass[rc]
	s.allocator[stackType].reserveSlot(slot, int(regSizeByRegClass[rc]))
}

// Allocates a slot of the specified register class
//
// NOTE: This does not associate the allocated slot with a value, use allocValue() for that.
func (s *stackmap) allocSlot(rc regclass) stackslot {
	regSizeGroup := regSizeGroupByRegClass[rc]

	// Try free slots first.
	freeSlots := s.freeSlots[regSizeGroup]
	if len(freeSlots) > 0 {
		slot := freeSlots[len(freeSlots)-1]
		s.freeSlots[regSizeGroup] = freeSlots[:len(freeSlots)-1]
		return slot
	}

	slotSize := uint32(regSizeByRegClass[rc])
	if slotSize == 0 {
		// If you hit this, most likely a new register class was added and tables not updated.
		panic("invalid state in stackmap.allocSlot(): slot size is zero")
	}

	stackType := stackTypeByRegClass[rc]
	allocator := &s.allocator[stackType]

	// Always allocate 8-byte regions of the stack to make it aligned to 64-bit units.
	alignedSlotSize := (slotSize + 7) & ^uint32(7)
	slot := allocator.allocSlot(int(alignedSlotSize))

	// Keep stack size always aligned to 8 bytes, because this is the unit that we allocate.
	// If we just allocated a smaller unit (like mask) then allocate more slots of the same
	// kind and add them to freeSlots - this way we would reuse the space we just aligned
	// for more masks, when needed, and kept the data aligned so we can assume uint64 to be
	// the allocation unit.
	if slotSize < 8 {
		extraOffset := int(alignedSlotSize) - int(slotSize)
		for extraOffset >= int(slotSize) {
			freeSlots = append(freeSlots, slot+stackslot(extraOffset))
			extraOffset -= int(slotSize)
		}
		s.freeSlots[regSizeGroup] = freeSlots
	}

	return slot
}

// frees an already allocated slot so it can be reused later.
func (s *stackmap) freeSlot(rc regclass, slot stackslot) {
	regSizeGroup := regSizeGroupByRegClass[rc]
	s.freeSlots[regSizeGroup] = append(s.freeSlots[regSizeGroup], slot)
}

func (s *stackmap) allocValue(rc regclass, valueID int) stackslot {
	slot := s.allocSlot(rc)

	if valueID >= 0 {
		idToSlot := s.idToSlot[rc]
		for i := len(idToSlot); i <= valueID; i++ {
			idToSlot = append(idToSlot, invalidstackslot)
		}
		idToSlot[valueID] = slot
		s.idToSlot[rc] = idToSlot
	}

	return slot
}

func (s *stackmap) freeValue(rc regclass, valueID int) {
	slot := s.idToSlot[rc][valueID]
	if slot == invalidstackslot {
		panic("invalid stack slot used in stackmap.freeValue(): the value is not allocated")
	}
	s.freeSlot(rc, slot)
	s.idToSlot[rc][valueID] = invalidstackslot
}

func (s *stackmap) replaceValue(rc regclass, oldID, newID int) {
	if oldID < 0 || newID < 0 {
		panic("both value ids must be valid when exchanging a value")
	}

	idToSlot := s.idToSlot[rc]

	if len(idToSlot) <= newID {
		for len(idToSlot) <= newID {
			idToSlot = append(idToSlot, invalidstackslot)
		}
		s.idToSlot[rc] = idToSlot
	}

	idToSlot[newID] = idToSlot[oldID]
	idToSlot[oldID] = invalidstackslot
}

func (s *stackmap) slotOf(rc regclass, valueID int) stackslot {
	idToSlot := s.idToSlot[rc]
	if len(idToSlot) <= valueID {
		return invalidstackslot
	}
	return idToSlot[valueID]
}

func (s *stackmap) hasSlot(rc regclass, valueID int) bool {
	return s.slotOf(rc, valueID) != invalidstackslot
}

func (s *stackmap) stackSize(st stacktype) int {
	return s.stackSizeInUInt64Units(st) << 3
}

func (s *stackmap) stackSizeInUInt64Units(st stacktype) int {
	allocator := &s.allocator[st]
	offset := allocator.offset
	if len(allocator.reservedSlots) > 0 {
		last := allocator.reservedSlots[len(allocator.reservedSlots)-1]
		reservedEnd := last.offset + last.size
		if offset < reservedEnd {
			offset = reservedEnd
		}
	}
	return int((offset + 7) >> 3)
}
