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

func (b *bytecode) find(delims [][2]uint32, cols int) (out []vRegLayout) {
	// FIXME: don't encode knowledge about
	// vectorization width here...
	blockCount := (len(delims) + bcLaneCount - 1) / bcLaneCount
	regCount := blockCount * cols
	minimumVStackSize := b.vstacksize + regCount*vRegSize

	b.ensureVStackSize(minimumVStackSize)
	b.allocStacks()

	// vstack is 64-bit words but we'd like to
	// view it as 32-bit words, so we have to
	// do a little unsafe juggling
	err := evalfind(b, delims, cols)
	if err != nil {
		panic(b.err)
	}

	out = vRegLayoutFromVStackCast(&b.vstack, regCount)
	return
}
