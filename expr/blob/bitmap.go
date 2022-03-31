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

package blob

// Bitmap is a set of bit flags.
type Bitmap []byte

// MakeBitmap makes a bitmap sized for n bits.
func MakeBitmap(n int) Bitmap {
	return make(Bitmap, (n+7)/8)
}

// Get the nth bit.
func (b Bitmap) Get(n int) bool {
	if b == nil || n < 0 {
		return false
	}
	i, m := n/8, n%8
	if i >= len(b) {
		return false
	}
	return b[i]&(1<<m) != 0
}

// Set the nth bit.
func (b Bitmap) Set(n int) {
	i, m := n/8, n%8
	b[i] |= 1 << m
}

// Unset the nth bit.
func (b Bitmap) Unset(n int) {
	i, m := n/8, n%8
	b[i] &^= 1 << m
}
