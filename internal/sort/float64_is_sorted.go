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

package sort

// Code generated by generator.go; DO NOT EDIT.

func isSortedAscFloat64(seq []uint64) bool {
	if len(seq) <= 1 {
		return true
	}

	prev := seq[0]
	for _, curr := range seq {
		if curr < prev {
			return false
		}

		prev = curr
	}

	return true
}

func isSortedDescFloat64(seq []uint64) bool {
	if len(seq) <= 1 {
		return true
	}

	prev := seq[0]
	for _, curr := range seq {
		if curr > prev {
			return false
		}

		prev = curr
	}

	return true
}
