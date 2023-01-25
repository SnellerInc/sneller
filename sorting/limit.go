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

package sorting

// Limit stores raw values of LIMIT and OFFSET from a query.
type Limit struct {
	Limit, Offset int
}

// FinalRange calculates the range of rows that has to be actually output.
//
// It takes into account the number of rows.
func (l *Limit) FinalRange(rowsCount int) indicesRange {
	if l.Offset >= rowsCount {
		return indicesRange{start: rowsCount, end: rowsCount}
	}

	return indicesRange{start: l.Offset,
		end: minInt(l.Offset+l.Limit-1, rowsCount-1)}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}
