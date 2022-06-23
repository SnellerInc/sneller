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

// LimitKind describes how to interpret query limits parameters.
type LimitKind byte

const (
	// Use the first rows in range [0:limit]
	LimitToHeadRows LimitKind = iota

	// Use the top rows in range [len(collection) - limit:]
	LimitToTopRows

	// Use subrange of rows in range [offset:offset + limit]
	LimitToRange
)

// Limit stores raw values of LIMIT and OFFSET from a query.
type Limit struct {
	Kind          LimitKind
	Limit, Offset int
}

// FinalRange calculates the range of rows that has to be actually output.
//
// It takes into account the number of rows.
func (l *Limit) FinalRange(rowsCount int) indicesRange {
	switch l.Kind {
	case LimitToHeadRows:
		return indicesRange{start: 0,
			end: minInt(rowsCount-1, l.Limit-1)}

	case LimitToTopRows:
		return indicesRange{start: maxInt(rowsCount-l.Limit, 0),
			end: rowsCount - 1}

	case LimitToRange:
		if l.Offset >= rowsCount {
			return indicesRange{start: rowsCount, end: rowsCount}
		}

		return indicesRange{start: l.Offset,
			end: minInt(l.Offset+l.Limit-1, rowsCount-1)}
	}

	return indicesRange{}
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
