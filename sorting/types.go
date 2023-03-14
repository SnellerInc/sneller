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

// Direction encodes a sorting direction of column (SQL: ASC/DESC)
type Direction int

const (
	Ascending  Direction = 1  // Sort ascending
	Descending Direction = -1 // Sort descending
)

// NullsOrder encodes order of null values (SQL: NULL FIRST/NULLS LAST)
type NullsOrder int

const (
	NullsFirst NullsOrder = iota // Null values goes first
	NullsLast                    // Null values goes last
)
