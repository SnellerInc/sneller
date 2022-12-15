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

// This code is shamelessly borrowed from https://pkg.go.dev/github.com/lmas/Damerau-Levenshtein

package fuzzy

// Return the smallest int from a list
func minimum(is ...int) int {
	min := is[0]
	for _, i := range is {
		if min > i {
			min = i
		}
	}
	return min
}

var defaultTDL = new(100)

// Distance is a shortcut func for doing a quick and dirty calculation,
// without having to set up your own struct and stuff.
// Not thread safe!
func Distance(a, b string) int {
	return defaultTDL.Distance(a, b)
}

////////////////////////////////////////////////////////////////////////////////

// TrueDamerauLevenshtein is a struct that allocates memory only once, which is
// used when running Distance().
// This whole struct and associated functions are not thread safe in any way,
// that will be the callers responsibility! At least for now...
type TrueDamerauLevenshtein struct {
	maxSize int
	matrix  [][]int
	da      map[rune]int
}

// new initializes a new struct which allocates memory only once, to be used by
// Distance().
// maxSize sets an upper limit for both input strings used in Distance().
func new(maxSize int) *TrueDamerauLevenshtein {
	t := &TrueDamerauLevenshtein{
		maxSize: maxSize,
		da:      make(map[rune]int),
	}
	t.grow(maxSize)
	return t
}

// grow grows the internal memory matrix.
func (t *TrueDamerauLevenshtein) grow(n int) {
	// bytes.Buffer.Grow() grows it's internal slice by 2 * cap() + n for example, let's try it too
	s := 2*cap(t.matrix) + n
	t.matrix = make([][]int, s)
	for i := range t.matrix {
		t.matrix[i] = make([]int, s)
	}
	t.maxSize = s
}

// Distance calculates and returns the true Damerau–Levenshtein distance of string A and B.
// It's the caller's responsibility if he wants to trim whitespace or fix lower/upper cases.
//
// If either of string A or B is too large for the internal memory matrix, we will allocate a bigger
// matrix on the fly. If not, Distance() won't cause any other allocs.
func (t *TrueDamerauLevenshtein) Distance(a, b string) int {
	lenA, lenB := len(a), len(b)
	switch {
	case lenA < 1:
		return lenB
	case lenB < 1:
		return lenA
	case lenA >= t.maxSize-1:
		t.grow(lenA)
	case lenB >= t.maxSize-1:
		t.grow(lenB)
	}

	t.matrix[0][0] = lenA + lenB + 1
	for i := 0; i <= lenA; i++ {
		t.matrix[i+1][1] = i
		t.matrix[i+1][0] = t.matrix[0][0]
	}
	for j := 0; j <= lenB; j++ {
		t.matrix[1][j+1] = j
		t.matrix[0][j+1] = t.matrix[0][0]
	}

	for _, r := range a + b {
		t.da[r] = 0
	}

	for i := 1; i <= lenA; i++ {
		db := 0
		for j := 1; j <= lenB; j++ {
			i1 := t.da[rune(b[j-1])]
			j1 := db
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
				db = j
			}

			// By "conventional wisdom", the costs for the ins/del/trans operations are always +1
			t.matrix[i+1][j+1] = minimum(
				t.matrix[i][j]+cost,                  // substitution
				t.matrix[i+1][j]+1,                   // insertion
				t.matrix[i][j+1]+1,                   // deletion
				t.matrix[i1][j1]+(i-i1-1)+1+(j-j1-1), // transposition
			)
		}
		t.da[rune(a[i-1])] = i
	}
	return t.matrix[lenA+1][lenB+1]
}

func editDistanceRef(data, needle string) int {
	return new(100).Distance(data, needle)
}
