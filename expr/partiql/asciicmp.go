// Copyright (C) 2023 Sneller, Inc.
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

package partiql

func asciiUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return (b - 'a') + 'A'
	}

	return b
}

func equalASCII(anyCase, upperCaseOrNonLetter []byte) bool {
	if len(anyCase) != len(upperCaseOrNonLetter) {
		return false
	}

	for i := range anyCase {
		if asciiUpper(anyCase[i]) != upperCaseOrNonLetter[i] {
			return false
		}
	}

	return true
}
