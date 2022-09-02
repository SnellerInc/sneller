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

package expr

//go:generate go run terms.go -o simplify_gen.go simplify.rules
//go:generate goimports -w .

func staticSubstr(x String, i Integer, n Integer) String {
	start := int(i) - 1
	if start <= 0 {
		return x
	}
	if start >= len(x) {
		return String("")
	}
	length := int(n)
	if length < 0 {
		// According to the doc: "This number [length] can't be negative".
		// But the doc does not say what exactly do in such cases.
		return String("")
	}

	res := x[start:]
	if length > len(res) {
		length = len(res)
	}

	return res[:length]
}

func autoSimplify(e Node, h Hint) Node {
	better := simplify1(e, h)
	for better != nil {
		e = better
		e = Simplify(e, h)
		better = simplify1(better, h)
	}
	return e
}
