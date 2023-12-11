// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package expr

//go:generate go run terms.go -o simplify_gen.go -i simplify.rules
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
		// But the doc does not say what exactly do in such case.
		return String("")
	}

	res := x[start:]
	if length > len(res) {
		length = len(res)
	}

	return res[:length]
}

// staticArrayPosition evaluates ARRAY_POSITION(list, constant)
// according to the documentation.
func staticArrayPosition(l *List, c Constant) Node {
	if pos := l.Index(c); pos >= 0 {
		return Integer(pos + 1)
	}

	return Missing{}
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
