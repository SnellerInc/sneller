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

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// Quote produces SQL single-quoted strings;
// escape sequences are encoded using either the
// traditional ascii escapes (\n, \t, etc.)
// or extended unicode escapes (\u0100, etc.) where appropriate
func Quote(s string) string {
	var buf strings.Builder
	quote(&buf, s)

	return buf.String()
}

func quote(out *strings.Builder, s string) {
	var tmp []byte
	out.WriteByte('\'')
	for _, r := range s {
		switch {
		case r == '\'' || r == '/' || r == '\\': // non-standard escaped chars
			out.WriteRune('\\')
			out.WriteRune(r)

		case (r < utf8.RuneSelf && strconv.IsPrint(r)) || r == '"':
			out.WriteRune(r)

		default:
			tmp = strconv.AppendQuoteRuneToASCII(tmp[:0], r)
			out.Write(tmp[1 : len(tmp)-1])
		}
	}
	out.WriteByte('\'')
}
