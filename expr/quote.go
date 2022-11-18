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
