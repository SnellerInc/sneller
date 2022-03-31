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

package ion

import (
	"unicode/utf8"
)

// Portions below copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file
// distributed with the Go source.

// safeSet holds the value true if the ASCII character with the given array
// position can be represented inside a JSON string without any further
// escaping.
//
// All values are true except for the ASCII control characters (0-31), the
// double quote ("), and the backslash character ("\").
var safeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      true,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      true,
	'=':      true,
	'>':      true,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}

var hex = "0123456789abcdef"

func (s *scratch) quoted(in []byte) []byte {
	s.buf = append(s.buf[:0], '"')
	start := 0
	for i := 0; i < len(in); {
		if b := in[i]; b < utf8.RuneSelf {
			if safeSet[b] {
				i++
				continue
			}
			if start < i {
				s.buf = append(s.buf, in[start:i]...)
			}
			s.buf = append(s.buf, '\\')
			switch b {
			case '\\', '"':
				s.buf = append(s.buf, b)
			case '\n':
				s.buf = append(s.buf, 'n')
			case '\r':
				s.buf = append(s.buf, 'r')
			case '\t':
				s.buf = append(s.buf, 't')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				s.buf = append(s.buf, 'u', '0', '0', hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRune(in[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				s.buf = append(s.buf, in[start:i]...)
			}
			s.buf = append(s.buf, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				s.buf = append(s.buf, in[start:i]...)
			}
			s.buf = append(s.buf, '\\', 'u', '2', '0', '2')
			s.buf = append(s.buf, hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(in) {
		s.buf = append(s.buf, in[start:]...)
	}
	s.buf = append(s.buf, '"')
	return s.buf
}

func (s *scratch) string(str string) []byte {
	s.buf = append(s.buf[:0], '"')
	start := 0
	for i := 0; i < len(str); {
		if b := str[i]; b < utf8.RuneSelf {
			if safeSet[b] {
				i++
				continue
			}
			if start < i {
				s.buf = append(s.buf, str[start:i]...)
			}
			s.buf = append(s.buf, '\\')
			switch b {
			case '\\', '"':
				s.buf = append(s.buf, b)
			case '\n':
				s.buf = append(s.buf, 'n')
			case '\r':
				s.buf = append(s.buf, 'r')
			case '\t':
				s.buf = append(s.buf, 't')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				s.buf = append(s.buf, 'u', '0', '0', hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(str[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				s.buf = append(s.buf, str[start:i]...)
			}
			s.buf = append(s.buf, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				s.buf = append(s.buf, str[start:i]...)
			}
			s.buf = append(s.buf, '\\', 'u', '2', '0', '2')
			s.buf = append(s.buf, hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(str) {
		s.buf = append(s.buf, str[start:]...)
	}
	s.buf = append(s.buf, '"')
	return s.buf
}
