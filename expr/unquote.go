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
	"fmt"
	"unicode/utf8"
)

// Unescape converts special sequences \t, \n and also unicode
// chars \uhhhh into plain string.
func Unescape(buf []byte) (string, error) {
	var tmp []byte
	for i := 0; i < len(buf); i++ {
		c := buf[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(buf[i:])
			if r == utf8.RuneError {
				return "", fmt.Errorf("expr.Unescape: invalid rune 0x%x", buf[i:i+size])
			} else {
				tmp = append(tmp, buf[i:i+size]...)
			}
			i += size - 1
			continue
		} else if c != '\\' {
			tmp = append(tmp, c)
			continue
		}
		i++
		if i >= len(buf) {
			return "", fmt.Errorf("expr.Unescape: cannot unescape trailing \\")
		}
		c = buf[i]
		// from lex.rl:
		// escape_sequence = (("\\" [tvfnrab\\'/]) | ("\\u" xdigit{4}))
		switch c {
		case '\\':
			tmp = append(tmp, '\\')
		case 't':
			tmp = append(tmp, '\t')
		case 'n':
			tmp = append(tmp, '\n')
		case 'r':
			tmp = append(tmp, '\r')
		case 'v':
			tmp = append(tmp, '\v')
		case 'f':
			tmp = append(tmp, '\f')
		case 'a':
			tmp = append(tmp, '\a')
		case 'b':
			tmp = append(tmp, '\b')
		case '\'':
			tmp = append(tmp, '\'')
		case '/':
			tmp = append(tmp, '/')
		case 'u':
			r := rune(0)
			i++
			for j := i; j < i+4; j++ {
				if j >= len(buf) {
					return "", fmt.Errorf("expr.Unescape: invalid \\u escape sequence")
				}
				add := rune(buf[j])
				if add >= '0' && add <= '9' {
					add -= '0'
				} else if add >= 'A' && add <= 'F' {
					add -= 'A'
					add += 10
				} else if add >= 'a' && add <= 'f' {
					add -= 'a'
					add += 10
				} else {
					return "", fmt.Errorf("expr.Unescape: invalid hex digit %q", string(rune(buf[j])))
				}
				r = (r * 16) + add
			}
			i += 3
			if !utf8.ValidRune(r) {
				return "", fmt.Errorf("expr.Unescape: rune U%x is invalid", r)
			}
			tmp = utf8.AppendRune(tmp, r)
		default:
			return "", fmt.Errorf("expr.Unescape: unexpected backslash escape of %q (0x%[1]x)", c)
		}
	}
	return string(tmp), nil
}

// Unquote extracts the quoted and escaped SQL string
//
// See: Quote
func Unquote(s string) (string, error) {
	n := len(s)
	if n < 2 {
		return "", fmt.Errorf("expr.Unquote: string %q too short", s)
	}

	if s[0] != '\'' {
		return "", fmt.Errorf(`expr.Unquote: string does not start with "'"`)
	}

	if s[n-1] != '\'' {
		return "", fmt.Errorf(`expr.Unquote: string does not end with "'"`)
	}

	return Unescape([]byte(s[1 : n-1]))
}
