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

package fsutil

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// ErrBadPattern is returned by functions when a
// bad pattern is encountered.
var ErrBadPattern = errors.New("bad pattern")

// MaxCaptureGroups is the maximum number of
// capture groups that can appear in a table
// name template.
const MaxCaptureGroups = 8

// Matcher holds the results of a call to match.
// The caller should copy out the results as
// needed, as the Result field will be reused
// across calls to match.
type Matcher struct {
	// Result holds the expanded template
	Result []byte // expanded template

	// bookkeeping for capture groups
	caps [MaxCaptureGroups][2]string
}

// Match matches a pattern against the name. If
// template != "", this also attempts to expand
// the template with parts taken from name.
//
// The pattern syntax is:
//
//	pattern:
//	  { term }
//	term:
//	  '*'         matches any sequence of characters
//	              within a path segment
//	  '?'         matches any single non-/ character
//	  '[' [ '^' ] { char-range } ']'
//	              character class (may not be empty)
//	  '{' ident '}'
//	              matches a non-empty sequence of characters
//	              within a segment and captures the result
//	  c           matches character c (c != '*', '\\')
//	  '\\' c      matches character c
//	char-range:
//	  c           matches character c (c != '\\', '-', ']')
//	  '\\' c      matches character c
//	  lo '-' hi   matches character c for lo <= c <= hi
//	ident:
//	  ident-char { ident-char }
//	ident-char:
//	  'a' - 'z' | 'A' - 'Z' | '0' - '9' | '_'
//
// The template syntax is:
//
//	template:
//	  { term }
//	term:
//	  '$' '$'     expands to literal '$'
//	  '$' ident | '$' '{' ident '}'
//	              expands to the capture group
//	              named ident from pattern
//	  c           expands to character c (c != '$')
//	ident:
//	  ident-char { ident-char }
//	ident-char:
//	  'a' - 'z' | 'A' - 'Z' | '0' - '9' | '_'
//
// Multi-character wildcard groups (* and {...})
// will Match the shortest sequence possible.
// For example, the pattern "{x}-*-{y}" matched
// against "a-b-c-d" and expanded into the
// template "$x-y" will produce "a-c-d".
//
// This returns ErrBadPattern if pattern is
// malformed or has more than MaxCaptureGroups
// capture groups.
func (m *Matcher) Match(pattern, name string) (bool, error) {
	m.caps = [MaxCaptureGroups][2]string{}
	put := func(name, value string) bool {
		if name == "" || value == "" {
			return false
		}
		for i := range m.caps {
			if m.caps[i][0] == "" {
				m.caps[i][0] = name
				m.caps[i][1] = value
				return true
			}
			if m.caps[i][0] == name {
				return false
			}
		}
		return false
	}
	// match the pattern first
outer:
	for pattern != "" {
		wc, ident, part, rest, ok := splitmatch(pattern)
		if !ok {
			return false, ErrBadPattern
		}
		pattern = rest
		if wc && part == "" {
			// special handling for terminal wildcard
			got, rem := matchwc(name)
			name = rem
			if ident != "" {
				if got == "" {
					// disallow empty capture
					return false, nil
				}
				if !put(ident, got) {
					return false, ErrBadPattern
				}
			}
			break
		}
		slash := false
		for i := range name {
			if slash {
				// don't proceed past '/'
				break
			}
			slash = name[i] == '/'
			if ident != "" && i == 0 {
				// disallow empty capture
				continue
			}
			rem, found, ok := matchpart(part, name[i:])
			if !ok {
				return false, ErrBadPattern
			}
			if !found {
				if !wc {
					break
				}
				continue
			}
			if pattern != "" || rem == "" {
				if ident != "" {
					if !put(ident, name[:i]) {
						return false, ErrBadPattern
					}
				}
				name = rem
				continue outer
			}
		}
		// no match; check pattern syntax
		for pattern != "" {
			_, _, _, rest, ok := splitmatch(pattern)
			if !ok {
				return false, ErrBadPattern
			}
			pattern = rest
		}
		return false, nil
	}
	return name == "", nil
}

// Get gets a capture result from the last call
// to match. If a capture group with the given
// name was not found in the last call to match,
// this returns "".
func (m *Matcher) Get(name string) string {
	if name == "" {
		return ""
	}
	for i := range m.caps {
		if m.caps[i][0] == "" {
			break
		}
		if m.caps[i][0] == name {
			return m.caps[i][1]
		}
	}
	return ""
}

// Expand attempts to Expand a template from
// capture groups from the last call to match.
//
// The resulting slice is reused by the matcher
// and may be invalidated by subsequent calls.
// The caller should arrange to copy the result
// if needed between calls.
func (m *Matcher) Expand(template string) ([]byte, error) {
	m.Result = m.Result[:0]
	for template != "" {
		ident, part, rest, ok := splittemplate(template)
		if !ok {
			return nil, ErrBadPattern
		}
		if ident != "" {
			got := m.Get(ident)
			if got == "" {
				// no match for ident
				return nil, ErrBadPattern
			}
			m.Result = append(m.Result, got...)
		}
		m.Result = append(m.Result, part...)
		template = rest
	}
	return m.Result, nil
}

// ToGlob converts a pattern like
// "foo-{bar}.json" into "foo-*.json".
func ToGlob(pattern string) (string, error) {
	wc, id, part, rest, ok := splitmatch(pattern)
	if !ok {
		return "", ErrBadPattern
	}
	// avoid allocation if possible
	if rest == "" {
		if wc {
			if id != "" {
				return "*" + part, nil // pattern like "{id}part"
			}
			return pattern, nil // pattern like "*part"
		}
		return part, nil // pattern like "part"
	}
	var sb strings.Builder
	if wc {
		sb.WriteByte('*')
	}
	sb.WriteString(part)
	pattern = rest
	for pattern != "" {
		wc, _, part, rest, ok := splitmatch(pattern)
		if !ok {
			return "", ErrBadPattern
		}
		if wc {
			sb.WriteByte('*')
		}
		sb.WriteString(part)
		pattern = rest
	}
	return sb.String(), nil
}

func splitmatch(pattern string) (wc bool, ident, part, rest string, ok bool) {
	// check for wildcard (star or capture group)
	for pattern != "" {
		ch := pattern[0]
		if ch == '*' {
			if wc && ident != "" {
				// disallow star after capture group
				return wc, ident, part, rest, false
			}
		} else if ch == '{' {
			if wc {
				// disallow capture group after wildcard
				return wc, ident, part, rest, false
			}
			ident, pattern = splitident(pattern[1:])
			if ident == "" {
				// disallow empty ident
				return wc, ident, part, rest, false
			}
			if pattern == "" || pattern[0] != '}' {
				// require closing bracket
				return wc, ident, part, rest, false
			}
		} else {
			// no more wildcards
			break
		}
		wc = true
		pattern = pattern[1:]
	}
	// check for a non-wildcard segment
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\\' {
			if i >= len(pattern)-1 {
				// require next character
				return wc, ident, part, rest, false
			}
			i++
		} else if ch == '*' || ch == '{' {
			// don't proceed past wildcard
			return wc, ident, pattern[:i], pattern[i:], true
		}
	}
	// we consumed the whole pattern
	return wc, ident, pattern, "", true
}

// matchpart matches part against the prefix of
// name, returning the remaining part of name,
// whether or not a match was found, and whether
// or not part was syntatically valid.
func matchpart(part, name string) (rem string, found, ok bool) {
	found = true
	for part != "" {
		found = found && name != ""
		switch part[0] {
		case '?':
			if found {
				found = name[0] != '/'
				_, name = nextch(name)
			}
			part = part[1:]
		case '[':
			var ch rune
			if found {
				ch, name = nextch(name)
			}
			part = part[1:]
			if part == "" {
				// disallow trailing '['
				return "", false, false
			}
			negated := part[0] == '^'
			if negated {
				part = part[1:]
			}
			match := false
			for i := 0; ; i++ {
				if part != "" && part[0] == ']' && i > 0 {
					part = part[1:]
					break
				}
				lo, rest := getesc(part)
				if rest == "" {
					return "", false, false
				}
				hi := lo
				if rest[0] == '-' {
					hi, rest = getesc(rest[1:])
					if rest == "" {
						return "", false, false
					}
				}
				part = rest
				match = match || lo <= ch && ch <= hi
			}
			found = found && match != negated
		case '\\':
			// handle escaped characters
			part = part[1:]
			if part == "" {
				// disallow trailing '\'
				return "", false, false
			}
			fallthrough
		default:
			if found {
				found = part[0] == name[0]
				name = name[1:]
			}
			part = part[1:]
		}
	}
	return name, found, true
}

func getesc(part string) (ch rune, rest string) {
	if part == "" || part[0] == '-' || part[0] == ']' {
		return 0, ""
	}
	if part[0] == '\\' {
		part = part[1:]
		if part == "" {
			return 0, ""
		}
	}
	return nextch(part)
}

func matchwc(s string) (match, rem string) {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i:]
}

func splittemplate(template string) (ident, part, rest string, ok bool) {
	if template != "" && template[0] == '$' {
		template = template[1:]
		if template == "" {
			// disallow terminal '$'
			return ident, part, rest, false
		}
		if template[0] == '$' {
			// TODO: consume consecutive $$s?
			return "", "$", template[1:], true
		}
		brace := template[0] == '{'
		if brace {
			template = template[1:]
		}
		ident, template = splitident(template)
		if ident == "" {
			// disallow empty ident
			return ident, part, rest, false
		}
		if brace {
			if template == "" || template[0] != '}' {
				// check for end brace
				return ident, part, rest, false
			}
			template = template[1:]
		}
	}
	i := strings.IndexByte(template, '$')
	if i < 0 {
		part = template
	} else {
		part = template[:i]
		rest = template[i:]
	}
	return ident, part, rest, true
}

// ValidCaptureName returns whether or not ident
// is a valid capture group name for a glob
// pattern.
func ValidCaptureName(ident string) bool {
	_, rest := splitident(ident)
	return rest == ""
}

func splitident(s string) (ident, rest string) {
	for i := range s {
		if !isident(s[i]) {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

func isident(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

func nextch(s string) (ch rune, rest string) {
	ch, skip := utf8.DecodeRuneInString(s)
	if ch == utf8.RuneError && skip <= 1 {
		return ch, ""
	}
	return ch, s[skip:]
}
