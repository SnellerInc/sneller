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

package main

import (
	"io"
	"math/rand"
	"strings"
)

type WrappedBuilder struct {
	Width  int
	Indent string

	lines  []string
	tokens []string
	length int
}

func (b *WrappedBuilder) Append(s string) {
	if b.length+len(s) > b.Width {
		b.flush()
	}

	b.length += len(s)
	b.tokens = append(b.tokens, s)
}

func (b *WrappedBuilder) WriteTo(f io.Writer) {
	b.flush()
	for i := range b.lines {
		io.WriteString(f, b.lines[i])
		io.WriteString(f, "\n")
	}
}

func (b *WrappedBuilder) flush() {
	if len(b.tokens) == 0 {
		return
	}

	if len(b.tokens) == 1 && b.tokens[0] == b.Indent {
		return
	}

	b.lines = append(b.lines, strings.Join(b.tokens, ""))
	b.length = len(b.Indent)
	b.tokens = []string{b.Indent}
}

func shuffle(s []string, r *rand.Rand) {
	for i := int32(len(s)) - 1; i > 0; i-- {
		j := r.Int31n(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}
