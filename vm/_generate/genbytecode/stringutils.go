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
