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
	"bufio"
	"os"
	"strings"
	"testing"
)

// every built-in function
// that isn't marked 'private'
// should have corresponding documentation
// in the reference manual
func TestBuiltinDocs(t *testing.T) {
	const docfile = "../doc/sneller-SQL.md"

	f, err := os.Open(docfile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)

	unseen := make(map[string]struct{})
	for op := BuiltinOp(0); op < Unspecified; op++ {
		if !builtinInfo[op].private {
			name := op.String()
			unseen[name] = struct{}{}
		}
	}

	for op := OpNone + 1; op < maxAggregateOp; op++ {
		if !op.private() {
			unseen[op.String()] = struct{}{}
		}
	}

	for s.Scan() {
		if len(unseen) == 0 {
			break
		}
		text := s.Text()
		if !strings.HasPrefix(text, "#### ") {
			continue
		}
		terms := normalizeTitle(text)
		for i := range terms {
			delete(unseen, terms[i])
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	for name := range unseen {
		t.Errorf("no documentation for builtin or aggregate %s", name)
	}
}

func normalizeTitle(s string) []string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '_':
			return r
		default:
			return ' '
		}
	}, s)

	return strings.Split(s, " ")
}
