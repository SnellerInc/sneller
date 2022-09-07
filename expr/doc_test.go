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
	for name, op := range name2Builtin {
		if !builtinInfo[op].private {
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
