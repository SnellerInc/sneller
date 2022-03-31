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

	var unseen []string
	for name, op := range name2Builtin {
		info := &builtinInfo[op]
		if info.private {
			continue
		}
		unseen = append(unseen, name)
	}

	for s.Scan() {
		if len(unseen) == 0 {
			break
		}
		text := s.Text()
		if !strings.HasPrefix(text, "#### ") {
			continue
		}
		for i := 0; i < len(unseen); i++ {
			if strings.Contains(text, unseen[i]) {
				unseen[i] = unseen[len(unseen)-1]
				unseen = unseen[:len(unseen)-1]
				i--
				continue
			}
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	for i := range unseen {
		t.Errorf("no documentation for builtin %s", unseen[i])
	}
}
