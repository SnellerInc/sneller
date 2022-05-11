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

package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type emptyenv struct{}

func (e emptyenv) Stat(_, _ expr.Node) (TableHandle, error) {
	return e, nil
}

func (e emptyenv) Open() (vm.Table, error) {
	return nil, fmt.Errorf("cannot open emptyenv table")
}

func (e emptyenv) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

type twosplit struct{}

func (t twosplit) Split(e expr.Node, _ TableHandle) (Subtables, error) {
	sub := make(SubtableList, 2)
	for i := range sub {
		newstr := expr.Identifier(expr.ToString(e) + fmt.Sprintf("-part%d", i+1))
		sub[i] = Subtable{
			Transport: &LocalTransport{},
			Table: &expr.Table{
				Binding: expr.Bind(newstr, ""),
			},
		}
	}
	return sub, nil
}

func TestSplit(t *testing.T) {
	env := emptyenv{}
	tcs := []struct {
		query string
		lines []string
	}{
		{
			query: `SELECT COUNT(*) FROM foo`,
			lines: []string{
				"foo",
				"COUNT(*) AS $_0_0",
				// describes table -> [tables...] mapping
				"UNION MAP foo [foo-part1 foo-part2]",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\"",
			},
		},
	}

	for i := range tcs {
		query := tcs[i].query
		lines := tcs[i].lines
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			s, err := partiql.Parse([]byte(query))
			if err != nil {
				t.Fatal(err)
			}
			split, err := NewSplit(s, env, twosplit{})
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Join(lines, "\n") + "\n"
			if got := split.String(); got != want {
				t.Errorf("got plan\n%s", got)
				t.Errorf("wanted plan\n%s", want)
			}
		})
	}
}
