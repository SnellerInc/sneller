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
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type filterEnv struct {
	env testenv
	// all filters passed to Filter on any handle
	// returned by Stat
	filters []string
}

func (e *filterEnv) Stat(t *expr.Table) (TableHandle, error) {
	th, err := e.env.Stat(t)
	if err != nil {
		return nil, err
	}
	e.filters = nil
	return &filterHandle{
		th:  th,
		env: e,
	}, nil
}

func (e *filterEnv) Schema(*expr.Table) expr.Hint {
	return nil
}

type filterHandle struct {
	th  TableHandle
	env *filterEnv
}

func (h *filterHandle) Open() (vm.Table, error) {
	return h.th.Open()
}

func (h *filterHandle) Filter(f expr.Node) TableHandle {
	h.env.filters = append(h.env.filters, expr.ToString(f))
	return h
}

// TestFilter tests that filters are pushed down to any
// TableHandle implementing Filterable.
func TestFilter(t *testing.T) {
	tcs := []struct {
		query       string
		newFilters  []string
		execFilters []string
	}{{
		query:       `SELECT * FROM 'parking.10n'`,
		newFilters:  nil,
		execFilters: nil,
	}, {
		query: `SELECT * FROM 'parking.10n' WHERE Make IS MISSING`,
		newFilters: []string{
			"Make IS MISSING",
		}, execFilters: []string{
			"Make IS MISSING",
		},
	}, {
		query: `SELECT * FROM 'parking.10n' WHERE BEFORE(IssueData, (SELECT LATEST(IssueData) FROM 'parking.10n' WHERE Make IS MISSING))`,
		newFilters: []string{
			"Make IS MISSING",
		}, execFilters: []string{
			"Make IS MISSING",
			"BEFORE(IssueData, `2000-01-01T00:00:00Z`)",
		},
	}, {
		query:       `SELECT * FROM (SELECT COUNT(*) AS foo FROM 'parking.10n') WHERE foo < 1000`,
		newFilters:  nil, // shouldn't push past COUNT(*)
		execFilters: nil,
	}}

	for i := range tcs {
		tc := &tcs[i]
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			// Test that filters are pushed
			// down when a tree is created
			// from a parsed query.
			env := filterEnv{
				env: testenv{t: t},
			}
			q, err := partiql.Parse([]byte(tc.query))
			if err != nil {
				t.Fatal(err)
			}
			tree, err := New(q, &env)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("tree:", tree)
			if !reflect.DeepEqual(env.filters, tc.newFilters) {
				t.Errorf("New: filter expression mismatch")
				t.Errorf("\tgot:  %q", env.filters)
				t.Errorf("\twant: %q", tc.newFilters)
			}
			// Test that filters are pushed
			// down when exec is called on
			// a deserialized tree.
			var buf ion.Buffer
			var st ion.Symtab
			err = tree.Encode(&buf, &st)
			if err != nil {
				t.Fatal(err)
			}
			tree, err = Decode(&env, &st, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			env.filters = nil
			var dst bytes.Buffer
			var stat ExecStats
			err = Exec(tree, &dst, &stat)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(env.filters, tc.execFilters) {
				t.Errorf("Exec: filter expression mismatch")
				t.Errorf("\tgot:  %q", env.filters)
				t.Errorf("\twant: %q", tc.execFilters)
			}
		})
	}
}
