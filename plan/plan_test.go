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

package plan

import (
	"context"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/exp/slices"
)

type sizeHandle int64

func (s sizeHandle) Open(_ context.Context) (vm.Table, error) {
	panic("nope!")
}

func (s sizeHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	panic("nope!")
}

func (s sizeHandle) Size() int64 { return int64(s) }

func TestDistribute(t *testing.T) {
	run := func(in []int64, val int, out []int) {
		t.Helper()
		parts := make([]TablePart, len(in))
		for i := range parts {
			parts[i].Handle = sizeHandle(in[i])
		}
		result := distribute(parts, val)
		if !slices.Equal(result, out) {
			t.Errorf("got %v want %v", result, out)
		}
	}

	run([]int64{0, 0, 0}, 3, []int{1, 1, 1})
	run([]int64{100, 200}, 3, []int{1, 2})
	run([]int64{100, 200}, 4, []int{1, 3})
	run([]int64{100, 200, 300}, 6, []int{1, 2, 3})
	run([]int64{100, 200, 300}, 12, []int{2, 4, 6})
	run([]int64{100, 200, 300}, 13, []int{2, 4, 7})
	run([]int64{102478}, 8, []int{8})
}

func TestNewSplit(t *testing.T) {
	testcases := []struct {
		name  string
		input string
	}{
		{
			name:  "issue 2561",
			input: `SELECT MISSING X, COUNT(*) FILTER (WHERE X)`,
		},
	}

	for i := range testcases {
		tc := &testcases[i]
		t.Run(tc.name, func(t *testing.T) {
			text := []byte(tc.input)
			q, err := partiql.Parse(text)
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewSplit(q, testEnv{})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

type testEnv struct{}

type testHandle struct{}

func (f testHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

func (f testHandle) Size() int64 { return 0 }

func (f testHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, nil
}

func (f testEnv) Stat(_ expr.Node, _ *Hints) (TableHandle, error) {
	return testHandle{}, nil
}
