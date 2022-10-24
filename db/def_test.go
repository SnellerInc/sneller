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

package db

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSynthDefinition(t *testing.T) {
	tmpdir := t.TempDir()
	ofs := NewDirFS(tmpdir)
	const db = "test-db"
	write := func(d *TableDefinition) {
		t.Helper()
		j, err := json.MarshalIndent(d, "", "\t")
		if err != nil {
			t.Fatal(err)
		}
		dp := TableDefinitionPath(db, d.Name)
		_, err = ofs.WriteFile(dp, j)
		if err != nil {
			t.Fatal(err)
		}
	}
	write(&TableDefinition{
		Name: "foo",
		Inputs: []Input{
			{Pattern: "file://foo-prefix/*.10n"},
			{Pattern: "file://foo-prefix/*.json"},
		},
	})
	write(&TableDefinition{
		Name: "bar",
		Inputs: []Input{
			{Pattern: "file://bar-prefix/*.json"},
		},
	})
	root, err := OpenDefinition(ofs, db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Definition{
		Name: db,
		Tables: []*TableDefinition{{
			Name: "bar",
			Inputs: []Input{
				{Pattern: "file://bar-prefix/*.json"},
			},
		}, {
			Name: "foo",
			Inputs: []Input{
				{Pattern: "file://foo-prefix/*.10n"},
				{Pattern: "file://foo-prefix/*.json"},
			},
		}},
	}
	if !reflect.DeepEqual(want, root) {
		t.Fatal(want, "!=", root)
	}
	// check that it exists
	f, err := ofs.Open(DefinitionPath(db))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestExpand(t *testing.T) {
	tmp := t.TempDir()
	for _, name := range []string{
		"foo.json",
		"foo-foo.json",
		"foo-bar.json",
		"prefix/foo/input-a.json",
		"prefix/foo/input-b.json",
		"prefix/bar/input-a.json",
	} {
		path := filepath.Join(tmp, name)
		if dir, _ := filepath.Split(path); dir != "" {
			err := os.MkdirAll(dir, 0750)
			if err != nil {
				t.Fatal(err)
			}
		}
		err := os.WriteFile(path, nil, 0640)
		if err != nil {
			t.Fatal(err)
		}
	}
	tn := newTenant(NewDirFS(tmp))
	def := &Definition{
		Name: "test-db",
		Tables: []*TableDefinition{{
			Name: "foo",
			Inputs: []Input{{
				Pattern: "file://foo.json",
			}},
		}, {
			Name: "foo-$bar",
			Inputs: []Input{{
				Pattern: "file://foo-{bar}.json",
			}},
		}, {
			Name: "prefix-$foo",
			Inputs: []Input{{
				Pattern: "file://prefix/{foo}/input-*.json",
			}},
		}},
	}
	got, err := def.Expand(tn, "*")
	if err != nil {
		t.Fatal(err)
	}
	want := []*TableDefinition{{
		Name: "foo",
		Inputs: []Input{{
			Pattern: "file://foo.json",
		}},
	}, {
		Name: "foo-bar",
		Inputs: []Input{{
			Pattern: "file://foo-bar.json",
		}},
	}, {
		Name: "foo-foo",
		Inputs: []Input{{
			Pattern: "file://foo-foo.json",
		}},
	}, {
		Name: "prefix-bar",
		Inputs: []Input{{
			Pattern: "file://prefix/bar/input-*.json",
		}},
	}, {
		Name: "prefix-foo",
		Inputs: []Input{{
			Pattern: "file://prefix/foo/input-*.json",
		}},
	}}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("expanded definitions did not match:")
		t.Errorf("  want: %s", tojson(want))
		t.Errorf("  got:  %s", tojson(got))
	}
}

func tojson(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
