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

func TestDecodeDefinition(t *testing.T) {
	checkFiles(t)
	dir := t.TempDir()
	err := os.MkdirAll(filepath.Join(dir, "db", "foo", "bar"), 0750)
	if err != nil {
		t.Fatal(err)
	}
	data := `
{
    "name": "bar",
    "input": [
        {
            "pattern": "s3://my-bucket/my-folder/*.json",
            "format": "json",
            "hints": "xyz-data-is-ignored-for-now"
        }
    ]
}
`

	ref := &Definition{
		Name: "bar",
		Inputs: []Input{
			{
				Pattern: "s3://my-bucket/my-folder/*.json",
				Format:  "json",
				Hints:   json.RawMessage(`"xyz-data-is-ignored-for-now"`),
			},
		},
	}
	dfs := NewDirFS(dir)
	defer dfs.Close()
	err = WriteDefinition(dfs, "foo", ref)
	if err != nil {
		t.Fatal(err)
	}

	s, err := OpenDefinition(os.DirFS(dir), "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, ref) {
		t.Fatal("results not equivalent")
	}

	err = os.Remove(filepath.Join(dir, "db/foo/bar/definition.json"))
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(dir, "db/foo/bar/definition.json"), []byte(data), 0640)
	if err != nil {
		t.Fatal(err)
	}
	s, err = OpenDefinition(os.DirFS(dir), "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, ref) {
		t.Fatal("results not equivalent")
	}
}
