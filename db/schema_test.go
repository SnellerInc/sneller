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
		Inputs: []Input{
			{
				Pattern: "s3://my-bucket/my-folder/*.json",
				Format:  "json",
				Hints:   json.RawMessage(`"xyz-data-is-ignored-for-now"`),
			},
		},
	}
	dfs := newDirFS(t, dir)
	err = WriteDefinition(dfs, "foo", "bar", ref)
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
