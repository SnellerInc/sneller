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

package elastic_proxy

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"testing"
)

func TestParseQueries(t *testing.T) {
	dir := os.DirFS("testqueries")
	queryFiles, err := fs.Glob(dir, "*.json")
	if err != nil {
		t.Fatalf("can't access testqueries folder: %v", err)
	}
	for _, qf := range queryFiles {
		queryJSON, err := fs.ReadFile(dir, qf)
		if err != nil {
			t.Errorf("can't access %q: %v", qf, err)
			continue
		}

		var q Query
		if err := json.Unmarshal([]byte(queryJSON), &q); err != nil {
			t.Errorf("can't unmarshal %q: %v", queryJSON, err)
		}
	}
}

func TestQueries(t *testing.T) {
	dir := os.DirFS("testqueries")
	queryFiles, err := fs.Glob(dir, "*.json")
	if err != nil {
		t.Fatalf("can't access testqueries folder: %v", err)
	}
	for _, qf := range queryFiles {
		queryJSON, err := fs.ReadFile(dir, qf)
		if err != nil {
			t.Errorf("can't access %q: %v", qf, err)
			continue
		}

		expectedSQL, err := fs.ReadFile(dir, qf+".sql")
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("skipping %q, because no SQL file exists", qf)
			} else {
				t.Errorf("can't access %q: %v", qf+".sql", err)
			}
			continue
		}

		t.Run(qf, func(t *testing.T) {
			var q Query
			if err := json.Unmarshal([]byte(queryJSON), &q); err != nil {
				t.Errorf("can't unmarshal %q: %v", queryJSON, err)
			}
			qc := QueryContext{
				TypeMapping: map[string]TypeMapping{
					"u_*": {
						Type: "text",
						Fields: map[string]string{
							"keyword": "keyword",
							"raw":     "keyword-ignore-case",
						},
					},
					"timestamp": {
						Type: "datetime",
					},
					"server_timestamp": {
						Type: "unix_nano_seconds",
					},
				},
			}
			e, err := q.Expression(&qc)
			if err != nil {
				t.Errorf("can't process query %q: %v", queryJSON, err)
			}

			sql := printExpr(e, true)
			if sql != string(expectedSQL) {
				t.Errorf("%q: expected: %s, got: %s", qf, expectedSQL, sql)
			}
		})
	}
}
