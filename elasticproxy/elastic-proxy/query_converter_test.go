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
