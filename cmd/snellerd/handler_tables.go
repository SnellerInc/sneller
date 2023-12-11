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

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/db"
)

func (s *server) tablesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}

	databaseName := r.URL.Query().Get("database")
	if databaseName == "" {
		http.Error(w, "no database", http.StatusBadRequest)
		return
	}

	pattern := r.URL.Query().Get("pattern")
	e, err := sneller.Environ(tenant, databaseName)
	if err != nil {
		s.logger.Printf("refusing tenant: newEnv: %s", err)
		http.Error(w, "bad tenant ID", http.StatusForbidden)
		return
	}
	tables, err := db.Tables(e.Root, databaseName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, "no such database", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("cannot list tables: %s", err), http.StatusInternalServerError)
		return
	}

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	out := make([]string, 0)
	for i := range tables {
		if pattern == "" || matchPattern(tables[i], pattern) {
			out = append(out, tables[i])
		}
	}
	writeResultResponse(w, http.StatusOK, out)
}
