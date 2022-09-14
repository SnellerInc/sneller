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

	out := make([]string, 0)
	for i := range tables {
		if pattern == "" || matchPattern(tables[i], pattern) {
			out = append(out, tables[i])
		}
	}
	writeResultResponse(w, http.StatusOK, out)
}
