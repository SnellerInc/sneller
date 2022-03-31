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
	"net/http"

	"github.com/SnellerInc/sneller/db"
)

func (s *server) databasesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}

	pattern := r.URL.Query().Get("pattern")

	e, err := environ(tenant, "")
	if err != nil {
		s.logger.Printf("unable to load databases for tenant '%v' %s\n", tenant, err)
		writeInternalServerResponse(w, err)
		return
	}
	res, err := db.List(e.(*fsEnv).root)
	if err != nil {
		s.logger.Printf("unable to load databases for tenant '%v' %s\n", tenant, err)
		writeInternalServerResponse(w, err)
		return
	}

	out := make([]database, 0)
	for i := range res {
		if pattern == "" || matchPattern(res[i], pattern) {
			out = append(out, database{
				Name: res[i],
			})
		}
	}
	writeResultResponse(w, http.StatusOK, out)
}

type database struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}
