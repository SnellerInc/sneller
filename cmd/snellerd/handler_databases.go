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
	"net/http"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/db"
)

func (s *server) databasesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}

	pattern := r.URL.Query().Get("pattern")

	e, err := sneller.Environ(tenant, "")
	if err != nil {
		s.logger.Printf("unable to load databases for tenant '%v' %s\n", tenant, err)
		writeInternalServerResponse(w, err)
		return
	}
	res, err := db.List(e.Root)
	if err != nil {
		s.logger.Printf("unable to load databases for tenant '%v' %s\n", tenant, err)
		writeInternalServerResponse(w, err)
		return
	}

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
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
