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
)

func (s *server) columnsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}

	databaseName := r.URL.Query().Get("database")
	if databaseName == "" {
		http.Error(w, "no database specified", http.StatusBadRequest)
		return
	}

	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, "no table specified", http.StatusBadRequest)
		return
	}

	pattern := r.URL.Query().Get("pattern")

	_ = tenant
	_ = pattern
	http.Error(w, "cannot find table", http.StatusNotImplemented)

	// database, err := s.schemas.NewSnellerDatabase(ctx, tenant, databaseName)
	// if err != nil {
	// 	if os.IsNotExist(err) {
	// 		http.Error(w, "cannot find database", http.StatusNotFound)
	// 		return
	// 	}

	// 	s.logger.Printf("unable to load sneller database '%v' for tenant '%v'\n", databaseName, tenant)
	// 	writeInternalServerResponse(w, err)
	// 	return
	// }

	// var columns []indexedColumn
	// for key, table := range database.Tables {
	// 	if key == tableName {
	// 		for index, table := range table.Columns {
	// 			if matchPattern(table.Name, pattern) {
	// 				columns = append(columns, indexedColumn{
	// 					Index:      index,
	// 					Name:       table.Name,
	// 					Type:       table.Type,
	// 					Nullable:   table.Nullable,
	// 					ColumnSize: table.ColumnSize,
	// 					Decimals:   table.Decimals,
	// 				})
	// 			}
	// 		}
	// 		writeResultResponse(w, http.StatusOK, columns)
	// 		return
	// 	}
	// }

	// http.Error(w, "cannot find table", http.StatusNotFound)
}

// type indexedColumn struct {
// 	Index      int    `json:"index"`
// 	Name       string `json:"name"`
// 	Type       string `json:"type"`
// 	Nullable   *bool  `json:"nullable,omitempty"`
// 	ColumnSize *int   `json:"columnSize,omitempty"`
// 	Decimals   *int   `json:"decimals,omitempty"`
// }
