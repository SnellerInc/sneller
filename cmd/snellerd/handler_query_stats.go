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
	//"time"
)

func (s *server) queryStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}
	_ = tenant

	queryID := r.URL.Query().Get("queryId")
	if queryID == "" {
		http.Error(w, "no query ID specified", http.StatusBadRequest)
		return
	}

	// TODO: Fetch the query statistics from DynamoDB
	_ = queryID
	w.WriteHeader(http.StatusNotImplemented)
	// stats := queryStatistics{
	// 	QueryID:      queryID,
	// 	Database:     "sf1",
	// 	Query:        "SELECT * FROM nations",
	// 	QueryStarted: time.Now().UTC(),
	// 	BytesScanned: 123456789,
	// 	ElapsedTime:  12345,
	// 	UnitsSpent:   54321,
	// 	Status:       "completed",
	// }
	// writeResultResponse(w, http.StatusOK, stats)
}

// type queryStatistics struct {
// 	QueryID      string    `json:"queryId"`
// 	Database     string    `json:"database"`
// 	Query        string    `json:"query"`
// 	QueryStarted time.Time `json:"queryStarted"`
// 	BytesScanned int64     `json:"bytesScanned"`
// 	ElapsedTime  int       `json:"elapsedTime"`
// 	UnitsSpent   int       `json:"unitsSpent"`
// 	Status       string    `json:"status"`
// }
