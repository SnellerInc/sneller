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
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
)

func (s *server) versionHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		encodingFormat := r.Header.Get("Accept")
		switch encodingFormat {
		case "text/plain":
		case "application/json":
		case "", "*/*":
			encodingFormat = "text/plain"
		default:
			http.Error(w, "invalid 'Accept' header", http.StatusBadRequest)
			return
		}

		endPoints := s.peers.Get()
		w.WriteHeader(http.StatusOK)
		w.Header().Add("Content-Type", encodingFormat)
		switch encodingFormat {
		case "text/plain":
			fmt.Fprintf(w, "Sneller daemon %s (cluster size: %d nodes)", version, len(endPoints))

		case "application/json":
			bi, _ := debug.ReadBuildInfo()
			json.NewEncoder(w).Encode(map[string]any{
				"date":         findSetting(bi, "vcs.time"),
				"revision":     findSetting(bi, "vcs.revision"),
				"cluster_size": len(endPoints),
			})
		default:
			panic("unexpected encoding format")
		}

	default:
		panic("unexpected HTTP method")
	}
}

func findSetting(bi *debug.BuildInfo, key string) *string {
	if bi != nil {
		for i := range bi.Settings {
			if bi.Settings[i].Key == key {
				return &bi.Settings[i].Value
			}
		}
	}
	return nil
}
