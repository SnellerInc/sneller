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
