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
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

type noUploadFS struct {
	db.InputFS
}

func (n *noUploadFS) WriteFile(path string, buf []byte) (string, error) {
	panic("noUploadFS.WriteFile")
}

func (n *noUploadFS) Create(path string) (blockfmt.Uploader, error) {
	panic("noUploadFS.Create")
}

func (s *server) inputsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	tenant, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}

	databaseName := r.URL.Query().Get("database")
	if databaseName == "" {
		http.Error(w, "no database", http.StatusBadRequest)
		return
	}
	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, "no table", http.StatusBadRequest)
		return
	}

	next := r.URL.Query().Get("next")

	// start is for backwards compatibility
	start := r.URL.Query().Get("start")
	if start != "" {
		if next != "" {
			http.Error(w, "cannot use both 'start' and 'next'", http.StatusBadRequest)
			return
		}
		next = start
	}

	max := -1
	maxtext := r.URL.Query().Get("max")
	if maxtext != "" {
		max, err = strconv.Atoi(maxtext)
		if err != nil {
			http.Error(w, "parsing max: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	var useNormalJSON bool
	encodingFormat := r.Header.Get("Accept")
	if r.Method == http.MethodGet {
		switch encodingFormat {
		case "application/x-ndjson", "application/x-jsonlines":
			useNormalJSON = false
		case "application/json":
			useNormalJSON = true
		case "", "*/*":
			encodingFormat = "application/x-ndjson"
			useNormalJSON = false
		default:
			http.Error(w, "invalid 'Accept' header", http.StatusBadRequest)
			return
		}
	}

	root, err := tenant.Root()
	if err != nil {
		http.Error(w, "couldn't open db+table", http.StatusInternalServerError)
		return
	}
	idx, err := db.OpenIndex(root, databaseName, tableName, tenant.Key())
	if err != nil {
		s.logger.Printf("handling /inputs: OpenIndex: %s", err)
		http.Error(w, "couldn't open index file", http.StatusInternalServerError)
		return
	}
	if max == 0 || r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	var it struct {
		Path     string `json:"path"`
		ETag     string `json:"etag"`
		Accepted bool   `json:"accepted"`
		Packfile string `json:"packfile,omitempty"`
	}
	enc := json.NewEncoder(w)
	count := 0
	idx.Inputs.Backing = &noUploadFS{root}
	w.Header().Set("Content-Type", encodingFormat)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	indirect := idx.Indirect.OrigObjects()
	if useNormalJSON {
		w.Write([]byte("["))
	}

	err = idx.Inputs.Walk(next, func(path, etag string, id int) bool {
		if ctx.Err() != nil {
			return false
		}
		if start == "" && path == next {
			// skip the item itself
			return true
		}
		it.Path = path
		it.ETag = etag
		it.Accepted = id >= 0
		// FIXME: we only produce packfile information
		// when the reference is inline in the index;
		// we'd have to load indirect blocks to handle
		// the other cases
		it.Packfile = ""
		if id >= indirect && (id-indirect) < len(idx.Inline) {
			it.Packfile = idx.Inline[id-indirect].Path
		}
		if useNormalJSON && count > 0 {
			w.Write([]byte(","))
		}
		err = enc.Encode(&it)
		if err != nil {
			s.logger.Printf("writing index inputs: %s", err)
			return false
		}
		count++
		return max < 0 || count < max
	})
	if useNormalJSON {
		w.Write([]byte("]"))
	}
	if err != nil {
		s.logger.Printf("index.Inputs.Walk: %s", err)
	}
}
