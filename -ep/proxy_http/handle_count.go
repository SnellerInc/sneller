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

package proxy_http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"

	"github.com/gorilla/mux"
)

func CountProxy(t *Config, l *Logging, w http.ResponseWriter, r *http.Request) (handled bool) {
	handled = true

	// only handle the configured index
	l.Index = mux.Vars(r)["index"]
	if _, ok := t.Mapping[l.Index]; !ok {
		handled = false
		return
	}

	if t.Elastic.User != "" || t.Elastic.Password != "" {
		username, password, ok := r.BasicAuth()
		if !ok || username != t.Elastic.User || password != t.Elastic.Password {
			log.Printf("%s %v[%s]: unauthorized", r.Method, r.URL, r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	if t.Sneller.EndPoint == "" {
		msg := fmt.Sprintf("no Sneller endpoint defined for %s", r.Host)
		http.Error(w, msg, http.StatusNotFound)
		log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
		return
	}

	w.Header().Add("X-Elastic-Product", "Elasticsearch")

	pq := prepareQuery(t, l, w, r)
	if pq == nil {
		return
	}

	// note that "track_total_hits" is ignored for the count API and
	// it will always count all records. See the discussion here:
	// https://discuss.elastic.co/t/count-api-with-track-total-hits/308912
	pq.ej.TrackTotalHits = &elastic_proxy.TrackTotalHits{Limit: -1, Enabled: true}

	var validCount = 0
	if pq.ej.Size != nil && *pq.ej.Size != validCount {
		msg := "cannot set size for count"
		http.Error(w, msg, http.StatusBadRequest)
		log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
		return
	}
	pq.ej.Size = &validCount

	err := execute(t, l, pq, true)
	w.Header().Add("X-Sneller-Proxy-ID", l.QueryID)
	if err != nil {
		msg := fmt.Sprintf("error executing query: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
		return
	}

	// Set count value
	if l.Result.Hits.Total != nil {
		l.Result.Count = &l.Result.Hits.Total.Value
	} else {
		defaultValue := int64(0)
		l.Result.Count = &defaultValue
	}
	l.Result.Hits = nil

	// Write all headers
	w.Header().Add("X-Sneller-Cache-Hits", strconv.Itoa(l.Sneller.CacheHits))
	w.Header().Add("X-Sneller-Cache-Misses", strconv.Itoa(l.Sneller.CacheMisses))
	w.Header().Add("X-Sneller-Bytes-Scanned", strconv.Itoa(l.Sneller.BytesScanned))
	w.Header().Add("Content-Type", "application/json")
	for header, values := range pq.headers {
		for _, value := range values {
			w.Header().Add(header, value)
		}
	}

	if t.CompareWithElastic {
		compareWithElastic(t, l, pq)
	}

	w.WriteHeader(http.StatusOK)
	e := json.NewEncoder(w)
	if prettyValue := r.URL.Query().Get("pretty"); prettyValue != "" {
		e.SetIndent("", "  ")
	}
	e.Encode(l.Result)
	return
}
