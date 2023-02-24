// Copyright (C) 2023 Sneller, Inc.
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
	"time"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/gorilla/mux"
)

// MappingProxy handles GET endpoint for Mapping API
//
// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html
func MappingProxy(config *Config, l *Logging, w http.ResponseWriter, r *http.Request) bool {
	indices := []string{mux.Vars(r)["index"]}

	result := make(map[string]any)
	for _, index := range indices {
		m, ok := config.Mapping[index]
		if !ok {
			return false
		}
		if m.ElasticMapping == nil {
			l.Index = index
			m.ElasticMapping = obtainElasticMapping(config, l, w, r)
			if m.ElasticMapping == nil {
				// break on any error
				return true
			}
		}

		result[index] = m.ElasticMapping
	}

	writeResult(w, r, result)
	return true
}

func obtainElasticMapping(config *Config, l *Logging, w http.ResponseWriter, r *http.Request) *elastic_proxy.ElasticMapping {
	m := config.Mapping[l.Index]

	// Query Sneller engine
	SQL := fmt.Sprintf(`WITH subset AS (SELECT * FROM %q.%q LIMIT %d) SELECT SNELLER_DATASHAPE(*) FROM subset`, m.Database, m.Table, elasticMappingLimitMax)
	if !executeQuery(config, l, w, r, SQL) {
		return nil
	}

	const fieldsName = "fields" // see the spec of SNELLER_DATASHAPE in sneller/core/oss/doc/sneller-SQL.md
	raw, ok := l.SnellerResult[fieldsName]
	if !ok {
		writeError(w, r, http.StatusInternalServerError, "query didn't return %q key", fieldsName)
		return nil
	}

	// convert into Elasticsearch Mapping structure
	fields, ok := raw.(map[string]any)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, "wrong Go type returned")
		return nil
	}

	return elastic_proxy.DataShapeToElasticMapping(fields)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, f string, args ...any) {
	msg := fmt.Sprintf(f, args...)
	log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
	http.Error(w, msg, status)
}

func writeResult(w http.ResponseWriter, r *http.Request, resultData any) {
	w.WriteHeader(http.StatusOK)
	e := json.NewEncoder(w)
	if prettyValue := r.URL.Query().Get("pretty"); prettyValue != "" {
		e.SetIndent("", "  ")
	}
	e.Encode(resultData)
}

func executeQuery(t *Config, l *Logging, w http.ResponseWriter, r *http.Request, SQL string) bool {
	if t.Sneller.EndPoint == "" {
		writeError(w, r, http.StatusNotFound, "no Sneller endpoint defined for %s", r.Host)
		return false
	}

	defer func() {
		duration := time.Since(l.Start)
		l.Duration = duration
		if l.Result != nil {
			l.Result.Took = int(duration.Milliseconds())
		}
	}()

	w.Header().Add("X-Elastic-Product", "Elasticsearch")

	l.SQL = SQL
	m := t.Mapping[l.Index]

	tokenLast4 := t.Sneller.Token
	if len(tokenLast4) > 4 {
		tokenLast4 = tokenLast4[len(tokenLast4)-4:]
	}

	l.Sneller = &SnellerLogging{
		EndPoint:   t.Sneller.EndPoint,
		TokenLast4: tokenLast4,
		Database:   m.Database,
		Table:      m.Table,
	}

	// Execute query
	response, err := elastic_proxy.ExecuteQuery(t.Sneller.EndPoint, t.Sneller.Token, m.Database, l.SQL, t.Sneller.Timeout)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "error executing query: %v", err)
		return false
	}
	defer response.Body.Close()

	// Update ID to synchronize with Sneller query ID
	if queryID := response.Header.Get("X-Sneller-Query-ID"); queryID != "" {
		l.QueryID = queryID
	}

	w.Header().Add("X-Sneller-Proxy-ID", l.QueryID)

	// Obtain the ION result and translate to hash-map
	dec := ion.NewDecoder(ion.NewReader(response.Body))
	err = dec.DecodeTo(&l.SnellerResult)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "error decoding Ion response: %v", err)
		return false
	}
	for k, vv := range l.SnellerResult {
		l.SnellerResult[k] = elastic_proxy.ConvertION(vv) // make sure we properly deal with ION timestamps
	}

	// Parse the statistics
	err = parseStatistics(l, dec)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "error decoding Ion response: %v", err)
		return false
	}

	if len(l.SnellerResult) == 0 {
		writeError(w, r, http.StatusInternalServerError, "got empty Sneller result")
		return false
	}

	// Write all headers
	w.Header().Add("X-Sneller-Cache-Hits", strconv.Itoa(l.Sneller.CacheHits))
	w.Header().Add("X-Sneller-Cache-Misses", strconv.Itoa(l.Sneller.CacheMisses))
	w.Header().Add("X-Sneller-Bytes-Scanned", strconv.Itoa(l.Sneller.BytesScanned))
	w.Header().Add("Content-Type", "application/json")

	return true
}
