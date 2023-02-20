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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"

	"github.com/amazon-ion/ion-go/ion"
)

type proxyQuery struct {
	body        []byte
	queryParams url.Values
	ej          elastic_proxy.ElasticJSON
	headers     map[string][]string
}

func prepareQuery(t *Config, l *Logging, w http.ResponseWriter, r *http.Request) *proxyQuery {
	// start with a base query
	pq := proxyQuery{
		ej: elastic_proxy.ElasticJSON{},
	}

	// step 0: obtain query parameters
	l.QueryParams = r.URL.Query()
	if len(l.QueryParams) > 0 {
		if err := parseQueryParams(l.QueryParams, &pq.ej); err != nil {
			msg := fmt.Sprintf("error decoding query parameters: %v", err)
			http.Error(w, msg, http.StatusBadRequest)
			log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
			return nil
		}
	}

	// step 1: parse elastic query
	if r.Body != nil {
		var err error
		pq.body, err = io.ReadAll(r.Body)
		if err != nil {
			msg := fmt.Sprintf("error reading body: %v", err)
			http.Error(w, msg, http.StatusInternalServerError)
			log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
			return nil
		}

		if len(pq.body) > 0 {
			if err := json.Unmarshal(pq.body, &l.Request); err != nil {
				msg := fmt.Sprintf("invalid JSON request: %v", err)
				http.Error(w, msg, http.StatusBadRequest)
				log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
			}

			if err := json.Unmarshal(pq.body, &pq.ej); err != nil {
				msg := fmt.Sprintf("error decoding body: %v", err)
				http.Error(w, msg, http.StatusBadRequest)
				log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
				return nil
			}
		} else {
			l.Request = make(map[string]any, 0)
		}
	}

	return &pq
}

func parseQueryParams(q url.Values, ej *elastic_proxy.ElasticJSON) error {
	if trackTotalHits := q.Get("track_total_hits"); trackTotalHits != "" {
		if err := json.Unmarshal([]byte(trackTotalHits), &ej.TrackTotalHits); err != nil {
			return err
		}
	}

	if query := q.Get("q"); query != "" {
		var qs elastic_proxy.QueryString
		if err := json.Unmarshal([]byte(query), &qs); err != nil {
			return err
		}
		if ej.Query == nil {
			ej.Query = &elastic_proxy.Query{}
		}
		ej.Query.QueryString = &qs
	}

	if sizeText := q.Get("size"); sizeText != "" {
		size, err := strconv.Atoi(sizeText)
		if err != nil {
			return fmt.Errorf("invalid size query parameter %q", sizeText)
		}
		ej.Size = &size
	}

	if analyzer := q.Get("analyzer"); analyzer != "" {
		ej.Query.QueryString.Analyzer = &analyzer
	}

	if analyzeWildcard := q.Get("analyze_wildcard"); analyzeWildcard != "" {
		err := json.Unmarshal([]byte(analyzeWildcard), &ej.Query.QueryString.AnalyzeWildcard)
		if err != nil {
			return fmt.Errorf("invalid analyze_wildcard query parameter %q", analyzeWildcard)
		}
	}

	if defaultOperator := q.Get("default_operator"); defaultOperator != "" {
		ej.Query.QueryString.DefaultOperator = &defaultOperator
	}

	if defaultField := q.Get("df"); defaultField != "" {
		ej.Query.QueryString.DefaultField = &defaultField
	}

	if sort := q.Get("sort"); sort != "" {
		fields := strings.Split(sort, ",")
		if len(fields) > 0 {
			ej.Sort = make([]elastic_proxy.SortField, 0, len(fields))
			for _, sf := range fields {
				parts := strings.Split(sf, ":")
				field := parts[0]
				order := "desc"
				if len(parts) == 2 {
					order = parts[1]
				}
				ej.Sort = append(ej.Sort, elastic_proxy.SortField{
					Field: field,
					Order: elastic_proxy.Ordering(order),
				})
			}
		}
	}
	return nil
}

func execute(t *Config, l *Logging, pq *proxyQuery, alwaysDetermineCount bool) error {
	defer func() {
		duration := time.Since(l.Start)
		l.Duration = duration
		if l.Result != nil {
			l.Result.Took = int(duration.Milliseconds())
		}
	}()

	ignoreTotalHits := false
	if !alwaysDetermineCount && t.Mapping[l.Index].IgnoreTotalHits {
		ignoreTotalHits = true
	}
	ignoreTotalSumOtherDocCount := t.Mapping[l.Index].IgnoreSumOtherDocCount
	if ignoreTotalHits {
		ignoreTotalSumOtherDocCount = true
	}

	// determine query context
	qc := elastic_proxy.QueryContext{
		Query:                  pq.ej,
		Index:                  l.Index,
		Database:               t.Mapping[l.Index].Database,
		Table:                  t.Mapping[l.Index].Table,
		IgnoreTotalHits:        ignoreTotalHits,
		IgnoreSumOtherDocCount: ignoreTotalSumOtherDocCount,
		TypeMapping:            t.Mapping[l.Index].TypeMapping,
	}

	// step 2: generate SQL
	sqlExpr, err := pq.ej.SQL(&qc)
	if err != nil {
		return err
	}
	l.SQL = elastic_proxy.PrintExprPretty(sqlExpr)

	tokenLast4 := t.Sneller.Token
	if len(tokenLast4) > 4 {
		tokenLast4 = tokenLast4[len(tokenLast4)-4:]
	}
	l.Sneller = &SnellerLogging{
		EndPoint:   t.Sneller.EndPoint,
		TokenLast4: tokenLast4,
		Database:   qc.Database,
		Table:      qc.Table,
	}

	// step 3: execute query
	response, err := elastic_proxy.ExecuteQuery(t.Sneller.EndPoint, t.Sneller.Token, qc.Database, l.SQL, t.Sneller.Timeout)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// Update ID to synchronize with Sneller query ID
	if queryID := response.Header.Get("X-Sneller-Query-ID"); queryID != "" {
		l.QueryID = queryID
	}

	// Forward all headers
	pq.headers = make(map[string][]string)
	for header := range response.Header {
		if strings.HasPrefix(header, "X-Sneller-") {
			pq.headers[header] = response.Header.Values(header)
		}
	}

	// Obtain the ION result and translate to hash-map
	dec := ion.NewDecoder(ion.NewReader(response.Body))
	err = dec.DecodeTo(&l.SnellerResult)
	if err != nil {
		return err
	}
	for k, vv := range l.SnellerResult {
		l.SnellerResult[k] = elastic_proxy.ConvertION(vv) // make sure we properly deal with ION timestamps
	}

	// step 5: parse the statistics
	var stats struct {
		CacheHits    int `ion:"hits,omitempty"`
		CacheMisses  int `ion:"misses,omitempty"`
		BytesScanned int `ion:"scanned,omitempty"`
	}
	err = dec.DecodeTo(&stats)
	if err != nil {
		return err
	}
	l.Sneller.CacheHits = stats.CacheHits
	l.Sneller.CacheMisses = stats.CacheMisses
	l.Sneller.BytesScanned = stats.BytesScanned

	if len(l.SnellerResult) == 0 {
		return errors.New("got empty Sneller result (probably invalid SQL query generation)")
	}

	// step 6: convert result
	l.Result, l.Preprocessed, err = pq.ej.ConvertResult(&qc, l.SnellerResult)
	return err
}
