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

package proxy_http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/amazon-ion/ion-go/ion"
)

type proxyQuery struct {
	body        []byte
	queryParams url.Values
	ej          elastic_proxy.ElasticJSON
	headers     map[string][]string
}

func prepareQuery(c *HandlerContext) *proxyQuery {
	// start with a base query
	pq := proxyQuery{
		ej: elastic_proxy.ElasticJSON{},
	}

	// step 0: obtain query parameters
	c.Logging.QueryParams = c.Request.URL.Query()
	if len(c.Logging.QueryParams) > 0 {
		if err := parseQueryParams(c.Logging.QueryParams, &pq.ej); err != nil {
			c.BadRequest("error decoding query parameters: %v", err)
			return nil
		}
	}

	// step 1: parse elastic query
	if c.Request.Body == nil {
		return &pq
	}

	var err error
	pq.body, err = io.ReadAll(c.Request.Body)
	if err != nil {
		c.InternalServerError("error reading body: %v", err)
		return nil
	}

	if len(pq.body) > 0 {
		if err := json.Unmarshal(pq.body, &c.Logging.Request); err != nil {
			c.BadRequest("invalid JSON request: %v", err)
			return nil
		}

		if err := json.Unmarshal(pq.body, &pq.ej); err != nil {
			c.BadRequest("error decoding body: %v", err)
			return nil
		}
	} else {
		c.Logging.Request = make(map[string]any, 0)
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

func execute(c *HandlerContext, pq *proxyQuery, alwaysDetermineCount bool) error {
	defer func() {
		duration := time.Since(c.Logging.Start)
		c.Logging.Duration = duration
		if c.Logging.Result != nil {
			c.Logging.Result.Took = int(duration.Milliseconds())
		}
	}()

	ignoreTotalHits := false
	if !alwaysDetermineCount && c.Mapping.IgnoreTotalHits {
		ignoreTotalHits = true
	}
	ignoreTotalSumOtherDocCount := c.Mapping.IgnoreSumOtherDocCount
	if ignoreTotalHits {
		ignoreTotalSumOtherDocCount = true
	}

	// determine query context
	ts := make([]elastic_proxy.TableSource, len(c.Mapping.Sources))
	for i, s := range c.Mapping.Sources {
		ts[i] = elastic_proxy.TableSource{Database: s.Database, Table: s.Table}
	}
	qc := elastic_proxy.QueryContext{
		Query:                  pq.ej,
		Index:                  c.Logging.Index,
		TableSources:           ts,
		IgnoreTotalHits:        ignoreTotalHits,
		IgnoreSumOtherDocCount: ignoreTotalSumOtherDocCount,
		TypeMapping:            c.Mapping.TypeMapping,
	}

	// step 2: generate SQL
	sqlExpr, err := pq.ej.SQL(&qc)
	if err != nil {
		return err
	}
	c.Logging.SQL = elastic_proxy.PrintExprPretty(sqlExpr)

	tokenLast4 := c.Config.Sneller.Token
	if len(tokenLast4) > 4 {
		tokenLast4 = tokenLast4[len(tokenLast4)-4:]
	}
	c.Logging.Sneller = &SnellerLogging{
		EndPoint:   c.Config.Sneller.EndPoint.String(),
		TokenLast4: tokenLast4,
		Sources:    c.Mapping.Sources,
	}

	// step 3: execute query
	client := &http.Client{
		Timeout: c.Config.Sneller.Timeout,
	}
	response, err := elastic_proxy.ExecuteQuery(client, c.Config.Sneller.EndPoint, c.Config.Sneller.Token, c.Logging.SQL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// Update ID to synchronize with Sneller query ID
	if queryID := response.Header.Get("X-Sneller-Query-ID"); queryID != "" {
		c.Logging.QueryID = queryID
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
	err = dec.DecodeTo(&c.Logging.SnellerResult)
	if err != nil {
		return err
	}
	for k, vv := range c.Logging.SnellerResult {
		c.Logging.SnellerResult[k] = elastic_proxy.ConvertION(vv) // make sure we properly deal with ION timestamps
	}

	// step 5: parse the statistics
	err = parseStatistics(c.Logging, dec)
	if err != nil {
		return err
	}

	if len(c.Logging.SnellerResult) == 0 {
		return errors.New("got empty Sneller result (probably invalid SQL query generation)")
	}

	// step 6: convert result
	c.Logging.Result, c.Logging.Preprocessed, err = pq.ej.ConvertResult(&qc, c.Logging.SnellerResult)
	return err
}

func writeResult(c *HandlerContext, resultData any) {
	w := c.Writer
	w.WriteHeader(http.StatusOK)
	e := json.NewEncoder(w)
	if prettyValue := c.Request.URL.Query().Get("pretty"); prettyValue != "" {
		e.SetIndent("", "  ")
	}
	e.Encode(resultData)
}

func setCommonHeaders(c *HandlerContext) {
	c.AddHeader("X-Sneller-Cache-Hits", strconv.Itoa(c.Logging.Sneller.CacheHits))
	c.AddHeader("X-Sneller-Cache-Misses", strconv.Itoa(c.Logging.Sneller.CacheMisses))
	c.AddHeader("X-Sneller-Bytes-Scanned", strconv.Itoa(c.Logging.Sneller.BytesScanned))
	c.AddHeader("Content-Type", "application/json")
}

func parseStatistics(l *Logging, dec *ion.Decoder) error {
	var stats struct {
		CacheHits    int `ion:"hits,omitempty"`
		CacheMisses  int `ion:"misses,omitempty"`
		BytesScanned int `ion:"scanned,omitempty"`
	}
	err := dec.DecodeTo(&stats)
	if err != nil {
		return err
	}
	l.Sneller.CacheHits = stats.CacheHits
	l.Sneller.CacheMisses = stats.CacheMisses
	l.Sneller.BytesScanned = stats.BytesScanned
	return nil
}
