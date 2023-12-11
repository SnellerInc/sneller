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
	"log"
	"net/http"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/gorilla/mux"
)

func CountProxy(c *HandlerContext) (handled bool) {
	handled = true

	// only handle the configured index
	if !c.SelectIndex(mux.Vars(c.Request)["index"]) {
		handled = false
		return
	}

	if c.NeedsAuthentication() {
		username, password, ok := c.Request.BasicAuth()
		if !ok || !c.Authenticate(username, password) {
			log.Printf("%s %v[%s]: unauthorized", c.Request.Method, c.Request.URL, c.Request.RemoteAddr)
			c.Writer.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	if !c.HasSnellerEndpoint() {
		c.NotFound("no Sneller endpoint defined for %s", c.Request.Host)
		return
	}

	c.AddHeader("X-Elastic-Product", "Elasticsearch")

	pq := prepareQuery(c)
	if pq == nil {
		return
	}

	// note that "track_total_hits" is ignored for the count API and
	// it will always count all records. See the discussion here:
	// https://discuss.elastic.co/t/count-api-with-track-total-hits/308912
	pq.ej.TrackTotalHits = &elastic_proxy.TrackTotalHits{Limit: -1, Enabled: true}

	var validCount = 0
	if pq.ej.Size != nil && *pq.ej.Size != validCount {
		c.BadRequest("cannot set size for count")
		return
	}
	pq.ej.Size = &validCount

	err := execute(c, pq, true)
	c.AddHeader("X-Sneller-Proxy-ID", c.Logging.QueryID)
	if err != nil {
		c.InternalServerError("error executing query: %v", err)
		return
	}

	// Set count value
	if c.Logging.Result.Hits.Total != nil {
		c.Logging.Result.Count = &c.Logging.Result.Hits.Total.Value
	} else {
		defaultValue := int64(0)
		c.Logging.Result.Count = &defaultValue
	}
	c.Logging.Result.Hits = nil

	// Write all headers
	setCommonHeaders(c)
	for header, values := range pq.headers {
		for _, value := range values {
			c.AddHeader(header, value)
		}
	}

	if c.Config.CompareWithElastic {
		compareWithElastic(c, pq)
	}

	writeResult(c, c.Logging.Result)
	return
}
