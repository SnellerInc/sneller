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
	"log"
	"net/http"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/gorilla/mux"
)

func SearchProxy(c *HandlerContext) bool {
	return search(c, false)
}

func AsyncSearchProxy(c *HandlerContext) bool {
	return search(c, true)
}

func search(c *HandlerContext, isAsync bool) (handled bool) {
	handled = true

	// only handle the configured index
	if !c.SelectIndex(mux.Vars(c.Request)["index"]) {
		return false
	}

	if c.NeedsAuthentication() {
		username, password, ok := c.Request.BasicAuth()
		if !ok || !c.Authenticate(username, password) {
			r := c.Request
			log.Printf("%s %v[%s]: unauthorized", r.Method, r.URL, r.RemoteAddr)
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

	// use the default track_total_hits for searching (if not set)
	if pq.ej.TrackTotalHits == nil {
		pq.ej.TrackTotalHits = &elastic_proxy.DefaultTrackTotalHits
	}

	err := execute(c, pq, false)
	c.AddHeader("X-Sneller-Proxy-ID", c.Logging.QueryID)
	if err != nil {
		c.InternalServerError("error executing query: %v", err)
		return
	}

	// Write all headers
	setCommonHeaders(c)
	for header, values := range pq.headers {
		for _, value := range values {
			c.AddHeader(header, value)
		}
	}

	// Write data as JSON
	var resultData any
	if isAsync {
		type asyncResult struct {
			IsPartial        bool                         `json:"is_partial"`
			IsRunning        bool                         `json:"is_running"`
			StartTimeMs      int64                        `json:"start_time_in_millis"`
			ExpirationTimeMs int64                        `json:"expiration_time_in_millis"`
			Response         *elastic_proxy.ElasticResult `json:"response"`
		}
		resultData = asyncResult{
			IsPartial:        false,
			IsRunning:        false,
			StartTimeMs:      c.Logging.Start.UnixMilli(),
			ExpirationTimeMs: c.Logging.Start.UnixMilli() + 4320000000,
			Response:         c.Logging.Result,
		}
	} else {
		resultData = c.Logging.Result
	}

	if c.Config.CompareWithElastic {
		compareWithElastic(c, pq)
	}

	writeResult(c, resultData)
	return
}
