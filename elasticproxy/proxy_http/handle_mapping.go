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
	"fmt"
	"strings"
	"time"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/gorilla/mux"
)

// MappingProxy handles GET endpoint for Mapping API
//
// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html
func MappingProxy(c *HandlerContext) bool {
	indices := []string{mux.Vars(c.Request)["index"]}

	result := make(map[string]any)
	for _, index := range indices {
		if !c.SelectIndex(index) {
			return false
		}

		elasticMapping, err := c.Cache.Fetch(index)
		if err != nil {
			c.VerboseLog("cannot fetch Elastic mapping from cache: %s", err)
		}

		if elasticMapping == nil {
			elasticMapping = obtainElasticMapping(c)
			if elasticMapping == nil {
				// break on any error
				return true
			}

			err = c.Cache.Store(index, elasticMapping)
			if err != nil {
				c.VerboseLog("cannot store Elastic mapping in cache: %s", err)
			}
		} else {
			c.VerboseLog("fetched Elastic mapping from cache")
		}

		result[index] = elasticMapping
	}

	writeResult(c, result)
	return true
}

func obtainElasticMapping(c *HandlerContext) *elastic_proxy.ElasticMapping {
	// Query Sneller engine
	if !c.HasSnellerEndpoint() {
		c.NotFound("no Sneller endpoint defined for %s", c.Request.Host)
		return nil
	}

	var from strings.Builder
	if len(c.Mapping.Sources) > 1 {
		from.WriteRune('(')
	}
	for i, s := range c.Mapping.Sources {
		if i > 0 {
			from.WriteString(" ++ ")
		}
		from.WriteString(s.SQL())
	}
	if len(c.Mapping.Sources) > 1 {
		from.WriteRune(')')
	}

	SQL := fmt.Sprintf(`WITH subset AS (SELECT * FROM %s LIMIT %d) SELECT SNELLER_DATASHAPE(*) FROM subset`,
		from.String(),
		elasticMappingLimitMax)

	if !executeQuery(c, SQL) {
		return nil
	}

	const fieldsName = "fields" // see the spec of SNELLER_DATASHAPE in sneller/core/oss/doc/sneller-SQL.md
	raw, ok := c.Logging.SnellerResult[fieldsName]
	if !ok {
		c.InternalServerError("query didn't return %q key", fieldsName)
		return nil
	}

	// convert into Elasticsearch Mapping structure
	fields, ok := raw.(map[string]any)
	if !ok {
		c.InternalServerError("wrong Go type returned")
		return nil
	}

	return elastic_proxy.DataShapeToElasticMapping(fields)
}

func executeQuery(c *HandlerContext, SQL string) bool {
	defer func() {
		duration := time.Since(c.Logging.Start)
		c.Logging.Duration = duration
		if c.Logging.Result != nil {
			c.Logging.Result.Took = int(duration.Milliseconds())
		}
	}()

	c.AddHeader("X-Elastic-Product", "Elasticsearch")

	c.Logging.SQL = SQL

	tokenLast4 := c.Config.Sneller.Token
	if len(tokenLast4) > 4 {
		tokenLast4 = tokenLast4[len(tokenLast4)-4:]
	}

	c.Logging.Sneller = &SnellerLogging{
		EndPoint:   c.Config.Sneller.EndPoint.String(),
		TokenLast4: tokenLast4,
		Sources:    c.Mapping.Sources,
	}

	// Execute query
	response, err := elastic_proxy.ExecuteQuery(
		c.Client,
		c.Config.Sneller.EndPoint,
		c.Config.Sneller.Token,
		c.Logging.SQL)

	if err != nil {
		c.InternalServerError("error executing query: %v", err)
		return false
	}
	defer response.Body.Close()

	// Update ID to synchronize with Sneller query ID
	if queryID := response.Header.Get("X-Sneller-Query-ID"); queryID != "" {
		c.Logging.QueryID = queryID
	}

	c.AddHeader("X-Sneller-Proxy-ID", c.Logging.QueryID)

	// Obtain the ION result and translate to hash-map
	dec := ion.NewDecoder(ion.NewReader(response.Body))
	err = dec.DecodeTo(&c.Logging.SnellerResult)
	if err != nil {
		c.InternalServerError("error decoding Ion response: %v", err)
		return false
	}
	for k, vv := range c.Logging.SnellerResult {
		c.Logging.SnellerResult[k] = elastic_proxy.ConvertION(vv) // make sure we properly deal with ION timestamps
	}

	// Parse the statistics
	err = parseStatistics(c.Logging, dec)
	if err != nil {
		c.InternalServerError("error decoding Ion response: %v", err)
		return false
	}

	if len(c.Logging.SnellerResult) == 0 {
		c.InternalServerError("got empty Sneller result")
		return false
	}

	// Write all headers
	setCommonHeaders(c)

	return true
}
