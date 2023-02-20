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
	"net/http"
	"net/url"
	"strings"
	"time"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"

	"github.com/google/uuid"
)

type LogDetail struct {
	LogRequest       bool
	LogQueryParams   bool
	LogSQL           bool
	LogSnellerResult bool
	LogPreprocessed  bool
	LogResult        bool
}

type Logging struct {
	Revision       string                       `json:"revision"`
	SourceIP       string                       `json:"sourceIp"`
	TenantID       string                       `json:"tenantId,omitempty"`
	QueryID        string                       `json:"queryId"`
	Start          time.Time                    `json:"start"`
	Index          string                       `json:"index"`
	Duration       time.Duration                `json:"duration"`
	HttpStatusCode int                          `json:"httpStatusCode"`
	Sneller        *SnellerLogging              `json:"sneller,omitempty"`
	Request        map[string]any               `json:"request,omitempty"`
	QueryParams    url.Values                   `json:"queryParameters,omitempty"`
	SQL            string                       `json:"sql,omitempty"`
	SnellerResult  map[string]any               `json:"snellerResult,omitempty"`
	Preprocessed   map[string]any               `json:"preprocessed,omitempty"`
	Result         *elastic_proxy.ElasticResult `json:"result,omitempty"`
	ElasticResult  map[string]any               `json:"elasticResult,omitempty"`
	ElasticDiff    string                       `json:"diff,omitempty"`
}

type SnellerLogging struct {
	EndPoint     string `json:"endpoint"`
	Database     string `json:"database"`
	Table        string `json:"table"`
	TokenLast4   string `json:"tokenLast4"`
	CacheHits    int    `json:"cacheHits,omitempty"`
	CacheMisses  int    `json:"cacheMisses,omitempty"`
	BytesScanned int    `json:"bytesScanned,omitempty"`
}

func NewLogging(r *http.Request) Logging {
	sourceIP := r.Header.Get("X-Forwarded-For")
	if sourceIP != "" {
		ips := strings.Split(sourceIP, ",")
		sourceIP = strings.TrimSpace(ips[0])
	} else {
		// only useful when there is no reverse proxy
		remoteAddr := r.RemoteAddr
		parts := strings.Split(remoteAddr, ":")
		sourceIP = parts[0]
	}

	return Logging{
		Revision: Version,
		SourceIP: sourceIP,
		QueryID:  uuid.New().String(), // will be overwritten if we get an actual Sneller response
		Start:    time.Now(),
	}

}
