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
	"net/http"
	"net/url"
	"strings"
	"time"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

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
	EndPoint     string               `json:"endpoint"`
	Sources      []mappingEntrySource `json:"sources"`
	TokenLast4   string               `json:"tokenLast4"`
	CacheHits    int                  `json:"cacheHits,omitempty"`
	CacheMisses  int                  `json:"cacheMisses,omitempty"`
	BytesScanned int                  `json:"bytesScanned,omitempty"`
}

func newLogging(r *http.Request) *Logging {
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

	return &Logging{
		Revision: Version,
		SourceIP: sourceIP,
		QueryID:  uuid.New().String(), // will be overwritten if we get an actual Sneller response
		Start:    time.Now(),
	}

}
