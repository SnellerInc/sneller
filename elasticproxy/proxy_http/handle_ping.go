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
	"net/http"
	"os"
	"strconv"
)

func Ping(t *Config, w http.ResponseWriter, r *http.Request) bool {
	type pingVersionResult struct {
		Number                           string `json:"number"`                              // e.g. "7.0.0"
		BuildFlavor                      string `json:"build_flavor"`                        // e.g. "oss" or "default"
		BuildType                        string `json:"build_type"`                          // e.g. "docker"
		BuildHash                        string `json:"build_hash"`                          // e.g. "b7e28a7"
		BuildDate                        string `json:"build_date"`                          // e.g. "2019-04-05T22:55:32.697037Z"
		BuildSnapshot                    bool   `json:"build_snapshot"`                      // e.g. false
		LuceneVersion                    string `json:"lucene_version"`                      // e.g. "8.0.0"
		MinimumWireCompatibilityVersion  string `json:"minimum_wire_compatibility_version"`  // e.g. "6.7.0"
		MinimumIndexCompatibilityVersion string `json:"minimum_index_compatibility_version"` // e.g. "6.0.0-beta1"
	}
	type pingResult struct {
		Name        string            `json:"name"`
		ClusterName string            `json:"cluster_name"`
		ClusterUUID string            `json:"cluster_uuid"`
		Version     pingVersionResult `json:"version"`
		TagLine     string            `json:"tagline"`
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	data, _ := json.Marshal(&pingResult{
		Name:        hostName,
		ClusterName: "docker-cluster",
		ClusterUUID: "hwp6VaSASoyyOw4hARontQ",
		Version: pingVersionResult{
			Number:                           "7.17.1-sneller-" + Version,
			BuildFlavor:                      "default",
			BuildType:                        "docker",
			BuildHash:                        "e5acb99f822233d62d6444ce45a4543dc1c8059a",
			BuildDate:                        "2022-02-23T22:20:54.153567231Z",
			BuildSnapshot:                    false,
			LuceneVersion:                    "8.11.1",
			MinimumWireCompatibilityVersion:  "6.8.0",
			MinimumIndexCompatibilityVersion: "6.0.0-beta1",
		},
		TagLine: "You Know, for Search",
	})
	w.Header().Add("X-elastic-product", "Elasticsearch")
	w.Header().Add("content-type", "application/json; charset=UTF-8")
	w.Header().Add("content-length", strconv.Itoa(len(data)))

	w.WriteHeader(http.StatusOK)
	w.Write(data)

	return true
}
