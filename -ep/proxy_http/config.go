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

import elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"

type Config struct {
	Elastic struct {
		EndPoint   string `json:"endpoint,omitempty"`
		User       string `json:"user,omitempty"`
		Password   string `json:"password,omitempty"`
		ESPassword string `json:"esPassword,omitempty"`
		IgnoreCert bool   `json:"ignoreCert,omitempty"`
	} `json:"elastic,omitempty"`
	Sneller struct {
		EndPoint string `json:"endpoint,omitempty"`
		Token    string `json:"token,omitempty"`
		Timeout  int    `json:"timeout,omitempty"`
	} `json:"sneller,omitempty"`
	Mapping            map[string]mappingEntry `json:"mapping"`
	CompareWithElastic bool                    `json:"compareWithElastic,omitempty"`
}

const elasticMappingLimitMax = 1_000_000

type mappingEntry struct {
	Database               string                               `json:"database"`
	Table                  string                               `json:"table"`
	IgnoreTotalHits        bool                                 `json:"ignoreTotalHits"`
	IgnoreSumOtherDocCount bool                                 `json:"ignoreSumOtherDocCount"`
	TypeMapping            map[string]elastic_proxy.TypeMapping `json:"typeMapping,omitempty"`

	// purposely not serialized, the value is obtained from Sneller directly
	ElasticMapping *elastic_proxy.ElasticMapping
}
