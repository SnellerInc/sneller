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
	"fmt"
	"net/url"
	"time"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"
)

type Config struct {
	Elastic struct {
		EndPoint   string `json:"endpoint,omitempty"`
		User       string `json:"user,omitempty"`
		Password   string `json:"password,omitempty"`
		ESPassword string `json:"esPassword,omitempty"`
		IgnoreCert bool   `json:"ignoreCert,omitempty"`
	} `json:"elastic,omitempty"`
	Sneller            configSneller            `json:"sneller,omitempty"`
	Mapping            map[string]*mappingEntry `json:"mapping"`
	CompareWithElastic bool                     `json:"compareWithElastic,omitempty"`
}

const elasticMappingLimitMax = 1_000_000

type configSneller struct {
	EndPoint *url.URL
	Token    string
	Timeout  time.Duration
}

func (c *configSneller) UnmarshalJSON(data []byte) error {
	type sneller struct {
		EndPoint string `json:"endpoint,omitempty"`
		Token    string `json:"token,omitempty"`
		Timeout  int    `json:"timeout,omitempty"`
	}
	raw := &sneller{}
	if err := json.Unmarshal(data, (*sneller)(raw)); err != nil {
		return err
	}

	c.Token = raw.Token

	u, err := url.Parse(raw.EndPoint)
	if err != nil {
		return fmt.Errorf("field 'endpoint': %s", err)
	}
	c.EndPoint = u

	switch {
	case raw.Timeout < 0:
		return fmt.Errorf("field 'token': cannot be negative")
	case raw.Timeout == 0:
		c.Timeout = elastic_proxy.DefaultHTTPTimeout
	default:
		c.Timeout = time.Duration(raw.Timeout) * time.Second
	}

	return nil
}

type mappingEntry struct {
	Sources                []mappingEntrySource                 `json:"sources"`
	IgnoreTotalHits        bool                                 `json:"ignoreTotalHits"`
	IgnoreSumOtherDocCount bool                                 `json:"ignoreSumOtherDocCount"`
	TypeMapping            map[string]elastic_proxy.TypeMapping `json:"typeMapping,omitempty"`
}

type mappingEntrySource struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (me *mappingEntrySource) SQL() string {
	if me.Database != "" {
		return fmt.Sprintf("%q.%q", me.Database, me.Table)
	}
	return fmt.Sprintf("%q", me.Table)
}

// UnmarshalJSON allows the old format that only allowed a single source
func (me *mappingEntry) UnmarshalJSON(data []byte) error {
	type _mappingEntry mappingEntry
	if err := json.Unmarshal(data, (*_mappingEntry)(me)); err != nil {
		return err
	}

	if me.Sources == nil {
		var sme struct {
			Database               string                               `json:"database"`
			Table                  string                               `json:"table"`
			IgnoreTotalHits        bool                                 `json:"ignoreTotalHits"`
			IgnoreSumOtherDocCount bool                                 `json:"ignoreSumOtherDocCount"`
			TypeMapping            map[string]elastic_proxy.TypeMapping `json:"typeMapping,omitempty"`
		}
		if err := json.Unmarshal(data, &sme); err != nil {
			return err
		}
		me.Sources = []mappingEntrySource{
			{
				Database: sme.Database,
				Table:    sme.Table,
			},
		}
	}

	return nil
}

type ElasticMapping = elastic_proxy.ElasticMapping
