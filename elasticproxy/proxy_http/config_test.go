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
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestLoadConfiguration(t *testing.T) {
	// given
	f, err := os.Open("testdata/config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// when
	var configs map[string]*Config
	err = json.NewDecoder(f).Decode(&configs)
	if err != nil {
		t.Fatal(err)
	}

	// then
	config, ok := configs["*"]
	if !ok {
		t.Fatal("no config found")
	}

	want := new(Config)
	want.Elastic.EndPoint = "http://elastic:9200"
	want.Elastic.User = "elastic"
	want.Elastic.Password = "ep-pass"
	want.Elastic.ESPassword = "esp-pass"
	want.Elastic.IgnoreCert = true

	want.Sneller.EndPoint, _ = url.Parse("http://snellerd:9180/")
	want.Sneller.Token = "token"
	want.Sneller.Timeout = 42 * time.Second

	want.Mapping = map[string]*mappingEntry{
		"flights": {
			Sources: []mappingEntrySource{
				{
					Database: "test",
					Table:    "flights",
				},
			},
			IgnoreTotalHits:        true,
			IgnoreSumOtherDocCount: true,
		},
		"news": {
			Sources: []mappingEntrySource{
				{
					Database: "test",
					Table:    "news",
				},
			},
			IgnoreTotalHits:        true,
			IgnoreSumOtherDocCount: true,
		},
	}

	if !reflect.DeepEqual(config, want) {
		t.Logf("got : %+v", config)
		t.Logf("want: %+v", want)
		t.Errorf("wrong settings")
	}
}
