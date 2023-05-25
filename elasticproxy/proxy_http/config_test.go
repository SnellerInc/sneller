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
