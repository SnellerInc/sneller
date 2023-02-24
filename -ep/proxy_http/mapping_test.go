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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"
	"github.com/SnellerInc/elasticproxy/helpers"

	"github.com/gorilla/mux"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

const (
	handledTrue  = 1
	handledFalse = 2
)

func TestMappingHandler(t *testing.T) {
	config := &Config{}
	config.Mapping = make(map[string]mappingEntry)
	config.Mapping["cached"] = mappingEntry{
		ElasticMapping: &elastic_proxy.ElasticMapping{
			Properties: map[string]elastic_proxy.MappingValue{
				"name":    {Type: "string"},
				"surname": {Type: "string"},
				"age":     {Type: "number"},
				"owner":   {Type: "bool"},
			},
		},
	}

	handled := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		logger := NewLogging(r)
		if MappingProxy(config, &logger, w, r) {
			handled = handledTrue
		} else {
			handled = handledFalse
		}
	}

	req := func(url string) *http.Request {
		return httptest.NewRequest(http.MethodGet, url, nil)
	}

	testcases := []struct {
		name    string
		r       *http.Request
		handled int
	}{
		{
			name:    "unknown index",
			r:       req("/unknown/_mapping"),
			handled: handledFalse,
		},
		{
			name:    "known index with cached mapping",
			r:       req("/cached/_mapping"),
			handled: handledTrue,
		},
	}

	router := mux.NewRouter()
	router.HandleFunc("/{index}/_mapping", handler).Methods(http.MethodGet)

	for i := range testcases {
		tc := testcases[i]
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			router.ServeHTTP(w, tc.r)
			resp := w.Result()
			defer resp.Body.Close()

			if handled != tc.handled {
				t.Logf("got:  %d", tc.handled)
				t.Logf("want: %d", handled)
				t.Errorf("wrong value of handled")
			}

			if handled == handledTrue {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}

				if resp.StatusCode != 200 {
					t.Logf("response body: %s", body)
					t.Fatalf("unexpected error code %d", resp.StatusCode)
				}

				var mappings map[string]*elastic_proxy.ElasticMapping
				err = json.Unmarshal(body, &mappings)
				if err != nil {
					t.Logf("body: %q", body)
					t.Fatal(err)
				}

				want := map[string]*elastic_proxy.ElasticMapping{
					"cached": config.Mapping["cached"].ElasticMapping,
				}
				if !reflect.DeepEqual(mappings, want) {
					t.Logf("got:  %v", mappings)
					t.Logf("want: %v", want)
					t.Errorf("wrong structure")
				}
			}
		})
	}
}

func TestIntegrationMappingsHandler(t *testing.T) {
	env, err := helpers.ParseEnvFile()
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("no .env file found")
			return
		}
		t.Fatal(err)
	}

	config := &Config{}
	config.Mapping = make(map[string]mappingEntry)
	config.Mapping[env.Elasticsearch.IndexFlight] = mappingEntry{
		Database: env.Sneller.Database,
		Table:    env.Sneller.TableFlight,
	}
	config.Mapping[env.Elasticsearch.IndexNews] = mappingEntry{
		Database: env.Sneller.Database,
		Table:    env.Sneller.TableNews,
	}
	config.Sneller.EndPoint = env.Sneller.Endpoint
	config.Sneller.Token = env.Sneller.Token

	handler := func(w http.ResponseWriter, r *http.Request) {
		logger := NewLogging(r)
		MappingProxy(config, &logger, w, r)
	}

	router := mux.NewRouter()
	router.HandleFunc("/{index}/_mapping", handler).Methods(http.MethodGet)
	srv := httptest.NewServer(router)
	defer srv.Close()

	req := func(url string, args ...any) *http.Request {
		r, err := http.NewRequest(http.MethodGet, srv.URL+fmt.Sprintf(url, args...), nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	testdata := func(name string) []byte {
		f, err := os.Open(fmt.Sprintf("testdata/%s", name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		b, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}

		return b
	}

	testcases := []struct {
		r    *http.Request
		path string
	}{
		{
			path: "flights_mapping.json",
			r:    req("/%s/_mapping", env.Elasticsearch.IndexFlight),
		},
		{
			r:    req("/%s/_mapping", env.Elasticsearch.IndexNews),
			path: "news_mapping.json",
		},
	}

	for i := range testcases {
		tc := testcases[i]
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.DefaultClient.Do(tc.r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}

			if resp.StatusCode != 200 {
				t.Fatalf("unexpected error code %d", resp.StatusCode)
			}

			// check if we can deserialize to the elastic-proxy structure
			var mapping map[string]*elastic_proxy.ElasticMapping
			err = json.Unmarshal(body, &mapping)
			if err != nil {
				t.Logf("body: %q", body)
				t.Fatal(err)
			}

			// deserialize to an untyped map, to create diff of JSONs
			var got, want map[string]any
			err = json.Unmarshal(body, &got)
			if err != nil {
				t.Logf("body: %q", body)
				t.Fatal(err)
			}

			err = json.Unmarshal(testdata(tc.path), &want)
			if err != nil {
				t.Fatal(err)
			}

			diff := gojsondiff.New().CompareObjects(want, got)
			if diff.Modified() {
				f := formatter.NewAsciiFormatter(want, formatter.AsciiFormatterDefaultConfig)
				diffstr, err := f.Format(diff)
				if err != nil {
					t.Fatal(err)
				}

				t.Error(diffstr)
			}
		})
	}
}
