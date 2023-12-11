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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"testing"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/gorilla/mux"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

const (
	handledTrue  = 1
	handledFalse = 2
)

type testCache struct {
	storedMapping  *ElasticMapping
	fetchedMapping *ElasticMapping
}

func (c *testCache) Store(idxName string, mapping *ElasticMapping) error {
	c.storedMapping = mapping
	return nil
}

func (c *testCache) Fetch(idxName string) (*ElasticMapping, error) {
	if idxName == "cached" {
		return c.fetchedMapping, nil
	}

	return nil, nil
}

func TestMappingHandler(t *testing.T) {
	config := &Config{}
	config.Mapping = map[string]*mappingEntry{
		"cached": {
			Sources: []mappingEntrySource{
				{
					Table: "cached",
				},
			},
		},
	}

	cache := &testCache{
		fetchedMapping: &elastic_proxy.ElasticMapping{
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
		c := NewHandlerContext(config, nil, w, r, false, func(string, ...any) {})
		c.Cache = cache
		if MappingProxy(c) {
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
					"cached": cache.fetchedMapping,
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
	srv := launchElasticSearchTestServer(t, RoundTripFn(snellerdHandler), new(testCache))
	defer srv.Close()
	elasticProxyEndPoint := srv.URL

	req := func(url string, args ...any) *http.Request {
		r, err := http.NewRequest(http.MethodGet, elasticProxyEndPoint+fmt.Sprintf(url, args...), nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	testcases := []struct {
		r    *http.Request
		path string
	}{
		{
			r:    req("/%s/_mapping", "kibana_sample_data_flights"),
			path: "flights_mapping.json",
		},
		{
			r:    req("/%s/_mapping", "news"),
			path: "news_mapping.json",
		},
	}

	for i := range testcases {
		tc := testcases[i]
		t.Run(tc.path, func(t *testing.T) {
			testReadMappingResponse(t, tc.r, tc.path)
		})
	}
}

func TestIntegrationWithMemcacheMappingsHandler(t *testing.T) {
	client := memcached(t)
	cache := NewMemcacheMappingCache(client, t.Name(), t.Name(), 1)

	snellerdCalls := 0
	snellerdWrapper := func(r *http.Request) *http.Response {
		snellerdCalls += 1
		return snellerdHandler(r)
	}

	srv := launchElasticSearchTestServer(t, RoundTripFn(snellerdWrapper), cache)
	defer srv.Close()
	elasticProxyEndPoint := srv.URL

	req := func(url string, args ...any) *http.Request {
		r, err := http.NewRequest(http.MethodGet, elasticProxyEndPoint+fmt.Sprintf(url, args...), nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	r := req("/%s/_mapping", "kibana_sample_data_flights")
	path := "flights_mapping.json"

	// read for the first time - cache miss
	testReadMappingResponse(t, r, path)
	if snellerdCalls != 1 {
		t.Fatalf("expected single request to snellerd, got %d", snellerdCalls)
	}

	// read for the second time - cache hit is expected
	testReadMappingResponse(t, r, path)
	if snellerdCalls != 1 {
		t.Fatalf("expected single request to snellerd, got %d", snellerdCalls)
	}
}

// ==================================================

func launchElasticSearchTestServer(t *testing.T, roundTrip RoundTripFn, cache MappingCache) *httptest.Server {
	const (
		snellerEndPoint = "http://localhost:1234"
		snellerDatabase = "test"

		flightsIndex = "kibana_sample_data_flights"
		flightsTable = "sample_flights"

		newsIndex = "news"
		newsTable = "news"
	)

	config := &Config{}
	config.Mapping = make(map[string]*mappingEntry)
	config.Mapping[flightsIndex] = &mappingEntry{
		Sources: []mappingEntrySource{
			{
				Database: snellerDatabase,
				Table:    flightsTable,
			},
		},
	}
	config.Mapping[newsIndex] = &mappingEntry{
		Sources: []mappingEntrySource{
			{
				Database: snellerDatabase,
				Table:    newsTable,
			},
		},
	}
	config.Sneller.EndPoint, _ = url.Parse(snellerEndPoint)

	handler := func(w http.ResponseWriter, r *http.Request) {
		client := &http.Client{
			Transport: roundTrip,
		}
		c := NewHandlerContext(config, client, w, r, false, func(string, ...any) {})
		c.Cache = cache
		MappingProxy(c)
	}

	router := mux.NewRouter()
	router.HandleFunc("/{index}/_mapping", handler).Methods(http.MethodGet)
	srv := httptest.NewServer(router)

	return srv
}

func testReadMappingResponse(t *testing.T, req *http.Request, path string) {
	resp, err := http.DefaultClient.Do(req)
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

	err = json.Unmarshal(testdata(t, path), &want)
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
}

func snellerdHandler(r *http.Request) *http.Response {
	if r.Method != http.MethodPost {
		panic("wrong request: POST expected")
	}

	if r.URL.Path != "/query" {
		panic("wrong request: invalid path")
	}

	post, err := io.ReadAll(r.Body)
	if err != nil {
		panic("cannot read request body")
	}

	SQL := string(post)
	sm := regexp.MustCompile(`"test"\."([^"]+)"`).FindStringSubmatch(SQL)
	if len(sm) != 2 {
		panic("wrong query: cannot extract table")
	}

	path := fmt.Sprintf("./testdata/%s_mapping.ion", sm[1])
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("wrong query: cannot open %s: %s", path, err))
	}

	return &http.Response{
		StatusCode: 200,
		Body:       f,
	}
}

func testdata(t *testing.T, name string) []byte {
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

type RoundTripFn func(*http.Request) *http.Response

func (fn RoundTripFn) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r), nil
}
