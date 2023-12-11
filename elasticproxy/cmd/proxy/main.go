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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/elasticproxy/proxy_http"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/acme/autocert"
)

type config struct {
	proxy_http.Config
	LogFolder string `json:"logFolder,omitempty"`

	syncMutex        sync.Mutex
	lastTimestamp    string
	timestampCounter int

	ReverseProxy http.HandlerFunc
}

func (c *config) baseName(t time.Time) string {
	c.syncMutex.Lock()
	defer c.syncMutex.Unlock()

	timestamp := t.Format("20060102-1504")
	if timestamp != c.lastTimestamp {
		c.timestampCounter = 0
		c.lastTimestamp = timestamp
	}
	name := fmt.Sprintf("%s-%03d", timestamp, c.timestampCounter)
	c.timestampCounter++
	return name
}

type tenantConfig map[string]*config

var (
	verbose    bool = false
	verboseLog      = func(format string, v ...any) {}
)

const (
	memcacheItemTimeout = 60 * 60
)

func main() {
	ver, ok := Version()
	if ok {
		proxy_http.Version = ver
	}

	useTLS := flag.Bool("tls", false, "Enable TLS (automatically gets TLS certificates)")
	configFile := flag.String("config", "config.json", "Configuration file")
	endpoint := flag.String("endpoint", "localhost:8888", "Default endpoint (only for non-TLS mode)")
	memcacheEndpoint := flag.String("memcache", "", "Optional memcache address")
	verboseFlag := flag.Bool("v", false, "Verbose logging")
	flag.Parse()

	if *verboseFlag {
		verboseLog = log.Printf
		verbose = true
	}

	tenants, err := loadTenantConfiguration(*configFile)
	if err != nil {
		log.Fatalf("can't load %q: %v", *configFile, err)
	}

	var memcacheClient *memcache.Client
	if *memcacheEndpoint != "" {
		memcacheClient = memcache.New(*memcacheEndpoint)
	}

	withTenantConfig := func(f func(t *config, c *proxy_http.HandlerContext) bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.Host
			t, ok := tenants[r.Host]
			if !ok {
				if t, ok = tenants["*"]; !ok {
					tenantID = "*"
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}

			client := &http.Client{
				Timeout: t.Config.Sneller.Timeout,
			}
			c := proxy_http.NewHandlerContext(&t.Config, client, w, r, verbose, verboseLog)
			if memcacheClient != nil {
				c.Memcache.Client = memcacheClient
				c.Memcache.TenantID = tenantID
				c.Memcache.Secret = tenantID
				c.Memcache.ExpirationTime = memcacheItemTimeout
			}
			baseName := path.Join(t.LogFolder, t.baseName(c.Logging.Start))

			handled := f(t, c)
			if !handled {
				if t.ReverseProxy == nil {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				t.ReverseProxy(w, r)
			} else if t.LogFolder != "" {
				writeIndentedJSON(baseName+"-step1-request.json", c.Logging.Request)
				writeText(baseName+"-step2.sql", c.Logging.SQL)
				writeIndentedJSON(baseName+"-step3-sneller-result.json", c.Logging.SnellerResult)
				writeIndentedJSON(baseName+"-step3-sneller-stats.json", c.Logging.Sneller)
				writeIndentedJSON(baseName+"-step4-preprocessed.json", c.Logging.Preprocessed)
				writeIndentedJSON(baseName+"-step5-result.json", c.Logging.Result)
				writeIndentedJSON(baseName+"-step6-elastic-result.json", c.Logging.ElasticResult)
				writeText(baseName+"-step7-elastic-diff.json", c.Logging.ElasticDiff)
			}
		}
	}

	withConfig := func(f func(c *proxy_http.HandlerContext) bool) http.HandlerFunc {
		return withTenantConfig(func(t *config, c *proxy_http.HandlerContext) bool {
			return f(c)
		})
	}

	r := mux.NewRouter()
	r.HandleFunc("/sneller/version", proxy_http.VersionHandler).Methods(http.MethodGet)
	r.HandleFunc("/{index}/_count", withConfig(proxy_http.CountProxy)).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/{index}/_search", withConfig(proxy_http.SearchProxy)).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/{index}/_async_search", withConfig(proxy_http.AsyncSearchProxy)).Methods(http.MethodPost)
	r.HandleFunc("/_bulk", withConfig(proxy_http.BulkProxy)).Methods(http.MethodPost)
	r.HandleFunc("/{target}/_bulk", withConfig(proxy_http.BulkProxy)).Methods(http.MethodPost)
	r.HandleFunc("/{index}/_mapping", withConfig(func(c *proxy_http.HandlerContext) bool {
		if proxy_http.Forward(c.Config, c.Writer, c.Request) {
			return true
		}

		return proxy_http.MappingProxy(c)
	})).Methods(http.MethodGet)
	r.HandleFunc("/sneller/mapping/{index}", withConfig(proxy_http.MappingProxy)).Methods(http.MethodGet)
	r.HandleFunc("/", withTenantConfig(func(t *config, c *proxy_http.HandlerContext) bool {
		// always forward "ping" to the underlying Elastic instance (if set)
		if t.ReverseProxy != nil {
			return false
		}

		return proxy_http.Ping(&t.Config, c.Writer, c.Request)
	})).Methods(http.MethodGet)
	r.PathPrefix("/").Handler(http.HandlerFunc(withConfig(func(c *proxy_http.HandlerContext) bool {
		return false
	})))

	var l net.Listener
	if *useTLS {
		var hosts []string
		for host := range tenants {
			if host == "*" {
				log.Fatal("cannot use host '*' in TLS mode")
			}
			hosts = append(hosts, host)
			verboseLog("listening on https://%s", host)
		}
		certManager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(hosts...),
			Cache:      autocert.DirCache("certs"),
		}
		l = certManager.Listener()
	} else {
		verboseLog("listening on http://%s", *endpoint)
		l, err = net.Listen("tcp", *endpoint)
		if err != nil {
			log.Fatalf("can't listen on %q: %v", *endpoint, err)
		}
	}

	err = http.Serve(l, &handler{r})
	if err != nil {
		log.Fatal(err)
	}
}

type handler struct {
	http.Handler
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if verbose {
		start := time.Now()
		ww := loggingResponseWriter{ResponseWriter: w}
		defer func() {
			duration := time.Since(start).Milliseconds()
			log.Printf("%s %s (remote: %s, result: %d, forwarded: %v, took: %dms)", r.Method, r.Host+r.URL.Path, r.RemoteAddr, ww.statusCode, ww.forwarded, duration)
		}()
		w = &ww
	}
	h.Handler.ServeHTTP(w, r)
}

func loadTenantConfiguration(path string) (tenantConfig, error) {
	configReader, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer configReader.Close()

	var tenants tenantConfig
	err = json.NewDecoder(configReader).Decode(&tenants)
	if err != nil {
		return nil, err
	}

	// create a reverse-proxy for the underlying Elastic endpoint
	for _, t := range tenants {
		if t.LogFolder != "" {
			err := os.MkdirAll(t.LogFolder, 0755)
			if err != nil {
				log.Printf("Unable to create folder %q (logs may be missing)", t.LogFolder)
			}
		}

		if t.Config.Elastic.EndPoint != "" {
			rp, err := proxy_http.ReverseProxyForConfig(&t.Config)
			if err != nil {
				return nil, err
			}

			t.ReverseProxy = func(w http.ResponseWriter, r *http.Request) {
				if lrw, ok := w.(*loggingResponseWriter); ok {
					lrw.forwarded = true
				}
				rp(w, r)
			}
		}
	}

	return tenants, nil
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	forwarded  bool
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func writeText(fileName, text string) {
	if text == "" {
		return
	}
	os.WriteFile(fileName, []byte(text), 0644)
}

func writeIndentedJSON(fileName string, v any) {
	if reflect.ValueOf(v).IsZero() {
		return
	}
	w, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer w.Close()
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	e.Encode(v)
}
