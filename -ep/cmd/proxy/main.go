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

	"github.com/SnellerInc/elasticproxy/proxy_http"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/acme/autocert"
)

var verbose = flag.Bool("v", false, "Verbose logging")

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

func main() {
	ver, ok := Version()
	if ok {
		proxy_http.Version = ver
	}

	useTLS := flag.Bool("tls", false, "Enable TLS (automatically gets TLS certificates)")
	configFile := flag.String("config", "config.json", "Configuration file")
	endpoint := flag.String("endpoint", "localhost:8888", "Default endpoint (only for non-TLS mode)")
	flag.Parse()

	tenants, err := loadTenantConfiguration(*configFile)
	if err != nil {
		panic(fmt.Sprintf("can't load %q: %v", *configFile, err))
	}

	withTenantConfig := func(f func(t *config, l *proxy_http.Logging, w http.ResponseWriter, r *http.Request) bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			t, ok := tenants[r.Host]
			if !ok {
				if t, ok = tenants["*"]; !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}

			l := proxy_http.NewLogging(r)
			baseName := path.Join(t.LogFolder, t.baseName(l.Start))

			handled := f(t, &l, w, r)
			if !handled {
				if t.ReverseProxy == nil {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				t.ReverseProxy(w, r)
			} else if t.LogFolder != "" {
				writeIndentedJSON(baseName+"-step1-request.json", l.Request)
				writeText(baseName+"-step2.sql", l.SQL)
				writeIndentedJSON(baseName+"-step3-sneller-result.json", l.SnellerResult)
				writeIndentedJSON(baseName+"-step3-sneller-stats.json", l.Sneller)
				writeIndentedJSON(baseName+"-step4-preprocessed.json", l.Preprocessed)
				writeIndentedJSON(baseName+"-step5-result.json", l.Result)
				writeIndentedJSON(baseName+"-step6-elastic-result.json", l.ElasticResult)
				writeText(baseName+"-step7-elastic-diff.json", l.ElasticDiff)
			}
		}
	}

	withConfig := func(f func(t *proxy_http.Config, l *proxy_http.Logging, w http.ResponseWriter, r *http.Request) bool) http.HandlerFunc {
		return withTenantConfig(func(t *config, l *proxy_http.Logging, w http.ResponseWriter, r *http.Request) bool {
			return f(&t.Config, l, w, r)
		})
	}

	r := mux.NewRouter()
	r.HandleFunc("/sneller/version", proxy_http.VersionHandler).Methods(http.MethodGet)
	r.HandleFunc("/{index}/_count", withConfig(proxy_http.CountProxy)).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/{index}/_search", withConfig(proxy_http.SearchProxy)).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/{index}/_async_search", withConfig(proxy_http.AsyncSearchProxy)).Methods(http.MethodPost)
	r.HandleFunc("/_bulk", withConfig(proxy_http.BulkProxy)).Methods(http.MethodPost)
	r.HandleFunc("/{target}/_bulk", withConfig(proxy_http.BulkProxy)).Methods(http.MethodPost)
	r.HandleFunc("/", withTenantConfig(func(t *config, l *proxy_http.Logging, w http.ResponseWriter, r *http.Request) bool {
		// always forward "ping" to the underlying Elastic instance (if set)
		if t.ReverseProxy != nil {
			return false
		}

		return proxy_http.Ping(&t.Config, w, r)
	})).Methods(http.MethodGet)
	r.PathPrefix("/").Handler(http.HandlerFunc(withConfig(func(t *proxy_http.Config, l *proxy_http.Logging, w http.ResponseWriter, r *http.Request) bool {
		return false
	})))

	var l net.Listener
	if *useTLS {
		var hosts []string
		for host := range tenants {
			if host == "*" {
				panic("cannot use host '*' in TLS mode")
			}
			hosts = append(hosts, host)
			if *verbose {
				log.Printf("listening on https://%s", host)
			}
		}
		certManager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(hosts...),
			Cache:      autocert.DirCache("certs"),
		}
		l = certManager.Listener()
	} else {
		if *verbose {
			log.Printf("listening on http://%s", *endpoint)
		}
		l, err = net.Listen("tcp", *endpoint)
		if err != nil {
			panic(fmt.Sprintf("can't listen on %q: %v", *endpoint, err))
		}
	}

	err = http.Serve(l, &handler{r})
	if err != nil {
		panic(err)
	}
}

type handler struct {
	http.Handler
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if *verbose {
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
