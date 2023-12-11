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
	"crypto/tls"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func ReverseProxyForConfig(cfg *Config) (http.HandlerFunc, error) {
	user := cfg.Elastic.User
	password := cfg.Elastic.ESPassword
	if password == "" {
		password = cfg.Elastic.Password
	}

	remote, err := url.Parse(cfg.Elastic.EndPoint)
	if err != nil {
		return nil, err
	}

	elasticReverseProxy := httputil.NewSingleHostReverseProxy(remote)
	elasticReverseProxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		log.Printf("http: proxy error for %s %s: %v", req.Method, req.URL.String(), err)
		w.WriteHeader(http.StatusBadGateway)
	}

	baseDirector := elasticReverseProxy.Director
	elasticReverseProxy.Director = func(req *http.Request) {
		baseDirector(req)

		// rewrite 'Host' header to match the target
		req.Host = remote.Host

		// update authorization header when forwarding requests
		// to the backing Elastic cluster
		if user != "" && password != "" {
			req.SetBasicAuth(user, password)
		}
	}

	if cfg.Elastic.IgnoreCert {
		// Copy the default transport, but adjust the
		// TLS configuration to skip certificate validation
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		elasticReverseProxy.Transport = transport
	}

	return func(w http.ResponseWriter, r *http.Request) {
		elasticReverseProxy.ServeHTTP(w, r)
	}, nil
}
