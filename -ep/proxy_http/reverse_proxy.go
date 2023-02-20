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
