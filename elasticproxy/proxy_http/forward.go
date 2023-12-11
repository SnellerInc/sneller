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

import "net/http"

// Forward forwards query to ElasticSearch if it's configured and returns true.
// Otherwise does nothing and returns false.
func Forward(t *Config, w http.ResponseWriter, r *http.Request) bool {
	if t.Elastic.EndPoint == "" {
		return false
	}

	rp, err := ReverseProxyForConfig(t)
	if err == nil {
		rp(w, r)
	}

	return err == nil
}
