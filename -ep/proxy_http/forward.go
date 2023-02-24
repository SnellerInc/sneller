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
