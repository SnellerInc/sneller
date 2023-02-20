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

import "net/http"

func BulkProxy(t *Config, l *Logging, w http.ResponseWriter, r *http.Request) bool {
	return false
	// cloud := elastic_proxy.Cloud{
	// 	Endpoint: t.Sneller.EndPoint,
	// 	Token:    t.Sneller.Token,
	// }

	// response, custHeaders, err :=
	// elastic_proxy.ProxyIngestion(&cloud, t.Sneller.Schema, t.Sneller.Table, r.Body)
}
