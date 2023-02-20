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

package elastic_proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	defaultTimeoutSec = 30
)

func ExecuteQuery(endPoint, token, db, sql string, timeout int) (*http.Response, error) {
	q := make(url.Values)
	q.Add("database", db)

	u, err := url.Parse(endPoint)
	if err != nil {
		return nil, err
	}
	u.Path = "/executeQuery"
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/ion")

	if timeout == 0 {
		timeout = defaultTimeoutSec
	}
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("http error %d (%s): %s", resp.StatusCode, resp.Status, string(respBody))
	}

	return resp, nil
}
