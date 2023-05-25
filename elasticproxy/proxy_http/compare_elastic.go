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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

func compareWithElastic(c *HandlerContext, pq *proxyQuery) error {
	url, err := url.Parse(c.Config.Elastic.EndPoint)
	if err != nil {
		return err
	}
	url.Path = fmt.Sprintf("%s/_search", c.Logging.Index)
	if len(pq.queryParams) > 0 {
		url.RawQuery = pq.queryParams.Encode()
	}
	req := http.Request{
		Method: http.MethodPost,
		URL:    url,
		Body:   io.NopCloser(bytes.NewReader(pq.body)),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
	}

	password := c.Config.Elastic.ESPassword
	if password == "" {
		password = c.Config.Elastic.Password
	}
	if c.Config.Elastic.User != "" || password != "" {
		req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.Config.Elastic.User+":"+password)))
	}

	resp, err := http.DefaultClient.Do(&req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("elastic returned HTTP status %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	elasticResult, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(elasticResult, &c.Logging.ElasticResult)
	if err != nil {
		return err
	}

	var gotJSON []byte
	gotJSON, _ = json.Marshal(c.Logging.Result)
	var gotMap map[string]any
	json.Unmarshal(gotJSON, &gotMap)

	// Make sure 'took' matches
	gotMap["took"] = c.Logging.ElasticResult["took"]

	diff := gojsondiff.New().CompareObjects(c.Logging.ElasticResult, gotMap)
	if !diff.Modified() {
		return nil
	}

	var expected any
	json.Unmarshal(elasticResult, &expected)
	c.Logging.ElasticDiff, err = formatter.NewAsciiFormatter(expected, formatter.AsciiFormatterConfig{
		ShowArrayIndex: true,
		Coloring:       false,
	}).Format(diff)
	return err
}
