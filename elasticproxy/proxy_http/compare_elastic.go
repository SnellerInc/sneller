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
