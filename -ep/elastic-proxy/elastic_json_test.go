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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

const comparePrecision = 7

func TestAggregations(t *testing.T) {
	folder := "testaggs"
	step0 := ".json"
	step1 := ".json.sql"

	dir := os.DirFS(folder)
	aggsFiles, err := fs.Glob(dir, "*"+step0)
	if err != nil {
		t.Fatalf("can't access %q folder: %v", folder, err)
	}
	for _, af := range aggsFiles {
		test := af[:len(af)-len(step0)]
		t.Run(test, func(t *testing.T) {
			elasticJSONData, err := fs.ReadFile(dir, test+step0)
			if err != nil {
				t.Fatalf("can't access %q: %v", test+step0, err)
			}

			var ej ElasticJSON
			if err := json.Unmarshal([]byte(elasticJSONData), &ej); err != nil {
				t.Fatalf("can't unmarshal %q: %v", elasticJSONData, err)
			}

			// determine query context
			qc := QueryContext{
				Query:           ej,
				Table:           "table",
				IgnoreTotalHits: false,
				TypeMapping: map[string]TypeMapping{
					"timestamp": {
						Type: "datetime",
					},
				},
			}

			// step 1: generate SQL
			sql, err := ej.SQL(&qc)
			if err != nil {
				t.Fatalf("can't transform aggregation %q: %v", elasticJSONData, err)
			}

			gotSQL := PrintExprPretty(sql)
			destFile := path.Join(folder, test)
			compareOrWriteText(t, gotSQL, destFile+step1, WithNormalizeSQL())
		})
	}
}

func TestPreProcess(t *testing.T) {
	folder := "test-preprocess"

	input := "-input.json"
	output := "-output.json"

	dir := os.DirFS(folder)
	aggsFiles, err := fs.Glob(dir, "*"+input)
	if err != nil {
		t.Fatalf("can't access results folder: %v", err)
	}
	for _, af := range aggsFiles {
		test := af[:len(af)-len(input)]
		t.Run(test, func(t *testing.T) {
			destFile := path.Join(folder, test)

			// step 0: parse input
			rawJSON, err := fs.ReadFile(dir, test+input)
			if err != nil {
				t.Fatalf("can't access %q: %v", test+input, err)
			}

			var rawData map[string]any
			if err := json.Unmarshal([]byte(rawJSON), &rawData); err != nil {
				t.Fatalf("can't unmarshal %q: %v", rawJSON, err)
			}

			preProcessData, err := preProcess(rawData)
			if err != nil {
				t.Fatalf("can't pre-process %q: %v", test+input, err)
			}

			compareOrWriteJSON(t, preProcessData, destFile+output)
		})
	}
}

func runTest(t *testing.T, folder, database, table, index string, typeMapping map[string]TypeMapping) {
	now, _ := time.Parse(time.RFC3339, "2022-06-25T12:34:56Z")
	testNow = &now

	// If true perform queries to local snellerd instance
	const testLocalSnellerd = true

	// If true perform queries to local elasticsearch instance
	const testWithElasticSearch = true

	t.Logf("query local snellerd: %v, test elastic search: %v", testLocalSnellerd, testWithElasticSearch)

	env, err := parseEnvFile()
	if err != nil {
		t.Fatal(err)
	}

	var esClient *elasticsearch.Client
	if testWithElasticSearch {
		if err = hostReachable(env.elasticsearch.endpoint); err != nil {
			t.Fatalf("Can't resolve %s: %s", env.elasticsearch.endpoint, err)
		}
		if err = hostReachable(env.s3.endpoint); err != nil {
			t.Fatalf("Can't resolve %s: %s", env.s3.endpoint, err)
		}
		esClient, err = elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{env.elasticsearch.endpoint},
			Username:  env.elasticsearch.username,
			Password:  env.elasticsearch.password,
		})
		if err != nil {
			t.Fatalf("can't create elastic client: %v", err)
		}
	}

	step0 := "-0-input.json"
	step1 := "-1-query.sql"
	step2 := "-2-output.ion"
	step3 := "-3-output.json"
	step4 := "-4-processed.json"
	step5 := "-5-result.json"
	step6 := "-6-elastic.json"

	dir := os.DirFS(folder)
	aggsFiles, err := fs.Glob(dir, "*"+step0)
	if err != nil {
		t.Fatalf("can't access results folder: %v", err)
	}
	for _, af := range aggsFiles {
		test := af[:len(af)-len(step0)]
		t.Run(test, func(t *testing.T) {
			destFile := path.Join(folder, test)

			// step 0: parse input
			elasticJSONData, err := fs.ReadFile(dir, test+step0)
			if err != nil {
				t.Fatalf("can't access %q: %v", test+step0, err)
			}

			var ej ElasticJSON
			if err := json.Unmarshal([]byte(elasticJSONData), &ej); err != nil {
				t.Fatalf("can't unmarshal %q: %v", elasticJSONData, err)
			}

			// determine query context
			qc := QueryContext{
				Query:           ej,
				Database:        database,
				Table:           table,
				Index:           index,
				IgnoreTotalHits: false,
				TypeMapping:     typeMapping,
			}

			// step 1: generate SQL
			sql, err := ej.SQL(&qc)
			if err != nil {
				t.Fatalf("can't transform aggregation %q: %v", elasticJSONData, err)
			}

			gotSQL := PrintExprPretty(sql)
			compareOrWriteText(t, gotSQL, destFile+step1, WithNormalizeSQL())

			// step 2: write/load ION result
			if testLocalSnellerd {
				s := &env.sneller
				resp, err := ExecuteQuery(s.endpoint, s.token, s.database, gotSQL, 0)
				if err != nil {
					t.Logf("query: %s", gotSQL)
					t.Fatalf("can't execute query: %v", err)
				}

				defer resp.Body.Close()

				w, err := os.Create(destFile + step2)
				if err != nil {
					t.Fatalf("can't create %q: %v", destFile+step2, err)
				}

				defer w.Close()
				_, err = io.Copy(w, resp.Body)
				if err != nil {
					t.Fatalf("can't write to %q: %v", destFile+step2, err)
				}
				w.Close()
			}

			ionResult, err := os.Open(destFile + step2)
			if err != nil {
				t.Fatalf("can't access %q: %v", test+step2, err)
			}
			defer ionResult.Close()

			// step 3: generate JSON from ION
			var v map[string]any
			dec := ion.NewDecoder(ion.NewReader(ionResult))
			err = dec.DecodeTo(&v)
			if err != nil {
				t.Fatalf("can't unmarshal binary ION %q: %v", test+step2, err)
			}

			// make sure we properly deal with ION timestamps
			for k, vv := range v {
				v[k] = ConvertION(vv)
			}

			// obtain the statistics
			var stats struct {
				CacheHits    int `ion:"hits,omitempty"`
				CacheMisses  int `ion:"misses,omitempty"`
				BytesScanned int `ion:"scanned,omitempty"`
			}
			err = dec.DecodeTo(&stats)
			if err != nil {
				t.Fatalf("can't unmarshal stats: %v", err)
			}

			compareOrWriteJSON(t, v, destFile+step3)

			// step 4+5: preprocess and convert the result
			er, processedData, err := ej.ConvertResult(&qc, v)
			if processedData != nil {
				compareOrWriteJSON(t, processedData, destFile+step4)
			}
			if err != nil {
				t.Errorf("can't process results: %v", err)
			}

			compareOrWriteJSON(t, er, destFile+step5)

			// step 6 (optional check with Elastic)
			if esClient != nil {
				body, err := dir.Open(test + step0)
				if err != nil {
					t.Fatalf("can't open %q", test+step0)
				}

				result, err := esClient.Search(
					esClient.Search.WithContext(context.Background()),
					esClient.Search.WithBody(body),
					esClient.Search.WithPretty(),
					esClient.Search.WithIndex(index),
				)
				if err != nil {
					body.Close()
					t.Errorf("query failed against Elastic: %v", err)
					return
				}
				defer result.Body.Close()

				var elasticResult map[string]any
				err = json.NewDecoder(result.Body).Decode(&elasticResult)
				if err != nil {
					t.Errorf("can't decode ElasticSearch JSON: %v", err)
				}
				elasticResult["took"] = 0
				if hits, ok := elasticResult["hits"].(map[string]any); ok {
					if _, ok := hits["max_score"]; ok {
						hits["max_score"] = nil
					}
				}
				if er.Hits != nil {
					er.Hits.MaxScore = nil
				}

				compareOrWriteJSON(t, elasticResult, destFile+step6)
				compareJSON(t, "Different result from actual Elastic", er, elasticResult)
			}
		})
	}
}

func TestResultProcessing(t *testing.T) {
	typeMapping := map[string]TypeMapping{
		"timestamp": {
			Type: "datetime",
		},
	}

	env, err := parseEnvFile()
	if err != nil {
		t.Fatal(err)
	}

	runTest(t, "testdata-new", env.sneller.database, env.sneller.tableFlight, env.elasticsearch.indexFlight, typeMapping)
}

func TestResultProcessingNews(t *testing.T) {
	typeMapping := map[string]TypeMapping{
		"title": {
			Type: "text",
			Fields: map[string]string{
				"keyword": "keyword",
				"raw":     "keyword-ignore-case",
			},
		},
	}

	env, err := parseEnvFile()
	if err != nil {
		t.Fatal(err)
	}

	runTest(t, "testdata-news", env.sneller.database, env.sneller.tableNews, env.elasticsearch.indexNews, typeMapping)
}

func hostReachable(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}

	ips, err := net.LookupIP(u.Hostname())
	if err != nil {
		return err
	}

	if len(ips) == 0 {
		return fmt.Errorf("no IP addresses found")
	}

	return nil
}

func TestSplitWithPrefix(t *testing.T) {
	testData := []struct {
		prefix string
		text   string
		name   string
		index  int
	}{
		{BucketPrefix, "$bucket:name1:name2%1", "name1:name2", 1},
		{KeyPrefix, "$bucket:name1:name2%1", "", -1},
		{KeyPrefix, "$key:name%1:name2%1", "name%1:name2", 1},
	}
	for _, test := range testData {
		t.Run(test.prefix+":"+test.text, func(t *testing.T) {
			name, index := splitWithPrefix(test.prefix, test.text)
			if name != test.name {
				t.Fatalf("got name %q, expected name %q", name, test.name)
			}
			if index != test.index {
				t.Fatalf("got index %d, expected index %d", index, test.index)
			}
		})
	}
}

func TestKeyGroups(t *testing.T) {
	rec := map[string]any{
		"$key:aa%0":    0,
		"$key:aa:bb%0": 0,
		"$key:aa%2":    0,
		"doc_count":    0,
		"$key:aa%1":    0,
	}
	kg, err := keyGroups(rec)
	if err != nil {
		t.Fatalf("can't process record: %v", err)
	}

	// check key groups
	if len(kg) != 2 {
		t.Fatal("expected two key groups")
	}
	if len(kg[0]) != 3 {
		t.Error("expected 3 columns in key-group 0")
	} else {
		for i := 0; i < 3; i++ {
			expected := fmt.Sprintf("$key:aa%%%d", i)
			if kg[0][i] != expected {
				t.Errorf("got %q, but expected %q in key-group 0:%d", kg[0][i], expected, i)
			}
		}
	}
	if len(kg[1]) != 1 {
		t.Error("expected 1 column in key-group 1")
	} else {
		expected := "$key:aa:bb%0"
		if kg[1][0] != expected {
			t.Errorf("got %q, but expected %q in key-group 0:%d", kg[0][0], expected, 0)
		}
	}
}

func normalizeSQL(sql string) string {
	sql = regexp.MustCompile(`\-\-.*\n`).ReplaceAllString(sql, "")
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	return strings.TrimSpace(sql)
}

func WithNormalizeSQL() func(string) string {
	return normalizeSQL
}

type testDirectory struct {
	directory  string
	jsonSuffix string
	sqlSuffix  string
	table      string
}

type testEntry struct {
	name string
	json string
	sql  string
}

func (t *testDirectory) files() ([]testEntry, error) {
	dir := os.DirFS(t.directory)
	tmp, err := fs.Glob(dir, "*"+t.jsonSuffix)
	if err != nil {
		return nil, err
	}

	var result []testEntry
	for _, json := range tmp {
		var te testEntry
		basename := json[:len(json)-len(t.jsonSuffix)]
		te.name = fmt.Sprintf("%s-%s", t.directory, basename)
		te.json = path.Join(t.directory, json)
		te.sql = path.Join(t.directory, basename+t.sqlSuffix)

		_, err := os.Stat(te.sql)
		if err == nil {
			result = append(result, te)
		} else {
			fmt.Printf("err: %s\n", err)
		}
	}

	return result, nil
}

func TestTransformToSQL(t *testing.T) {
	dirs := []testDirectory{
		{
			directory:  "testaggs",
			jsonSuffix: ".json",
			sqlSuffix:  ".json.sql",
			table:      "table",
		},
		{
			directory:  "testdata-new",
			jsonSuffix: "-0-input.json",
			sqlSuffix:  "-1-query.sql",
			table:      "sample_flights",
		},
	}

	for i := range dirs {
		files, err := dirs[i].files()
		if err != nil {
			t.Fatal(err)
		}

		table := dirs[i].table

		for i := range files {
			te := files[i]
			t.Run(te.name, func(t *testing.T) {
				elasticJSONData, err := os.ReadFile(te.json)
				if err != nil {
					t.Fatal(err)
				}

				var ej ElasticJSON
				if err := json.Unmarshal([]byte(elasticJSONData), &ej); err != nil {
					t.Fatalf("can't unmarshal %q: %v", elasticJSONData, err)
				}

				// determine query context
				qc := QueryContext{
					Query:           ej,
					Table:           table,
					IgnoreTotalHits: false,
					TypeMapping: map[string]TypeMapping{
						"timestamp": {
							Type: "datetime",
						},
					},
				}

				// step 1: generate SQL
				sql, err := ej.SQL(&qc)
				if err != nil {
					t.Fatalf("can't transform aggregation %q: %v", elasticJSONData, err)
				}

				got := PrintExprPretty(sql)
				expected, err := os.ReadFile(te.sql)
				if err != nil {
					t.Fatalf("can't read %q: %s", te.sql, err)
				}

				got = normalizeSQL(got)
				want := normalizeSQL(string(expected))
				if got != want {
					t.Logf("got:  %s", got)
					t.Logf("want: %s", want)
					t.Errorf("queries don't match")
				}
			})
		}
	}
}

func compareOrWriteText(t *testing.T, gotString string, fileName string, process ...func(in string) string) {
	expected, err := os.ReadFile(fileName)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("cannot read %q: %v", fileName, err)
		}

		// file doesn't exist, so write out result as output
		err = os.WriteFile(fileName, []byte(gotString), 0640)
		if err != nil {
			t.Fatalf("cannot write %q: %v", fileName, err)
		}

		return
	}

	expectedString := string(expected)

	normalizedGot := gotString
	normalizedExpected := expectedString
	for _, p := range process {
		normalizedGot = p(normalizedGot)
		normalizedExpected = p(normalizedExpected)
	}

	if normalizedGot == normalizedExpected {
		return
	}

	t.Fatalf("Output mismatched: %s\nEXPECTED:\n%s\n\nGOT:\n%s", fileName, expectedString, gotString)
}

func compareOrWriteJSON(t *testing.T, got any, fileName string) {
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Errorf("cannot marshal result as JSON: %v", err)
		return
	}

	expectedJSON, err := os.ReadFile(fileName)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("cannot read %q: %v", fileName, err)
		}

		// file doesn't exist, so write out result as JSON output
		err = os.WriteFile(fileName, gotJSON, 0640)
		if err != nil {
			t.Errorf("cannot write %q: %v", fileName, err)
		}

		return
	}

	var expected any
	err = json.Unmarshal(expectedJSON, &expected)
	if err != nil {
		t.Errorf("cannot unmarshal expected JSON from %q: %v", fileName, err)
	}

	msg := fmt.Sprintf("Unexpected JSON in %q", fileName)
	compareJSON(t, msg, got, expected)
}

func compareJSON(t *testing.T, msg string, gotObject, expectedObject any) {
	gotJSON, err := coercedJSON(gotObject)
	if err != nil {
		t.Errorf("cannot marshal result as JSON: %v", err)
		return
	}

	expectedJSON, err := coercedJSON(expectedObject)
	if err != nil {
		t.Errorf("cannot marshal result as JSON: %v", err)
		return
	}

	diff, err := gojsondiff.New().Compare(expectedJSON, gotJSON)
	if err != nil {
		t.Errorf("cannot compare JSON: %v", err)
		return
	}
	if !diff.Modified() {
		return
	}

	var expected any
	json.Unmarshal(expectedJSON, &expected)
	diffString, err := formatter.NewAsciiFormatter(expected, formatter.AsciiFormatterConfig{
		ShowArrayIndex: true,
		Coloring:       false,
	}).Format(diff)
	if err != nil {
		t.Fatalf("cannot show JSON difference: %v", err)
	}

	t.Errorf("%s: %s", msg, diffString)
}

func coercedJSON(v any) ([]byte, error) {
	vJSON, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	err = json.Unmarshal(vJSON, &m)
	if err != nil {
		return nil, err
	}

	r := coerceData(m)
	return json.Marshal(r)
}

func coerceData(v any) any {
	switch vv := v.(type) {
	case []any:
		for i, v := range vv {
			vv[i] = coerceData(v)
		}
		return vv
	case map[string]any:
		for k, v := range vv {
			switch k {
			case "_id":
				if _, ok := v.(string); ok {
					// The "_id" field will change during different ingestions,
					// so this field is ignored
					vv[k] = "<id>"
					continue
				}
			case "_score":
				vv[k] = float64(1)
				continue
			}
			vv[k] = coerceData(v)
		}
		return vv
	case float64:
		if vv == 0 {
			return vv
		}
		k := math.Ceil(math.Log10(math.Abs(vv)))
		ratio := math.Pow(10, comparePrecision-k)
		rounded := math.Round(vv*ratio) / ratio
		return rounded
	default:
		return vv
	}
}
