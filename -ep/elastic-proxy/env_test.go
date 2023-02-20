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
	"bufio"
	"fmt"
	"os"
	"strings"
)

type snellerSettings struct {
	endpoint               string
	token                  string
	database               string
	tableFlight, tableNews string
}

type elasticsearchSettings struct {
	endpoint               string
	username               string
	password               string
	indexFlight, indexNews string
}

type s3Settings struct {
	endpoint string
}

type environmentSettings struct {
	sneller       snellerSettings
	elasticsearch elasticsearchSettings
	s3            s3Settings
}

func parseEnvFile() (*environmentSettings, error) {
	path := "../docker/.env"
	m, err := readEnvFile(path)
	if err != nil {
		return nil, err
	}

	env := &environmentSettings{}
	fields := []struct {
		key string
		val *string
	}{
		{"SNELLER_TOKEN", &env.sneller.token},
		{"SNELLER_DATABASE", &env.sneller.database},
		{"SNELLER_TABLE1", &env.sneller.tableFlight},
		{"SNELLER_TABLE2", &env.sneller.tableNews},
		{"ELASTIC_ENDPOINT", &env.elasticsearch.endpoint},
		{"ELASTIC_PASSWORD", &env.elasticsearch.password},
		{"ELASTIC_INDEX1", &env.elasticsearch.indexFlight},
		{"ELASTIC_INDEX2", &env.elasticsearch.indexNews},
		{"S3_ENDPOINT", &env.s3.endpoint},
	}

	for i := range fields {
		value, ok := m[fields[i].key]
		if !ok {
			return nil, fmt.Errorf("%s: field %q not found", path, fields[i].key)
		}

		*fields[i].val = value
	}

	// some hardcoded constants
	env.sneller.endpoint = "http://localhost:9180"
	env.elasticsearch.username = DefaultElasticUser

	return env, nil
}

func readEnvFile(path string) (map[string]string, error) {
	env := make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		return env, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	n := 1
	for s.Scan() {
		line := s.Text()

		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return env, fmt.Errorf("%s:%d wrong syntax, expected 'key=value', got %q", path, n, line)
		}

		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		env[key] = val
		n += 1
	}

	return env, s.Err()
}
