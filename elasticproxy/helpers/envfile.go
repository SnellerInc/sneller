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

// Package helpers provides .env file parser. It is used
// by integration tests.
package helpers

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Settings struct {
	Sneller       SnellerSettings
	Elasticsearch ElasticsearchSettings
}

type SnellerSettings struct {
	Endpoint               string
	Token                  string
	Database               string
	TableFlight, TableNews string
}

type ElasticsearchSettings struct {
	Endpoint               string
	Username               string
	Password               string
	IndexFlight, IndexNews string
}

const path = "../docker/.env"

// ParseEnvFile parses .env files used in tests.
func ParseEnvFile() (*Settings, error) {
	m, err := ReadEnvFile(path)
	if err != nil {
		return nil, err
	}

	env := &Settings{}
	fields := []struct {
		key string
		val *string
	}{
		{"SNELLER_TOKEN", &env.Sneller.Token},
		{"SNELLER_DATABASE", &env.Sneller.Database},
		{"SNELLER_TABLE1", &env.Sneller.TableFlight},
		{"SNELLER_TABLE2", &env.Sneller.TableNews},
		{"ELASTIC_ENDPOINT", &env.Elasticsearch.Endpoint},
		{"ELASTIC_PASSWORD", &env.Elasticsearch.Password},
		{"ELASTIC_INDEX1", &env.Elasticsearch.IndexFlight},
		{"ELASTIC_INDEX2", &env.Elasticsearch.IndexNews},
	}

	for i := range fields {
		value, ok := m[fields[i].key]
		if !ok {
			return nil, fmt.Errorf("%s: field %q not found", path, fields[i].key)
		}

		*fields[i].val = value
	}

	// some hardcoded constants
	env.Sneller.Endpoint = "http://localhost:9180"

	const DefaultElasticUser = "elastic" // keep in sync with elastic-proxy/const.go
	env.Elasticsearch.Username = DefaultElasticUser

	return env, nil
}

func ReadEnvFile(path string) (map[string]string, error) {
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
