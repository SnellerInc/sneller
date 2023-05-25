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
	"encoding/json"
	"testing"
)

func TestNotSupported(t *testing.T) {
	var singleQueryJSON = `{ "dis_max": {}}`
	var q andQueries
	if err := json.Unmarshal([]byte(singleQueryJSON), &q); err != ErrNotSupported {
		t.Fatalf("expected %q to be not supported", singleQueryJSON)
	}
}

func TestLiteralString(t *testing.T) {
	var literal = `"Search"`
	var v JSONLiteral
	if err := json.Unmarshal([]byte(literal), &v); err != nil {
		t.Fatalf("can't unmarshal %q: %v", literal, err)
	}
	s, ok := v.Value.(string)
	if !ok {
		t.Fatal("expected a string")
	}
	if s != "Search" {
		t.Fatal("expected 'Search'")
	}
}

func TestLiteralDecimalFloat(t *testing.T) {
	var literal = `12.3`
	var v JSONLiteral
	if err := json.Unmarshal([]byte(literal), &v); err != nil {
		t.Fatalf("can't unmarshal %q: %v", literal, err)
	}
	f, ok := v.Value.(float64)
	if !ok {
		t.Fatal("expected a float")
	}
	if f != 12.3 {
		t.Fatal("expected 12.3")
	}
}

func TestLiteralDecimalInteger(t *testing.T) {
	var literal = `12`
	var v JSONLiteral
	if err := json.Unmarshal([]byte(literal), &v); err != nil {
		t.Fatalf("can't unmarshal %q: %v", literal, err)
	}
	f, ok := v.Value.(int64)
	if !ok {
		t.Fatal("expected an integer")
	}
	if f != 12 {
		t.Fatal("expected 12")
	}
}

func TestShortField(t *testing.T) {
	var f = `"test"`
	var mf field
	if err := json.Unmarshal([]byte(f), &mf); err != nil {
		t.Fatalf("can't unmarshal %q: %v", f, err)
	}
	s, ok := mf.Query.Value.(string)
	if !ok {
		t.Fatal("expected a string value")
	}
	if s != "test" {
		t.Fatal("expected 'test'")
	}
}
func TestLongField(t *testing.T) {
	var f = `{ "query": "test" }`
	var mf field
	if err := json.Unmarshal([]byte(f), &mf); err != nil {
		t.Fatalf("can't unmarshal %q: %v", f, err)
	}
	s, ok := mf.Query.Value.(string)
	if !ok {
		t.Fatal("expected a string value")
	}
	if s != "test" {
		t.Fatal("expected 'test'")
	}
}

func TestLiteralBoolean(t *testing.T) {
	var literal = `true`
	var v JSONLiteral
	if err := json.Unmarshal([]byte(literal), &v); err != nil {
		t.Fatalf("can't unmarshal %q: %v", literal, err)
	}
	b, ok := v.Value.(bool)
	if !ok {
		t.Fatal("expected a boolean")
	}
	if b != true {
		t.Fatal("expected true")
	}

}

func TestUnmarshSingleQuery(t *testing.T) {
	var singleQueryJSON = `{ "match": { "title": "Search" }}`
	var q andQueries
	if err := json.Unmarshal([]byte(singleQueryJSON), &q); err != nil {
		t.Fatalf("can't unmarshal %q: %v", singleQueryJSON, err)
	}
	if len(q) != 1 {
		t.Fatalf("expected a single item")
	}
	if q[0].Match == nil || len(*q[0].Match) != 1 {
		t.Fatalf("expected a single match field")
	}
	m, ok := (*q[0].Match)["title"]
	if !ok {
		t.Fatalf("expected a 'title' as the match field")
	}
	if m.Value != "Search" {
		t.Fatalf("expected value 'Search' for 'title' in the match field")
	}
}

func TestUnmarshMultiQuery(t *testing.T) {
	var multiQueryJSON = `[
		{ "match": { "title":   "Search"        }},
		{ "match": { "content": "Elasticsearch" }}
	  ]`
	var q andQueries
	if err := json.Unmarshal([]byte(multiQueryJSON), &q); err != nil {
		t.Fatalf("can't unmarshal %q: %v", multiQueryJSON, err)
	}
	if len(q) != 2 {
		t.Fatalf("expected a two items")
	}

	if q[0].Match == nil || len(*q[0].Match) != 1 {
		t.Fatalf("expected a single match field")
	}
	m, ok := (*q[0].Match)["title"]
	if !ok {
		t.Fatalf("expected a 'title' as the match field")
	}
	if m.Value != "Search" {
		t.Fatalf("expected value 'Search' for 'title' in the match field")
	}

	if q[1].Match == nil || len(*q[1].Match) != 1 {
		t.Fatalf("expected a single match field")
	}
	m, ok = (*q[1].Match)["content"]
	if !ok {
		t.Fatalf("expected a 'content' as the match field")
	}
	if m.Value != "Elasticsearch" {
		t.Fatalf("expected value 'Elasticsearch' for 'content' in the match field")
	}
}

func TestUnmarshalTerm(t *testing.T) {
	var termJSON = `{
		"user.id": {
		  "value": "kimchy",
		  "boost": 1.1,
		  "case_insensitive": true
		}
	  }`
	var tm term
	if err := json.Unmarshal([]byte(termJSON), &tm); err != nil {
		t.Fatalf("can't unmarshal %q: %v", termJSON, err)
	}
	if tm.Field != "user.id" {
		t.Fatalf("expected field 'user.id', got %s", tm.Field)
	}
	if tm.Value.Value.(string) != "kimchy" {
		t.Fatal("expected value 'kimchy'")
	}
	if tm.Boost == nil || *tm.Boost != boostValue(1.1) {
		t.Fatal("expected boost 1.1")
	}
	if tm.CaseInsensitive == nil || *tm.CaseInsensitive != true {
		t.Fatal("expected boost 1.1")
	}
}

func TestUnmarshalTerms(t *testing.T) {
	var termJSON = `{
		"user.id": ["kimchy", "elkbee" ],
		"boost": 2.2
	  }`
	var tms terms
	if err := json.Unmarshal([]byte(termJSON), &tms); err != nil {
		t.Fatalf("can't unmarshal %q: %v", termJSON, err)
	}
	if tms.Field != "user.id" {
		t.Fatal("expected field 'user.id'")
	}
	if len(tms.Values) != 2 {
		t.Fatal("expected two values")
	}
	if tms.Values[0].Value.(string) != "kimchy" {
		t.Fatal("expected 1st value 'kimchy'")
	}
	if tms.Values[1].Value.(string) != "elkbee" {
		t.Fatal("expected 2nd value 'elkbee'")
	}
	if tms.Boost == nil || *tms.Boost != boostValue(2.2) {
		t.Fatal("expected boost 2.2")
	}
}
