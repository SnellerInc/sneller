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

package main

import (
	"encoding/json"
	"fmt"
	"os"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: elastic2sql <table>")
		os.Exit(1)
	}
	table := os.Args[1]

	var ej elastic_proxy.ElasticJSON
	if err := json.NewDecoder(os.Stdin).Decode(&ej); err != nil {
		fmt.Fprintf(os.Stderr, "Query parsing error: %v\n", err)
		os.Exit(1)
	}

	qc := elastic_proxy.QueryContext{
		TableSources: []elastic_proxy.TableSource{
			{Table: table},
		},
		IgnoreTotalHits: false,
	}
	sqlExpr, err := ej.SQL(&qc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SQL translation error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(elastic_proxy.PrintExprPretty(sqlExpr))
}
