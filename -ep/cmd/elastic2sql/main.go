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

package main

import (
	"encoding/json"
	"fmt"
	"os"

	elastic_proxy "github.com/SnellerInc/elasticproxy/elastic-proxy"
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
		Table:           table,
		IgnoreTotalHits: false,
	}
	sqlExpr, err := ej.SQL(&qc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SQL translation error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(elastic_proxy.PrintExprPretty(sqlExpr))
}
