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

	"github.com/SnellerInc/sneller/db"
)

// entry point for 'sdb def ...'
func def(creds db.Tenant, dbname, tablename string) {
	ofs := outfs(creds)

	def, err := db.OpenDefinition(ofs, dbname, tablename)
	if err != nil {
		exitf("error reading definition: %s", err)
	}
	jsonDef, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		exitf("error creating JSON from definition: %s", err)
	}
	fmt.Println(string(jsonDef))
}

func init() {
	addApplet(applet{
		name: "def",
		help: "<db> <table>",
		desc: `show the table definition`,
		run: func(args []string) bool {
			if len(args) != 3 {
				return false
			}
			def(creds(), args[1], args[2])
			return true
		},
	})
}
