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
	"os"

	"github.com/SnellerInc/sneller/db"
)

func load(defpath string) *db.Definition {
	f, err := os.Open(defpath)
	if err != nil {
		exitf("%s", err)
	}
	defer f.Close()
	s, err := db.DecodeDefinition(f)
	if err != nil {
		exitf("%s", err)
	}
	return s
}

// entry point for 'sdb create ...'
func create(creds db.Tenant, dbname, tblname, defpath string) {
	ofs := outfs(creds)
	s := load(defpath)
	err := db.WriteDefinition(ofs, dbname, tblname, s)
	if err != nil {
		exitf("writing new definition: %s", err)
	}
}

func init() {
	addApplet(applet{
		name: "create",
		help: "<db> <table> <definition.json>",
		desc: `create a new table from a def
The command
  $ sdb create <db> <table> definition.json
uploads a copy of definition.json to
the tenant root file system at
  /db/<db>/<table>/definition.json

The definition.json is expected to be a JSON
document with the following structure:

  {
    "inputs": [
      {"pattern": "s3://bucket/path/to/*.json", "format": "json"},
      {"pattern": "s3://another/path/*.json.gz", "format": "json.gz"}
    ]
  }

`,
		run: func(args []string) bool {
			if len(args) != 4 {
				return false
			}
			create(creds(), args[1], args[2], args[3])
			return true
		},
	})
}
