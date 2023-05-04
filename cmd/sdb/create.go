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
