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
