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
	"flag"
	"fmt"
	"os"

	"github.com/SnellerInc/sneller/db"
)

func inputs(creds db.Tenant, args []string) {
	var seek string
	var limit int
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.StringVar(&seek, "seek", "", "seek position within inputs")
	flags.IntVar(&limit, "limit", 1000, "limit on files to iterate")
	flags.Parse(args[1:])
	args = flags.Args()
	if len(args) != 2 {
		flags.Usage()
		return
	}
	dbname := args[0]
	table := args[1]

	ofs := outfs(creds)
	idx, err := db.OpenIndex(ofs, dbname, table, creds.Key())
	if err != nil {
		exitf("opening index: %s", err)
	}
	idx.Inputs.Backing = ofs
	err = idx.Inputs.Walk(seek, func(name, etag string, id int) bool {
		fmt.Printf("%s %s %d\n", name, etag, id)
		limit--
		return limit > 0
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	addApplet(applet{
		name: "inputs",
		help: "<db> <table>",
		desc: `inputs [-seek ...] [-limit 1000] <db> <table>
The command
  $ sdb inputs <db> <table>
lists all of the input files that have
been recorded by the index file belonging
to the specified database and table starting
at the seek position and continuing up to the final
input or the provided limit, whichever comes first.

The output of this command is a series
of newline-delimited JSON records describing
the name of the file and whether or not it
was successfully read into the table.
`,
		run: func(args []string) bool {
			inputs(creds(), args)
			return true
		},
	})
}
