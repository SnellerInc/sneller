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
