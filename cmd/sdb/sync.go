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
	"errors"
	"flag"
	"time"

	"github.com/SnellerInc/sneller/db"
)

func sync(args []string) {
	var force bool
	var dashm int64
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.BoolVar(&force, "f", false, "force rebuild")
	flags.Int64Var(&dashm, "m", 100*giga, "maximum input bytes read per index update")
	flags.Parse(args[1:])
	args = flags.Args()
	if len(args) != 2 {
		flags.Usage()
		return
	}
	dbname := args[0] // database name
	tblpat := args[1] // table pattern

	var err error
	for {
		c := db.Config{
			Align:         1024 * 1024, // maximum alignment with current span size
			RangeMultiple: 100,         // metadata once every 100MB
			Force:         force,
			MaxScanBytes:  dashm,
			GCMinimumAge:  5 * time.Minute,
		}
		if dashv {
			c.Logf = logf
			c.Verbose = true
		}
		err = c.Sync(creds(), dbname, tblpat)
		if !errors.Is(err, db.ErrBuildAgain) {
			break
		}
	}
	if err != nil {
		exitf("sync: %s", err)
	}
}

func init() {
	addApplet(applet{
		name: "sync",
		help: "[-f] [-m max-scan-bytes] <db> <table-pattern?>",
		desc: `sync a table index based on an existing def
the command
  $ sdb sync <db> <pattern>
synchronizes all the tables that match <pattern> within
the database <db> against the list of objects specified
in the associated definition.json files (see also "create")
`,
		run: func(args []string) bool {
			sync(args)
			return true
		},
	})
}
