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
	"path"
	"time"

	"github.com/SnellerInc/sneller/db"
)

func gc(creds db.Tenant, dbname, tblpat string) {
	ofs := root(creds)
	rmfs, ok := ofs.(db.RemoveFS)
	if !ok {
		exitf("GC unimplemented")
	}
	tables, err := db.Tables(ofs, dbname)
	if err != nil {
		exitf("listing db %s: %s", dbname, err)
	}
	conf := db.GCConfig{
		MinimumAge: 15 * time.Minute,
	}
	if dashv {
		conf.Logf = logf
	}
	key := creds.Key()
	for _, tab := range tables {
		match, err := path.Match(tblpat, tab)
		if err != nil {
			exitf("bad pattern %q: %s", tblpat, err)
		}
		if !match {
			continue
		}
		idx, err := db.OpenIndex(ofs, dbname, tab, key)
		if err != nil {
			exitf("opening index for %s/%s: %s", dbname, tab, err)
		}
		err = conf.Run(rmfs, dbname, idx)
		if err != nil {
			exitf("running gc on %s/%s: %s", dbname, tab, err)
		}
	}
}

func init() {
	addApplet(applet{
		name: "gc",
		help: "<db> <table-pattern?>",
		desc: `gc old objects from a db (+ table-pattern)
The command
  $ sdb gc <db> <table-pattern>
will perform garbage collection of all the objects
in the set of tables that match the glob pattern <table-pattern>.

A file is a candidate for garbage collection if
it is not pointed to by the current index file
and it was created more than 15 minutes ago.
`,
		run: func(args []string) bool {
			if len(args) < 2 || len(args) > 3 {
				return false
			}
			if len(args) == 2 {
				args = append(args, "*")
			}
			gc(creds(), args[1], args[2])
			return true
		},
	})
}
