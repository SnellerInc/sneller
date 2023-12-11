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
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/db"

	"golang.org/x/exp/maps"
)

var (
	dashv    bool
	dashh    bool
	rootpath string
)

const (
	mega = 1024 * 1024
	giga = 1024 * mega
)

func defaultRoot() string {
	r := os.Getenv("SNELLER_BUCKET")
	if r != "" {
		return r
	}
	wd, err := os.Getwd()
	if err == nil {
		return wd
	}
	return "."
}

func init() {
	flag.BoolVar(&dashv, "v", false, "verbose")
	flag.BoolVar(&dashh, "h", false, "show usage help")
	flag.StringVar(&rootpath, "root", defaultRoot(), "file system root (either directory path or s3 bucket)")
}

func exitf(f string, args ...interface{}) {
	if len(f) == 0 || f[len(f)-1] != '\n' {
		f += "\n"
	}
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func root(creds db.Tenant) db.InputFS {
	root, err := creds.Root()
	if err != nil {
		exitf("creds.Root: %s", err)
	}
	return root
}

func outfs(creds db.Tenant) db.OutputFS {
	r := root(creds)
	ofs, ok := r.(db.OutputFS)
	if !ok {
		exitf("root %T does not support writing", r)
	}
	return ofs
}

func logf(f string, args ...interface{}) {
	if f[len(f)-1] != '\n' {
		f += "\n"
	}
	fmt.Fprintf(os.Stderr, f, args...)
}

func creds() db.Tenant {
	if rootpath == "" {
		exitf("-root not specified")
	}
	if bucket, ok := strings.CutPrefix(rootpath, "s3://"); ok {
		t, err := auth.S3TenantFromEnv(context.Background(), bucket)
		if err != nil {
			exitf("deriving tenant creds: %s", err)
		}
		return t
	}
	return db.NewLocalTenantFromPath(rootpath)
}

type packed interface {
	io.ReadCloser
	io.ReaderAt
}

type applet struct {
	name string
	help string // list of options
	desc string // text description of what the command does

	run func(args []string) bool // execute command, returns false if args are invalid
}

var applets = map[string]applet{}

func addApplet(app applet) {
	applets[app.name] = app
}

func sortedApplets() []applet {
	vals := maps.Values(applets)
	slices.SortFunc(vals, func(x, y applet) int {
		return strings.Compare(x.name, y.name)
	})
	return vals
}

func main() {
	prog := os.Args[0]

	showAppletHelp := func(name string, app *applet, indent string, short bool) {
		fmt.Fprintf(os.Stderr, "%s%s %s %s\n",
			indent, prog, name, app.help)
		desc := app.desc
		if short {
			desc = strings.Split(desc, "\n")[0]
		}
		fmt.Fprintf(os.Stderr, "%s    %s\n", indent, desc)
	}

	originalUsage := flag.Usage

	showHelp := func() {
		fmt.Fprintf(os.Stderr, "Usage:\n  sdb [-version] [-build] [-root rootfs] command args...\n")
		fmt.Fprintf(os.Stderr, "  -version show program version\n")
		fmt.Fprintf(os.Stderr, "  -build   show program build info\n")
		fmt.Fprintf(os.Stderr, "Available commands:\n")
		apps := sortedApplets()
		for i := range apps {
			showAppletHelp(apps[i].name, &apps[i], "  ", true)
		}

		fmt.Fprintf(os.Stderr, "\n")
		originalUsage()
	}

	addApplet(applet{
		name: "help",
		run: func(args []string) bool {
			if len(args) == 1 {
				showHelp()
				return true
			}
			if len(args) == 2 {
				app, ok := applets[args[1]]
				if !ok {
					exitf("no such applet %s", args[0])
				}
				showAppletHelp(args[1], &app, "", false)
				return true
			}
			return false
		},
	})

	flag.Usage = showHelp

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		showHelp()
		os.Exit(1)
	}

	for i := range args {
		if args[i] == "-h" {
			dashh = true
		}
	}

	cmd := args[0]
	app, ok := applets[cmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "commands: ")
		i := 0
		for name := range applets {
			if i > 0 {
				fmt.Fprintf(os.Stderr, ", ")
			}
			io.WriteString(os.Stderr, name)
			i++
		}

		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	}

	if dashh {
		showAppletHelp(cmd, &app, "", false)
		return
	}

	validArgs := app.run(args)
	if !validArgs {
		exitf("usage: %s %s", cmd, app.help)
	}
}
