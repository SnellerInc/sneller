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
	"io"
	"os"
	"path"

	"github.com/SnellerInc/sneller/db"
)

func fetch(creds db.Tenant, files ...string) {
	ofs := root(creds)
	for i := range files {
		if dashv {
			logf("fetching %s...\n", files[i])
		}
		f, err := ofs.Open(files[i])
		if err != nil {
			exitf("%s", err)
		}
		dstf, err := os.Create(path.Base(files[i]))
		if err != nil {
			exitf("creating %s: %s", path.Base(files[i]), err)
		}
		_, err = io.Copy(dstf, f)
		f.Close()
		dstf.Close()
		if err != nil {
			os.Remove(dstf.Name())
			exitf("copying bytes: %s", err)
		}
	}
}

func init() {
	addApplet(applet{
		name: "fetch",
		help: "<file> ...",
		desc: `copy files from the tenant rootfs
The command
  $ sdb fetch <file> ...
fetches the associated file from the rootfs (see -root)
and stores it in the current directory in a file with
the same basename as the respective remote file.

For example,
  $ sdb fetch db/foo/bar/baz.ion.zst
would create a local file called baz.ion.zst.

See also the "unpack" command for unpacking *.ion.zst and *.zion files.
`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			fetch(creds(), args[1:]...)
			return true
		},
	})
}
